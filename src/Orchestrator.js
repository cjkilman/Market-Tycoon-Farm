/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, ScriptApp, 
  getMasterBatchFromControlTable, withSheetLock, getOrCreateSheet, 
cacheAllCorporateAssetsTrigger, triggerLedgerImportCycle, fuzAPI, _fetchProcessedLootData, 
runLootLedgerDelta, Ledger_Import_CorpJournal, syncContracts, runIndustryLedgerPhase,
  runLootDeltaPhase, runContractLedgerPhase, runAllLedgerImports, LoggerEx, writeDataToSheet, guardedSheetTransaction, atomicSwapAndFlush, deleteTriggersByName */

// Global variable to track recursion depth for this lock type
var EXECUTION_LOCK_DEPTH_TRY = 0;
var EXECUTION_LOCK_DEPTH_WAIT = 0;

var LOCK_TIMEOUT_MS = LOCK_TIMEOUT_MS || 5000;
var LOCK_WAIT_TIMEOUT_MS = LOCK_WAIT_TIMEOUT_MS || 30000; 

const finalSheetName = 'Market_Data_Raw';
const tempSheetName = 'Market_Data_Temp';
const oldSheetName = 'Market_Data_Old';
const RETRY_DELAY_MS = 30 * 1000;
const PROP_KEY_FINALIZER_STEP = 'marketDataFinalizeStep';

// --- TIME GATING CONSTANTS ---
const HOURLY_RUN_INTERVAL_MS = 60 * 60 * 1000; // 60 minutes
const PROP_KEY_LAST_RUN_TS = 'MAINTENANCE_LAST_RUN_TS_'; 

if (typeof GLOBAL_STATE_KEY === 'undefined') {
  var GLOBAL_STATE_KEY = 'GLOBAL_SYSTEM_STATE';
}

// State Machine Constants
const STATE_FLAGS = {
  NEW_RUN: 'NEW_RUN',
  PROCESSING: 'PROCESSING',
  FINALIZING: 'FINALIZING'
};
const PROP_KEY_SETUP_STAGE = 'marketDataSetupStage';
const SETUP_STAGE = {
  DELETE: 'DELETE',
  RECREATE: 'RECREATE'
};

/**
 * Helper to create a new one-time "retry" trigger.
 */
function scheduleOneTimeTrigger(functionName, delayMs) {
  if (typeof functionName !== 'string' || functionName.trim() === '') {
    throw new Error(`CRITICAL SCHEDULER ERROR: Invalid function name provided.`);
  }

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const systemState = SCRIPT_PROP.getProperty(GLOBAL_STATE_KEY) || 'RUNNING';

  try {
    deleteTriggersByName(functionName);
    if (systemState === 'MAINTENANCE') {
      console.warn(`Blocking trigger for ${functionName}: MAINTENANCE mode.`);
      return; 
    }
    ScriptApp.newTrigger(functionName).timeBased().after(delayMs).create();
    console.log(`Created trigger for ${functionName} in ~${Math.round(delayMs / 60000)} min.`);
  } catch (e) {
    console.error(`Failed to create trigger: ${e.message}`);
  }
}

/**
 * Helper to delete triggers by name.
 */
function deleteTriggersByName(functionName) {
  if (typeof functionName !== 'string' || functionName.trim() === '') return 0;

  let deletedCount = 0;
  try {
    const allTriggers = ScriptApp.getProjectTriggers();
    allTriggers.forEach(trigger => {
      if (trigger.getHandlerFunction() === functionName &&
        trigger.getEventType() === ScriptApp.EventType.CLOCK) {
        try {
          ScriptApp.deleteTrigger(trigger);
          deletedCount++;
        } catch (e) {}
      }
    });
  } catch (e) {
    console.error(`Error deleting triggers: ${e.message}`);
  }
  return deletedCount; 
}

/**
 * Internal reset helper.
 */
function _resetMarketDataJobState(error) {
  console.warn(`RESETTING Market Data Job State: ${error ? error.message : 'Manual'}.`);

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const keysToDelete = [
      'marketDataJobStep', 'marketDataRequestIndex', 'marketDataNextWriteRow',
      'marketDataFinalizeStep', 'marketDataSetupStep', 'marketDataJobLeaseUntil',
      'marketDataJobIsActive'
  ];

  try {
      keysToDelete.forEach(k => SCRIPT_PROP.deleteProperty(k));
  } catch (propError) {
    console.error(`Error deleting properties: ${propError.message}`);
  }

  deleteTriggersByName('updateMarketDataSheet');
  deleteTriggersByName('finalizeMarketDataUpdate');
  console.log("Market data job state reset complete.");
}

/**
 * Wraps a function in a ScriptLock tryLock().
 */
function executeWithTryLock(funcToRun, functionName, timeoutMs = LOCK_TIMEOUT_MS) {
  const lock = LockService.getScriptLock();

  if (typeof funcToRun !== 'function') return false; 
  if (!functionName) functionName = 'UnknownFunction';

  if (lock.tryLock(timeoutMs)) {
    try {
      const systemState = PropertiesService.getScriptProperties().getProperty(GLOBAL_STATE_KEY) || 'RUNNING';
      if (systemState === 'MAINTENANCE') {
        console.warn(`Skipping ${functionName}: MAINTENANCE mode.`);
        return null; 
      }
      console.log(`--- Starting Execution (TryLock): ${functionName} ---`);
      return funcToRun();
    } catch (e) {
      console.error(`Unhandled exception in ${functionName}: ${e.message}`);
    } finally {
      lock.releaseLock();
      console.log(`Script Lock released for ${functionName}.`);
    }
  } else {
    console.warn(`Skipping execution of ${functionName}: Script Lock was busy.`);
    return null;
  }
}

function masterOrchestrator() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');
  const currentMinute = new Date().getMinutes();
  const NOW_MS = new Date().getTime();

  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  const leaseUntil = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_LEASE) || '0', 10);
  let isJobActive = leaseUntil > NOW_MS;

  // Heartbeat Check
  if (!isJobActive && leaseUntil > 0 && leaseUntil <= NOW_MS) {
    console.warn(`Master orchestrator: Found expired lease. Clearing.`);
    SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
    isJobActive = false; 
  }

  // Priority 1: Finalization
  if (marketDataStep === STATE_FLAGS.FINALIZING) {
    console.log(`Master orchestrator: Finalizing Market Data.`);
    scheduleOneTimeTrigger("finalizeMarketDataUpdate", 30000);
    return;
  }

  // --- NEW 30-MINUTE DISPATCH LOGIC ---
  if (currentMinute === 0 || currentMinute === 30) { 
    if (isJobActive) {
      console.log(`Master orchestrator: Job active. Skipping dispatch.`);
    } else {
      console.log(`Master orchestrator: DISPATCHING NEW MARKET DATA JOB.`);
      const NEW_LEASE = NOW_MS + 280000; 
      SCRIPT_PROP.setProperty(PROP_KEY_LEASE, NEW_LEASE.toString());
      updateMarketDataSheet(); 
    }
  } 
  
  else if (currentMinute === 15 || currentMinute === 45) { 
    console.log(`Master orchestrator: In Maintenance window.`);

    // Nudge Market Data if stuck
    if (marketDataStep === STATE_FLAGS.PROCESSING || marketDataStep === STATE_FLAGS.NEW_RUN) { 
        console.log(`Master orchestrator: Nudging Market Data job.`);
        updateMarketDataSheet(); 
        return; 
    }

    // Run Maintenance Jobs (Assets, Ledgers)
    console.log(`Dispatching MAINTENANCE JOBS.`);
    const result = executeWithTryLock(runMaintenanceJobs, 'runMaintenanceJobs');

    if (result === null) {
      console.warn(`Maintenance skipped (Lock). Retry scheduled.`);
      scheduleOneTimeTrigger('runMaintenanceJobs', 120000);
    }
  }
}

/**
 * MAINTENANCE JOB RUNNER
 * Runs heavy background tasks during Quiet Windows.
 */
function runMaintenanceJobs() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const QUEUE_INDEX_KEY = 'MAINTENANCE_QUEUE_INDEX';
  
  // --- UPDATED JOB QUEUE (Gemini V3) ---
  const JOB_QUEUE = [
    'cacheAllCorporateAssetsTrigger', // Asset Cache (Resumable)
    'runLootAndJournalSync',          // Journals
    'runContractSync',                // Contracts
    'runIndustrySync'                 // Industry (New Ferrari Engine)
  ];
  
  const wrapperFuncName = 'runMaintenanceJobs';
  const retryDelayMs = 120000; 
  const NOW_MS = new Date().getTime();

  let currentIndex = parseInt(SCRIPT_PROP.getProperty(QUEUE_INDEX_KEY) || '0', 10);
  if (currentIndex >= JOB_QUEUE.length) currentIndex = 0;

  const currentJobName = JOB_QUEUE[currentIndex];
  console.log(`[Maintenance] Queue: ${currentIndex}/${JOB_QUEUE.length}. Job: ${currentJobName}`);

  // HOURLY RUN CHECK
  const lastRunKey = PROP_KEY_LAST_RUN_TS + currentJobName;
  const lastRunTimestamp = parseInt(SCRIPT_PROP.getProperty(lastRunKey) || '0', 10);
  
  // Skip if ran recently (except Assets, which manages its own schedule)
  if (currentJobName !== 'cacheAllCorporateAssetsTrigger' && (NOW_MS - lastRunTimestamp) < HOURLY_RUN_INTERVAL_MS) {
      console.log(`[Maintenance] Skipping ${currentJobName}: Ran recently.`);
      let nextIndex = (currentIndex + 1) % JOB_QUEUE.length;
      SCRIPT_PROP.setProperty(QUEUE_INDEX_KEY, nextIndex.toString());
      return; 
  }

  try {
    const fn = this[currentJobName]; 
    
    if (typeof fn === 'function') {
        fn(); 
        console.log(`[Maintenance] Job ${currentJobName} executed.`);
        
        if (currentJobName !== 'cacheAllCorporateAssetsTrigger') {
             SCRIPT_PROP.setProperty(lastRunKey, NOW_MS.toString());
             let nextIndex = (currentIndex + 1) % JOB_QUEUE.length;
             SCRIPT_PROP.setProperty(QUEUE_INDEX_KEY, nextIndex.toString());
        }
    } else {
        console.error(`CRITICAL: Function ${currentJobName} not found!`);
        let nextIndex = (currentIndex + 1) % JOB_QUEUE.length;
        SCRIPT_PROP.setProperty(QUEUE_INDEX_KEY, nextIndex.toString());
    }
    
  } catch (e) {
      console.error(`[Maintenance] Job ${currentJobName} Failed: ${e.message}`);
      scheduleOneTimeTrigger(wrapperFuncName, retryDelayMs);
  }
}

/**
 * Market Data Worker (Nitro Edition)
 */
function _updateMarketDataSheetWorker() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_WRITE_INDEX = 'marketDataNextWriteRow';
  const PROP_KEY_CHUNK_SIZE = 'marketDataChunkSize';
  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  const PROP_KEY_SETUP_STAGE = 'marketDataSetupStage';
  
  // --- NITRO CONFIGURATION ---
  const [MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, SOFT_LIMIT_MS, RESCHEDULE_DELAY_MS]
    = [8000, 500, 275000, 10000]; 

  const tempSheetName = 'Market_Data_Temp';
  const COLUMN_COUNT = 9;
  const START_ROW = 2;
  const DATA_SHEET_HEADERS = ["cacheKey", "type_id", "location_type", "location_id", "sell_min", "buy_max", "sell_volume", "buy_volume", "last_updated"];

  const START_TIME = new Date().getTime();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const masterRequests = getMasterBatchFromControlTable(ss);

  let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP) || STATE_FLAGS.NEW_RUN;

  // --- Phase 1: NEW_RUN ---
  if (currentStep === STATE_FLAGS.NEW_RUN || !masterRequests || masterRequests.length === 0) {
    console.log(`State: ${STATE_FLAGS.NEW_RUN}. Preparing cycle.`);

    if (!masterRequests || masterRequests.length === 0) {
      _resetMarketDataJobState(new Error("Control Table empty"));
      return;
    }

    const setupResult = guardedSheetTransaction(() => {
      const ss_inner = SpreadsheetApp.getActiveSpreadsheet();
      let sheet = ss_inner.getSheetByName(tempSheetName);

      if (sheet) {
        sheet.clearContents();
      } else {
        sheet = ss_inner.insertSheet(tempSheetName);
      }
      sheet.getRange(1, 1, 1, COLUMN_COUNT).setValues([DATA_SHEET_HEADERS]);
      sheet.hideSheet();
      return true; 
    }, 60000);

    if (!setupResult.success) {
      console.log(`Setup skipped (Lock Busy). Retrying.`);
      scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
      return;
    }

    console.log("Setup complete. Transitioning to PROCESSING.");
    SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, '0'); 
    SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
    currentStep = 'PROCESSING';
    SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'PROCESSING');
    return; 
  } 

  // --- Phase 2: WRITE (Nitro Mode) ---
  if (currentStep === 'PROCESSING' || currentStep === 'WRITE') {

    const masterRequests_stable = getMasterBatchFromControlTable(ss);
    let allRowsToWrite = [];

    try {
      const marketDataCrates = fuzAPI.getDataForRequests(masterRequests_stable);
      const currentTimeStamp = new Date();
      marketDataCrates.forEach(crate => {
        if (crate && crate.fuzObjects) {
          crate.fuzObjects.forEach(item => {
            if (item && item.type_id != null) {
              allRowsToWrite.push([
                "", item.type_id,
                crate.market_type || '', crate.market_id || '',
                item.sell?.min ?? '', item.buy?.max ?? '',
                item.sell?.volume ?? 0, item.buy?.volume ?? 0,
                currentTimeStamp
              ]);
            }
          });
        }
      });

      if (allRowsToWrite.length === 0) {
        SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.FINALIZING);
        scheduleOneTimeTrigger('finalizeMarketDataUpdate', RESCHEDULE_DELAY_MS);
        return;
      }
    } catch (e) {
      scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS * 2);
      return;
    }

    const ss_stable = SpreadsheetApp.getActiveSpreadsheet();
   
    let writeState = {
      logInfo: console.log, logError: console.error, logWarn: console.warn,
      nextBatchIndex: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_WRITE_INDEX) || '0'),
      ss: ss_stable,
      metrics: { startTime: START_TIME },
      config: {
        // --- NITRO SETTINGS ---
        MAX_CELLS_PER_CHUNK: 60000,    
        TARGET_WRITE_TIME_MS: 5000,    
        MAX_FACTOR : 2.0,              
        THROTTLE_THRESHOLD_MS: 2000,   
        THROTTLE_PAUSE_MS: 100,        
        currentChunkSize: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || MIN_CHUNK_SIZE.toString()),
        MAX_CHUNK_SIZE: MAX_CHUNK_SIZE,
        MIN_CHUNK_SIZE: MIN_CHUNK_SIZE,
        SOFT_LIMIT_MS: SOFT_LIMIT_MS
      }
    };

    const STRICT_MIN_CHUNK = 500;
    if (writeState.nextBatchIndex === 0) {
      writeState.config.currentChunkSize = STRICT_MIN_CHUNK;
    }

    const writeResult = writeDataToSheet(tempSheetName, allRowsToWrite, START_ROW, 1, writeState);

    if (writeResult.success) {
      console.log("Write SUCCESS. Transitioning to FINALIZING.");
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.FINALIZING);
      SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
      SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
      SCRIPT_PROP.deleteProperty(PROP_KEY_WRITE_INDEX);
      scheduleOneTimeTrigger('finalizeMarketDataUpdate', RESCHEDULE_DELAY_MS);
    }
    else if (writeResult.bailout_reason === "PREDICTIVE_BAILOUT" || writeResult.error.includes("ServiceTimeoutFailure")) {
      console.warn("Write phase hit limit. Rescheduling.");
      const nextIndex = writeResult.state.nextBatchIndex.toString();
      const nextChunkSize = writeResult.state.config.currentChunkSize.toString();
      SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, nextIndex);
      SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, nextChunkSize);
      Utilities.sleep(1000);
      scheduleOneTimeTrigger('updateMarketDataSheet', 60000);
    }
    else {
      _resetMarketDataJobState(new Error(`Write Failure: ${writeResult.error}`));
    }
  }
}

function updateMarketDataSheet() {
  const funcName = 'updateMarketDataSheet';
  const result = executeWithTryLock(_updateMarketDataSheetWorker, funcName);
  if (result === null) console.warn(`${funcName} skipped (Lock).`);
}

function manualResetMarketDataJobAndDispatch() {
  _resetMarketDataJobState(new Error("Manual reset"));
  scheduleOneTimeTrigger('updateMarketDataSheet', 5000);
  console.log("RESET & DISPATCHED.");
}

function finalizeMarketDataUpdate() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const finalSheetName = 'Market_Data_Raw'; 
  const tempSheetName = 'Market_Data_Temp'; 
  const RETRY_DELAY_MS = 60 * 1000; 

  const funcName = 'finalizeMarketDataUpdate';

  const result = executeWithTryLock(() => {

    if (SCRIPT_PROP.getProperty(PROP_KEY_STEP) !== STATE_FLAGS.FINALIZING) {
      _resetMarketDataJobState(new Error(`Wrong state.`));
      return;
    }

    const swapResult = guardedSheetTransaction(() => {
      const ss_inner = SpreadsheetApp.getActiveSpreadsheet();
      return atomicSwapAndFlush(ss_inner, finalSheetName, tempSheetName);
    }, 60000);

    if (swapResult.success === true) {
      _resetMarketDataJobState(null);
      console.log("SUCCESS: Finalization complete.");
      return true;
    } else {
      console.warn(`Swap failed: ${swapResult.error}. Retrying.`);
      scheduleOneTimeTrigger('finalizeMarketDataUpdate', RETRY_DELAY_MS);
    }

  }, funcName);
}

// --- WORKER FUNCTIONS (Unlocked) ---

function runLootAndJournalSync() {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('MASTER_SYNC') : console);
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  try {
    const lootData = _fetchProcessedLootData(); 
    if (lootData) runLootDeltaPhase(ss); 
  } catch (e) { log.error('Loot Sync failed', e); }

  try {
    Ledger_Import_CorpJournal(ss, { division: 3, sinceDays: 30 }); 
  } catch (e) { log.error('Corp Journal Import failed', e); }
}

function runContractSync() {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('MASTER_SYNC') : console);
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  try { runContractLedgerPhase(ss); } 
  catch (e) { log.error('Contract Sync failed', e); }
}

function runIndustrySync() {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('MASTER_SYNC') : console);
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  try { runIndustryLedgerPhase(ss); } 
  catch (e) { log.error('Industry Ledger Phase failed', e); }
}

function setupStaggeredTriggers() {
  console.log("Setting up/Resetting orchestrator triggers...");

  const managedFunctions = [
    'fuzAPI.cacheRefres',
    'triggerCacheWarmerWithRetry',
    'updateMarketDataSheet',
    'finalizeMarketDataUpdate',
    'cleanupOldSheet',
    'masterOrchestrator',
    'cacheAllCorporateAssetsTrigger',
    'runLootAndJournalSync', 
    'runContractSync', 
    'runIndustrySync', 
    'runMaintenanceJobs' 
  ];

  managedFunctions.forEach(funcName => deleteTriggersByName(funcName));

  try {
    ScriptApp.newTrigger('masterOrchestrator')
      .timeBased().everyMinutes(15).create();
    console.log('SUCCESS: Created 15-minute trigger for masterOrchestrator.');

  } catch (e) {
    console.error(`Failed to create new triggers: ${e.message}.`);
  }
}

function bumpMarketDataJob() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
  scheduleOneTimeTrigger('updateMarketDataSheet', 5000);
}

function manualResetMarketDataJob() {
  _resetMarketDataJobState(new Error("Manual reset"));
}