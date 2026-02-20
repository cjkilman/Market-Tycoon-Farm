/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, ScriptApp, 
  getMasterBatchFromControlTable, withSheetLock, getOrCreateSheet, 
cacheAllCorporateAssetsTrigger, triggerLedgerImportCycle, fuzAPI, _fetchProcessedLootData, 
runLootLedgerDelta, Ledger_Import_CorpJournal, syncContracts, runIndustryLedgerPhase,
  runLootDeltaPhase, runContractLedgerPhase,  LoggerEx, writeDataToSheet, guardedSheetTransaction, atomicSwapAndFlush, deleteTriggersByName, pauseSheet, wakeUpSheet, prepareTempSheet */

// Global variable to track recursion depth for this lock type
var EXECUTION_LOCK_DEPTH_TRY = 0;
var EXECUTION_LOCK_DEPTH_WAIT = 0;

var LOCK_TIMEOUT_MS = 5000;
var LOCK_WAIT_TIMEOUT_MS = 30000;

const finalSheetName = 'Market_Data_Raw';
const tempSheetName = 'Market_Data_Temp';
const MARKET_NAMED_RANGE = 'NR_MARKET_DATA';
const RETRY_DELAY_MS = 30 * 1000;
const PROP_KEY_FINALIZER_STEP = 'marketDataFinalizeStep';

// --- TIME GATING CONSTANTS ---
const HOURLY_RUN_INTERVAL_MS = 60 * 60 * 1000;
const JOURNAL_RUN_INTERVAL_MS = 10 * 60 * 1000;
const PROP_KEY_LAST_RUN_TS = 'MAINTENANCE_LAST_RUN_TS_';
const PROP_KEY_HISTORY_DONE = 'HISTORY_PULL_COMPLETED_DATE';

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

// ... [Keep scheduleOneTimeTrigger, deleteTriggersByName, _resetMarketDataJobState unchanged] ...
// (Assumed lines 35-100 are standard helpers)

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
 * Grabs Regional Pricing from Market Price Tracker.
 * ESI limits 1 type_ID per request so we keep it centralized for URL Fetch App
 */
function syncESIRegionData() {
  const log = LoggerEx.withTag('REGION_SYNC');
  const sourceId = "1L37sYZPznkNu3EJy554nmaclXQl6DpvERc_N6ans76M";
  const targetSheetName = "ESI_Region";
  const NAMED_RANGE_NAME = "ESI_Region_Data"; // Change this to your actual Named Range name
  
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const targetSheet = ss.getSheetByName(targetSheetName);
  
  if (!targetSheet) return;

  try {
    const sourceData = SpreadsheetApp.openById(sourceId)
                        .getSheetByName("Publish_ESI_Region_market_orders")
                        .getDataRange()
                        .getValues();
    
    // 1. CLEAR & WRITE
    targetSheet.clearContents(); 
    const newRange = targetSheet.getRange(1, 1, sourceData.length, sourceData[0].length);
    newRange.setValues(sourceData);
    
    // 2. UPDATE NAMED RANGE (The "Range Stretcher")
    // This tells the whole workbook exactly where the new data lives.
    ss.setNamedRange(NAMED_RANGE_NAME, newRange);
    log.info(`Named Range '${NAMED_RANGE_NAME}' updated to ${sourceData.length} rows.`);

    // 3. THE TRIM
    const lastRow = sourceData.length;
    const currentMax = targetSheet.getMaxRows();
    if (currentMax > lastRow) {
      targetSheet.deleteRows(lastRow + 1, currentMax - lastRow);
    }
    
    log.info("ESI_Region: Sync & Named Range Update Complete.");
  } catch (e) {
    log.error("ESI_Region Sync Error: " + e.message);
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
        } catch (e) { }
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

/**
 * Wraps a function in a ScriptLock waitLock().
 */
function executeWithWaitLock(funcToRun, functionName, timeoutMs = LOCK_WAIT_TIMEOUT_MS) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(timeoutMs);
  } catch (e) {
    console.error(`Could not acquire Script Lock for ${functionName}.`);
    throw e;
  }

  try {
    const systemState = PropertiesService.getScriptProperties().getProperty(GLOBAL_STATE_KEY) || 'RUNNING';
    if (systemState === 'MAINTENANCE') {
      console.warn(`Skipping ${functionName}: MAINTENANCE mode.`);
      return null;
    }

    console.log(`--- Starting Execution (WaitLock): ${functionName} ---`);
    return funcToRun();
  } catch (e) {
    console.error(`Unhandled exception in ${functionName}: ${e.message}`);
    throw e;
  } finally {
    lock.releaseLock();
    console.log(`Script Lock released for ${functionName}.`);
  }
}

// ... [masterOrchestrator logic] ...

function masterOrchestrator() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');
  const PROP_KEY_MARKET_LAST_RUN = 'MARKET_DATA_LAST_RUN_TS';
  const lastMarketRun = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_MARKET_LAST_RUN) || '0', 10);

  const currentMinute = new Date().getMinutes();
  const NOW_MS = new Date().getTime();

  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  const leaseUntil = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_LEASE) || '0', 10);
  let isJobActive = leaseUntil > NOW_MS;

  if (!isJobActive && leaseUntil > 0 && leaseUntil <= NOW_MS) {
    console.warn(`Orchestrator: Expired lease found. Clearing.`);
    SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
    isJobActive = false;
  }


  // NEW: Check for pending COGS Finalization and process it immediately.
  if (_nudgeCogsFinalizer()) {
    return;
  }

  if (marketDataStep === STATE_FLAGS.FINALIZING) {
    const lock = LockService.getScriptLock();
    if (lock.tryLock(0)) {
      lock.releaseLock();
      console.log(`Orchestrator: Finalizing Market Data.`);
      scheduleOneTimeTrigger("finalizeMarketDataUpdate", 5000);
    } else {
      console.log(`Orchestrator: Finalizer already running (Lock busy). Skipping.`);
    }
    return;
  }

  console.log(`Orchestrator (min ${currentMinute}): High-Frequency Check.`);

  const timeSinceLastRun = NOW_MS - lastMarketRun;
  const RUN_INTERVAL_MS = 28 * 60 * 1000;

  if (timeSinceLastRun > RUN_INTERVAL_MS) {
    if (isJobActive) {
      console.log(`Orchestrator: Market Data Active. Skipping NEW dispatch.`);
    } else {
      console.log(`Orchestrator: DISPATCHING NEW MARKET DATA JOB (30m Cycle).`);
      const launchResult = updateMarketDataSheet();

      if (launchResult !== null) {
        const FRESH_NOW_MS = new Date().getTime();
        const NEW_LEASE = FRESH_NOW_MS + 300000;
        SCRIPT_PROP.setProperty(PROP_KEY_LEASE, NEW_LEASE.toString());
        console.log("Orchestrator: Lock acquired. Lease set.");
      } else {
        console.warn("Orchestrator: Lock BUSY. Dispatch aborted (will retry in 5m).");
      }
    }
    return;
  }

  if (marketDataStep === STATE_FLAGS.PROCESSING || marketDataStep === STATE_FLAGS.NEW_RUN) {
    console.log(`Orchestrator: Market Data Active (${marketDataStep}). Nudging.`);
    updateMarketDataSheet();
    return;
  }

  console.log(`Orchestrator: Market Data Idle. Attempting Maintenance.`);
  executeWithTryLock(runMaintenanceJobs, 'runMaintenanceJobs');
}

function forceResetMaint() {
  const props = PropertiesService.getScriptProperties();
  props.deleteProperty('BOM_MAINTENANCE_LEASE');
  props.deleteProperty('LAST_RUN_generateFullBOMData');
  props.setProperty('MAINTENANCE_QUEUE_INDEX', '0');
  console.log("State cleared. BOM Engine is now next in queue.");
}

function runMaintenanceJobs() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  
  // 1. Priority Lock: Maintenance must yield to active Market Data syncs
  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');
  const manualSync = SCRIPT_PROP.getProperty('MANUAL_SYNC_ACTIVE');
  if (marketDataStep === 'PROCESSING' || marketDataStep === 'NEW_RUN' || marketDataStep === 'FINALIZING' || manualSync === 'TRUE') {
    console.warn("[Maintenance] Aborted: Market Engine or Manual Sync is active.");
    return;
  }

  const NOW_MS = new Date().getTime();
  const STANDARD_INTERVAL = 3600000; // 60m default
  
  // 2. Job Registry with targeted intervals
  const JOB_QUEUE = [
    { name: 'generateFullBOMData', interval: 2700000, lease: 1200000 }, // 45m run / 20m lease
    { name: 'runLootDeltaPhase', interval: STANDARD_INTERVAL },
    { name: 'Ledger_Import_CorpJournal', interval: 1800000 }, // 30m
    { name: 'runContractLedgerPhase', interval: STANDARD_INTERVAL },
    { name: 'runIndustryLedgerPhase', interval: STANDARD_INTERVAL },
    { name: 'cacheAllCorporateAssetsTrigger', interval: STANDARD_INTERVAL }
  ];

  const QUEUE_INDEX_KEY = 'MAINTENANCE_QUEUE_INDEX';
  let currentIndex = parseInt(SCRIPT_PROP.getProperty(QUEUE_INDEX_KEY) || '0', 10);
  if (currentIndex >= JOB_QUEUE.length) currentIndex = 0;

  let iterations = 0;
  while (iterations < JOB_QUEUE.length) {
    const job = JOB_QUEUE[currentIndex];
    const lastRunKey = 'LAST_RUN_' + job.name;
    const lastRunTs = parseInt(SCRIPT_PROP.getProperty(lastRunKey) || '0', 10);
    const isDue = (NOW_MS - lastRunTs) >= job.interval;

    // 3. Lease Management: If the job is due, the lease is ignored/cleared.
    if (job.name === 'generateFullBOMData') {
      const activeLease = parseInt(SCRIPT_PROP.getProperty('BOM_MAINTENANCE_LEASE') || '0', 10);
      if (isDue) {
        SCRIPT_PROP.deleteProperty('BOM_MAINTENANCE_LEASE');
      } else if (activeLease > NOW_MS) {
        currentIndex = (currentIndex + 1) % JOB_QUEUE.length;
        iterations++;
        continue;
      }
    }

    // 4. Execution Logic
    if (isDue) {
      console.log(`[Maintenance] Dispatching: ${job.name}`);
      
      if (job.lease) {
        SCRIPT_PROP.setProperty('BOM_MAINTENANCE_LEASE', (NOW_MS + job.lease).toString());
      }

      try {
        // Fallback check for function scope
        const fn = this[job.name] || eval(job.name); 
        if (typeof fn === 'function') {
          fn();
          SCRIPT_PROP.setProperty(lastRunKey, NOW_MS.toString());
          SCRIPT_PROP.setProperty(QUEUE_INDEX_KEY, ((currentIndex + 1) % JOB_QUEUE.length).toString());
          console.log(`[Maintenance] ${job.name} completed successfully.`);
          return; // One job per Orchestrator tick to save RAM
        }
      } catch (e) {
        console.error(`[Maintenance] Critical Failure in ${job.name}: ${e.message}`);
      }
    }

    currentIndex = (currentIndex + 1) % JOB_QUEUE.length;
    iterations++;
  }
  console.log("Maintenance Cycle: All jobs are currently within their interval windows.");
}

/**
 * Market Data Worker (Nitro Edition - HYBRID)
 * Phase 1: Surgical Pause (Prevent creation crash)
 * Phase 2: Live Write (No pause, allows dashboard use)
 */
function updateMarketDataSheet() {
  if (isSdeJobRunning()) {
    console.warn("ABORT: SDE Update in progress. Parking Market Tycoon.");
    return; 
  }

  if (!isEngineRunning_()) {
    console.warn("ABORT: Engine is parked. Market Tycoon skipping fetch.");
    return;
  }
  const START_TIME = new Date().getTime();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_WRITE_INDEX = 'marketDataNextWriteRow';
  const PROP_KEY_CHUNK_SIZE = 'marketDataChunkSize';
  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  const PROP_KEY_MARKET_LAST_RUN = 'MARKET_DATA_LAST_RUN_TS';

  SCRIPT_PROP.setProperty(PROP_KEY_MARKET_LAST_RUN, START_TIME.toString());

  const COLUMN_COUNT = 9;
  const START_ROW = 2;
  const DATA_SHEET_HEADERS = ["cacheKey", "type_id", "location_type", "location_id", "sell_min", "buy_max", "sell_volume", "buy_volume", "last_updated"];

  var ss_anchor = SpreadsheetApp.getActiveSpreadsheet();
  const masterRequests = getMasterBatchFromControlTable(ss_anchor);

  let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP) || STATE_FLAGS.NEW_RUN;

  // --- Phase 1: NEW_RUN (SURGICAL PAUSE) ---
  if (currentStep === STATE_FLAGS.NEW_RUN || !masterRequests || masterRequests.length === 0) {
    console.log(`State: ${STATE_FLAGS.NEW_RUN}.`);

    if (!masterRequests || masterRequests.length === 0) {
      _resetMarketDataJobState(new Error("Control Table empty"));
      return;
    }

    const setupResult = guardedSheetTransaction(() => {
      const result = prepareTempSheet(ss_anchor, tempSheetName, DATA_SHEET_HEADERS);
      if (!result.success) {
        throw new Error(result.error || "Unknown Prep Failure");
      }
      if (result.state) {
        result.state.hideSheet();
      }
      return true;
    }, 60000);

    if (!setupResult.success) {
      console.warn(`[Worker] Sheet prep failed: ${setupResult.error}`);
      scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
      return;
    }

    SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, '0');
    SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
    currentStep = 'PROCESSING';
    SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'PROCESSING');

    scheduleOneTimeTrigger('updateMarketDataSheet', 1000);
    return;
  }

  // --- Phase 2: WRITE (Nitro Mode - LIVE/UNPAUSED) ---
  if (currentStep === 'PROCESSING' || currentStep === 'WRITE') {

    const masterRequests_stable = getMasterBatchFromControlTable(ss_anchor);
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
        console.error("Worker: allRowsToWrite is empty! Aborting write to prevent data wipe.");
        _resetMarketDataJobState(new Error("Zero rows returned from API - Aborted Write"));
        return;
      }
    } catch (e) {
      scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS * 2);
      return;
    }

    ss_anchor = SpreadsheetApp.getActiveSpreadsheet();

    let writeState = {
      logInfo: console.log, logError: console.error, logWarn: console.warn,
      nextBatchIndex: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_WRITE_INDEX) || '0'),
      ss: ss_anchor,
      metrics: { startTime: START_TIME },
      config: {
        ...(typeof NITRO_CONFIG !== 'undefined' ? NITRO_CONFIG : {}),
        MAX_CELLS_PER_CHUNK: 40000,
        MAX_CHUNK_SIZE: 2000,
        currentChunkSize: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || '1000')
      }
    };

    const writeResult = writeDataToSheet(tempSheetName, allRowsToWrite, START_ROW, 1, writeState);

    if (writeResult.success) {
      console.log("Write SUCCESS. Transitioning to FINALIZING.");
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.FINALIZING);
      SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
      SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
      SCRIPT_PROP.deleteProperty(PROP_KEY_WRITE_INDEX);
      scheduleOneTimeTrigger('finalizeMarketDataUpdate', RESCHEDULE_DELAY_MS);
    }
    else if (writeResult.bailout_reason === "PREDICTIVE_BAILOUT" || (writeResult.error && writeResult.error.includes("timed out"))) {
      const reason = writeResult.error ? writeResult.error : "Predictive Bailout";
      console.warn(`Write phase interrupted. Reason: ${reason}. Rescheduling.`);

      const nextIndex = writeResult.state.nextBatchIndex.toString();
      let nextChunkSize = writeResult.state.config.currentChunkSize;

      if (writeResult.error) {
        nextChunkSize = Math.max(100, Math.floor(nextChunkSize / 2)); 
      }

      SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, nextIndex);
      SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, nextChunkSize.toString());
      Utilities.sleep(1000);
      scheduleOneTimeTrigger('updateMarketDataSheet', 30000);
    }
    else {
      // --- CRITICAL FIX START ---
      if (writeResult.error && (writeResult.error.includes("Lock Failed") || writeResult.error.includes("Lock timeout"))) {
        console.warn("Lock Conflict detected. Pausing for Sheet to breathe. DO NOT RESET.");
        
        // Preserve the current index so it picks up where it left off
        const nextIndex = (writeResult.state.nextBatchIndex || 0).toString();
        SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, nextIndex);
        
        // Force a 30-second delay to allow Google Sheets to finish calculations
        scheduleOneTimeTrigger('updateMarketDataSheet', 30000); 
      } else {
        // Only reset on actual data corruption or API failures
        _resetMarketDataJobState(new Error(`Write Failure: ${writeResult.error}`));
      }
      // --- CRITICAL FIX END ---
    }
  }
}

function finalizeMarketDataUpdate() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const finalSheetName = 'Market_Data_Raw';
  const tempSheetName = 'Market_Data_Temp';

  const funcName = 'finalizeMarketDataUpdate';

  executeWithTryLock(() => {

    if (SCRIPT_PROP.getProperty(PROP_KEY_STEP) !== 'FINALIZING') {
      _resetMarketDataJobState(new Error(`Wrong state.`));
      return;
    }

    var ss_inner = SpreadsheetApp.getActiveSpreadsheet();

    // [CRITICAL FIX] REFRESH CONNECTION
    // The previous 'ss_inner' is dead after the long flush. Get a new one.
    ss_inner = SpreadsheetApp.getActiveSpreadsheet();

    const repairMap = { ['NR_MARKET_DATA']: 'A:G' };

    const transactionResult = guardedSheetTransaction(() => {
      // Uses the new "Hot Swap" (Overwrite) logic, now includes Named Range update
      return atomicSwapAndFlush(ss_inner, finalSheetName, tempSheetName, repairMap);
    }, 60000);



    let swapSuccess = (transactionResult.success && transactionResult.state.success);

    if (swapSuccess) {
      syncESIRegionData();
      _resetMarketDataJobState(null);
      console.log("SUCCESS: Finalization complete.");

    } else {
      console.warn(`[Finalizer] Swap Failed: ${transactionResult.error || transactionResult.state.errorMessage}`);
    }

  }, funcName);
}