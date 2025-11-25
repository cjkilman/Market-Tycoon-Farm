/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, ScriptApp, 
  getMasterBatchFromControlTable, withSheetLock, getOrCreateSheet, 
cacheAllCorporateAssetsTrigger, triggerLedgerImportCycle, fuzAPI, _fetchProcessedLootData, 
runLootLedgerDelta, Ledger_Import_CorpJournal, syncContracts, runIndustryLedgerPhase,
  runLootDeltaPhase, runContractLedgerPhase, runAllLedgerImports, LoggerEx, writeDataToSheet, guardedSheetTransaction, atomicSwapAndFlush, deleteTriggersByName */

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

/**
 * HIGH-FREQUENCY MASTER ORCHESTRATOR (5-Minute Heartbeat)
 */
function masterOrchestrator() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');

  // [CHANGE 1] Get the stored timestamp of the last successful dispatch
  const PROP_KEY_MARKET_LAST_RUN = 'MARKET_DATA_LAST_RUN_TS';
  const lastMarketRun = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_MARKET_LAST_RUN) || '0', 10);

  const currentMinute = new Date().getMinutes();
  const NOW_MS = new Date().getTime();

  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  const leaseUntil = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_LEASE) || '0', 10);
  let isJobActive = leaseUntil > NOW_MS;

  // --- HEARTBEAT ---
  if (!isJobActive && leaseUntil > 0 && leaseUntil <= NOW_MS) {
    console.warn(`Orchestrator: Expired lease found. Clearing.`);
    SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
    isJobActive = false;
  }

  // --- PRIORITY 1: FINALIZATION ---
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

  // --- [CHANGE 2] PRIORITY 2: MARKET DATA (Robust 30m Cycle) ---
  const timeSinceLastRun = NOW_MS - lastMarketRun;
  const RUN_INTERVAL_MS = 28 * 60 * 1000; // 28 mins (buffer for 30m cycle)

  if (timeSinceLastRun > RUN_INTERVAL_MS) {
    if (isJobActive) {
      console.log(`Orchestrator: Market Data Active. Skipping NEW dispatch.`);
    } else {
      console.log(`Orchestrator: DISPATCHING NEW MARKET DATA JOB (30m Cycle).`);

      // 1. Attempt Execution FIRST
      const launchResult = updateMarketDataSheet();

      // 2. Only Set Lease if Lock was Acquired (launchResult is not null)
      if (launchResult !== null) {
        const NEW_LEASE = NOW_MS + 300000;
        SCRIPT_PROP.setProperty(PROP_KEY_LEASE, NEW_LEASE.toString());
        console.log("Orchestrator: Lock acquired. Lease set.");
      } else {
        console.warn("Orchestrator: Lock BUSY. Dispatch aborted (will retry in 5m).");
        // We do NOT set the lease here. 
        // This ensures the next 5-min heartbeat sees "Job Inactive" and retries immediately.
      }
    }
    return;
  }

  // --- PRIORITY CHECK: NUDGE ---
  if (marketDataStep === STATE_FLAGS.PROCESSING || marketDataStep === STATE_FLAGS.NEW_RUN) {
    console.log(`Orchestrator: Market Data Active (${marketDataStep}). Nudging.`);
    updateMarketDataSheet();
    return;
  }

  // --- PRIORITY 3: MAINTENANCE ---
  console.log(`Orchestrator: Market Data Idle. Attempting Maintenance.`);
  executeWithTryLock(runMaintenanceJobs, 'runMaintenanceJobs');
}

/**
 * MAINTENANCE JOB RUNNER
 */
function runMaintenanceJobs() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const QUEUE_INDEX_KEY = 'MAINTENANCE_QUEUE_INDEX';

  const JOB_QUEUE = [
    'cacheAllCorporateAssetsTrigger',
    'runLootAndJournalSync',
    'runContractSync'
  ];

  // Don't run heavy maintenance if Market Data is in critical phase
  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');
  if (marketDataStep === STATE_FLAGS.FINALIZING) return;

  const retryDelayMs = 120000;
  const NOW_MS = new Date().getTime();

  let currentIndex = parseInt(SCRIPT_PROP.getProperty(QUEUE_INDEX_KEY) || '0', 10);
  if (currentIndex >= JOB_QUEUE.length) currentIndex = 0;

  const currentJobName = JOB_QUEUE[currentIndex];

  // Hourly Check (Skip if ran recently)
  const lastRunKey = PROP_KEY_LAST_RUN_TS + currentJobName;
  const lastRunTimestamp = parseInt(SCRIPT_PROP.getProperty(lastRunKey) || '0', 10);

  // *** FIXED: Removed Exclusion for Asset Cache ***
  // Now applies 1-hour check to ALL jobs, including Asset Cache
  if ((NOW_MS - lastRunTimestamp) < HOURLY_RUN_INTERVAL_MS) {
    // Rotate Queue
    let nextIndex = (currentIndex + 1) % JOB_QUEUE.length;
    SCRIPT_PROP.setProperty(QUEUE_INDEX_KEY, nextIndex.toString());
    return;
  }

  console.log(`[Maintenance] Executing: ${currentJobName}`);

  try {
    const fn = this[currentJobName];
    if (typeof fn === 'function') {
      // Try to run. Note: executeWithTryLock inside the workers prevents conflicts.
      fn();

      // *** FIXED: Removed Exclusion for Asset Cache ***
      // Now saves timestamp for ALL jobs
      SCRIPT_PROP.setProperty(lastRunKey, NOW_MS.toString());

      let nextIndex = (currentIndex + 1) % JOB_QUEUE.length;
      SCRIPT_PROP.setProperty(QUEUE_INDEX_KEY, nextIndex.toString());

    } else {
      let nextIndex = (currentIndex + 1) % JOB_QUEUE.length;
      SCRIPT_PROP.setProperty(QUEUE_INDEX_KEY, nextIndex.toString());
    }
  } catch (e) {
    console.error(`[Maintenance] Failed: ${e.message}`);
  }
}

function forceReleaseStuckScriptLock() {
  const lock = LockService.getScriptLock();
  try { lock.releaseLock(); console.log("Lock released."); } catch (e) { }
}

/**
 * TURBO MAINTENANCE WORKER
 * UPDATED: Increased Soft Limit to 4.5 minutes.
 */
function runHourlyJobQueue() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const QUEUE_INDEX_KEY = 'MAINTENANCE_QUEUE_INDEX';
  const START_TIME = new Date().getTime();

  // --- TUNED LIMIT: 4.5 Minutes ---
  const SOFT_LIMIT_MS = 270000;

  const JOB_QUEUE = [
    'cacheAllCorporateAssetsTrigger',
    'runContractSync',
    'runIndustrySync'
  ];

  let currentIndex = parseInt(SCRIPT_PROP.getProperty(QUEUE_INDEX_KEY) || '0', 10);
  if (currentIndex >= JOB_QUEUE.length) currentIndex = 0;

  let jobsChecked = 0;
  while (jobsChecked < JOB_QUEUE.length && (new Date().getTime() - START_TIME) < SOFT_LIMIT_MS) {

    const jobName = JOB_QUEUE[currentIndex];
    const lastRunKey = PROP_KEY_LAST_RUN_TS + jobName;
    const lastRun = parseInt(SCRIPT_PROP.getProperty(lastRunKey) || '0', 10);
    const NOW_MS = new Date().getTime();

    // RESCUE LOGIC: Check for stuck Asset Cache
    let forceRun = false;
    if (jobName === 'cacheAllCorporateAssetsTrigger') {
      const assetResumeIndex = SCRIPT_PROP.getProperty('AssetCache_NextRow');
      if (assetResumeIndex && parseInt(assetResumeIndex) > 0) {
        console.warn(`[Maintenance] RESCUING stuck Asset Cache job at index ${assetResumeIndex}.`);
        forceRun = true;
      }
    }

    if (forceRun || (NOW_MS - lastRun) > HOURLY_RUN_INTERVAL_MS) {
      console.log(`[Maintenance] Running: ${jobName}`);

      try {
        const fn = this[jobName];
        if (typeof fn === 'function') fn();
        SCRIPT_PROP.setProperty(lastRunKey, NOW_MS.toString());
      } catch (e) {
        console.error(`Job ${jobName} Failed: ${e.message}`);
      }
    } else {
      console.log(`[Maintenance] Skipping ${jobName}: Cooldown active.`);
    }

    currentIndex = (currentIndex + 1) % JOB_QUEUE.length;
    jobsChecked++;
  }
  SCRIPT_PROP.setProperty(QUEUE_INDEX_KEY, currentIndex.toString());
}

function forceResetMarketTimer() {
  PropertiesService.getScriptProperties().deleteProperty('MARKET_DATA_LAST_RUN_TS');
  console.log("Timer reset. Market Data will run on next heartbeat.");
}

/**
 * Market Data Worker (Nitro Edition - TUNED)
 */
function _updateMarketDataSheetWorker() {
  const START_TIME = new Date().getTime();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_WRITE_INDEX = 'marketDataNextWriteRow';
  const PROP_KEY_CHUNK_SIZE = 'marketDataChunkSize';
  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  const PROP_KEY_MARKET_LAST_RUN = 'MARKET_DATA_LAST_RUN_TS'; // [ADDED]

const [MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, SOFT_LIMIT_MS, RESCHEDULE_DELAY_MS]
    = [8000, 1000, 190000, 10000];

  // --- [CRITICAL FIX] HEARTBEAT TIMESTAMP ---
  // Update the timestamp IMMEDIATELY. 
  // This confirms we hold the lock and are active.
  // It forces the Orchestrator to back off for another 30 mins.
  SCRIPT_PROP.setProperty(PROP_KEY_MARKET_LAST_RUN, START_TIME.toString());
  // ------------------------------------------

  const COLUMN_COUNT = 9;
  const START_ROW = 2;
  const DATA_SHEET_HEADERS = ["cacheKey", "type_id", "location_type", "location_id", "sell_min", "buy_max", "sell_volume", "buy_volume", "last_updated"];
  var ss_anchor = []; //Anchor the main reference Headers.apply. Then refresh 
  ss_anchor = SpreadsheetApp.getActiveSpreadsheet();
  const masterRequests = getMasterBatchFromControlTable(ss_anchor);

  let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP) || STATE_FLAGS.NEW_RUN;

  // --- Phase 1: NEW_RUN ---
  if (currentStep === STATE_FLAGS.NEW_RUN || !masterRequests || masterRequests.length === 0) {
    console.log(`State: ${STATE_FLAGS.NEW_RUN}.`);

    if (!masterRequests || masterRequests.length === 0) {
      _resetMarketDataJobState(new Error("Control Table empty"));
      return;
    }

    const setupResult = guardedSheetTransaction(() => {
      ss_anchor = SpreadsheetApp.getActiveSpreadsheet();
      let sheet = ss_anchor.getSheetByName(tempSheetName);

      if (sheet) {
        const lastRow = sheet.getMaxRows();
        if (lastRow > 1) sheet.getRange(2, 1, lastRow - 1, sheet.getMaxColumns()).clearContent();
      } else {
        sheet = ss_anchor.insertSheet(tempSheetName);
      }
      sheet.getRange(1, 1, 1, COLUMN_COUNT).setValues([DATA_SHEET_HEADERS]);
      sheet.hideSheet();
      return true;
    }, 60000);

    if (!setupResult.success) {
      scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
      return;
    }

    SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, '0');
    SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
    currentStep = 'PROCESSING';
    SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'PROCESSING');

    // [OPTIMIZATION] Instant Dispatch - Start writing immediately
    scheduleOneTimeTrigger('updateMarketDataSheet', 1000);
    return;
  }

  // --- Phase 2: WRITE (Nitro Mode) ---
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
        SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.FINALIZING);
        scheduleOneTimeTrigger('finalizeMarketDataUpdate', RESCHEDULE_DELAY_MS);
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
        // --- TUNING: "STABLE CRUISER" (Guaranteed Delivery) ---
        
        MAX_CELLS_PER_CHUNK: 50000, 
        MAX_CHUNK_SIZE: 500, // Cap at 500 rows to prevent Deep Sheet Timeouts
        
        // 1. DISABLE ACCELERATION
        MAX_FACTOR: 1.0, 

        // 2. FORCE PAUSES
        THROTTLE_THRESHOLD_MS: -1, 

        // 3. THE BREATHER
        // 2 seconds is usually enough for 500 rows.
        THROTTLE_PAUSE_MS: 2000, 

        currentChunkSize: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || '500'),
        MIN_CHUNK_SIZE: 250,
        TARGET_WRITE_TIME_MS: 1000, 
        SOFT_LIMIT_MS: SOFT_LIMIT_MS
      }
    };

    // Enforce safety start
    if (writeState.nextBatchIndex === 0) writeState.config.currentChunkSize = 500;

    // 4. Execute Write
    const writeResult = writeDataToSheet(tempSheetName, allRowsToWrite, START_ROW, 1, writeState);

    if (writeResult.success) {
      console.log("Write SUCCESS. Transitioning to FINALIZING.");
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.FINALIZING);
      SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
      SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
      SCRIPT_PROP.deleteProperty(PROP_KEY_WRITE_INDEX);
      scheduleOneTimeTrigger('finalizeMarketDataUpdate', RESCHEDULE_DELAY_MS);
    }
    // *** FIXED LOGIC: Catch Bailouts and Timeouts ***
    else if (writeResult.bailout_reason === "PREDICTIVE_BAILOUT" ||
      (writeResult.error && (
        writeResult.error.includes("ServiceTimeoutFailure") ||
        writeResult.error.includes("Service timed out") ||
        writeResult.error.includes("Exceeded maximum execution time")
      ))) {

      // Clean Log Message
      const reason = writeResult.error ? writeResult.error : "Soft Time Limit Reached (Predictive)";
      console.warn(`Write phase interrupted. Reason: ${reason}. Rescheduling.`);

      const nextIndex = writeResult.state.nextBatchIndex.toString();

      // Halve the chunk size on error to prevent death spiral
      let nextChunkSize = writeResult.state.config.currentChunkSize;
      if (writeResult.error) {
        nextChunkSize = Math.max(MIN_CHUNK_SIZE, Math.floor(nextChunkSize / 2));
      }

      SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, nextIndex);
      SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, nextChunkSize.toString());
      Utilities.sleep(1000);
      scheduleOneTimeTrigger('updateMarketDataSheet', 30000);
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
  return result; // <--- ADD THIS LINE
}

function manualResetMarketDataJobAndDispatch() {
  _resetMarketDataJobState(new Error("Manual reset"));
  scheduleOneTimeTrigger('updateMarketDataSheet', 5000);
  console.log("RESET & DISPATCHED.");
}

function finalizeMarketDataUpdate() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  var ss_anchor = {};
  const funcName = 'finalizeMarketDataUpdate';

  executeWithTryLock(() => { // <--- Removed 'result =' as we handle it inside

    if (SCRIPT_PROP.getProperty(PROP_KEY_STEP) !== STATE_FLAGS.FINALIZING) {
      _resetMarketDataJobState(new Error(`Wrong state.`));
      return;
    }

    // 1. DEFINE REPAIR MAP (Critical for Self-Healing)
    // Assuming your market data typically covers specific columns. 
    // Use an open-ended range starting at Row 2 (headers are likely Row 1).
    const repairMap = {
      [MARKET_NAMED_RANGE]: 'A2:I' // <--- Adjust 'G' to your actual last column
    };

    // 2. RUN TRANSACTION
    const transactionResult = guardedSheetTransaction(() => {
      ss_anchor = SpreadsheetApp.getActiveSpreadsheet();

      // PASS THE MAP HERE
      return atomicSwapAndFlush(ss_anchor, finalSheetName, tempSheetName, repairMap);
    }, 60000);

    // 3. UNWRAP RESULT (Fixing the Silent Failure Bug)
    let swapSuccess = false;
    let swapError = null;

    if (transactionResult.success) {
      // Wrapper succeeded, check the inner function result
      swapSuccess = transactionResult.state.success;
      swapError = transactionResult.state.errorMessage;
    } else {
      // Wrapper failed (Timeout/Exception)
      swapSuccess = false;
      swapError = transactionResult.error;
    }

    // 4. CHECK LOGIC
    if (swapSuccess) {

      // *** POST-SWAP RESIZE (Your code was good here) ***
      try {
        ss_anchor = SpreadsheetApp.getActiveSpreadsheet();
        const finalSheet = ss_anchor.getSheetByName(finalSheetName);
        if (finalSheet) {
          const lastRow = finalSheet.getLastRow();
          const lastCol = finalSheet.getLastColumn();

          if (lastRow > 1) {
            // Re-define range to hug the new data exactly
            const range = finalSheet.getRange(2, 1, lastRow - 1, lastCol);
            ss_anchor.setNamedRange(MARKET_NAMED_RANGE, range);
            console.log(`[Finalizer] Resized Named Range: ${MARKET_NAMED_RANGE}`);
          }
        }
      } catch (nrError) {
        console.warn(`[Finalizer] Range Resize Warning: ${nrError.message}`);
      }
      // **************************************************

      _resetMarketDataJobState(null);
      console.log("SUCCESS: Finalization complete.");

    } else {
      // 5. FAILURE HANDLING
      console.warn(`[Finalizer] Swap Failed: ${swapError}`);

      // If sheet missing, fatal error
      if (swapError && swapError.includes("not found")) {
        _resetMarketDataJobState(new Error("Fatal: Temp sheet missing."));
        return;
      }

      // Otherwise retry
      // Ensure you have a trigger handler for this specific job if you want retries
      // scheduleOneTimeTrigger('finalizeMarketDataTrigger', 120000); 
    }

  }, funcName);
}

// --- WORKER FUNCTIONS ---
function runLootAndJournalSync() {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('MASTER_SYNC') : console);
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  try {
    runLootDeltaPhase(ss);
  } catch (e) { log.error('Loot Sync failed', e); }
  try { Ledger_Import_CorpJournal(ss, { division: 3, sinceDays: 30 }); }
  catch (e) { log.error('Corp Journal Import failed', e); }
}

function runContractSync() {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('MASTER_SYNC') : console);
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  try { runContractLedgerPhase(ss); }
  catch (e) { log.error('Contract Sync failed', e); }
}

function setupStaggeredTriggers() {
  console.log("Setting up 5-MIN TRIGGER...");
  const managedFunctions = [
    'fuzAPI.cacheRefres', 'triggerCacheWarmerWithRetry', 'updateMarketDataSheet',
    'finalizeMarketDataUpdate', 'cleanupOldSheet', 'masterOrchestrator',
    'cacheAllCorporateAssetsTrigger', 'runLootAndJournalSync', 'runContractSync',
    'runIndustrySync', 'runMaintenanceJobs'
  ];
  managedFunctions.forEach(funcName => deleteTriggersByName(funcName));

  try {
    ScriptApp.newTrigger('masterOrchestrator')
      .timeBased().everyMinutes(5).create();
    console.log('SUCCESS: Created 5-minute trigger for masterOrchestrator.');
  } catch (e) {
    console.error(`Failed to create triggers: ${e.message}.`);
  }
}

function runLootDeltaPhase(ss) {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('LOOT_PHASE') : console);
  try {
    log.info('Running _fetchProcessedLootData...');
    const lootData = _fetchProcessedLootData();
    if (lootData) {
      log.info('Executing loot delta calculation...');
      if (typeof _runLootDeltaImport === 'function') {
        _runLootDeltaImport(ss, lootData, null, null, false);
      } else {
        log.warn('_runLootDeltaImport function is missing.');
      }
    } else {
      log.warn('Loot Data fetch returned null.');
    }
  } catch (e) {
    log.error('Loot Delta Phase Failed', e);
    throw e;
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

function forceReleaseStuckScriptLock() {
  const lock = LockService.getScriptLock();
  try { lock.releaseLock(); console.log("Lock released."); } catch (e) { }
}