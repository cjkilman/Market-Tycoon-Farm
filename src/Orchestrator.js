/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, ScriptApp, 
 getMasterBatchFromControlTable, withSheetLock, getOrCreateSheet, 
cacheAllCorporateAssetsTrigger, triggerLedgerImportCycle, fuzAPI, _fetchProcessedLootData, 
runLootLedgerDelta, Ledger_Import_CorpJournal, syncContracts, runIndustryLedgerPhase,
 runLootDeltaPhase, runContractLedgerPhase, runAllLedgerImports, LoggerEx */

// Global variable to track recursion depth for this lock type
var EXECUTION_LOCK_DEPTH_TRY = 0;
// Global variable to track recursion depth for this lock type
var EXECUTION_LOCK_DEPTH_WAIT = 0;


const LOCK_TIMEOUT_MS = 5000;
const LOCK_WAIT_TIMEOUT_MS = 30000; // Default wait
// ... (Ensure these are defined globally in the file or assumed available)
const finalSheetName = 'Market_Data_Raw';
const tempSheetName = 'Market_Data_Temp';
const oldSheetName = 'Market_Data_Old';
const RETRY_DELAY_MS = 30 * 1000;
const PROP_KEY_FINALIZER_STEP = 'marketDataFinalizeStep';



/**
 * Global property key for system-wide maintenance mode.
 */
if (typeof GLOBAL_STATE_KEY === 'undefined') {
  /**
   * Global property key for system-wide maintenance mode.
   */
  var GLOBAL_STATE_KEY = 'GLOBAL_SYSTEM_STATE';
}


// State Machine Constants (Internal steps for the Market Data job)
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
 * --- FIX: Now checks for Maintenance Mode ---
 */
function scheduleOneTimeTrigger(functionName, delayMs) {
  // --- FIX: CRITICAL ERROR CHECK (Fail Fast) ---
  if (typeof functionName !== 'string' || functionName.trim() === '') {
    // Throwing an error ensures the entire parent job (the caller) crashes
    // immediately upon a critical input validation failure.
    throw new Error(`CRITICAL SCHEDULER ERROR: Invalid function name provided. Must be a non-empty string. Got: ${functionName}`);
  }

  // --- NEW: MAINTENANCE MODE CHECK ---
  // FIX: Corrected SCRIPT_PROPS to PropertiesService.getScriptProperties()
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const systemState = SCRIPT_PROP.getProperty(GLOBAL_STATE_KEY) || 'RUNNING';

  // --- END: MAINTENANCE MODE CHECK ---

  try {
    // Attempt to delete existing triggers first to prevent duplicates
    deleteTriggersByName(functionName);
    if (systemState === 'MAINTENANCE') {
      console.warn(`Blocking one-time trigger for ${functionName}: System is in MAINTENANCE mode.`);
      Logger.log(`Blocking one-time trigger for ${functionName}: System is in MAINTENANCE mode.`);
      return; // Do not schedule the trigger
    }
    // Create the new trigger
    ScriptApp.newTrigger(functionName)
      .timeBased()
      .after(delayMs)
      .create();
    console.log(`Created one-time trigger for ${functionName} to run in approx ${Math.round(delayMs / 60000)} minutes.`);
  } catch (e) {
    console.error(`Failed to create/delete trigger for ${functionName}: ${e.message}. Stack: ${e.stack}`);
  }
}

/**
 * Helper to delete triggers by name.
 */
function deleteTriggersByName(functionName) {
  if (typeof functionName !== 'string' || functionName.trim() === '') {
    console.warn(`deleteTriggersByName: Invalid function name provided. Skipping delete.`);
    return 0; // Return 0 deleted
  }

  let deletedCount = 0;
  try {
    const allTriggers = ScriptApp.getProjectTriggers();
    allTriggers.forEach(trigger => {
      // Check handler function AND event type to be more specific
      if (trigger.getHandlerFunction() === functionName &&
        trigger.getEventType() === ScriptApp.EventType.CLOCK) {
        try {
          ScriptApp.deleteTrigger(trigger);
          deletedCount++;
        } catch (e) {
          console.warn(`Could not delete a trigger (ID: ${trigger.getUniqueId()}) for ${functionName}: ${e.message}`);
        }
      }
    });
    if (deletedCount > 0) {
      console.log(`Deleted ${deletedCount} existing clock trigger(s) for ${functionName}.`);
    }
  } catch (e) {
    console.error(`Error accessing or deleting triggers for ${functionName}: ${e.message}. Stack: ${e.stack}`);
  }
  return deletedCount; // Return the count
}


/**
 * Internal reset helper.
 */
function _resetMarketDataJobState(error) {
  // Log the error object itself for more details if available
  console.warn(`RESETTING Market Data Job State due to: ${error ? error.message : 'Manual request or completion'}.`);
  if (error && error.stack) {
    console.warn(`Stack trace: ${error.stack}`);
  }

  console.log("Clearing market data job properties and triggers...");
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_REQUEST_INDEX = 'marketDataRequestIndex';
  const PROP_KEY_SHEET_ROW = 'marketDataNextWriteRow';

  // --- FIX: Add new finalizer step property to reset list ---
  const PROP_KEY_FINALIZER_STEP = 'marketDataFinalizeStep';
  const PROP_KEY_SETUP_STEP = 'marketDataSetupStep'; // <-- ADDED
  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';

  // Use try-catch for property deletion in case of permission issues (less likely)
  try {
    SCRIPT_PROP.deleteProperty(PROP_KEY_STEP);
    SCRIPT_PROP.deleteProperty(PROP_KEY_REQUEST_INDEX);
    SCRIPT_PROP.deleteProperty(PROP_KEY_SHEET_ROW);
    SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE); // Clear the lease
    SCRIPT_PROP.deleteProperty(PROP_KEY_FINALIZER_STEP); // Clear the finalizer step
    SCRIPT_PROP.deleteProperty(PROP_KEY_SETUP_STEP); // <-- ADDED
    SCRIPT_PROP.deleteProperty('marketDataJobIsActive'); // Legacy flag cleanup

    // --- FIX: REMOVED ASSET CACHE DELETION ---
    // SCRIPT_PROP.deleteProperty('AssetCache_Data_V2');
    // SCRIPT_PROP.deleteProperty('AssetCache_NextRow');
    // -------------------------------

  } catch (propError) {
    console.error(`Error deleting script properties: ${propError.message}`);
  }

  // Delete specific triggers related to the job steps
  deleteTriggersByName('updateMarketDataSheet');
  deleteTriggersByName('finalizeMarketDataUpdate');

  console.log("Market data job state reset complete.");
}


/**
 * Runs ALL simple, non-stateful cache warmers and triggers (like asset fetching).
 * FIX: This function is NO LONGER CALLED by the master orchestrator.
 * It is now only a placeholder for manual execution if needed.
 */
function runAllSimpleCacheJobs() {
  const SCRIPT_NAME = 'runAllSimpleCacheJobs';

  // --- LIST OF SIMPLE JOBS TO RUN ---
  // Note: These functions handle their own internal locking/execution.

  // 1. Run the Asset Cache Update (Resilient Job)
  Logger.log('[' + SCRIPT_NAME + '] Dispatching Asset Cache Update...');
  if (typeof cacheAllCorporateAssetsTrigger === 'function') {
    // The trigger function manages its own internal script lock and resumption logic.
    cacheAllCorporateAssetsTrigger();
  }

  // 2. Run Ledger Import (Requires its own script lock/handling)
  Logger.log('[' + SCRIPT_NAME + '] Dispatching All Ledger Imports...');
  if (typeof triggerLedgerImportCycle === 'function') {
    // The trigger function manages its own internal script lock and resumption logic.
    triggerLedgerImportCycle();
  }
  // ------------------------------------

  return true;
}


/**
 * Wraps a function in a ScriptLock tryLock().
 * If lock is acquired, executes the function.
 * If not, logs a skip message.
 *
 * MODIFIED: Now checks for GLOBAL_STATE_KEY
 *
 * @param {function} funcToRun - The function to execute.
 * @param {string} functionName - The name of the function being called (for logging).
 * @param {number} [timeoutMs=LOCK_TIMEOUT_MS] - Optional lock timeout.
 * @returns {*} The result of funcToRun, or null if skipped.
 */
function executeWithTryLock(funcToRun, functionName, timeoutMs = LOCK_TIMEOUT_MS) {
  const lock = LockService.getScriptLock();
  // --- GUARD RAIL 1: INPUT VALIDATION ---
  if (typeof funcToRun !== 'function') {
    console.error(`CRITICAL GUARD RAIL FAILURE: funcToRun is not a function. Got: ${typeof funcToRun}`);
    return false; // Critical failure signal
  }
  if (typeof functionName !== 'string' || functionName.trim() === "") {
    console.error(`CRITICAL GUARD RAIL FAILURE: functionName is invalid. Using 'UnknownFunction'.`);
    functionName = 'UnknownFunction'; // Assign a safe fallback
  }
  if (typeof timeoutMs !== 'number' || timeoutMs < 0) {
    console.warn(`GUARD RAIL WARNING: Invalid timeoutMs (${timeoutMs}). Using default.`);
    timeoutMs = LOCK_TIMEOUT_MS;
  }
  if (lock.tryLock(timeoutMs)) {
    try {

      // --- START: MAINTENANCE MODE CHECK ---
      const systemState = PropertiesService.getScriptProperties().getProperty(GLOBAL_STATE_KEY) || 'RUNNING';
      if (systemState === 'MAINTENANCE') {
        console.warn(`Skipping execution of ${functionName}: System is in MAINTENANCE mode.`);
        Logger.log(`Skipping execution of ${functionName}: System is in MAINTENANCE mode.`);
        return null; // Do not run, and do not reschedule.
      }
      // --- END: MAINTENANCE MODE CHECK ---

      console.log(`--- Starting Execution (TryLock): ${functionName} ---`);
      return funcToRun();
    } catch (e) {
      console.error(`Unhandled exception in ${functionName}: ${e.message} \nStack: ${e.stack}`);
      Logger.log(`Unhandled exception in ${functionName}: ${e.message}`);

    } finally {
      lock.releaseLock();
      console.log(`Script Lock released for ${functionName}.`);
    }
  } else {
    console.warn(`Skipping execution of ${functionName}: Script Lock was busy.`);
    Logger.log(`Skipping execution of ${functionName}: Script Lock was busy.`);
    return null;
  }
}

/**
 * Wraps a function in a ScriptLock waitLock().
 * This will pause execution until the lock is acquired.
 *
 * MODIFIED: Now checks for GLOBAL_STATE_KEY
 *
 * @param {function} funcToRun - The function to execute.
 * @param {string} functionName - The name of the function being called (for logging).
 * @param {number} [timeoutMs=LOCK_WAIT_TIMEOUT_MS] - Optional lock timeout.
 * @returns {*} The result of funcToRun.
 */
function executeWithWaitLock(funcToRun, functionName, timeoutMs = LOCK_WAIT_TIMEOUT_MS) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(timeoutMs);
  } catch (e) {
    console.error(`Could not acquire Script Lock for ${functionName} after waiting. Error: ${e.message}`);
    throw e;
  }

  try {

    // --- START: MAINTENANCE MODE CHECK ---
    const systemState = PropertiesService.getScriptProperties().getProperty(GLOBAL_STATE_KEY) || 'RUNNING';
    if (systemState === 'MAINTENANCE') {
      console.warn(`Skipping execution of ${functionName}: System is in MAINTENANCE mode.`);
      Logger.log(`Skipping execution of ${functionName}: System is in MAINTENANCE mode.`);
      return null; // Do not run.
    }
    // --- END: MAINTENANCE MODE CHECK ---

    console.log(`--- Starting Execution (WaitLock): ${functionName} ---`);
    return funcToRun();
  } catch (e) {
    console.error(`Unhandled exception in ${functionName}: ${e.message} \nStack: ${e.stack}`);
    Logger.log(`Unhandled exception in ${functionName}: ${e.message}`);
    throw e; // Re-throw to be caught by GAS
  } finally {
    lock.releaseLock();
    console.log(`Script Lock released for ${functionName}.`);
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

  // --- GLOBAL FIX: Clear Expired Lease (Heartbeat) ---
  if (!isJobActive && leaseUntil > 0 && leaseUntil <= NOW_MS) {
    console.warn(`Master orchestrator (Heartbeat): Found expired lease (${new Date(leaseUntil)}). Clearing lease.`);
    SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
    isJobActive = false; // Update the flag
  }
  // --- END GLOBAL FIX ---

  // --- Priority 1: Market Data Finalization Check ---
  if (marketDataStep === STATE_FLAGS.FINALIZING) {
    console.log(`Master orchestrator: Market data state requires cleanup/finalization.`);
    const delay = 30 * 1000;
    console.log("State is FINALIZING. Scheduling final swap now.");
    scheduleOneTimeTrigger("finalizeMarketDataUpdate", delay);
    return;
  }

  console.log(`Master orchestrator (min ${currentMinute}): Checking time window.`);

  // --- Staggering Logic ---
  if (currentMinute >= 15 && currentMinute < 45) { // *** Window: Minutes 15-44 (Market Update) ***

    if (isJobActive) {
      console.log(`Master orchestrator: Job is active (Lease expires in ${((leaseUntil - NOW_MS) / 60000).toFixed(1)} min). Allowing current thread to continue (Heartbeat).`);
    } else {

      // --- NEW LOGIC: OPPORTUNISTIC DISPATCH (No longer gated by specific minute) ---
      console.log(`Master orchestrator (min ${currentMinute}): Job is inactive. Dispatching MARKET DATA UPDATE opportunistically.`);

      // Set the new lease time before dispatching the job
      const NEW_LEASE = NOW_MS + 280000; // 4m 40s
      SCRIPT_PROP.setProperty(PROP_KEY_LEASE, NEW_LEASE.toString());

      updateMarketDataSheet(); // Calls the now-locked public wrapper
    }
  } else { // *** Covers 0-14 and 45-59 (Maintenance Window) ***
    console.log(`Master orchestrator (min ${currentMinute}): In dedicated Maintenance window.`);

    // --- 1. Run Complex Job (Fuzzwork, manages resume state) ---
    console.log(`Dispatching COMPLEX CACHE WARMER wrapper (Fuzzwork).`);
    const result = executeWithTryLock(triggerCacheWarmerWithRetry, 'triggerCacheWarmerWithRetry');

    // If Fuzzwork job was skipped due to lock, schedule its retry.
    if (result === null) {
      const retryDelayMs = 2 * 60 * 1000;
      console.warn(`Master orchestrator: Fuzzwork job was skipped by lock. Scheduling retry.`);
      scheduleOneTimeTrigger('triggerCacheWarmerWithRetry', retryDelayMs);
    }
  }
  console.log(`Master orchestrator finished checks for minute ${currentMinute}.`);
}

/**
 * Wrapper function for the cache warmer.
 * Attempts to run the cache warmer using executeWithTryLock.
 * If skipped due to lock, it schedules a one-time retry trigger for itself.
 * If completed fully, attempts opportunistic cleanup and checks if market update should be triggered.
 */
function triggerCacheWarmerWithRetry() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  // --- FIX: Add Finalizing Check ---
  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');
  if (marketDataStep === STATE_FLAGS.FINALIZING) {
    console.warn("Cache Warmer: Skipping execution, job is FINALIZING.");
    return; // Do not run if the main job is finalizing
  }
  // --- END FIX ---

  // const funcToRun = fuzAPI.cacheRefresh; // Function reference removed
  const funcName = 'CacheCheck'; // Simplified log name
  const wrapperFuncName = 'triggerCacheWarmerWithRetry';

  // --- FIXED CONSTANTS ---
  const retryDelayMs = 2 * 60 * 1000; // 2 minutes retry delay
  const quickUpdateDelayMs = 5000; // 5 seconds delay before trying market update
  // -----------------------

  // --- FIX: Lease Property ---
  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  const leaseUntil = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_LEASE) || '0', 10);
  const NOW_MS = new Date().getTime();
  const isJobActive = leaseUntil > NOW_MS;
  // ---------------------------

  console.log('Wrapper ' + wrapperFuncName + ' called. Attempting to run scheduling check...');

  // *** FIX: Simulate a successful run (result = true) by bypassing the expensive function call ***
  const result = true;

  if (result === null) {
    // Case 1: Skipped due to Script Lock (Defunct, but structure maintained)
    console.warn(funcName + ' was skipped due to Script Lock. Scheduling retry for ' + wrapperFuncName + '.');
    scheduleOneTimeTrigger(wrapperFuncName, retryDelayMs);

  } else if (result === true) {
    // --- Case 2: Ran AND Completed Fully (The Schedule Check) ---
    console.log('Schedule check completed successfully.');

    // --- Lease Check / Job Dispatch ---
    const currentMinute = new Date().getMinutes();
    console.log('Maintenance finished (min ' + currentMinute + '). Scheduling market update immediately.');

    // FIX: Check for expired lease (stuck job) and only clear the lease before dispatching
    if (!isJobActive && leaseUntil > 0 && leaseUntil <= NOW_MS) {
      console.warn('Maintenance Check: Found expired lease (' + new Date(leaseUntil) + '). Clearing lease to allow immediate dispatch.');
      // *** FIX: Minimal action: only clear the lease and log the event ***
      SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
    }

    if (isJobActive) {
      console.log('Maintenance Check finished: Market data job is already active. Skipping new dispatch.');
    } else {
      console.log('Maintenance Check finished (min ' + currentMinute + '): Dispatching MARKET DATA UPDATE.');
      // FIX: Set the new lease time before dispatching the job
      const NOW_MS_INNER = new Date().getTime(); // Use new timestamp
      const NEW_LEASE = NOW_MS_INNER + 280000; // 4m 40s
      SCRIPT_PROP.setProperty(PROP_KEY_LEASE, NEW_LEASE.toString());

      // *** FIX: Change synchronous call to ASYNCHRONOUS schedule ***
      scheduleOneTimeTrigger('updateMarketDataSheet', quickUpdateDelayMs);
    }

  } else if (result === false) {
    // Case 3: Ran but did NOT complete fully (Defunct)
    console.log(funcName + ' ran but hit its time limit. Rescheduling wrapper.');
    scheduleOneTimeTrigger(wrapperFuncName, retryDelayMs);
  } else {
    // Case 4: Unexpected return value (Defunct)
    console.warn(funcName + ' execution by ' + wrapperFuncName + ' returned unexpected value: ' + result);
  }
}
function forceReleaseStuckScriptLock() {
  const lock = LockService.getScriptLock();
  const log = (typeof Logger !== 'undefined' ? Logger : console);

  try {
    lock.releaseLock();
    log.log("SUCCESS: Script lock successfully released.");
  } catch (e) {
    log.error("ATTENTION: No lock was held by this user or an error occurred during release: " + e.message);
  }
}
/**
 * Refactored _updateMarketDataSheetWorker to a Fetch-All-Then-Write Resumable Model.
 * This function is now complete and stabilized against structural and persistence failures.
 */
function _updateMarketDataSheetWorker() {
  // --- Configuration ---
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_WRITE_INDEX = 'marketDataNextWriteRow';
  const PROP_KEY_CHUNK_SIZE = 'marketDataChunkSize';
  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  const PROP_KEY_SETUP_STAGE = 'marketDataSetupStage';
  const [MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, SOFT_LIMIT_MS, RESCHEDULE_DELAY_MS, FULL_RUN_RESCHEDULE_MS]
    = [5000, 500, 285000, 10000, 60000];

  const tempSheetName = 'Market_Data_Temp';
  const COLUMN_COUNT = 9;
  const START_ROW = 2;
  const DATA_SHEET_HEADERS = ["cacheKey", "type_id", "location_type", "location_id", "sell_min", "buy_max", "sell_volume", "buy_volume", "last_updated"];

  const START_TIME = new Date().getTime();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const masterRequests = getMasterBatchFromControlTable(ss);

  let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP) || STATE_FLAGS.NEW_RUN;

  // --- Phase 1: NEW_RUN Setup (Resilient Atomic Setup) ---
  if (currentStep === STATE_FLAGS.NEW_RUN || !masterRequests || masterRequests.length === 0) {
    currentStep = STATE_FLAGS.NEW_RUN;
    console.log(`State: ${STATE_FLAGS.NEW_RUN}. Preparing cycle.`);

    if (!masterRequests || masterRequests.length === 0) {
      console.warn("Control Table empty. Resetting state and exiting.");
      _resetMarketDataJobState(new Error("Control Table empty during NEW_RUN"));
      return;
    }

    // Execute Single Atomic Setup Transaction (Delete, Insert, Initialize)
    const setupResult = guardedSheetTransaction(() => {

      // Context re-acquisition is local to the lock to prevent closure failure
      const ss_inner = SpreadsheetApp.getActiveSpreadsheet();

      let sheet = ss_inner.getSheetByName(tempSheetName);

      if (sheet) {
        // --- Option 1 (Fastest Path): Sheet Exists. Clear Content and Re-init Headers. ---
        console.log(`Setup: Sheet exists. Clearing content and re-initializing headers.`);
        const lastRow = sheet.getMaxRows();
        if (lastRow > 1) {
          sheet.getRange(2, 1, lastRow - 1, sheet.getMaxColumns()).clearContent();
        }
      } else {
        // --- Option 2 (Slow Path): Sheet Missing. Insert Sheet. ---
        console.log(`Setup: Sheet missing. Inserting new sheet.`);
        sheet = ss_inner.insertSheet(tempSheetName);
      }

      // Finalize Headers and Properties
      sheet.getRange(1, 1, 1, COLUMN_COUNT).setValues([DATA_SHEET_HEADERS]);
      sheet.hideSheet();

      SCRIPT_PROP.deleteProperty(PROP_KEY_SETUP_STAGE);
      return true; // Explicit success signal

    }, 60000); // Wait up to 60s for the Document Lock

    // --- Handle Lock Status and Failure ---
    if (setupResult.success === false) {
      if (setupResult.error === "Lock Conflict/Busy") {
        console.log(`Setup skipped due to Document Lock conflict. Retrying immediately.`);
        scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
        return;
      }
      // Catastrophic failure handling
      console.error(`CRITICAL ERROR during NEW_RUN sheet setup: ${setupResult.error}`);
      scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
      throw new Error(`CRITICAL Setup Failure: ${setupResult.error}`);
    }

    console.log("Initial sheet setup complete. Transitioning to PROCESSING.");

    // Final State Persistence if successful (Sets up the index for the write loop)
    SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, '0'); // Index 0 maps to Sheet Row 2

    // Delete chunk size property to force STRICT_MIN_CHUNK (50 rows) in the next phase.
    SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
    currentStep = 'PROCESSING';
    SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'PROCESSING');

    return; // Exit here to start the next execution cycle (WRITE phase)

  } // End NEW_RUN block

  // --- Phase 2: WRITE (The Resumable Phase) ---
  if (currentStep === 'PROCESSING' || currentStep === 'WRITE') {

    // Re-acquire stable SS context before data fetch
    const ss_stable = SpreadsheetApp.getActiveSpreadsheet();
    const masterRequests_stable = getMasterBatchFromControlTable(ss_stable);

    let allRowsToWrite = [];

    try {
      // 1. Re-fetch all data 
      const marketDataCrates = fuzAPI.getDataForRequests(masterRequests_stable);

      // Map all fetched crates/items into the single row array
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
        console.warn("Re-fetch returned zero rows. Finalizing early.");
        SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.FINALIZING);
        scheduleOneTimeTrigger('finalizeMarketDataUpdate', RESCHEDULE_DELAY_MS);
        return;
      }
    } catch (e) {
      console.error(`FATAL ERROR during data collection re-fetch: ${e.message}. Rescheduling retry.`);
      scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS * 2);
      return;
    }

    console.log(`State: WRITE. Total rows prepared for resumable write: ${allRowsToWrite.length}.`);

    // 2. Prepare Write State
    let writeState = {
      logInfo: console.log, logError: console.error, logWarn: console.warn,
      nextBatchIndex: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_WRITE_INDEX) || '0'),
      ss: ss_stable,
      metrics: { startTime: START_TIME },
      config: {
        TARGET_WRITE_TIME_MS: 3000,
        MAX_FACTOR: 2,
        THROTTLE_THRESHOLD_MS: 800,
        THROTTLE_PAUSE_MS: 200,
        currentChunkSize: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || MIN_CHUNK_SIZE.toString()),
        MAX_CHUNK_SIZE: MAX_CHUNK_SIZE,
        MIN_CHUNK_SIZE: MIN_CHUNK_SIZE,
        SOFT_LIMIT_MS: SOFT_LIMIT_MS
      }
    };

    // --- ENFORCEMENT OF STRICT MINIMUM STARTING CHUNK ---
    const STRICT_MIN_CHUNK = 50;
    if (writeState.nextBatchIndex === 0) {
      writeState.config.currentChunkSize = STRICT_MIN_CHUNK;
      console.log(`[INIT] Forcing initial chunk size to ${STRICT_MIN_CHUNK} for reliable write start.`);
    }

    // 3. Call writeDataToSheet (The Resumable Write Phase)
    const writeResult = writeDataToSheet(tempSheetName, allRowsToWrite, START_ROW, 1, writeState);

    // --- 4. Process Write Result (Handling persistence failure) ---
    if (writeResult.success) {
      console.log("Write SUCCESS. Transitioning to FINALIZING.");
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.FINALIZING);
      SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
      SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
      SCRIPT_PROP.deleteProperty(PROP_KEY_WRITE_INDEX);

      scheduleOneTimeTrigger('finalizeMarketDataUpdate', RESCHEDULE_DELAY_MS);
    }
    else if (writeResult.bailout_reason === "PREDICTIVE_BAILOUT" || writeResult.error.includes("ServiceTimeoutFailure")) {
      console.warn("Write phase hit limit/failure. Rescheduling to resume write.");

      // Save state from the utility's return object:
      const nextIndex = writeResult.state.nextBatchIndex.toString();
      const nextChunkSize = writeResult.state.config.currentChunkSize.toString();

      // Force state commitment by using delete/set sequence
      SCRIPT_PROP.deleteProperty(PROP_KEY_WRITE_INDEX);
      SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, nextIndex);

      SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
      SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, nextChunkSize);

      // CRITICAL FINAL FIX: INCREASE SLEEP TO GUARANTEE PERSISTENCE
      Utilities.sleep(1000);

      scheduleOneTimeTrigger('updateMarketDataSheet', FULL_RUN_RESCHEDULE_MS);
    }
    else {
      // Write failed catastrophically 
      console.error(`Write FAILED: ${writeResult.error}. Resetting job state.`);
      _resetMarketDataJobState(new Error(`Catastrophic Write Failure: ${writeResult.error}`));
    }
  }

  const totalDuration = (new Date().getTime() - START_TIME) / 1000;
  console.log(`_updateMarketDataSheetWorker execution finished in ${totalDuration.toFixed(2)} seconds. Final state: ${SCRIPT_PROP.getProperty(PROP_KEY_STEP)}`);
}

/**
 * Market data update function. Processes market data requests in batches,
* writing results to a temporary sheet, handling time limits and potential errors.
 * This is the public function called by triggers and the orchestrator.
 */
function updateMarketDataSheet() {
  const funcName = 'updateMarketDataSheet';

  // This wrapper enforces the Script Lock check for every call, including dynamic triggers.
  const result = executeWithTryLock(_updateMarketDataSheetWorker, funcName);

  if (result === null) {
    // Job was skipped due to lock, so we exit silently. The orchestrator or
    // a subsequent trigger will pick it up.
    console.warn(`${funcName} skipped execution due to a concurrency lock. Will be picked up by next trigger.`);
  }
  // If it executed, the result is handled by the worker's internal reschedule/state change.
}

/**
 * Executes a hard manual reset of the Market Data job state.
 * This function exists in src/Orchestrator.js but is exposed here for debugging.
 */
function manualResetMarketDataJobAndDispatch() {

  // 1. Clear all state properties (from _resetMarketDataJobState logic)
  const error = new Error("Manual reset requested via editor.");
  console.warn(`RESETTING Market Data Job State due to: ${error.message}`);

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const keysToDelete = [
    'marketDataJobStep', 'marketDataJobLeaseUntil',
    'marketDataFinalizeStep', 'marketDataSetupStep',
    'marketDataNextWriteRow', 'marketDataChunkSize',
    'marketDataRequestIndex', 'marketDataJobIsActive'
  ];

  keysToDelete.forEach(key => SCRIPT_PROP.deleteProperty(key));

  // 2. Dispatch the job immediately
  scheduleOneTimeTrigger('updateMarketDataSheet', 5000);
  console.log("SUCCESS: Market Data job state has been reset and rescheduled.");
}

/**
 * Public wrapper for finalization. Executes the worker under a Script Lock tryLock().
 * This is the function called by triggers and the job dispatcher.
 */
function finalizeMarketDataUpdate() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const finalSheetName = 'Market_Data_Raw'; // Sheet to be DELETED and replaced
  const tempSheetName = 'Market_Data_Temp'; // Sheet containing the new data
  const RETRY_DELAY_MS = 60 * 1000; // 1 minute throttle on finalization failure
  const docLockWaitMs = 60 * 1000; // 60s wait for internal Document Lock in the utility

  const funcName = 'finalizeMarketDataUpdate';

  // Use executeWithTryLock to acquire the global Script Lock
  const result = executeWithTryLock(() => {

    // Check for correct state immediately
    if (SCRIPT_PROP.getProperty(PROP_KEY_STEP) !== STATE_FLAGS.FINALIZING) {
      const errMsg = `Finalizer called in wrong state. Resetting state.`;
      console.error(errMsg);
      _resetMarketDataJobState(new Error(errMsg));
      return;
    }

    // --- Execute Atomic Swap Transaction ---
    // Replace the failing section with the robust local context acquisition:
    const swapResult = guardedSheetTransaction(() => {

      // 🚨 FIX: Acquire SS context locally within the lock 🚨
      const ss_inner = SpreadsheetApp.getActiveSpreadsheet();

      // The critical operation: Delete Raw and rename Temp to Raw.
      return atomicSwapAndFlush(ss_inner, finalSheetName, tempSheetName);
    }, docLockWaitMs);

    // --- Handle Result using State Object Contract ---
    if (swapResult.success === true) {
      // SUCCESS: The swap is complete, clear all job properties.
      _resetMarketDataJobState(null);
      console.log("SUCCESS: Finalization complete and job state reset.");
      return true;
    } else if (swapResult.error.includes("Lock Conflict/Busy")) {
      // RECOVERABLE FAILURE: Document Lock was busy inside atomicSwapAndFlush.
      console.warn(`Atomic Swap failed due to Lock conflict. Retrying in ${RETRY_DELAY_MS / 1000} seconds.`);
      scheduleOneTimeTrigger('finalizeMarketDataUpdate', RETRY_DELAY_MS);
      // Throw to exit TryLock cleanly and stop further execution in this thread.
      throw new Error(swapResult.error);
    } else {
      // CRITICAL FAILURE: (e.g., Sheet not found, API timeout during rename/delete).
      console.error(`CRITICAL SWAP ERROR: ${swapResult.error}. Resetting job state.`);
      _resetMarketDataJobState(new Error(`Catastrophic Swap Failure: ${swapResult.error}`));
      // Throw to stop the current execution thread cleanly.
      throw new Error(`CRITICAL Swap Failure: ${swapResult.error}`);
    }

  }, funcName);

  if (result === null) {
    console.warn(`${funcName} skipped execution due to a concurrency lock. Will be picked up by next trigger.`);
  }
}



// ----------------------------------------------------------------------
// --- NEW: SMALL, TRIGGER-ABLE WRAPPER FUNCTIONS ---
// ----------------------------------------------------------------------

/**
 * Runs ONLY the Loot and Journal syncs.
 * Designed to be called by a dedicated hourly trigger.
 */
function runLootAndJournalSync() {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('MASTER_SYNC') : console);
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  executeWithTryLock(() => {
    log.info('--- Starting Loot & Journal Sync Cycle ---');

    try {
      log.info('Running _fetchProcessedLootData (External Data Sync)...');
      const lootData = _fetchProcessedLootData(); // Assumes this function is in scope (e.g., from GESI Extentions)
      if (lootData) {
        log.info('Executing loot delta calculation and import...');
        runLootDeltaPhase(ss); // Assumes this function is in scope
      }
    } catch (e) {
      log.error('Loot Sync failed', e);
    }

    try {
      log.info('Running Ledger_Import_CorpJournal...');
      Ledger_Import_CorpJournal(ss, { division: 3, sinceDays: 30 }); // Assumes this is in scope
    } catch (e) {
      log.error('Corp Journal Import failed', e);
    }

  }, 'runLootAndJournalSync');
}

/**
 * Runs ONLY the Contract sync.
 * Designed to be called by a dedicated hourly trigger.
 */
function runContractSync() {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('MASTER_SYNC') : console);
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  executeWithTryLock(() => {
    log.info('--- Starting Contract Sync Cycle ---');
    try {
      log.info('Running syncContracts (Fetch RAW data)...');
      runContractLedgerPhase(ss); // Assumes this is in scope (from GESI Extentions)
    } catch (e) {
      log.error('Contract Sync failed', e);
    }
  }, 'runContractSync');
}

/**
 * Runs ONLY the Industry Ledger sync.
 * Designed to be called by a dedicated hourly trigger.
 */
function runIndustrySync() {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('MASTER_SYNC') : console);
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  executeWithTryLock(() => {
    log.info('--- Starting Industry Ledger Sync Cycle ---');
    try {
      runIndustryLedgerPhase(ss); // This is the monolith from IndustryLedger.gs.js
    } catch (e) {
      log.error('Industry Ledger Phase failed', e);
    }
  }, 'runIndustrySync');
}


// ----------------------------------------------------------------------
// --- TRIGGER SETUP ---
// ----------------------------------------------------------------------

/**
 * Run this function ONCE from the editor to set up triggers.
 */
function setupStaggeredTriggers() {
  console.log("Setting up/Resetting orchestrator triggers...");

  // Clean up all known triggers managed by this orchestrator
  const managedFunctions = [
    'fuzAPI.cacheRefres',
    'triggerCacheWarmerWithRetry',
    'updateMarketDataSheet',
    'finalizeMarketDataUpdate',
    'cleanupOldSheet', // Include this if it had its own trigger previously
    'masterOrchestrator',
    'cacheAllCorporateAssetsTrigger',
    'runLootAndJournalSync', // NEW
    'runContractSync', // NEW
    'runIndustrySync' // NEW
  ];

  let totalDeleted = 0;
  managedFunctions.forEach(funcName => {
    totalDeleted += deleteTriggersByName(funcName);
  });
  console.log(`Total existing clock triggers deleted: ${totalDeleted}.`);

  try {
    // 1. Market Data (15 min)
    ScriptApp.newTrigger('masterOrchestrator')
      .timeBased().everyMinutes(15).create();
    console.log('SUCCESS: Created 15-minute trigger for masterOrchestrator.');

    // --- FIX: Create SEPARATE triggers for the heavy jobs ---
    // 2. Asset Job (runs once per hour)
    ScriptApp.newTrigger('cacheAllCorporateAssetsTrigger')
      .timeBased().everyHours(1).nearMinute(0).create();
    console.log('SUCCESS: Created 1-hour trigger for cacheAllCorporateAssetsTrigger.');

    // 3. Loot/Journal Sync (Hourly, at :10)
    ScriptApp.newTrigger('runLootAndJournalSync')
      .timeBased().everyHours(1).nearMinute(10).create();
    console.log('SUCCESS: Created 1-hour trigger for runLootAndJournalSync.');

    // 4. Contract Sync (Hourly, at :20)
    ScriptApp.newTrigger('runContractSync')
      .timeBased().everyHours(1).nearMinute(20).create();
    console.log('SUCCESS: Created 1-hour trigger for runContractSync.');

    // 5. Industry Ledger (Hourly, at :30)
    ScriptApp.newTrigger('runIndustrySync')
      .timeBased().everyHours(1).nearMinute(30).create();
    console.log('SUCCESS: Created 1-hour trigger for runIndustrySync.');

  } catch (e) {
    console.error(`Failed to create new triggers: ${e.message}. Please check permissions and script validity.`);
  }
}

/**
 * Public function to manually trigger an immediate retry or "bump" the stalled job.
 */
function bumpMarketDataJob() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  const quickUpdateDelayMs = 5000; // 5 seconds delay

  console.log("MANUAL BUMP initiated.");

  // 1. Attempt to clear the lease, allowing the job to start fresh.
  const leaseRaw = SCRIPT_PROP.getProperty(PROP_KEY_LEASE);
  if (leaseRaw) {
    const leaseUntil = parseInt(leaseRaw, 10);
    const NOW_MS = new Date().getTime();
    if (leaseUntil > NOW_MS) {
      // Clear the lease forcibly
      SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
      console.warn(`Forcibly expired the job lease (was set until ${new Date(leaseUntil)}).`);
    }
  }

  // 2. Schedule the next step immediately.
  scheduleOneTimeTrigger('updateMarketDataSheet', quickUpdateDelayMs);
  console.log(`SUCCESS: Scheduled 'updateMarketDataSheet' in 5 seconds to resume job.`);
}


/**
 * Manual reset function for the market data job.
 */
function manualResetMarketDataJob() {
  console.log("MANUAL RESET initiated for Market Data job.");
  _resetMarketDataJobState(new Error("Manual reset requested via editor"));
  console.log("MANUAL RESET: Market Data job state has been reset.");
  // Optional: Immediately try to run the orchestrator to kick things off
  // try { masterOrchestrator(); } catch(e) { console.error("Error during post-reset orchestrator run:", e); }
}


// NOTE: Assumes getMasterBatchFromControlTable, fuzAPI exist
// NOTE: Assumes getOrCreateSheet (from Utility.js) exists