// Global variable to track recursion depth for this lock type
var EXECUTION_LOCK_DEPTH_TRY = 0;
// Global variable to track recursion depth for this lock type
var EXECUTION_LOCK_DEPTH_WAIT = 0;


const LOCK_TIMEOUT_MS = 5000;
const LOCK_WAIT_TIMEOUT_MS = 30000; // Default wait

/**
 * Global property key for system-wide maintenance mode.
 */
if (typeof GLOBAL_STATE_KEY === 'undefined') {
  /**
   * Global property key for system-wide maintenance mode.
   */
  var  GLOBAL_STATE_KEY = 'GLOBAL_SYSTEM_STATE';
}


// State Machine Constants (Internal steps for the Market Data job)
const STATE_FLAGS = {
  NEW_RUN: 'NEW_RUN',
  PROCESSING: 'PROCESSING',
  FINALIZING: 'FINALIZING'
};

/**
 * Helper to create a new one-time "retry" trigger.
 */
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
  // SCRIPT_PROPS must be globally defined
  const systemState = SCRIPT_PROPS.getProperty(GLOBAL_STATE_KEY) || 'RUNNING';

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
  } catch (propError) {
    console.error(`Error deleting script properties: ${propError.message}`);
  }

  // Delete specific triggers related to the job steps
  deleteTriggersByName('updateMarketDataSheet');
  deleteTriggersByName('finalizeMarketDataUpdate');

  console.log("Market data job state reset complete.");
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
      throw e; // Re-throw to be caught by GAS
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

/**
 * This is the single "master" function you will set on a trigger (every 15 min).
 * It runs jobs based on time windows, prioritizing finalization.
 * Includes cooldown check for cache warmer.
 */
function masterOrchestrator() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');
  const currentMinute = new Date().getMinutes();
  const NOW_MS = new Date().getTime(); // Consistent timestamp for checks

  // --- FIX: Switch from boolean flag to lease timestamp ---
  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  const leaseUntil = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_LEASE) || '0', 10);
  const isJobActive = leaseUntil > NOW_MS;
  // --------------------------------------------------------

  // --- Priority 1: Market Data Finalization Check (MUST be run first) ---
  if (marketDataStep === STATE_FLAGS.FINALIZING) {
    console.log(`Master orchestrator: Market data state requires cleanup/finalization.`);
    const delay = 30 * 1000;
    // Always schedule finalize; it handles cleanup internally if needed.
    console.log("State is FINALIZING. Scheduling final swap now.");
    scheduleOneTimeTrigger("finalizeMarketDataUpdate", delay);
    return; // Prioritize finalization
  }

  console.log(`Master orchestrator (min ${currentMinute}): Checking time window.`);

  // --- Staggering Logic ---
  if (currentMinute >= 15 && currentMinute < 45) { // *** Window: Minutes 15-44 (Market Update) ***

    if (isJobActive) {
      console.log(`Master orchestrator: Job is active (Lease expires in ${((leaseUntil - NOW_MS) / 60000).toFixed(1)} min). Skipping new dispatch.`);
    } else {
      // FIX: Check for expired lease (stuck job) and only clear the lease before dispatching
      if (leaseUntil > 0 && leaseUntil <= NOW_MS) {
        console.warn(`Master orchestrator: Lease expired (${new Date(leaseUntil)}). Clearing lease and re-dispatching.`);
        // *** FIX: Minimal action: only clear the lease and log the event ***
        SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
      }

      console.log(`Master orchestrator (min ${currentMinute}): Dispatching MARKET DATA UPDATE.`);
      // FIX: Set the new lease time before dispatching the job
      const NEW_LEASE = NOW_MS + 280000; // 4m 40s
      SCRIPT_PROP.setProperty(PROP_KEY_LEASE, NEW_LEASE.toString());

      updateMarketDataSheet(); // Calls the now-locked public wrapper
    }
  } else { // *** Covers 0-14 and 45-59 (Cache Warmer) ***
    console.log(`Master orchestrator (min ${currentMinute}): In cache warmer window.`);
    console.log(`Dispatching CACHE WARMER wrapper.`);

    // --- FIX: Check result of TryLock and schedule retry if skipped ---
    const result = executeWithTryLock(triggerCacheWarmerWithRetry, 'triggerCacheWarmerWithRetry');

    if (result === null) {
      const retryDelayMs = 2 * 60 * 1000; // 2 min delay
      console.warn(`Master orchestrator: Cache warmer dispatch was skipped by lock. Scheduling retry.`);
      scheduleOneTimeTrigger('triggerCacheWarmerWithRetry', retryDelayMs);
    }
    // --- END FIX ---
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

  const funcToRun = fuzzworkCacheRefresh_TimeGated;
  const funcName = 'fuzzworkCacheRefresh_TimeGated';
  const wrapperFuncName = 'triggerCacheWarmerWithRetry';

  // --- FIXED CONSTANTS ---
  const FULL_RUN_RESCHEDULE_MS = 285000; // 4m 45s - Predictive reschedule
  const retryDelayMs = 2 * 60 * 1000; // 2 minutes retry delay
  const quickUpdateDelayMs = 5000; // 5 seconds delay before trying market update
  // -----------------------

  // --- FIX: Lease Property ---
  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  const leaseUntil = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_LEASE) || '0', 10);
  const NOW_MS = new Date().getTime();
  const isJobActive = leaseUntil > NOW_MS;
  // ---------------------------

  // --- FIX: Log only the function name using .name property ---
  console.log(`Wrapper ${wrapperFuncName} called. Attempting to run function ${funcToRun.name} using executeWithTryLock...`);
  // --- END FIX ---

  const result = executeWithTryLock(funcToRun, funcName); // result is true (full run), false (incomplete), or null (skipped)

  if (result === null) {
    // --- Case 1: Skipped due to Script Lock ---
    // This logic is now handled by the masterOrchestrator, but kept as a redundant safety net.
    console.warn(`${funcName} was skipped due to Script Lock. Scheduling retry for ${wrapperFuncName}.`);
    scheduleOneTimeTrigger(wrapperFuncName, retryDelayMs);

  } else if (result === true) {
    // --- Case 2: Ran AND Completed Fully ---
    console.log(`${funcName} completed a full run successfully.`);

    // *** REMOVED OPPORTUNISTIC CLEANUP BLOCK ***

    // --- Lease Check / Job Dispatch ---
    const currentMinute = new Date().getMinutes();
    console.log(`Cache warmer finished (min ${currentMinute}). Scheduling market update immediately.`);

    // FIX: Check for expired lease (stuck job) and only clear the lease before dispatching
    if (!isJobActive && leaseUntil > 0 && leaseUntil <= NOW_MS) {
      console.warn(`Cache Warmer: Found expired lease (${new Date(leaseUntil)}). Clearing lease to allow immediate dispatch.`);
      // *** FIX: Minimal action: only clear the lease and log the event ***
      SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
    }

    if (isJobActive) {
      console.log(`Cache warmer finished: Market data job is already active. Skipping new dispatch.`);
    } else {
      console.log(`Cache warmer finished (min ${currentMinute}): Dispatching MARKET DATA UPDATE.`);
      // FIX: Set the new lease time before dispatching the job
      const NOW_MS = new Date().getTime();
      const NEW_LEASE = NOW_MS + 280000; // 4m 40s
      SCRIPT_PROP.setProperty(PROP_KEY_LEASE, NEW_LEASE.toString());

      // *** FIX: Change synchronous call to ASYNCHRONOUS schedule ***
      scheduleOneTimeTrigger('updateMarketDataSheet', quickUpdateDelayMs);
    }

    // --- END OF MODIFICATION ---

  } else if (result === false) {
    // --- Case 3: Ran but did NOT complete fully (hit time limit) ---
    console.log(`${funcName} ran but hit its time limit. Rescheduling wrapper.`);

    // --- FIX: ADD THIS LOGIC ---
    // The inner function no longer reschedules itself. The wrapper must do it
    // to ensure the lock is always present on every run.
    scheduleOneTimeTrigger(wrapperFuncName, retryDelayMs);
    // You could also use FULL_RUN_RESCHEDULE_MS if you prefer a longer delay
    // --- END FIX ---
    // --- Case 4: Unexpected return value ---
    console.warn(`${funcName} execution by ${wrapperFuncName} returned unexpected value: ${result}`);
  }
}


/**
 * Cache refresh function. Called by wrapper.
 * Processes the Fuzzworks cache queue in batches within time limits.
 * Sets cooldown timestamp upon successful completion.
 * @returns {boolean} True if a full run completed, false otherwise.
 */
function fuzzworkCacheRefresh_TimeGated() {
  // --- Configuration ---
  // --- FIX: Increased batch size for efficiency ---
  const SUB_BATCH_SIZE = 2500;
  // ----------------------------------------------
  const TIME_LIMIT_MS = 270000; // 4m 30s
  const PROP_KEY_RESUME = 'cacheRefresh_lastIndex';
  const PROP_KEY_COOLDOWN = 'cacheRefresh_lastFullCompletion';
  // --- End Configuration ---

  const properties = PropertiesService.getScriptProperties();
  const START_TIME = new Date().getTime();
  const NOW_MS = START_TIME;

  console.log("Starting Fuzzworks cache refresh cycle...");
  let completedFullRun = false; // Flag to track full completion

  try {
    // 1. Get all requests
    const allRequests = getMasterBatchFromControlTable();
    if (!allRequests || allRequests.length === 0) {
      console.log("Cache Refresh: Control Table empty. Resetting state.");
      properties.deleteProperty(PROP_KEY_RESUME);
      properties.deleteProperty(PROP_KEY_COOLDOWN);
      return true; // Consider empty table as a 'completed' state
    }
    console.log(`Total requests found: ${allRequests.length}`);

    // 2. Determine starting point
    const resumeIndexRaw = properties.getProperty(PROP_KEY_RESUME);
    let startIndex = resumeIndexRaw ? parseInt(resumeIndexRaw, 10) : 0;
    if (isNaN(startIndex) || startIndex < 0 || startIndex >= allRequests.length) {
      startIndex = 0;
      console.log(`Cache refresh: Starting/Restarting from index 0.`);
    } else {
      console.log(`Cache refresh: Resuming from index ${startIndex}.`);
    }

    let itemsProcessedThisRun = 0;

    // 3. Process requests in batches
    while (startIndex < allRequests.length) {
      const currentTime = new Date().getTime();
      // Time Limit Check
      if (currentTime - START_TIME > TIME_LIMIT_MS) {
        properties.setProperty(PROP_KEY_RESUME, startIndex.toString());
        console.warn(`⚠️ Cache refresh time limit hit after ${itemsProcessedThisRun} items. Next run starts at index ${startIndex}. RESCHEDULING SELF.`);
        // scheduleOneTimeTrigger('fuzzworkCacheRefresh_TimeGated', 30 * 1000);
        return false; // <-- Did not complete fully
      }

      // Process sub-batch
      const endIndex = Math.min(startIndex + SUB_BATCH_SIZE, allRequests.length);
      const currentSubBatch = allRequests.slice(startIndex, endIndex);
      if (currentSubBatch.length > 0) {
        console.log(`Processing sub-batch: Indices ${startIndex} to ${endIndex - 1} (${currentSubBatch.length} items)`);
        try {
          fuzAPI.getDataForRequests(currentSubBatch);
          itemsProcessedThisRun += currentSubBatch.length;
        } catch (apiError) {
          console.error(`Error processing sub-batch indices ${startIndex}-${endIndex - 1}: ${apiError.message}. Skipping batch.`);
        }
      }
      startIndex = endIndex;
    } // End while loop

    // 4. Full Completion: Reset resume state and set cooldown timestamp
    properties.deleteProperty(PROP_KEY_RESUME);
    properties.setProperty(PROP_KEY_COOLDOWN, NOW_MS.toString());
    console.log(`Cache refresh: Successfully processed all ${allRequests.length} items. Cooldown timestamp set. Index reset.`);
    completedFullRun = true; // <-- Set flag on full completion

  } catch (e) {
    console.error(`Unhandled error during cache refresh: ${e.message}\nStack: ${e.stack}`);
    completedFullRun = false; // Ensure flag is false on error
  } finally {
    const duration = (new Date().getTime() - START_TIME) / 1000;
    console.log(`Cache refresh execution block finished in ${duration.toFixed(2)} seconds.`);
  }
  // Return completion status
  return completedFullRun; // <-- RETURN STATUS
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
 * Note: This file contains the corrected _updateMarketDataSheetWorker function.
 * Other functions from the original Orchestrator.js file are omitted for brevity,
 * but would be present in the full file.
 */
/**
 * Note: This file contains the corrected _updateMarketDataSheetWorker function.
 * Other functions from the original Orchestrator.js file are omitted for brevity,
 * but would be present in the full file.
 */

// --- Assume other Orchestrator.js functions exist here ---
// ... (deleteTriggersByName, _resetMarketDataJobState, executeWithTryLock, etc.) ...

function _updateMarketDataSheetWorker() {
  // --- Configuration ---
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_REQUEST_INDEX = 'marketDataRequestIndex';
  const PROP_KEY_SHEET_ROW = 'marketDataNextWriteRow';

  // --- START: DYNAMIC CHUNKING LOGIC (from SDE script) ---
  const MAX_CHUNK_SIZE = 5000;     // Max rows to fetch at once
  const MIN_CHUNK_SIZE = 500;      // Min rows to fetch at once
  const TARGET_WRITE_TIME_MS = 3000; // Target time for setValues() to take (3 seconds)
  const PROP_KEY_CHUNK_SIZE = 'marketDataChunkSize'; // Property to store chunk size
  // --- END: DYNAMIC CHUNKING LOGIC ---

  // --- PREDICTIVE SCHEDULING CONSTANTS ---
  const SOFT_LIMIT_MS = 280000;      // 4m 40s - Soft limit
  const RESCHEDULE_DELAY_MS = 5000;  // 5 seconds - Used for error backoff
  const FULL_RUN_RESCHEDULE_MS = 285000; // 4m 45s - Used for predictive scheduling
  const SAFE_MARGIN_MS = 50000;      // 50s margin for hard timeout

  // --- LOCK WAIT TIME ---
  const docTryLockWaitMs = 5 * 1000; // Document Lock tryLock wait time

  // --- REMOVED DYNAMIC THROTTLING ---

  // --- FIX: Lease Property ---
  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  // ---------------------------

  // --- FIX: Setup Step Property ---
  const PROP_KEY_SETUP_STEP = 'marketDataSetupStep';
  // --------------------------------

  const tempSheetName = 'Market_Data_Temp';
  const DATA_SHEET_HEADERS = ["cacheKey", "type_id", "location_type", "location_id", "sell_min", "buy_max", "sell_volume", "buy_volume", "last_updated"];
  const COLUMN_COUNT = DATA_SHEET_HEADERS.length;

  const START_TIME = new Date().getTime();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = null; // Initialize as null
  let batchesProcessedThisRun = 0; // Initialize batchesProcessedThisRun here

  // --- State Initialization & Validation ---
  let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP) || STATE_FLAGS.NEW_RUN;
  console.log(`Current Step: ${currentStep}`);

  if (currentStep === STATE_FLAGS.FINALIZING) {
    console.warn(`State is ${STATE_FLAGS.FINALIZING}. Exiting _updateMarketDataSheetWorker.`);
    return;
  }

  const masterRequests = getMasterBatchFromControlTable(ss);

  // --- Phase 1: NEW_RUN Setup ---
  if (currentStep === STATE_FLAGS.NEW_RUN || !masterRequests || masterRequests.length === 0) {
    currentStep = STATE_FLAGS.NEW_RUN;
    console.log(`State: ${STATE_FLAGS.NEW_RUN}. Preparing cycle.`);

    if (!masterRequests || masterRequests.length === 0) {
      console.warn("Control Table empty. Resetting state and exiting.");
      _resetMarketDataJobState(new Error("Control Table empty during NEW_RUN"));
      return;
    }

    let setupStep = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_SETUP_STEP) || '1', 10);
    if (setupStep === 1) {
      console.log(`Cold Start detected (NEW_RUN, Setup Step 1). Handing off to Cache Warmer first.`);
      SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
      SCRIPT_PROP.setProperty(PROP_KEY_SETUP_STEP, '3'); 
      scheduleOneTimeTrigger('triggerCacheWarmerWithRetry', 5000); // 5 sec delay
      return; // Exit this execution completely
    }

    console.log("Acquiring Document Lock for initial sheet setup...");
    console.log(`Resuming setup at step ${setupStep}.`);

    try {
      // Lease is already set by the masterOrchestrator
      SCRIPT_PROP.deleteProperty('marketDataJobIsActive');
      // --- REMOVED THROTTLE DURATION RESET ---
      // --- ADDED CHUNK SIZE RESET ---
      SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE); 

      withSheetLock(() => {
        // --- STEP 1: Handle first-time creation (setupStep <= 1) ---
        if (setupStep <= 1) {
          SpreadsheetApp.flush();
          sheet = getOrCreateSheet(ss, tempSheetName, DATA_SHEET_HEADERS);
          if (!sheet) throw new Error(`Failed to create/verify sheet in Step 1`);
          console.log(`[Setup Step 1] Ensuring temp sheet '${tempSheetName}' exists and is hidden.`);
          sheet.hideSheet();
          SCRIPT_PROP.deleteProperty(PROP_KEY_SETUP_STEP);
        }
        // --- STEP 2: Handle ERROR RECOVERY (setupStep === 2) ---
        else if (setupStep === 2) {
          if (new Date().getTime() - START_TIME > (SOFT_LIMIT_MS - SAFE_MARGIN_MS)) {
            throw new Error("Aggressive time limit hit before Setup Step 2 (Clear Content). Rescheduling setup.");
          }
          sheet = getOrCreateSheet(ss, tempSheetName, DATA_SHEET_HEADERS);
          if (!sheet) throw new Error(`Failed to create/verify sheet in Step 2`);
          console.warn(`[Setup Step 2] RECOVERY: Clearing content from sheet due to previous error.`);
          const lastRow = sheet.getLastRow(); 
          if (lastRow > 1) {
            console.log(`Clearing content from row 2 to ${lastRow}.`);
            sheet.getRange(2, 1, lastRow - 1, sheet.getMaxColumns())
              .clearContent();
          }
          console.log(`[Setup Step 2] RECOVERY: Resetting headers.`);
          sheet.getRange(1, 1, 1, COLUMN_COUNT).setValues([DATA_SHEET_HEADERS]);
          SCRIPT_PROP.deleteProperty(PROP_KEY_SETUP_STEP);
        }
        // --- STEP 3: Post-Handoff (setupStep === 3) ---
        else if (setupStep === 3) {
          sheet = getOrCreateSheet(ss, tempSheetName, DATA_SHEET_HEADERS);
          if (!sheet) throw new Error(`Failed to create/verify sheet in Step 3`);
          console.log(`[Setup Step 3] Post-handoff check. Ensuring sheet is hidden.`);
          sheet.hideSheet();
          SCRIPT_PROP.deleteProperty(PROP_KEY_SETUP_STEP);
        }
      }, 60000); // 60-second lock wait time for setup

      console.log("Initial sheet setup complete.");

      // Reset state properties for the new run
      SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, '0');
      SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, '2'); // Data starts at row 2
      currentStep = STATE_FLAGS.PROCESSING;
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.PROCESSING);
      console.log(`Initialization complete. Transitioning to ${STATE_FLAGS.PROCESSING}.`);

    } catch (setupError) {
      console.error(`CRITICAL ERROR during NEW_RUN sheet setup (Step ${setupStep} failed): ${setupError.message}. Rescheduling.`);
      SCRIPT_PROP.setProperty(PROP_KEY_SETUP_STEP, '2'); // Force '2' to clear
      scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
      throw setupError;
    }
  } // --- End NEW_RUN ---


  // --- Phase 2: PROCESSING Loop ---
  if (currentStep === STATE_FLAGS.PROCESSING) {
    console.log(`State: ${STATE_FLAGS.PROCESSING}. Starting fetch/write loop.`);

    let requestStartIndex = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_REQUEST_INDEX) || '0');
    let nextWriteRow = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_SHEET_ROW) || '2');

    // --- NEW: Read chunk size state once ---
    let currentChunkSize = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || MAX_CHUNK_SIZE.toString());
    // --- END NEW ---

    sheet = ss.getSheetByName(tempSheetName);
    if (!sheet) {
      const errMsg = `Sheet ${tempSheetName} disappeared during PROCESSING phase. Resetting state.`;
      _resetMarketDataJobState(new Error(errMsg));
      throw new Error(errMsg); // Halt execution
    }
    console.log(`Resuming from request index: ${requestStartIndex}, next write row: ${nextWriteRow}, chunk size: ${currentChunkSize}`);

    // --- Main Processing Loop ---
    while (requestStartIndex < masterRequests.length) {
      const currentTime = new Date().getTime();

      // --- Time Limit Check (Soft Limit) ---
      if (currentTime - START_TIME > SOFT_LIMIT_MS) {
        SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
        SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
        // --- NEW: Save current chunk size ---
        SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, currentChunkSize.toString());
        
        scheduleOneTimeTrigger('updateMarketDataSheet', FULL_RUN_RESCHEDULE_MS);
        console.warn(`WARNING: Time limit hit after processing ${batchesProcessedThisRun} batches in this run. Saved state (index ${requestStartIndex}, row ${nextWriteRow}). PREDICTIVE RESCHEDULED for ${FULL_RUN_RESCHEDULE_MS / 60000} minutes.`);
        return; // Exit current execution
      }

      // --- REMOVED ADAPTIVE THROTTLE ---

      // --- Prepare Batch & Fetch Data ---
      // --- MODIFIED: Use dynamic chunk size ---
      const requestEndIndex = Math.min(requestStartIndex + currentChunkSize, masterRequests.length);
      const requestsForThisRun = masterRequests.slice(requestStartIndex, requestEndIndex);

      if (requestsForThisRun.length === 0) {
        console.warn("Requests for this run is unexpectedly empty. Breaking loop.");
        break; // Exit loop if no requests left to process
      }
      console.log(`Processing batch: Request indices ${requestStartIndex} to ${requestEndIndex - 1} (${requestsForThisRun.length} requests)`);

      let marketData;
      try {
        marketData = fuzAPI.getDataForRequests(requestsForThisRun);
      } catch (apiError) {
        console.error(`Error calling fuzAPI.getDataForRequests for indices ${requestStartIndex}-${requestEndIndex - 1}: ${apiError.message}. Skipping batch and saving state.`);
        SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
        SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
        // --- NEW: Save current chunk size ---
        SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, currentChunkSize.toString());
        scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS * 2);
        return; // Exit
      }

      if (!marketData || !Array.isArray(marketData) || marketData.length === 0) {
        console.warn(`API returned no data for requests ${requestStartIndex + 1}-${requestEndIndex}. Advancing index.`);
        requestStartIndex = requestEndIndex; // Advance index even if no data
        SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString()); // Save advanced index
        continue; // Continue to next iteration
      }

      // --- Prepare Rows ---
      let allRowsToWrite = [];
      const currentTimeStamp = new Date();
      marketData.forEach(crate => {
        if (crate && crate.fuzObjects && Array.isArray(crate.fuzObjects)) {
          crate.fuzObjects.forEach(item => {
            if (item && item.type_id != null) {
              allRowsToWrite.push([
                "", // Placeholder
                item.type_id,
                crate.market_type || '', 
                crate.market_id || '', 
                item.sell?.min ?? '', 
                item.buy?.max ?? '',  
                item.sell?.volume ?? 0,
                item.buy?.volume ?? 0,
                currentTimeStamp
              ]);
            } else {
              console.warn(`Skipping invalid item in crate for market ${crate.market_type}:${crate.market_id}`);
            }
          });
        } else {
          console.warn(`Skipping invalid crate structure received from API.`);
        }
      });


      // --- Write Batch (Document Lock) ---
      if (allRowsToWrite.length > 0) {
        if (allRowsToWrite[0].length !== COLUMN_COUNT) {
          console.error(`CRITICAL: Column count mismatch! Expected ${COLUMN_COUNT}, got ${allRowsToWrite[0].length}. Skipping write for batch ${requestStartIndex}.`);
          requestStartIndex = requestEndIndex; // Advance index
          SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
          continue; // Skip batch
        }

        const timeBeforeWrite = new Date().getTime();
        if (timeBeforeWrite - START_TIME > (SOFT_LIMIT_MS - SAFE_MARGIN_MS)) {
          SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
          SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
          // --- NEW: Save current chunk size ---
          SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, currentChunkSize.toString());
          scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
          console.warn(`WARNING: Aggressive time limit hit (less than ${SAFE_MARGIN_MS}ms remaining). Saved state. RESCHEDULED to avoid hard timeout.`);
          return; // Exit current execution
        }

        // --- Use Document TryLock ---
        const docLock = LockService.getDocumentLock();
        let lockAcquired = false;
        try {
          console.log(`Attempting to acquire Document Lock (TryLock ${docTryLockWaitMs}ms) to write ${allRowsToWrite.length} rows starting at row ${nextWriteRow}...`);
          lockAcquired = docLock.tryLock(docTryLockWaitMs);

          if (lockAcquired) {
            const writeStartTime = new Date().getTime(); 
            console.log(`Document Lock acquired. Attempting sheet.getRange(${nextWriteRow}, 1, ${allRowsToWrite.length}, ${COLUMN_COUNT}).setValues(...)`);
            const range = sheet.getRange(nextWriteRow, 1, allRowsToWrite.length, COLUMN_COUNT);
            range.setValues(allRowsToWrite);
            console.log(`Write successful.`);
            const currentWriteDurationMs = new Date().getTime() - writeStartTime; 

            // --- Update State ONLY if Write Succeeded ---
            nextWriteRow += allRowsToWrite.length;
            requestStartIndex = requestEndIndex;
            batchesProcessedThisRun++;
            SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
            SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());

            // --- NEW: DYNAMIC CHUNK SIZE ADJUSTMENT ---
            const adjustmentFactor = TARGET_WRITE_TIME_MS / currentWriteDurationMs;
            currentChunkSize = Math.round(currentChunkSize * adjustmentFactor);
            currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.min(MAX_CHUNK_SIZE, currentChunkSize));
            SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, currentChunkSize.toString()); // Save for next run
            console.log(`Batch ${batchesProcessedThisRun} written successfully (${(currentWriteDurationMs / 1000).toFixed(2)}s). State saved. Next index: ${requestStartIndex}. Next row: ${nextWriteRow}. Next chunk size: ${currentChunkSize}`);
            // --- END NEW ---

          } else {
            // --- Lock NOT Acquired ---
            console.warn(`Document Lock busy for write attempt (index ${requestStartIndex}). Saving current state and rescheduling.`);
            SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
            SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());

            // --- NEW: AGGRESSIVE HALVING ---
            currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.round(currentChunkSize / 2));
            SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, currentChunkSize.toString());
            console.warn(`Backing off: new chunk size is ${currentChunkSize}`);
            // --- END NEW ---

            scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
            console.warn(`WARNING: Rescheduled due to Document Lock conflict.`);
            return; // Exit
          }
        } catch (writeError) {
          console.error(`Error during batch write (starting index ${requestStartIndex}): ${writeError.message}. Rescheduling.`);
          if (writeError.stack) { console.error(`Stack: ${writeError.stack}`); }
          SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
          SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());

          // --- NEW: AGGRESSIVE HALVING ON ERROR ---
          currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.round(currentChunkSize / 2));
          SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, currentChunkSize.toString());
          console.warn(`Backing off on error: new chunk size is ${currentChunkSize}`);
          // --- END NEW ---

          scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
          console.warn(`WARNING: Rescheduled due to write error (e.g., service timeout).`);
          return; // Exit
        } finally {
          if (lockAcquired) {
            try { docLock.releaseLock(); console.log("Document Lock released."); }
            catch (rlErr) { console.error("CRITICAL: Failed to release Document Lock!", rlErr); }
          }
        }
        // --- End Document TryLock ---

      } else {
        // No valid rows prepared
        console.log(`No valid rows to write for batch starting index ${requestStartIndex}. Advancing index.`);
        requestStartIndex = requestEndIndex; // Advance index
        SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
      }
    } // End while loop

    // --- Post-Loop Check ---
    if (requestStartIndex >= masterRequests.length) {
      console.log("All batches processed successfully. Setting state to FINALIZING.");
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.FINALIZING);
      SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
      // --- NEW: Clear chunk size on success ---
      SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
      
      scheduleOneTimeTrigger('finalizeMarketDataUpdate', RESCHEDULE_DELAY_MS);
    } else {
      console.warn("Processing loop finished unexpectedly. Saving state.");
      SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
      SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
      // --- NEW: Save current chunk size ---
      SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, currentChunkSize.toString());
    }

  } // --- End if PROCESSING ---

  const totalDuration = (new Date().getTime() - START_TIME) / 1000;
  console.log(`_updateMarketDataSheetWorker execution finished in ${totalDuration.toFixed(2)} seconds. Final state: ${SCRIPT_PROP.getProperty(PROP_KEY_STEP)}`);
}


/**
 * Final sheet update using Rename Swap. Ensures atomicity.
 * Called by masterOrchestrator when state is FINALIZING.
 */
function finalizeMarketDataUpdate() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const tempSheetName = 'Market_Data_Temp';
  const finalSheetName = 'Market_Data_Raw';
  const oldSheetName = 'Market_Data_Old'; // Sheet to be deleted later by cleanupOldSheet

  // --- FINALIZER RETRY LOGIC PROPERTIES ---
  const PROP_KEY_FINALIZER_STEP = 'marketDataFinalizeStep';
  const RETRY_DELAY_MS = 30 * 1000; // 30 seconds wait time
  // --- END FINALIZER RETRY LOGIC PROPERTIES ---

  // --- ADDED: TIME CHECK CONSTANTS ---
  const START_TIME = new Date().getTime();
  const SOFT_LIMIT_MS = 280000; // 4m 40s
  const SAFE_MARGIN_MS = 90000; // 1m 30s
  // --- END ---

  console.log("Attempting to finalize market data update...");

  // Use executeWithWaitLock: This is a CRITICAL, short operation.
  // Uses executeWithWaitLock from Orchestrator.js
  executeWithWaitLock(() => {
    const currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP);
    if (currentStep !== STATE_FLAGS.FINALIZING) {
      const errMsg = `Finalizer called unexpectedly (state: ${currentStep}). Resetting job state.`;
      console.error(errMsg);
      _resetMarketDataJobState(new Error(errMsg)); // Reset state
      return; // Exit
    }

    // Determine where to start/resume
    let step = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_FINALIZER_STEP) || '1', 10);
    console.log(`Resuming finalization at step ${step}.`);

    // --- NEW LOGIC: START AT STEP 1 HAND-OFF ---
    if (step === 1) {
      console.log("Handing off to Step 1 Deletion Worker.");
      // The worker handles the lock, step advance to '2', and re-schedules this function.
      _deleteOldSheetWorker();
      return; // Exit here. The worker handles the lock and next schedule.
    }
    // --- END NEW LOGIC ---

    console.log(`State is ${STATE_FLAGS.FINALIZING}. Starting atomic sheet swap (Step ${step} and up).`);
    const ss = SpreadsheetApp.getActiveSpreadsheet();

    // *** NOTE: Steps 2 & 3 must remain together inside one lock ***
    try {
      // Use WaitLock for the swap - it MUST succeed or fail loudly
      // Uses withSheetLock from Utility.js
      withSheetLock(() => {
        const tempSheet = ss.getSheetByName(tempSheetName);
        const liveSheet = ss.getSheetByName(finalSheetName);

        // --- Pre-swap Validation ---
        if (!tempSheet) {
          const errMsg = `Critical: Sheet '${tempSheetName}' is missing during finalization! Data lost.`;
          console.error(errMsg);
          // This is an unrecoverable failure. Reset the entire job.
          _resetMarketDataJobState(new Error(errMsg));
          // Throw an error to stop the rest of this execution.
          // The outer catch block will NOT reschedule because the state is now clear.
          throw new Error(errMsg);
        }
        if (tempSheet.getLastRow() <= 1) {
          SCRIPT_PROP.deleteProperty(PROP_KEY_FINALIZER_STEP);
          throw new Error(`Critical: Sheet '${tempSheetName}' is empty or has only headers. Cannot swap.`);
        }

        console.log("Acquired Document Lock for sheet swap (WaitLock).");

        // --- STEP 1 LOGIC IS REMOVED HERE ---

        // --- STEP 2: RENAME LIVE to OLD ---
        if (step === 2) {
          // --- FIX: Aggressive Time Check ---
          if (new Date().getTime() - START_TIME > (SOFT_LIMIT_MS - SAFE_MARGIN_MS)) {
            throw new Error("Aggressive time limit hit before Step 2 (Rename Raw->Old). Rescheduling finalizer.");
          }
          console.log(`[Step 2] Renaming '${finalSheetName}' to '${oldSheetName}'.`);
          if (liveSheet) {
            liveSheet.setName(oldSheetName);
            console.log(`Renamed to '${oldSheetName}'.`);
          } else {
            console.log(`Sheet '${finalSheetName}' not found. Skipping rename to old.`);
          }
          SCRIPT_PROP.setProperty(PROP_KEY_FINALIZER_STEP, '3');
          step = 3;
        }

        // --- STEP 3: RENAME TEMP to LIVE ---
        if (step === 3) {
          // --- FIX: Aggressive Time Check ---
          if (new Date().getTime() - START_TIME > (SOFT_LIMIT_MS - SAFE_MARGIN_MS)) {
            throw new Error("Aggressive time limit hit before Step 3 (Rename Temp->Raw). Rescheduling finalizer.");
          }
          console.log(`[Step 3] Renaming '${tempSheetName}' to '${finalSheetName}'.`);
          tempSheet.setName(finalSheetName);
          tempSheet.showSheet(); // Make visible
          console.log(`Renamed to '${finalSheetName}' and shown.`);

          // Success: Clear state property for completion
          SCRIPT_PROP.deleteProperty(PROP_KEY_FINALIZER_STEP);
        }

        console.log("Atomic sheet swap successful.");

      }, 60000); // 60-second lock wait for critical swap

      // --- Post-Swap Cleanup ---
      _resetMarketDataJobState(null);
      console.log("SUCCESS: Finalization complete. Job state reset.");

    } catch (swapError) {
      console.error(`CRITICAL ERROR during finalization swap (Step ${step} failed): ${swapError.message}. Retrying...`);

      // Save current step (which was set at the start of the failing step)
      SCRIPT_PROP.setProperty(PROP_KEY_FINALIZER_STEP, step.toString());

      // Reschedule itself for a retry after a delay, avoiding full job state reset
      // Uses scheduleOneTimeTrigger from Orchestrator.js
      scheduleOneTimeTrigger('finalizeMarketDataUpdate', RETRY_DELAY_MS);
      console.log(`Finalization job scheduled to retry in ${RETRY_DELAY_MS / 1000} seconds.`);

      // Rethrow to maintain executeWithWaitLock error flow
      throw swapError;
    }

  }, "finalizeMarketDataUpdate"); // Name for executeWithWaitLock
}


// New function for Step 1: Delete Old Sheet
function _deleteOldSheetWorker() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_FINALIZER_STEP = 'marketDataFinalizeStep';
  const oldSheetName = 'Market_Data_Old';
  const RETRY_DELAY_MS = 30 * 1000;

  // Ensure this function is only called when step is 1
  if (parseInt(SCRIPT_PROP.getProperty(PROP_KEY_FINALIZER_STEP) || '1', 10) !== 1) {
    console.warn("Skipping _deleteOldSheetWorker: Step is not 1.");
    return;
  }

  // Use WaitLock for the critical delete operation
  executeWithWaitLock(() => {
    const ss = SpreadsheetApp.getActiveSpreadsheet();

    // Use smaller Document Lock wait time for non-critical delete
    // Uses the 'withSheetLock' function from Utility.js
    withSheetLock(() => {
      const oldSheet = ss.getSheetByName(oldSheetName);
      console.log(`[Step 1] Deleting existing '${oldSheetName}' sheet.`);

      if (oldSheet) {
        ss.deleteSheet(oldSheet);
        console.log(`Sheet '${oldSheetName}' deleted.`);
        // Utilities.sleep(45000);
        SpreadsheetApp.flush(); // Force the delete operation to complete
      } else {
        console.log(`Sheet '${oldSheetName}' not found; skipping deletion.`);
      }

      // Success: Advance state to Step 2 and exit
      SCRIPT_PROP.setProperty(PROP_KEY_FINALIZER_STEP, '2');
    }, 10000); // 10-second lock wait time for deletion

  }, "_deleteOldSheetWorker");

  // If no error was thrown, schedule the next phase (main finalize)
  scheduleOneTimeTrigger('finalizeMarketDataUpdate', RETRY_DELAY_MS);
  console.log("Step 1 complete. Scheduled next phase (Step 2) for retry.");
}

/**
 * Run this function ONCE from the editor to set up triggers.
 */
function setupStaggeredTriggers() {
  console.log("Setting up/Resetting orchestrator triggers...");

  // Clean up all known triggers managed by this orchestrator
  const managedFunctions = [
    'fuzzworkCacheRefresh_TimeGated',
    'triggerCacheWarmerWithRetry',
    'updateMarketDataSheet',
    'finalizeMarketDataUpdate',
    'cleanupOldSheet', // Include this if it had its own trigger previously
    'masterOrchestrator',
    'runAllLedgerImports', // Assuming GESI might use a similar pattern
    'triggerLedgerImportCycle'
  ];

  let totalDeleted = 0;
  managedFunctions.forEach(funcName => {
    totalDeleted += deleteTriggersByName(funcName);
  });
  console.log(`Total existing clock triggers deleted: ${totalDeleted}.`);

  try {
    // Setup the main Master Orchestrator Trigger (every 15 min)
    ScriptApp.newTrigger('masterOrchestrator')
      .timeBased().everyMinutes(15).create();
    console.log('SUCCESS: Created 15-minute trigger for masterOrchestrator.');

    // Add back GESI trigger if needed (assuming it runs independently)
    // FIX: Change to every 2 hours
    ScriptApp.newTrigger('triggerLedgerImportCycle')
      .timeBased().everyHours(2).create(); // Changed from .hourly() to .everyHours(2)
    console.log('SUCCESS: Created 2-hour trigger for triggerLedgerImportCycle.');

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