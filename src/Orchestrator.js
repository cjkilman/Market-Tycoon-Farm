// Global variable to track recursion depth for this lock type
var EXECUTION_LOCK_DEPTH_TRY = 0;
// Global variable to track recursion depth for this lock type
var EXECUTION_LOCK_DEPTH_WAIT = 0;

// State Machine Constants (Internal steps for the Market Data job)
const STATE_FLAGS = {
  NEW_RUN: 'NEW_RUN',
  PROCESSING: 'PROCESSING',
  FINALIZING: 'FINALIZING'
};

/**
 * Helper to create a new one-time "retry" trigger.
 */
function scheduleOneTimeTrigger(functionName, delayMs) {
  // --- FIX: CRITICAL ERROR CHECK (Fail Fast) ---
  if (typeof functionName !== 'string' || functionName.trim() === '') {
    // Throwing an error ensures the entire parent job (the caller) crashes
    // immediately upon a critical input validation failure.
    throw new Error(`CRITICAL SCHEDULER ERROR: Invalid function name provided. Must be a non-empty string. Got: ${functionName}`);
  }
  // --- END FIX ---
  try {
    // Attempt to delete existing triggers first to prevent duplicates
    deleteTriggersByName(functionName);

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
 * Attempts to lock and execute a function, waiting up to 30s.
 * If the lock is busy, it returns null (skips) without error.
 * If the function executes, returns the function's return value.
 * Uses Script Lock.
 */
function executeWithTryLock(func, funcName) {
  const lock = LockService.getScriptLock();
  let functionResult = null; // Variable to store the function's result

  if (lock.tryLock(30000)) {
    const isOuterLock = (EXECUTION_LOCK_DEPTH_TRY === 0);
    EXECUTION_LOCK_DEPTH_TRY++;

    try {
      if (isOuterLock) {
        deleteTriggersByName(funcName);
        console.log(`--- Starting Execution (TryLock): ${funcName} ---`);
      } else {
        console.log(`--- Entering nested execution (TryLock): ${funcName} ---`);
      }

      // Execute the function and store its result
      functionResult = func(); // <-- STORE RESULT

      if (isOuterLock) {
        console.log(`--- Finished Execution (TryLock): ${funcName} ---`);
      } else {
        console.log(`--- Exiting nested execution (TryLock): ${funcName} ---`);
      }

    } catch (e) {
      console.error(`${funcName} failed: ${e.message}\nStack: ${e.stack}`);
      // --- FIX: REMOVED ALL RESET LOGIC ---
      // The worker functions are now responsible for their own stateful retries.
      // --- END FIX ---
      throw e; // Re-throw error
    } finally {
      EXECUTION_LOCK_DEPTH_TRY--;
      try {
        lock.releaseLock();
        if (isOuterLock) {
          console.log(`Script Lock released for ${funcName}.`);
        }
      } catch (lockError) {
        console.error(`CRITICAL: Failed to release Script Lock for ${funcName}: ${lockError.message}`);
      }
    }
    // Return the result from the executed function
    return functionResult; // <-- RETURN RESULT
  } else {
    console.warn(`${funcName} was skipped because another process held the Script Lock.`);
    // Return null to indicate it was skipped
    return null; // <-- RETURN NULL ON SKIP
  }
}

/**
 * Locks and executes a function, waiting up to 30s.
 * If the lock is busy after waiting, it THROWS AN ERROR.
 * Uses Script Lock.
 */
function executeWithWaitLock(func, funcName) {
  const lock = LockService.getScriptLock();
  let functionResult = undefined; // Use undefined initially

  try {
    lock.waitLock(30000);
    console.log(`Script Lock acquired for ${funcName} (WaitLock).`);
  } catch (e) {
    console.error(`Could not acquire Script Lock for ${funcName} after waiting. Error: ${e.message}`);
    throw e;
  }

  const isOuterLock = (EXECUTION_LOCK_DEPTH_WAIT === 0);
  EXECUTION_LOCK_DEPTH_WAIT++;

  try {
    if (isOuterLock) {
      deleteTriggersByName(funcName);
      console.log(`--- Starting Execution (WaitLock): ${funcName} ---`);
    } else {
      console.log(`--- Entering nested execution (WaitLock): ${funcName} ---`);
    }

    // Execute the function and store result
    functionResult = func(); // <-- STORE RESULT

    if (isOuterLock) {
      console.log(`--- Finished Execution (WaitLock): ${funcName} ---`);
    } else {
      console.log(`--- Exiting nested execution (WaitLock): ${funcName} ---`);
    }
  } catch (e) {
    console.error(`${funcName} failed during execution: ${e.message}\nStack: ${e.stack}`);
    // --- FIX: REMOVED ALL RESET LOGIC ---
    // The worker functions are now responsible for their own stateful retries.
    // --- END FIX ---
    throw e; // Re-throw error
  } finally {
    EXECUTION_LOCK_DEPTH_WAIT--;
    try {
      lock.releaseLock();
      if (isOuterLock) {
        console.log(`Script Lock released for ${funcName}.`);
      }
    } catch (lockError) {
      console.error(`CRITICAL: Failed to release Script Lock for ${funcName}: ${lockError.message}`);
    }
  }
  // Return the result from the executed function
  return functionResult; // <-- RETURN RESULT
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
    console.log(`${funcName} ran but hit its time limit and rescheduled itself.`);
    // Do nothing extra, the inner function handles its own rescheduling.

  } else {
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
        scheduleOneTimeTrigger('fuzzworkCacheRefresh_TimeGated', 30 * 1000);
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

function _updateMarketDataSheetWorker() {
  // --- Configuration ---
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_REQUEST_INDEX = 'marketDataRequestIndex';
  const PROP_KEY_SHEET_ROW = 'marketDataNextWriteRow';

  // --- FIX: BATCH_SIZE set to 800. ---
  const BATCH_SIZE = 800; // Increased batch size
  // ------------------------------------

  // --- PREDICTIVE SCHEDULING CONSTANTS ---
  const SOFT_LIMIT_MS = 280000;      // 4m 40s - Soft limit
  const RESCHEDULE_DELAY_MS = 5000;  // 5 seconds - Used for error backoff
  const FULL_RUN_RESCHEDULE_MS = 285000; // 4m 45s - Used for predictive scheduling
  const SAFE_MARGIN_MS = 90000;      // 1m 30s margin for hard timeout
  // ----------------------------------------

  // --- FIX: REDUCE LOCK WAIT TIME ---
  const docTryLockWaitMs = 5 * 1000; // Document Lock tryLock wait time (reduced from 15s)
  // ----------------------------------

  // --- NEW: Adaptive Throttling based on last write time ---
  const PROP_KEY_THROTTLE_DURATION = 'marketDataLastWriteDurationMs';
  const THROTTLE_BASE_SLEEP_MS = 500;     // Min 0.5s sleep between writes (THROTTLE_MIN_SLEEP_MS)
  const THROTTLE_LATENCY_FACTOR = 1.5;    // Sleep for 1.5x the last write duration
  const THROTTLE_MAX_SLEEP_MS = 45000;    // Max 45s sleep
  const THROTTLE_FAILURE_PENALTY_MS = 10000; // Add 10s to duration on failure
  // --- END NEW ---

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
  let sheet = ss.getSheetByName(tempSheetName);
  let batchesProcessedThisRun = 0; // Initialize batchesProcessedThisRun here

  // --- State Initialization & Validation ---
  let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP) || STATE_FLAGS.NEW_RUN;
  console.log(`Current Step: ${currentStep}`);

  if (currentStep === STATE_FLAGS.FINALIZING) {
    console.warn(`State is ${STATE_FLAGS.FINALIZING}. Exiting _updateMarketDataSheetWorker.`);
    return;
  }

  const masterRequests = getMasterBatchFromControlTable();

  // --- Phase 1: NEW_RUN Setup ---
  if (currentStep === STATE_FLAGS.NEW_RUN || !sheet || !masterRequests || masterRequests.length === 0) {
    currentStep = STATE_FLAGS.NEW_RUN;
    console.log(`State: ${STATE_FLAGS.NEW_RUN}. Preparing cycle.`);

    if (!masterRequests || masterRequests.length === 0) {
      console.warn("Control Table empty. Resetting state and exiting.");
      // FIX: Resetting state on empty control table is still necessary to clear old properties
      _resetMarketDataJobState(new Error("Control Table empty during NEW_RUN"));
      return;
    }

    // --- FIX: Delegate to Cache Warmer on a true Cold Start ---
    // A cold start is defined by NEW_RUN and setupStep being 1 (or null)
    let setupStep = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_SETUP_STEP) || '1', 10);
    if (setupStep === 1) {
      console.log(`Cold Start detected (NEW_RUN, Setup Step 1). Handing off to Cache Warmer first.`);
      // Clear the lease so the cache warmer can run
      SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
      // Schedule the cache warmer to run, which will then schedule this job
      scheduleOneTimeTrigger('triggerCacheWarmerWithRetry', 5000); // 5 sec delay
      return; // Exit this execution completely
    }
    // --- END FIX ---

    console.log("Acquiring Document Lock for initial sheet setup...");
    console.log(`Resuming setup at step ${setupStep}.`);

    try {
      // Lease is already set by the masterOrchestrator
      SCRIPT_PROP.deleteProperty('marketDataJobIsActive');
      SCRIPT_PROP.deleteProperty(PROP_KEY_THROTTLE_DURATION);

      withSheetLock(() => {
        // --- STEP 1: Create/Get Sheet (Should be skipped if we are here) ---
        if (setupStep <= 1) {
          // This block should now be unreachable due to the handoff above
          // But it remains as a safety net.
          console.log(`[Setup Step 1] Ensuring temp sheet '${tempSheetName}' exists.`);
          const expectedDataRows = masterRequests.length;
          sheet = getOrCreateSheet(ss, tempSheetName, DATA_SHEET_HEADERS);
          if (!sheet) throw new Error(`Failed to create or verify sheet ${tempSheetName}`);

          sheet.hideSheet();
          SCRIPT_PROP.setProperty(PROP_KEY_SETUP_STEP, '2');
          setupStep = 2;
        }

        // --- STEP 2: Clear Content (This is now the main entry point for a warm NEW_RUN) ---
        if (setupStep === 2) {
          // Check time before starting heavy operation
          if (new Date().getTime() - START_TIME > (SOFT_LIMIT_MS - SAFE_MARGIN_MS)) {
            throw new Error("Aggressive time limit hit before Setup Step 2 (Clear Content). Rescheduling setup.");
          }
          console.log(`[Setup Step 2] Clearing content from sheet.`);
          const lastRow = sheet.getLastRow();
          if (lastRow > 1) {
            console.log(`Clearing content from row 2 to ${lastRow}.`);
            sheet.getRange(2, 1, lastRow - 1, sheet.getMaxColumns()).clearContent();
          }

          // Success: Clear setup state property
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
      // Save the current failed step
      SCRIPT_PROP.setProperty(PROP_KEY_SETUP_STEP, setupStep.toString());
      // Reschedule the *same function* to try the setup again
      scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);

      // We do NOT reset the whole job, just retry the setup.
      // We re-throw the error to stop this execution.
      throw setupError;
    }
  } // --- End NEW_RUN ---


  // --- Phase 2: PROCESSING Loop ---
  if (currentStep === STATE_FLAGS.PROCESSING) {
    console.log(`State: ${STATE_FLAGS.PROCESSING}. Starting fetch/write loop.`);

    let requestStartIndex = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_REQUEST_INDEX) || '0');
    let nextWriteRow = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_SHEET_ROW) || '2');

    // --- NEW: Read throttle state once ---
    let lastDurationMs = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_THROTTLE_DURATION) || '0');
    // --- END NEW ---

    // Re-verify sheet exists before loop
    sheet = ss.getSheetByName(tempSheetName);
    if (!sheet) {
      const errMsg = `Sheet ${tempSheetName} disappeared during PROCESSING phase. Resetting state.`;
      _resetMarketDataJobState(new Error(errMsg));
      throw new Error(errMsg); // Halt execution
    }
    console.log(`Resuming from request index: ${requestStartIndex}, next write row: ${nextWriteRow}`);

    // --- Main Processing Loop ---
    while (requestStartIndex < masterRequests.length) {
      const currentTime = new Date().getTime();

      // --- Time Limit Check (Soft Limit) ---
      if (currentTime - START_TIME > SOFT_LIMIT_MS) {
        // Save current state before rescheduling
        SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
        SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
        // (Throttle state is already saved on each loop)

        // FIX: Predictive reschedule using FULL_RUN_RESCHEDULE_MS
        scheduleOneTimeTrigger('updateMarketDataSheet', FULL_RUN_RESCHEDULE_MS);
        console.warn(`⚠️ Time limit hit after processing ${batchesProcessedThisRun} batches in this run. Saved state (index ${requestStartIndex}, row ${nextWriteRow}). PREDICTIVE RESCHEDULED for ${FULL_RUN_RESCHEDULE_MS / 60000} minutes.`);
        return; // Exit current execution
      }

      // --- NEW: ADAPTIVE THROTTLE ---
      // Calculate sleep time based on the *last* batch's performance
      let sleepMs = Math.max(THROTTLE_BASE_SLEEP_MS, lastDurationMs * THROTTLE_LATENCY_FACTOR);
      sleepMs = Math.min(THROTTLE_MAX_SLEEP_MS, sleepMs); // Cap at max

      console.log(`Throttling for ${sleepMs.toFixed(0)}ms (based on last write of ${lastDurationMs}ms)`);
      if (sleepMs > 0) {
        Utilities.sleep(sleepMs);
      }
      // --- END NEW ---

      // --- Prepare Batch & Fetch Data ---
      const requestEndIndex = Math.min(requestStartIndex + BATCH_SIZE, masterRequests.length);
      const requestsForThisRun = masterRequests.slice(requestStartIndex, requestEndIndex);

      if (requestsForThisRun.length === 0) {
        console.warn("Requests for this run is unexpectedly empty. Breaking loop.");
        break; // Exit loop if no requests left to process
      }
      console.log(`Processing batch: Request indices ${requestStartIndex} to ${requestEndIndex - 1} (${requestsForThisRun.length} requests)`);

      let marketData;
      try {
        // Get data (will use cache or fetch)
        marketData = fuzAPI.getDataForRequests(requestsForThisRun);
      } catch (apiError) {
        console.error(`Error calling fuzAPI.getDataForRequests for indices ${requestStartIndex}-${requestEndIndex - 1}: ${apiError.message}. Skipping batch and saving state.`);
        // Save current state and reschedule on API error
        SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
        SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
        scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS * 2); // Longer delay on API error?
        return; // Exit
      }


      // Check if API returned data
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
                item.sell?.volume ?? '',
                item.buy?.volume ?? '',
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
        // Column count validation
        if (allRowsToWrite[0].length !== COLUMN_COUNT) {
          console.error(`CRITICAL: Column count mismatch! Expected ${COLUMN_COUNT}, got ${allRowsToWrite[0].length}. Skipping write for batch ${requestStartIndex}.`);
          requestStartIndex = requestEndIndex; // Advance index
          SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
          continue; // Skip batch
        }

        // --- Aggressive Time Check: Reschedule if less than SAFE_MARGIN_MS remains ---
        const timeBeforeWrite = new Date().getTime();
        if (timeBeforeWrite - START_TIME > (SOFT_LIMIT_MS - SAFE_MARGIN_MS)) {
          // Log that we are stopping work to avoid hard timeout
          SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
          SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
          scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
          console.warn(`⚠️ Aggressive time limit hit (less than ${SAFE_MARGIN_MS}ms remaining). Saved state. RESCHEDULED to avoid hard timeout.`);
          return; // Exit current execution
        }
        // --- End Aggressive Time Check ---


        // --- Use Document TryLock ---
        const docLock = LockService.getDocumentLock();
        let lockAcquired = false;
        try {
          console.log(`Attempting to acquire Document Lock (TryLock ${docTryLockWaitMs}ms) to write ${allRowsToWrite.length} rows starting at row ${nextWriteRow}...`);
          lockAcquired = docLock.tryLock(docTryLockWaitMs);

          if (lockAcquired) {
            const writeStartTime = new Date().getTime(); // --- NEW ---
            console.log(`Document Lock acquired. Attempting sheet.getRange(${nextWriteRow}, 1, ${allRowsToWrite.length}, ${COLUMN_COUNT}).setValues(...)`);
            const range = sheet.getRange(nextWriteRow, 1, allRowsToWrite.length, COLUMN_COUNT);

            range.setValues(allRowsToWrite);

            console.log(`Write successful.`);
            const currentWriteDurationMs = new Date().getTime() - writeStartTime; // --- NEW ---

            // --- Update State ONLY if Write Succeeded ---
            nextWriteRow += allRowsToWrite.length;
            requestStartIndex = requestEndIndex;
            batchesProcessedThisRun++;
            SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
            SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());

            // --- NEW: ADAPTIVE THROTTLING (SAVE DURATION) ---
            lastDurationMs = currentWriteDurationMs; // Update duration for *this* execution's next loop
            SCRIPT_PROP.setProperty(PROP_KEY_THROTTLE_DURATION, lastDurationMs.toString()); // Save for *next* execution
            // --- END NEW ---

            console.log(`Batch ${batchesProcessedThisRun} written successfully (${(currentWriteDurationMs / 1000).toFixed(2)}s). State saved. Next index: ${requestStartIndex}. Next row: ${nextWriteRow}`);


          } else {
            // --- Lock NOT Acquired ---
            console.warn(`Document Lock busy for write attempt (index ${requestStartIndex}). Saving current state and rescheduling.`);
            SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
            SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());

            // --- NEW: ADAPTIVE THROTTLING (BACK-OFF PENALTY) ---
            const penaltyDuration = Math.min(THROTTLE_MAX_SLEEP_MS, lastDurationMs + THROTTLE_FAILURE_PENALTY_MS);
            SCRIPT_PROP.setProperty(PROP_KEY_THROTTLE_DURATION, penaltyDuration.toString());
            console.warn(`Backing off: new base sleep duration is ${penaltyDuration}ms`);
            // --- END NEW ---

            scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
            console.warn(`⚠️ Rescheduled due to Document Lock conflict.`);
            return; // Exit
          }
        } catch (writeError) {
          // Catch errors *during* the write (e.g., service timeout)
          console.error(`Error during batch write (starting index ${requestStartIndex}): ${writeError.message}. Rescheduling.`);
          if (writeError.stack) { console.error(`Stack: ${writeError.stack}`); }
          SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
          SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());

          // --- NEW: ADAPTIVE THROTTLING (BACK-OFF PENALTY) ---
          const penaltyDuration = Math.min(THROTTLE_MAX_SLEEP_MS, lastDurationMs + THROTTLE_FAILURE_PENALTY_MS);
          SCRIPT_PROP.setProperty(PROP_KEY_THROTTLE_DURATION, penaltyDuration.toString());
          console.warn(`Backing off: new base sleep duration is ${penaltyDuration}ms`);
          // --- END NEW ---

          scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
          console.warn(`⚠️ Rescheduled due to write error (e.g., service timeout).`);
          return; // Exit
        } finally {
          if (lockAcquired) {
            try { docLock.releaseLock(); console.log("Document Lock released."); }
            catch (rlErr) { console.error("CRITICAL: Failed to release Document Lock!", rlErr); }
          }
        }
        // --- End Document TryLock ---

      } else {
        // No valid rows prepared (e.g., all negative cache hits for this batch)
        console.log(`No valid rows to write for batch starting index ${requestStartIndex}. Advancing index.`);
        requestStartIndex = requestEndIndex; // Advance index
        SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
      }
    } // End while loop

    // --- Post-Loop Check ---
    if (requestStartIndex >= masterRequests.length) {
      console.log("All batches processed successfully. Setting state to FINALIZING.");
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.FINALIZING);
      // FIX: Delete the lease on success
      SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);

      // *** FIX: Force the finalization schedule immediately ***
      scheduleOneTimeTrigger('finalizeMarketDataUpdate', RESCHEDULE_DELAY_MS);
    } else {
      console.warn("Processing loop finished unexpectedly. Saving state.");
      SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
      SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
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
  executeWithWaitLock(() => {
    const currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP);
    if (currentStep !== STATE_FLAGS.FINALIZING) {
      const errMsg = `Finalizer called unexpectedly (state: ${currentStep}). Resetting job state.`;
      console.error(errMsg);
      _resetMarketDataJobState(new Error(errMsg)); // Reset state
      return; // Exit
    }

    console.log(`State is ${STATE_FLAGS.FINALIZING}. Starting atomic sheet swap.`);
    const ss = SpreadsheetApp.getActiveSpreadsheet();

    // Determine where to start/resume
    let step = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_FINALIZER_STEP) || '1', 10);
    console.log(`Resuming finalization at step ${step}.`);

    try {
      // Use WaitLock for the swap - it MUST succeed or fail loudly
      // NOTE: This lock is required for the critical structural changes.
      withSheetLock(() => {
        const tempSheet = ss.getSheetByName(tempSheetName);
        const liveSheet = ss.getSheetByName(finalSheetName);
        const oldSheet = ss.getSheetByName(oldSheetName);

        // --- Pre-swap Validation ---
        if (!tempSheet) {
          // If the temporary sheet is missing, the data is lost; fail hard.
          SCRIPT_PROP.deleteProperty(PROP_KEY_FINALIZER_STEP);
          throw new Error(`Critical: Sheet '${tempSheetName}' is missing during finalization! Data lost.`);
        }
        if (tempSheet.getLastRow() <= 1) {
          // If temp sheet is empty, something is wrong; fail hard.
          SCRIPT_PROP.deleteProperty(PROP_KEY_FINALIZER_STEP);
          throw new Error(`Critical: Sheet '${tempSheetName}' is empty or has only headers. Cannot swap.`);
        }

        console.log("Acquired Document Lock for sheet swap (WaitLock).");

        // --- STEP 1: DELETE OLD_OLD SHEET ---
        if (step <= 1) {
          // --- FIX: Aggressive Time Check ---
          if (new Date().getTime() - START_TIME > (SOFT_LIMIT_MS - SAFE_MARGIN_MS)) {
            throw new Error("Aggressive time limit hit before Step 1 (Delete). Rescheduling finalizer.");
          }
          console.log(`[Step 1] Deleting existing '${oldSheetName}' sheet.`);
          if (oldSheet) {
            ss.deleteSheet(oldSheet);
            console.log(`Sheet '${oldSheetName}' deleted.`);
          } else {
            console.log(`Sheet '${oldSheetName}' not found; skipping deletion.`);
          }
          SCRIPT_PROP.setProperty(PROP_KEY_FINALIZER_STEP, '2');
          step = 2;
        }

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
      // NOTE: _resetMarketDataJobState clears all properties *except* the finalizer step if it was just cleared.
      _resetMarketDataJobState(null);
      console.log("SUCCESS: Finalization complete. Job state reset.");

    } catch (swapError) {
      console.error(`CRITICAL ERROR during finalization swap (Step ${step} failed): ${swapError.message}. Retrying...`);

      // Save current step (which was set at the start of the failing step)
      // This is crucial: we save the last successful step to resume from next time
      SCRIPT_PROP.setProperty(PROP_KEY_FINALIZER_STEP, step.toString());

      // Reschedule itself for a retry after a delay, avoiding full job state reset
      scheduleOneTimeTrigger('finalizeMarketDataUpdate', RETRY_DELAY_MS);
      console.log(`Finalization job scheduled to retry in ${RETRY_DELAY_MS / 1000} seconds.`);

      // Rethrow to maintain executeWithWaitLock error flow
      throw swapError;
    }

  }, "finalizeMarketDataUpdate"); // Name for executeWithWaitLock
}


/**
 * Simple function to clean up the old market data sheet ('Market_Data_Old').
 * Intended to be called by masterOrchestrator during the FINALIZING phase or opportunistically.
 * Uses TryLock for Document Lock - ok if skipped.
 */
function cleanupOldSheet() {
  const oldSheetName = 'Market_Data_Old';
  const funcName = 'cleanupOldSheet';
  const docTryLockWaitMs = 10000; // Shorter wait for non-critical cleanup

  const docLock = LockService.getDocumentLock();
  let lockAcquired = false;
  try {
    lockAcquired = docLock.tryLock(docTryLockWaitMs);

    if (lockAcquired) {
      console.log(`Document Lock acquired for ${funcName}.`);
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const oldSheet = ss.getSheetByName(oldSheetName);

      if (oldSheet) {
        console.log(`Found sheet '${oldSheetName}'. Deleting...`);
        try {
          ss.deleteSheet(oldSheet);
          console.log(`Successfully deleted sheet '${oldSheetName}'.`);
        } catch (e) {
          console.error(`Failed to delete sheet '${oldSheetName}': ${e.message}`);
        }
      } else {
        console.log(`No sheet named '${oldSheetName}' found to delete.`);
      }
    } else {
      console.warn(`Could not acquire Document Lock for ${funcName} (TryLock). Deletion deferred.`);
      // No error thrown, it just didn't run this time.
    }
  } catch (e) {
    // Catch unexpected errors during the process
    console.error(`Unexpected error during ${funcName}: ${e.message}`);
    if (e.stack) { console.error(`Stack: ${e.stack}`); }
  } finally {
    if (lockAcquired) {
      try { docLock.releaseLock(); console.log("Document Lock released."); }
      catch (rlErr) { console.error("CRITICAL: Failed to release Document Lock!", rlErr); }
    }
  }
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