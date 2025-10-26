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
 * Moved UP to ensure it's defined before being called.
 */
function scheduleOneTimeTrigger(functionName, delayMs) {
  if (typeof functionName !== 'string' || functionName.trim() === '') {
    // Log the error but don't stop the script if possible
    console.error(`scheduleOneTimeTrigger: Invalid function name provided. Must be a non-empty string. Got: ${functionName}`);
    return; // Exit the helper function
  }
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
    // Log error if trigger creation fails
    console.error(`Failed to create/delete trigger for ${functionName}: ${e.message}. Stack: ${e.stack}`);
    // Depending on the error, you might want to throw it or handle it differently
    // For now, just logging the error.
  }
}

/**
 * Helper to delete triggers by name.
 * Moved UP for consistency.
 */
function deleteTriggersByName(functionName) {
  // Add input validation
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
          // Log specific error for deletion failure but continue trying others
          console.warn(`Could not delete a trigger (ID: ${trigger.getUniqueId()}) for ${functionName}: ${e.message}`);
        }
      }
    });
    if (deletedCount > 0) {
      console.log(`Deleted ${deletedCount} existing clock trigger(s) for ${functionName}.`);
    }
  } catch (e) {
    // Log error if fetching triggers fails
    console.error(`Error accessing or deleting triggers for ${functionName}: ${e.message}. Stack: ${e.stack}`);
    // Consider if this error should halt execution or just be logged
  }
  return deletedCount; // Return the count
}


/**
 * Internal reset helper.
 * Moved UP for consistency.
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

  // Use try-catch for property deletion in case of permission issues (less likely)
  try {
      SCRIPT_PROP.deleteProperty(PROP_KEY_STEP);
      SCRIPT_PROP.deleteProperty(PROP_KEY_REQUEST_INDEX);
      SCRIPT_PROP.deleteProperty(PROP_KEY_SHEET_ROW);
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
 *
 * @param {function} func The function to execute.
 * @param {string} funcName A name for logging.
 * @returns {any|null} The return value of func if executed, or null if skipped.
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
      if (funcName === 'updateMarketDataSheet' || funcName === 'finalizeMarketDataUpdate') {
        console.warn("Attempting to reset Market Data job state due to failure.");
        _resetMarketDataJobState(e);
      }
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
 * Returns the result of the executed function.
 *
 * @param {function} func The function to execute.
 * @param {string} funcName A name for logging.
 * @returns {any} The return value of func if executed.
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
    if (funcName === 'updateMarketDataSheet' || funcName === 'finalizeMarketDataUpdate') {
      console.warn("Attempting to reset Market Data job state due to execution failure.");
      _resetMarketDataJobState(e); // Pass the original error
    }
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

  // Cache Warmer Cooldown Configuration
  const PROP_KEY_COOLDOWN = 'cacheRefresh_lastFullCompletion';
  const COOLDOWN_MINUTES = 30; // 30 Minute cooldown

  // --- Priority 1: Market Data Finalization Check (MUST be run first) ---
  if (marketDataStep === STATE_FLAGS.FINALIZING) {
    console.log(`Master orchestrator: Market data state requires cleanup/finalization.`);
    // *** REMOVED cleanupOldSheet call here - it's handled opportunistically or in finalize ***
    // const cleanupAttempted = executeWithTryLock(cleanupOldSheet, 'cleanupOldSheet');
    const delay = 30 * 1000;
    // Always schedule finalize; it handles cleanup internally if needed.
    console.log("State is FINALIZING. Scheduling final swap now.");
    scheduleOneTimeTrigger("finalizeMarketDataUpdate", delay);
    return; // Prioritize finalization
  }

  console.log(`Master orchestrator (min ${currentMinute}): Checking time window.`);

  // --- Staggering Logic ---
  // Market Update: Minutes 15-44 (Two 15-min slots per hour)
  // Cache Warmer: Minutes 0-14 and 45-59 (Two 15-min slots per hour)

  if (currentMinute >= 15 && currentMinute < 45) { // *** Window: Minutes 15-44 ***
    // --- Window for Market Data Update ---
    console.log(`Master orchestrator (min ${currentMinute}): Dispatching MARKET DATA UPDATE.`);
    executeWithTryLock(updateMarketDataSheet, 'updateMarketDataSheet');
  } else { // *** Covers 0-14 and 45-59 ***
    // --- Window for Cache Warmer ---
    console.log(`Master orchestrator (min ${currentMinute}): In cache warmer window.`);

    // Cooldown Check
    let skipCacheWarmer = false;
    const lastCompletionRaw = SCRIPT_PROP.getProperty(PROP_KEY_COOLDOWN);
    if (lastCompletionRaw) {
        const lastCompletionMs = parseInt(lastCompletionRaw, 10);
        if (!isNaN(lastCompletionMs)) {
            const minutesSinceCompletion = (NOW_MS - lastCompletionMs) / (60 * 1000);
            if (minutesSinceCompletion < COOLDOWN_MINUTES) {
                console.log(`Skipping cache warmer dispatch: Last full completion was ${minutesSinceCompletion.toFixed(1)} minutes ago (within ${COOLDOWN_MINUTES} min cooldown).`);
                skipCacheWarmer = true;
            } else {
                 console.log(`Cooldown period (${COOLDOWN_MINUTES} min) has passed since last completion. Proceeding.`);
            }
        } else {
            console.warn("Invalid cache warmer cooldown timestamp found. Proceeding.");
            SCRIPT_PROP.deleteProperty(PROP_KEY_COOLDOWN); // Clean up
        }
    } else {
       console.log("No previous cache warmer completion timestamp found. Proceeding.");
    }

    if (!skipCacheWarmer) {
        console.log(`Dispatching CACHE WARMER wrapper.`);
        executeWithTryLock(triggerCacheWarmerWithRetry, 'triggerCacheWarmerWithRetry');
    }
    // End Cooldown Check
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
  const funcToRun = fuzzworkCacheRefresh_TimeGated;
  const funcName = 'fuzzworkCacheRefresh_TimeGated';
  const wrapperFuncName = 'triggerCacheWarmerWithRetry';
  const retryDelayMs = 2 * 60 * 1000; // 2 minutes retry delay
  const quickUpdateDelayMs = 5000; // 5 seconds delay before trying market update

  console.log(`Wrapper ${wrapperFuncName} called. Attempting to run ${funcName} using executeWithTryLock...`);
  const result = executeWithTryLock(funcToRun, funcName); // result is true (full run), false (incomplete), or null (skipped)

  if (result === null) {
    // --- Case 1: Skipped due to Script Lock ---
    console.warn(`${funcName} was skipped due to Script Lock. Scheduling retry for ${wrapperFuncName}.`);
    scheduleOneTimeTrigger(wrapperFuncName, retryDelayMs);

  } else if (result === true) {
    // --- Case 2: Ran AND Completed Fully ---
    console.log(`${funcName} completed a full run successfully.`);

    // *** Attempt Opportunistic Cleanup ***
    console.log(`Attempting opportunistic cleanup of Market_Data_Old...`);
    const cleanupAttempted = executeWithTryLock(cleanupOldSheet, 'cleanupOldSheet_Opportunistic');
    if (cleanupAttempted === null) {
      console.warn("Opportunistic cleanup was skipped due to lock.");
    } else {
      console.log("Opportunistic cleanup attempted (check logs for success/failure).");
    }
    // *** End Opportunistic Cleanup ***

    // Check if we are now in the market update window
    const currentMinute = new Date().getMinutes();
    if (currentMinute >= 15 && currentMinute < 45) {
        console.log(`Cache warmer finished, now in market update window (min ${currentMinute}). Scheduling market update.`);
        scheduleOneTimeTrigger('updateMarketDataSheet', quickUpdateDelayMs);
    } else {
        console.log(`Cache warmer finished, but not in market update window (min ${currentMinute}). No immediate update scheduled.`);
    }

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
  const SUB_BATCH_SIZE = 250;
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
      properties.deleteProperty(PROP_KEY_RESUME);
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
 * Uses TryLock for Document Lock during writes to save state on conflict.
 */
function updateMarketDataSheet() {
  // --- Configuration ---
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_REQUEST_INDEX = 'marketDataRequestIndex';
  const PROP_KEY_SHEET_ROW = 'marketDataNextWriteRow';

  const BATCH_SIZE = 50; // Keep batch size small due to sheet calculation timeouts
  const TIME_LIMIT_MS = 280000; // Time limit (4m 40s)
  const RESCHEDULE_DELAY_MS = 30 * 1000; // Reschedule delay
  const docTryLockWaitMs = 10000; // Document Lock tryLock wait time

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
    console.warn(`State is ${STATE_FLAGS.FINALIZING}. Exiting updateMarketDataSheet.`);
    return;
  }

  const masterRequests = getMasterBatchFromControlTable();

  // --- Phase 1: NEW_RUN Setup ---
  if (currentStep === STATE_FLAGS.NEW_RUN || !sheet || !masterRequests || masterRequests.length === 0) {
    currentStep = STATE_FLAGS.NEW_RUN;
    console.log(`State: ${STATE_FLAGS.NEW_RUN}. Preparing cycle.`);

    if (!masterRequests || masterRequests.length === 0) {
      console.warn("Control Table empty. Resetting state and exiting.");
      _resetMarketDataJobState(new Error("Control Table empty during NEW_RUN"));
      return;
    }

    console.log("Acquiring Document Lock for initial sheet setup...");
    try {
      // Initial setup still uses waitLock - it MUST complete or fail loudly.
      withSheetLock(() => {
        sheet = ss.getSheetByName(tempSheetName);
        // *** PASS maxRows to getOrCreateSheet ***
        const expectedDataRows = masterRequests.length;
        console.log(`Ensuring temp sheet '${tempSheetName}' exists with ${expectedDataRows} data rows.`);
        sheet = getOrCreateSheet(ss, tempSheetName, DATA_SHEET_HEADERS, expectedDataRows);

        if (!sheet) throw new Error(`Failed to create or verify sheet ${tempSheetName}`);

        // Additional checks after getOrCreateSheet returns
        if(sheet.getName() !== tempSheetName) {
           throw new Error(`getOrCreateSheet returned sheet with wrong name: ${sheet.getName()}`);
        }

        console.log(`Sheet '${tempSheetName}' ready. Hiding sheet.`);
        sheet.hideSheet();

         // Clear content below header just in case (optional, getOrCreateSheet might handle it)
         const lastRow = sheet.getLastRow();
         if (lastRow > 1) {
             console.log(`Clearing content from row 2 to ${lastRow}.`);
             sheet.getRange(2, 1, lastRow - 1, sheet.getMaxColumns()).clearContent();
         }


      }, 60000); // Increased lock wait time for setup

      console.log("Initial sheet setup complete.");
      SpreadsheetApp.flush(); // Flush after setup

      // Reset state properties for the new run
      SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, '0');
      SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, '2'); // Data starts at row 2
      currentStep = STATE_FLAGS.PROCESSING;
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.PROCESSING);
      console.log(`Initialization complete. Transitioning to ${STATE_FLAGS.PROCESSING}.`);

    } catch (setupError) {
        console.error(`CRITICAL ERROR during NEW_RUN sheet setup: ${setupError.message}. Resetting state.`);
         _resetMarketDataJobState(setupError);
         // Re-throw the error to ensure the execution stops and logs failure
         throw setupError;
    }
  } // --- End NEW_RUN ---


  // --- Phase 2: PROCESSING Loop ---
  if (currentStep === STATE_FLAGS.PROCESSING) {
    console.log(`State: ${STATE_FLAGS.PROCESSING}. Starting fetch/write loop.`);

    let requestStartIndex = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_REQUEST_INDEX) || '0');
    let nextWriteRow = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_SHEET_ROW) || '2');

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

      // --- Time Limit Check ---
      if (currentTime - START_TIME > TIME_LIMIT_MS) {
        // Save current state before rescheduling
        SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
        SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
        scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
        console.warn(`⚠️ Time limit hit after processing ${batchesProcessedThisRun} batches in this run. Saved state (index ${requestStartIndex}, row ${nextWriteRow}). RESCHEDULED.`);
        return; // Exit current execution
      }

      // --- Prepare Batch & Fetch Data ---
      const requestEndIndex = Math.min(requestStartIndex + BATCH_SIZE, masterRequests.length);
      const requestsForThisRun = masterRequests.slice(requestStartIndex, requestEndIndex);

      if (requestsForThisRun.length === 0) {
         console.warn("Requests for this run is unexpectedly empty. Breaking loop.");
         break; // Exit loop if no requests left to process
      }
      console.log(`Processing batch: Request indices ${requestStartIndex} to ${requestEndIndex - 1} (${requestsForThisRun.length} requests)`);

      let marketData;
      let isCacheMiss = false; // Flag to track if this batch involved a cache miss
      try {
          // Check cache first to determine if it's a miss
          const { cachedData, missingRequests } = fuzAPI._checkCacheForRequests(requestsForThisRun); // Assuming fuzAPI exposes this
          if (missingRequests.length > 0) {
              isCacheMiss = true;
          }
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

      // *** ADDED: Small delay *only* if it was a cache miss (i.e., putAll was called) ***
      if (isCacheMiss && marketData && marketData.length > 0) {
         console.log("Adding 1.5s delay after cache write to prevent service contention...");
         Utilities.sleep(1500); // 1.5 second sleep
      }
      // *** END ADDED DELAY ***

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


      // --- Write Batch using TryLock ---
      if (allRowsToWrite.length > 0) {
        // Column count validation
        if (allRowsToWrite[0].length !== COLUMN_COUNT) {
           console.error(`CRITICAL: Column count mismatch! Expected ${COLUMN_COUNT}, got ${allRowsToWrite[0].length}. Skipping write for batch ${requestStartIndex}.`);
           requestStartIndex = requestEndIndex; // Advance index
           SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
           continue; // Skip batch
        }

        // Pre-write time check
        const timeBeforeWrite = new Date().getTime();
        if (timeBeforeWrite - START_TIME > TIME_LIMIT_MS) {
          SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
          SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
          scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
          console.warn(`⚠️ Time limit hit just BEFORE write attempt for batch ${requestStartIndex}. Saved state. RESCHEDULED.`);
          return; // Exit
        }

        // --- Use Document TryLock ---
        const docLock = LockService.getDocumentLock();
        let lockAcquired = false;
        try {
          console.log(`Attempting to acquire Document Lock (TryLock ${docTryLockWaitMs}ms) to write ${allRowsToWrite.length} rows starting at row ${nextWriteRow}...`);
          lockAcquired = docLock.tryLock(docTryLockWaitMs);

          if (lockAcquired) {
            console.log(`Document Lock acquired. Attempting sheet.getRange(${nextWriteRow}, 1, ${allRowsToWrite.length}, ${COLUMN_COUNT}).setValues(...)`);
            const range = sheet.getRange(nextWriteRow, 1, allRowsToWrite.length, COLUMN_COUNT);
            range.setValues(allRowsToWrite);
            console.log(`Write successful.`);

            // --- Update State ONLY if Write Succeeded ---
            nextWriteRow += allRowsToWrite.length;
            requestStartIndex = requestEndIndex;
            batchesProcessedThisRun++;
            SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
            SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
            const duration = (new Date().getTime() - timeBeforeWrite) / 1000;
            console.log(`Batch ${batchesProcessedThisRun} written successfully (${duration.toFixed(2)}s). State saved. Next index: ${requestStartIndex}. Next row: ${nextWriteRow}`);

          } else {
            // --- Lock NOT Acquired ---
            console.warn(`Document Lock busy for write attempt (index ${requestStartIndex}). Saving current state and rescheduling.`);
            SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
            SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
            scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
            console.warn(`⚠️ Rescheduled due to Document Lock conflict.`);
            return; // Exit
          }
        } catch (writeError) {
          // Catch errors *during* the write (e.g., service timeout)
          console.error(`Error during batch write (starting index ${requestStartIndex}): ${writeError.message}. Rescheduling.`);
          if (writeError.stack) { console.error(`Stack: ${writeError.stack}`);}
          SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
          SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
          scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
          console.warn(`⚠️ Rescheduled due to write error (e.g., service timeout).`);
          return; // Exit
        } finally {
            if (lockAcquired) {
                try { docLock.releaseLock(); console.log("Document Lock released."); }
                catch (rlErr) { console.error("CRITICAL: Failed to release Document Lock!", rlErr);}
            }
        }
        // --- End Document TryLock ---

      } else {
        // No valid rows prepared (e.g., all negative cache hits for this batch)
        console.log(`No valid rows to write for batch starting index ${requestStartIndex}. Advancing index.`);
        requestStartIndex = requestEndIndex; // Advance index
        SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
      }
    } // --- End while loop ---

    // --- Post-Loop Check ---
    if (requestStartIndex >= masterRequests.length) {
      console.log("All batches processed successfully. Setting state to FINALIZING.");
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.FINALIZING);
      // No reschedule needed, master orchestrator takes over
    } else {
       console.warn("Processing loop finished unexpectedly. Saving state.");
       SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
       SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
    }

  } // --- End if PROCESSING ---

   const totalDuration = (new Date().getTime() - START_TIME) / 1000;
   console.log(`updateMarketDataSheet execution finished in ${totalDuration.toFixed(2)} seconds. Final state: ${SCRIPT_PROP.getProperty(PROP_KEY_STEP)}`);
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

    try {
      // Use WaitLock for the swap - it MUST succeed or fail loudly
      withSheetLock(() => {
        const tempSheet = ss.getSheetByName(tempSheetName);
        const liveSheet = ss.getSheetByName(finalSheetName);
        const oldSheet = ss.getSheetByName(oldSheetName);

        // --- Pre-swap Validation ---
        if (!tempSheet) {
          throw new Error(`Critical: Sheet '${tempSheetName}' is missing during finalization!`);
        }
        if (tempSheet.getLastRow() <= 1) {
           throw new Error(`Critical: Sheet '${tempSheetName}' is empty or has only headers. Cannot swap.`);
        }

        console.log("Acquired Document Lock for sheet swap (WaitLock).");
        SpreadsheetApp.flush(); // Flush before rename

        // --- Atomic Swap using Rename ---
        if (oldSheet) {
          try {
            console.log(`Deleting existing '${oldSheetName}' sheet...`);
            ss.deleteSheet(oldSheet);
            SpreadsheetApp.flush(); // Flush after delete
             console.log(`Sheet '${oldSheetName}' deleted.`);
          } catch (delErr) {
             console.warn(`Could not delete existing '${oldSheetName}': ${delErr.message}. Proceeding.`);
          }
        }

        if (liveSheet) {
          console.log(`Renaming '${finalSheetName}' to '${oldSheetName}'.`);
          liveSheet.setName(oldSheetName);
          SpreadsheetApp.flush(); // Flush after first rename
          console.log(`Renamed to '${oldSheetName}'.`);
        } else {
          console.log(`Sheet '${finalSheetName}' not found. Skipping rename to old.`);
        }

        console.log(`Renaming '${tempSheetName}' to '${finalSheetName}'.`);
        tempSheet.setName(finalSheetName);
        tempSheet.showSheet(); // Make visible
        SpreadsheetApp.flush(); // Flush after second rename
        console.log(`Renamed to '${finalSheetName}' and shown.`);

        console.log("Atomic sheet swap successful.");

      }, 60000); // 60-second lock wait for critical swap

      // --- Post-Swap Cleanup ---
      _resetMarketDataJobState(null); // Pass null for success
      console.log("SUCCESS: Finalization complete. Job state reset.");

    } catch (swapError) {
      console.error(`CRITICAL ERROR during finalization swap: ${swapError.message}. Resetting state.`);
      if (swapError.stack) { console.error(`Stack: ${swapError.stack}`);}
      _resetMarketDataJobState(swapError); // Reset state on error
      throw swapError; // Re-throw
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

  console.log(`Attempting cleanup of sheet: ${oldSheetName} using TryLock...`);

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
      if (e.stack) { console.error(`Stack: ${e.stack}`);}
  } finally {
      if (lockAcquired) {
          try { docLock.releaseLock(); console.log(`Document Lock released for ${funcName}.`);}
          catch(rlErr) { console.error(`CRITICAL: Failed to release Document Lock for ${funcName}!`, rlErr); }
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
     ScriptApp.newTrigger('triggerLedgerImportCycle')
       .timeBased().hourly().create(); // Or whatever frequency it needs
     console.log('SUCCESS: Created hourly trigger for triggerLedgerImportCycle.');

  } catch (e) {
    console.error(`Failed to create new triggers: ${e.message}. Please check permissions and script validity.`);
  }
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

