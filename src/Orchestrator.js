// This global variable tracks the lock depth for a single execution.
var EXECUTION_LOCK_DEPTH = 0;
/**
 * Locks and executes a function, ensuring single execution.
 * Tracks lock depth to only log messages for the outermost call.
 * @param {function} func The function to execute.
 * @param {string} funcName A name for logging and trigger cleanup.
 * @param {function} [onFailure] Optional callback to run if 'func' throws an error.
 * @returns {boolean} True if execution started, false if skipped due to lock.
 */
function executeLocked(func, funcName, onFailure) { // <-- Added onFailure
  const lock = LockService.getScriptLock();
  if (lock.tryLock(30000)) { // Wait up to 30 seconds

    const isOuterLock = (EXECUTION_LOCK_DEPTH === 0);
    EXECUTION_LOCK_DEPTH++; // Increment depth

    try {
      if (isOuterLock) {
        // Clear triggers ONLY for the top-level call to prevent nested calls
        // from clearing triggers set by their parents.
        deleteTriggersByName(funcName);
        console.log(`--- Starting Execution: ${funcName} ---`);
      }

      func(); // --- Run the code ---

      if (isOuterLock) {
        console.log(`--- Finished Execution: ${funcName} ---`);
      }
    } catch (e) {
      console.error(`${funcName} failed: ${e.message}\nStack: ${e.stack}`);

      // --- NEW: Custom Failure Handling ---
      if (onFailure) {
        try {
          console.warn(`Running custom failure handler for ${funcName}...`);
          onFailure(e); // Pass the error object to the handler
        } catch (onFailureError) {
          console.error(`CRITICAL: The custom failure handler for ${funcName} ALSO failed: ${onFailureError.message}\nStack: ${onFailureError.stack}`);
        }
      }
      // --- End New ---

      throw e; // Re-throw to mark execution as "Failed" in Google's dashboard
    } finally {
      EXECUTION_LOCK_DEPTH--; // Decrement depth
      try {
        lock.releaseLock();
        if (isOuterLock) {
          console.log(`Lock released for ${funcName}.`);
        }
      } catch (lockError) {
        console.warn(`Failed to release lock for ${funcName}: ${lockError.message}`);
      }
    }
    return true; // Execution happened
  } else {
    console.warn(`${funcName} was skipped because another process held the lock.`);
    return false; // Execution was skipped
  }
}


/**
 * Processes the master list of market requests in multiple small batches within a single execution.
 * It uses a time-gated while loop to maximize work and reschedules itself immediately if time expires.
 * Uses a Step Property to guarantee clean state transitions.
 */
function updateMarketDataSheet() {

  // --- LOCAL CONSTANTS ---
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_REQUEST_INDEX = 'marketDataRequestIndex';
  const PROP_KEY_SHEET_ROW = 'marketDataNextWriteRow';

  // --- MODIFIED: Reduced BATCH_SIZE ---
  const BATCH_SIZE = 1000; // Was 1500
  // --- END MODIFICATION ---

  const TIME_LIMIT_MS = 255000; // 4 minutes 15 seconds (leave buffer before 6 min limit)
  const RESCHEDULE_DELAY_MS = 60 * 1000; // 1 minute

  const DATA_SHEET_HEADERS = ["cacheKey", "type_id", "location_type", "location_id", "sell_min", "buy_max", "sell_volume", "buy_volume", "last_updated"];
  const COLUMN_COUNT = DATA_SHEET_HEADERS.length;

  // --- JOB STATES ---
  const STEP = {
    NEW_RUN: 'NEW_RUN',
    PROCESSING: 'PROCESSING',
    FINALIZING: 'FINALIZING'
  };

  // Pass the reset handler to executeLocked
  executeLocked(() => {

    const START_TIME = new Date().getTime();
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const tempSheetName = 'Market_Data_Temp';
    const finalSheetName = 'Market_Data_Raw';

    let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP) || STEP.NEW_RUN;
    let sheet = ss.getSheetByName(tempSheetName);
    const masterRequests = getMasterBatchFromControlTable(); // Assumes this function exists

    // ----------------------------------------------------------------------
    // --- STATE MACHINE EXECUTION ---
    // ----------------------------------------------------------------------

    // --- Handle call while FINALIZING ---
    if (currentStep === STEP.FINALIZING) {
      console.warn(`State: ${STEP.FINALIZING}. A previous job is awaiting finalization.`);
      console.log("Re-triggering 'finalizeMarketDataUpdate' to ensure completion and exiting current run.");
      deleteTriggersByName('finalizeMarketDataUpdate');
      ScriptApp.newTrigger('finalizeMarketDataUpdate')
        .timeBased()
        .after(RESCHEDULE_DELAY_MS / 2) // Trigger sooner than normal reschedule
        .create();
      return; // Exit this execution
    }

    // --- STEP 1: NEW_RUN ---
    if (currentStep === STEP.NEW_RUN || !sheet || !masterRequests || masterRequests.length === 0) {
      currentStep = STEP.NEW_RUN;
      console.log(`State: ${STEP.NEW_RUN}. Preparing for new cycle.`);

      if (!masterRequests || masterRequests.length === 0) {
        console.warn("Master Control Table is empty or unreadable. Cannot start job.");
        _resetMarketDataJobState(new Error("Master Control Table empty")); // Reset state cleanly
        return;
      }

      if (!sheet) {
        console.warn(`Temporary sheet "${tempSheetName}" not found. Creating a new one.`);
        sheet = ss.insertSheet(tempSheetName);
         if (!sheet) { // Check if sheet creation actually worked
            throw new Error(`Failed to create temporary sheet "${tempSheetName}".`);
         }
      }

      // Clear sheet and set headers
      sheet.clearContents(); // Clear everything first
      sheet.getRange(1, 1, 1, COLUMN_COUNT).setValues([DATA_SHEET_HEADERS]);
      // Delete extra columns if necessary
      const maxColumns = sheet.getMaxColumns();
      if (maxColumns > COLUMN_COUNT) {
        sheet.deleteColumns(COLUMN_COUNT + 1, maxColumns - COLUMN_COUNT);
      }
       // Delete extra rows below header if necessary (more robust clear)
      const maxRows = sheet.getMaxRows();
      if (maxRows > 1) {
         sheet.deleteRows(2, maxRows - 1);
      }
      SpreadsheetApp.flush(); // Ensure sheet cleanup is done before proceeding


      SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, '0');
      SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, '2'); // Data starts on row 2
      currentStep = STEP.PROCESSING;
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STEP.PROCESSING);
      console.log(`Initialization complete. Transitioning to ${STEP.PROCESSING}.`);
    }

    // --- STEP 2: PROCESSING (Core Logic) ---
    if (currentStep === STEP.PROCESSING) {
      console.log(`State: ${STEP.PROCESSING}. Running data fetch and write loop.`);

      let requestStartIndex = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_REQUEST_INDEX) || '0', 10);
      let nextWriteRow = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_SHEET_ROW) || '2', 10);
      let batchesProcessedInThisRun = 0;

      sheet = ss.getSheetByName(tempSheetName); // Re-validate sheet reference
      if (!sheet) {
        // This is fatal, throw error to trigger the reset handler
        throw new Error(`Sheet "${tempSheetName}" disappeared mid-process. Resetting.`);
      }

      // ⭐ CORE WHILE LOOP WITH TIME CHECK
      while (requestStartIndex < masterRequests.length) {
        let currentTime = new Date().getTime();

        // Check time limit at the start of each potential batch
        if (currentTime - START_TIME > TIME_LIMIT_MS) {
          SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
          SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
          deleteTriggersByName('updateMarketDataSheet'); // Prevent duplicate triggers
          ScriptApp.newTrigger('updateMarketDataSheet').timeBased().after(RESCHEDULE_DELAY_MS).create();
          console.warn(`⚠️ Time limit hit after ${batchesProcessedInThisRun} batches. Job RESCHEDULED.`);
          return; // Exit this execution
        }

        // --- A. Define Current Batch & Fetch Data ---
        const requestEndIndex = Math.min(requestStartIndex + BATCH_SIZE, masterRequests.length);
        const requestsForThisRun = masterRequests.slice(requestStartIndex, requestEndIndex);
        if (requestsForThisRun.length === 0) break; // Should not happen

        // Assumes fuzAPI exists and handles its own errors reasonably
        const marketData = fuzAPI.getDataForRequests(requestsForThisRun);
        if (!marketData || marketData.length === 0) {
          console.warn(`API returned no data for requests ${requestStartIndex + 1} to ${requestEndIndex}. Skipping batch.`);
          requestStartIndex = requestEndIndex; // Advance index even if no data
          continue; // Move to the next batch
        }

        // --- B. Flatten Data & Write Chunk ---
        let allRowsToWrite = [];
        const currentTimeStamp = new Date(); // Use consistent timestamp for the batch
        marketData.forEach(crate => {
          crate.fuzObjects.forEach(item => {
            // Ensure data aligns with DATA_SHEET_HEADERS
            allRowsToWrite.push([
              "", // cacheKey - often derived later or unused
              item.type_id,
              crate.market_type, // location_type
              crate.market_id,   // location_id
              item.sell.min,
              item.buy.max,
              item.sell.volume,
              item.buy.volume,
              currentTimeStamp
            ]);
          });
        });

        if (allRowsToWrite.length > 0) {
           if (allRowsToWrite[0].length !== COLUMN_COUNT) {
               throw new Error(`Column count mismatch! Expected ${COLUMN_COUNT}, got ${allRowsToWrite[0].length}. Data sample: ${JSON.stringify(allRowsToWrite[0])}`);
           }

          // Check for needed rows just before writing
          const currentMaxRows = sheet.getMaxRows();
          const requiredRows = nextWriteRow + allRowsToWrite.length -1; // -1 because row index is 1-based
          if (currentMaxRows < requiredRows) {
            // Insert only the exact number of rows needed
             const rowsToAdd = requiredRows - currentMaxRows;
             console.log(`Inserting ${rowsToAdd} rows into ${tempSheetName} to accommodate data up to row ${requiredRows}. Current max: ${currentMaxRows}`);
             sheet.insertRowsAfter(currentMaxRows, rowsToAdd);
             // SpreadsheetApp.flush(); // Optional: Flush after insert if issues occur
          }

          // --- Check #2: Before the potentially slow write operation --- ⏰
          currentTime = new Date().getTime(); // Update current time
          if (currentTime - START_TIME > TIME_LIMIT_MS) {
            SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString()); // Save index BEFORE write
            SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());       // Save row BEFORE write
            deleteTriggersByName('updateMarketDataSheet'); // Prevent duplicate triggers
            ScriptApp.newTrigger('updateMarketDataSheet').timeBased().after(RESCHEDULE_DELAY_MS).create();
            console.warn(`⚠️ Time limit hit BEFORE WRITE of batch ${batchesProcessedInThisRun + 1}. Job RESCHEDULED.`);
            return; // Exit this execution
          }

          // --- Perform the write ---
          console.log(`Writing batch ${batchesProcessedInThisRun + 1} (${allRowsToWrite.length} rows) to range ${sheet.getName()}!A${nextWriteRow}:${COLUMN_COUNT}`);
          sheet.getRange(nextWriteRow, 1, allRowsToWrite.length, COLUMN_COUNT).setValues(allRowsToWrite);

          // --- MODIFIED: Removed SpreadsheetApp.flush() ---
          // SpreadsheetApp.flush();
          // --- END MODIFICATION ---

          nextWriteRow += allRowsToWrite.length;
          requestStartIndex = requestEndIndex;
          batchesProcessedInThisRun++;
          console.log(`Wrote batch ${batchesProcessedInThisRun}. Next request index: ${requestStartIndex}. Next write row: ${nextWriteRow}`);
        } else {
           console.log(`No data formatted for writing for requests ${requestStartIndex + 1} to ${requestEndIndex}. Skipping write.`);
           requestStartIndex = requestEndIndex; // Advance index even if nothing written
        }
      } // End of WHILE loop

      // --- TRANSITION TO FINALIZING ---
      if (requestStartIndex >= masterRequests.length) {
        console.log("All batches processed. Scheduling finalization step.");
        SCRIPT_PROP.setProperty(PROP_KEY_STEP, STEP.FINALIZING);
        deleteTriggersByName('finalizeMarketDataUpdate'); // Ensure only one finalizer trigger
        ScriptApp.newTrigger('finalizeMarketDataUpdate')
          .timeBased()
          .after(RESCHEDULE_DELAY_MS / 2) // Schedule finalizer relatively quickly
          .create();
        // Clear continuation trigger for *this* function as we are done processing
        deleteTriggersByName('updateMarketDataSheet');
        return; // Exit this execution, handover to finalizer
      }
      // (If it exits due to time limit, the rescheduling logic inside the loop handles it)
    }
    // --- STEP 3: FINALIZING handled by finalizeMarketDataUpdate ---

  }, "updateMarketDataSheet", _resetMarketDataJobState); // Pass the failure handler
}

/**
 * A centralized "panic button" function.
 * Resets the market data state machine to NEW_RUN, clearing all
 * indices and deleting any pending self-continuation triggers.
 * This is used as an onFailure callback for fatal errors.
 */
function _resetMarketDataJobState(error) {
  console.warn(`FATAL ERROR DETECTED in Market Data Job: ${error ? error.message : 'Unknown error'}.`);
  console.log("Rolling back market data job to NEW_RUN state...");

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_REQUEST_INDEX = 'marketDataRequestIndex';
  const PROP_KEY_SHEET_ROW = 'marketDataNextWriteRow';

  // 1. Force state back to NEW_RUN
  SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'NEW_RUN'); // Explicitly set to NEW_RUN

  // 2. Clear all progress properties
  SCRIPT_PROP.deleteProperty(PROP_KEY_REQUEST_INDEX);
  SCRIPT_PROP.deleteProperty(PROP_KEY_SHEET_ROW);

  // 3. Delete any pending triggers for this job's functions
  deleteTriggersByName('updateMarketDataSheet');
  deleteTriggersByName('finalizeMarketDataUpdate');

  console.log("Market data job state has been successfully reset.");
}


/**
 * Dedicated function for the final sheet update using Clear & Copy.
 * Triggered by updateMarketDataSheet after all processing is complete.
 */
function finalizeMarketDataUpdate() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const DATA_SHEET_HEADERS = ["cacheKey", "type_id", "location_type", "location_id", "sell_min", "buy_max", "sell_volume", "buy_volume", "last_updated"];
  const COLUMN_COUNT = DATA_SHEET_HEADERS.length;
  const STEP = { FINALIZING: 'FINALIZING', NEW_RUN: 'NEW_RUN' };
  const tempSheetName = 'Market_Data_Temp';
  const finalSheetName = 'Market_Data_Raw';

  // Pass reset handler to executeLocked
  executeLocked(() => {
    const currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP);

    if (currentStep !== STEP.FINALIZING) {
      console.warn(`finalizeMarketDataUpdate called unexpectedly in state: ${currentStep}. Aborting.`);
      deleteTriggersByName('finalizeMarketDataUpdate'); // Clean up self trigger just in case
      return;
    }

    console.log(`State: ${STEP.FINALIZING}. Starting final sheet update (Clear & Copy).`);
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sourceSheet = ss.getSheetByName(tempSheetName);
    let targetSheet = ss.getSheetByName(finalSheetName);

    if (!sourceSheet) {
      // This is fatal, trigger reset
      throw new Error(`Cannot finalize: Temporary sheet "${tempSheetName}" disappeared!`);
    }

    SpreadsheetApp.flush(); // Ensure prior writes to temp sheet are complete

    // --- 1. Prepare Target Sheet ---
    if (!targetSheet) {
      console.warn(`Final sheet "${finalSheetName}" not found. Creating it.`);
      targetSheet = ss.insertSheet(finalSheetName);
       if (!targetSheet) { throw new Error(`Failed to create target sheet "${finalSheetName}".`);}
      targetSheet.getRange(1, 1, 1, COLUMN_COUNT).setValues([DATA_SHEET_HEADERS]);
    } else {
      console.log(`Clearing existing data from "${finalSheetName}"...`);
      // Clear everything except headers
      const maxRows = targetSheet.getMaxRows();
      if (maxRows > 1) {
        targetSheet.getRange(2, 1, maxRows - 1, targetSheet.getMaxColumns()).clearContent();
      }
      SpreadsheetApp.flush(); // Ensure clear completes before copy
    }

    // --- 2. Copy Data ---
    const sourceDataRange = sourceSheet.getDataRange();
    const sourceDataHeight = sourceDataRange.getHeight();

    if (sourceDataHeight > 1) { // Check if there's data beyond headers
      const sourceValuesRange = sourceSheet.getRange(2, 1, sourceDataHeight - 1, COLUMN_COUNT); // Copy only data rows
      console.log(`Copying ${sourceDataHeight - 1} rows from temp to final sheet...`);

      // Ensure target sheet has enough rows
      const targetMaxRows = targetSheet.getMaxRows();
      if (targetMaxRows < sourceDataHeight) {
          // Calculate exact number needed, +1 for header row already present
          const rowsToAdd = sourceDataHeight - targetMaxRows;
          console.log(`Inserting ${rowsToAdd} rows into ${finalSheetName}. Current max: ${targetMaxRows}, Source height: ${sourceDataHeight}`);
          targetSheet.insertRowsAfter(targetMaxRows, rowsToAdd);
          // SpreadsheetApp.flush(); // Optional flush after insert
      }

      // Copy values only
      console.log(`Copying data to range ${targetSheet.getName()}!A2:${COLUMN_COUNT}${sourceDataHeight}`);
      sourceValuesRange.copyTo(targetSheet.getRange(2, 1), SpreadsheetApp.CopyPasteType.PASTE_VALUES, false);
      SpreadsheetApp.flush(); // Ensure copy completes
      console.log("Data copy complete.");
    } else {
      console.log("No data found in temp sheet to copy.");
    }

    // --- 3. Reset Temp Sheet for Next Run ---
    console.log(`Resetting "${tempSheetName}" for next cycle...`);
    const tempMaxRows = sourceSheet.getMaxRows();
    if (tempMaxRows > 1) { // Only clear if there's more than a header row
      sourceSheet.getRange(2, 1, tempMaxRows - 1, sourceSheet.getMaxColumns()).clearContent();
    }
    // Optional: Reset temp sheet size more aggressively
    // if (tempMaxRows > 2) { // Keep only header row
    //     sourceSheet.deleteRows(2, tempMaxRows - 1);
    // }

    // --- 4. Clear Properties (Job Complete) ---
    SCRIPT_PROP.deleteProperty(PROP_KEY_STEP);
    SCRIPT_PROP.deleteProperty('marketDataRequestIndex'); // Use exact key names
    SCRIPT_PROP.deleteProperty('marketDataNextWriteRow');

    console.log("SUCCESS: Market Data Job complete and system reset.");

  }, "finalizeMarketDataUpdate", _resetMarketDataJobState); // Pass reset handler
}


/**
 * The "Grill Captain" orchestrator. Processes the master list using a time-gated
 * while loop to maximize work per run, ensuring the cache stays warm.
 * Includes batch-level error handling to skip problematic batches.
 */
function fuzzworkCacheRefresh_TimeGated() {
  const SUB_BATCH_SIZE = 250;
  const TIME_LIMIT_MS = 270000; // 4 minutes 30 seconds
  const PROP_KEY = 'cacheRefresh_lastIndex';
  const properties = PropertiesService.getScriptProperties();

  // No custom failure handler needed at top level; batch errors handled internally.
  executeLocked(() => {
    const START_TIME = new Date().getTime();

    const allRequests = getMasterBatchFromControlTable(); // Assumes this exists
    if (!allRequests || allRequests.length === 0) {
      console.log("Control Table empty. Resetting cache refresh batch index.");
      properties.deleteProperty(PROP_KEY);
      return;
    }

    let startIndex = parseInt(properties.getProperty(PROP_KEY) || '0', 10);
    if (startIndex >= allRequests.length || isNaN(startIndex)) { // Added NaN check
      startIndex = 0;
      console.log("Cache refresh: Resetting index or completed full cycle. Starting over.");
    }

    let itemsProcessedThisRun = 0;

    // --- Time-Gated While Loop ---
    while (startIndex < allRequests.length) {
      const currentTime = new Date().getTime();

      if (currentTime - START_TIME > TIME_LIMIT_MS) {
        properties.setProperty(PROP_KEY, startIndex.toString());
        console.warn(`⚠️ Cache refresh time limit hit after processing ${itemsProcessedThisRun} items. Next run starts at index ${startIndex}.`);
        deleteTriggersByName('fuzzworkCacheRefresh_TimeGated'); // Prevent duplicates
        ScriptApp.newTrigger('fuzzworkCacheRefresh_TimeGated').timeBased().after(30 * 1000).create();
        return;
      }

      // --- Batch-level error handling ---
      let endIndex = Math.min(startIndex + SUB_BATCH_SIZE, allRequests.length); // Define endIndex here
      try {
        const currentSubBatch = allRequests.slice(startIndex, endIndex);

        if (currentSubBatch.length > 0) {
          console.log(`Processing cache sub-batch: ${startIndex + 1} to ${endIndex}`);
          // Assumes fuzAPI exists and handles its own errors reasonably
          fuzAPI.getDataForRequests(currentSubBatch);
          itemsProcessedThisRun += currentSubBatch.length;
        }

        // Move to the next index (only on success)
        startIndex = endIndex;

      } catch (batchError) {
        console.error(`--- ERROR ON CACHE BATCH ---`);
        console.error(`Failed to process cache batch starting at index ${startIndex}: ${batchError.message}. Skipping this batch.`);
        // CRITICAL: Advance past the "poison pill" batch
        startIndex = endIndex; // Use the pre-calculated endIndex
        console.warn(`Advanced index to ${startIndex} to skip problematic batch.`);
        properties.setProperty(PROP_KEY, startIndex.toString()); // Save progress immediately after skip
      }
      // --- End Batch-level error handling ---

    } // --- End of While Loop ---

    properties.setProperty(PROP_KEY, '0'); // Reset to 0 for the next full cycle
    console.log(`Cache refresh: Finished processing all ${allRequests.length} items in this cycle.`);
    // Clear continuation trigger as we finished successfully
    deleteTriggersByName('fuzzworkCacheRefresh_TimeGated');

  }, "fuzzworkCacheRefresh_TimeGated");
}


/**
 * This is the single "master" function you will set on a trigger.
 * It runs every 15 minutes and decides which 30-minute job to start.
 * If a job is skipped due to a lock, it schedules a one-time retry.
 */
function masterOrchestrator() {
  const currentMinute = new Date().getMinutes();
  const RETRY_DELAY_MS = 10 * 60 * 1000; // 10 minutes

  // Determine which function to run based on the time window
  let funcToRun;
  let funcName;

  if (currentMinute < 15 || (currentMinute >= 30 && currentMinute < 45)) {
    // HH:00 and HH:30 window -> Cache Refresh
    funcToRun = fuzzworkCacheRefresh_TimeGated;
    funcName = 'fuzzworkCacheRefresh_TimeGated';
  } else {
    // HH:15 and HH:45 window -> Market Update
    funcToRun = updateMarketDataSheet;
    funcName = 'updateMarketDataSheet';
  }

  console.log(`Master orchestrator (min ${currentMinute}): Dispatching to ${funcName}`);

  // Determine the appropriate failure handler
  const failureHandler = (funcName === 'updateMarketDataSheet') ? _resetMarketDataJobState : null;

  // Execute the selected function under lock, with potential failure handler
  const success = executeLocked(funcToRun, funcName, failureHandler);

  if (!success) {
    // If skipped due to lock, schedule a one-time retry
    console.warn(`Scheduling one-time retry for ${funcName}.`);
    // Ensure the retry trigger calls the *actual* function, not the master orchestrator
    scheduleOneTimeTrigger(funcName, RETRY_DELAY_MS);
  }
}

/**
 * Helper to create a new one-time "retry" trigger.
 * Ensures only one retry trigger exists per function name.
 * @param {string} functionName The name of the function to trigger.
 * @param {number} delayMs The milliseconds from now to run the trigger.
 */
function scheduleOneTimeTrigger(functionName, delayMs) {
  try {
    // Delete any OTHER pending retry triggers for this same function.
    deleteTriggersByName(functionName);

    // Create the new one-time trigger
    ScriptApp.newTrigger(functionName)
      .timeBased()
      .after(delayMs)
      .create();
    console.log(`Created one-time trigger for ${functionName} to run in ${Math.round(delayMs / 60000)} minutes.`);
  } catch (e) {
      console.error(`Failed to create or delete trigger for ${functionName}: ${e.message}`);
      // Log error but don't halt master orchestrator if trigger creation fails (e.g., too many triggers)
      // Consider adding notifications here if trigger failures are critical.
  }
}

/**
 * Run this function ONCE from the editor to set up or reset
 * the main 15-minute staggered trigger.
 * This also helps re-authorize the script.
 */
function setupStaggeredTriggers() {
  console.log("Setting up/Resetting triggers...");

  // 1. Clean up ALL potentially relevant triggers first
  deleteTriggersByName('fuzzworkCacheRefresh_TimeGated');
  deleteTriggersByName('updateMarketDataSheet');
  deleteTriggersByName('finalizeMarketDataUpdate');
  deleteTriggersByName('masterOrchestrator'); // Clean up self

  // 2. Create the new 15-minute master trigger
   try {
    ScriptApp.newTrigger('masterOrchestrator')
      .timeBased()
      .everyMinutes(15)
      .create();
    console.log('SUCCESS: Created 15-minute trigger for masterOrchestrator.');
   } catch (e) {
       console.error(`Failed to create masterOrchestrator trigger: ${e.message}`);
       SpreadsheetApp.getUi().alert(`Failed to create master trigger: ${e.message}. Check Apps Script Quotas.`);
   }
}

/**
 * Helper to delete all existing installable CLOCK triggers for a given function name.
 * @param {string} functionName The name of the function whose triggers should be deleted.
 */
function deleteTriggersByName(functionName) {
  try {
    const allTriggers = ScriptApp.getProjectTriggers();
    let deletedCount = 0;
    allTriggers.forEach(trigger => {
      // Check handler function AND event type (only delete time-based/CLOCK triggers)
      if (trigger.getHandlerFunction() === functionName &&
          trigger.getEventType() === ScriptApp.EventType.CLOCK) {
        try {
          ScriptApp.deleteTrigger(trigger);
          deletedCount++;
        } catch (e) {
          // Log specific deletion error but continue trying others
          console.warn(`Could not delete a trigger for ${functionName}: ${e.message}`);
        }
      }
    });
    if (deletedCount > 0) {
      console.log(`Deleted ${deletedCount} existing clock trigger(s) for ${functionName}.`);
    }
  } catch (e) {
      // Catch errors getting project triggers (e.g., permissions issues)
      console.error(`Error accessing or deleting triggers for ${functionName}: ${e.message}`);
  }
}

// Ensure fuzAPI and getMasterBatchFromControlTable are defined elsewhere
// Example stubs (if needed for testing in isolation):
// const fuzAPI = { getDataForRequests: function(reqs) { console.log(`Simulating fuzAPI call for ${reqs.length} requests`); return reqs.map(r => ({ type_id: r.type_id, market_type: r.location_type, market_id: r.location_id, fuzObjects: [{ type_id: r.type_id, sell: {min: 10, volume: 100}, buy: {max: 5, volume: 50} }] })); } };
// function getMasterBatchFromControlTable() { console.log("Simulating read from Control Table"); return Array.from({length: 18356}, (_, i) => ({type_id: 34+i, location_type: 'station', location_id: 60003760})); }
