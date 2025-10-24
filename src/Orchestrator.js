
// This global variable tracks the lock depth for a single execution.
var EXECUTION_LOCK_DEPTH = 0;
/**
 * Locks and executes a function, ensuring single execution.
 * Tracks lock depth to only log messages for the outermost call.
 * @param {function} func The function to execute.
 * @param {string} funcName A name for logging and trigger cleanup.
 * @returns {boolean} True if execution started, false if skipped due to lock.
 */
function executeLocked(func, funcName) {
  const lock = LockService.getScriptLock();
  if (lock.tryLock(30000)) { // Wait up to 30 seconds

    const isOuterLock = (EXECUTION_LOCK_DEPTH === 0);
    EXECUTION_LOCK_DEPTH++; // Increment depth

    try {
      // --- Only run these for the outermost call ---
      if (isOuterLock) {
        deleteTriggersByName(funcName);
        console.log(`--- Starting Execution: ${funcName} ---`);
      }

      // --- Run the code regardless of depth ---
      func();

      // --- Only log for the outermost call ---
      if (isOuterLock) {
        console.log(`--- Finished Execution: ${funcName} ---`);
      }
    } catch (e) {
      console.error(`${funcName} failed: ${e.message}\nStack: ${e.stack}`);
      throw e;
    } finally {
      EXECUTION_LOCK_DEPTH--; // Decrement depth

      try {
        lock.releaseLock();
        if (isOuterLock) { // Only log release for the outer call
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
 * This is the single "master" function you will set on a trigger.
 * It runs every 15 minutes and decides which 30-minute job to start.
 */
function masterOrchestrator() {
  const currentMinute = new Date().getMinutes();

  // --- Staggering Logic ---
  // Window 1 (0-14 min)  -> Run Cache Refresh
  // Window 2 (15-29 min) -> Run Market Update
  // Window 3 (30-44 min) -> Run Cache Refresh
  // Window 4 (45-59 min) -> Run Market Update

  if (currentMinute < 15 || (currentMinute >= 30 && currentMinute < 45)) {
    // Runs in the HH:00 and HH:30 windows
    console.log(`Master orchestrator (min ${currentMinute}): Dispatching to fuzzworkCacheRefresh_TimeGated`);
    fuzzworkCacheRefresh_TimeGated();
  } else {
    // Runs in the HH:15 and HH:45 windows (i.e., minutes 15-29 or 45-59)
    console.log(`Master orchestrator (min ${currentMinute}): Dispatching to updateMarketDataSheet`);
    updateMarketDataSheet();
  }
}

/**
 * The "Grill Captain" orchestrator. Processes the master list using a time-gated
 * while loop to maximize work per run, ensuring the cache stays warm.
 * Trigger this on a schedule (e.g., every 5-10 minutes).
 */
function fuzzworkCacheRefresh_TimeGated() {
  const SUB_BATCH_SIZE = 250; // Process items in smaller chunks within the time limit
  const TIME_LIMIT_MS = 270000; // 4 minutes 30 seconds
  const PROP_KEY = 'cacheRefresh_lastIndex';
  const properties = PropertiesService.getScriptProperties();

  executeLocked(() => {
    const START_TIME = new Date().getTime();

    // 1. Get the full list from the Control Table
    const allRequests = getMasterBatchFromControlTable();
    if (!allRequests || allRequests.length === 0) {
      console.log("Control Table empty. Resetting batch index.");
      properties.deleteProperty(PROP_KEY);
      return;
    }

    // 2. Get the starting index from the last run
    let startIndex = parseInt(properties.getProperty(PROP_KEY) || '0', 10);
    if (startIndex >= allRequests.length) {
      startIndex = 0; // Reset after finishing a full cycle
      console.log("Cache refresh: Completed full cycle. Starting over.");
    }

    let itemsProcessedThisRun = 0;

    // --- Time-Gated While Loop ---
    while (startIndex < allRequests.length) {
      const currentTime = new Date().getTime();

      // Check time BEFORE starting the next sub-batch
      if (currentTime - START_TIME > TIME_LIMIT_MS) {
        // Time expired, save progress and exit
        properties.setProperty(PROP_KEY, startIndex.toString());
        console.warn(`⚠️ Cache refresh time limit hit after processing ${itemsProcessedThisRun} items. Next run starts at index ${startIndex}.`);
        ScriptApp.newTrigger('fuzzworkCacheRefresh_TimeGated').timeBased().after(30 * 1000).create();
        return;
      }

      // 3. Define and process the next sub-batch
      const endIndex = Math.min(startIndex + SUB_BATCH_SIZE, allRequests.length);
      const currentSubBatch = allRequests.slice(startIndex, endIndex);

      if (currentSubBatch.length > 0) {
        console.log(`Processing sub-batch: ${startIndex + 1} to ${endIndex}`);
        fuzAPI.getDataForRequests(currentSubBatch); // Process the small chunk
        itemsProcessedThisRun += currentSubBatch.length;
      }

      // 4. Move to the next index
      startIndex = endIndex;

    } // --- End of While Loop ---

    // If the loop finished naturally, we've processed everything
    properties.setProperty(PROP_KEY, '0'); // Reset to 0 for the next full cycle
    console.log(`Cache refresh: Finished processing all ${allRequests.length} items.`);

  }, "fuzzworkCacheRefresh_TimeGated");
}

/**
 * Fetches market data in batches based on the Market_Control sheet,
 * writes it to a temporary sheet, and handles rescheduling if time runs out.
 * This function uses script properties to maintain state between executions.
 */

// This global variable tracks the lock depth for a single execution.
var EXECUTION_LOCK_DEPTH = 0;
/**
 * Locks and executes a function, ensuring single execution.
 * Tracks lock depth to only log messages for the outermost call.
 * @param {function} func The function to execute.
 * @param {string} funcName A name for logging and trigger cleanup.
 * @returns {boolean} True if execution started, false if skipped due to lock.
 */
function executeLocked(func, funcName) {
  const lock = LockService.getScriptLock();
  if (lock.tryLock(30000)) { // Wait up to 30 seconds

    const isOuterLock = (EXECUTION_LOCK_DEPTH === 0);
    EXECUTION_LOCK_DEPTH++; // Increment depth

    try {
      // --- Only run these for the outermost call ---
      if (isOuterLock) {
        deleteTriggersByName(funcName);
        console.log(`--- Starting Execution: ${funcName} ---`);
      }

      // --- Run the code regardless of depth ---
      func();

      // --- Only log for the outermost call ---
      if (isOuterLock) {
        console.log(`--- Finished Execution: ${funcName} ---`);
      }
    } catch (e) {
      console.error(`${funcName} failed: ${e.message}\nStack: ${e.stack}`);
      throw e;
    } finally {
      EXECUTION_LOCK_DEPTH--; // Decrement depth

      try {
        lock.releaseLock();
        if (isOuterLock) { // Only log release for the outer call
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
 * This is the single "master" function you will set on a trigger.
 * It runs every 15 minutes and decides which 30-minute job to start.
 */
function masterOrchestrator() {
  const currentMinute = new Date().getMinutes();

  // --- Staggering Logic ---
  // Window 1 (0-14 min)  -> Run Cache Refresh
  // Window 2 (15-29 min) -> Run Market Update
  // Window 3 (30-44 min) -> Run Cache Refresh
  // Window 4 (45-59 min) -> Run Market Update

  if (currentMinute < 15 || (currentMinute >= 30 && currentMinute < 45)) {
    // Runs in the HH:00 and HH:30 windows
    console.log(`Master orchestrator (min ${currentMinute}): Dispatching to fuzzworkCacheRefresh_TimeGated`);
    fuzzworkCacheRefresh_TimeGated();
  } else {
    // Runs in the HH:15 and HH:45 windows (i.e., minutes 15-29 or 45-59)
    console.log(`Master orchestrator (min ${currentMinute}): Dispatching to updateMarketDataSheet`);
    updateMarketDataSheet();
  }
}

/**
 * The "Grill Captain" orchestrator. Processes the master list using a time-gated
 * while loop to maximize work per run, ensuring the cache stays warm.
 * Trigger this on a schedule (e.g., every 5-10 minutes).
 */
function fuzzworkCacheRefresh_TimeGated() {
  const SUB_BATCH_SIZE = 250; // Process items in smaller chunks within the time limit
  const TIME_LIMIT_MS = 270000; // 4 minutes 30 seconds
  const PROP_KEY = 'cacheRefresh_lastIndex';
  const properties = PropertiesService.getScriptProperties();

  executeLocked(() => {
    const START_TIME = new Date().getTime();

    // 1. Get the full list from the Control Table
    const allRequests = getMasterBatchFromControlTable();
    if (!allRequests || allRequests.length === 0) {
      console.log("Control Table empty. Resetting batch index.");
      properties.deleteProperty(PROP_KEY);
      return;
    }

    // 2. Get the starting index from the last run
    let startIndex = parseInt(properties.getProperty(PROP_KEY) || '0', 10);
    if (startIndex >= allRequests.length) {
      startIndex = 0; // Reset after finishing a full cycle
      console.log("Cache refresh: Completed full cycle. Starting over.");
    }

    let itemsProcessedThisRun = 0;

    // --- Time-Gated While Loop ---
    while (startIndex < allRequests.length) {
      const currentTime = new Date().getTime();

      // Check time BEFORE starting the next sub-batch
      if (currentTime - START_TIME > TIME_LIMIT_MS) {
        // Time expired, save progress and exit
        properties.setProperty(PROP_KEY, startIndex.toString());
        console.warn(`⚠️ Cache refresh time limit hit after processing ${itemsProcessedThisRun} items. Next run starts at index ${startIndex}.`);
        ScriptApp.newTrigger('fuzzworkCacheRefresh_TimeGated').timeBased().after(30 * 1000).create();
        return;
      }

      // 3. Define and process the next sub-batch
      const endIndex = Math.min(startIndex + SUB_BATCH_SIZE, allRequests.length);
      const currentSubBatch = allRequests.slice(startIndex, endIndex);

      if (currentSubBatch.length > 0) {
        console.log(`Processing sub-batch: ${startIndex + 1} to ${endIndex}`);
        fuzAPI.getDataForRequests(currentSubBatch); // Process the small chunk
        itemsProcessedThisRun += currentSubBatch.length;
      }

      // 4. Move to the next index
      startIndex = endIndex;

    } // --- End of While Loop ---

    // If the loop finished naturally, we've processed everything
    properties.setProperty(PROP_KEY, '0'); // Reset to 0 for the next full cycle
    console.log(`Cache refresh: Finished processing all ${allRequests.length} items.`);

  }, "fuzzworkCacheRefresh_TimeGated");
}

/**
 * Fetches market data in batches based on the Market_Control sheet,
 * writes it to a temporary sheet, and handles rescheduling if time runs out.
 * This function uses script properties to maintain state between executions.
 */
function updateMarketDataSheet() {

  // --- LOCAL CONSTANTS ---
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = MARKET_DATA_STEP_PROP; // Use specific key
  const PROP_KEY_REQUEST_INDEX = MARKET_DATA_REQ_INDEX_PROP;
  const PROP_KEY_SHEET_ROW = MARKET_DATA_SHEET_ROW_PROP;

  const BATCH_SIZE = 1000; // Reduced batch size for testing
 // const TIME_LIMIT_MS = 255000; // 4 minutes 15 seconds
  const DATA_SHEET_HEADERS = ["cacheKey", "type_id", "location_type", "location_id", "sell_min", "buy_max", "sell_volume", "buy_volume", "last_updated"];
  const COLUMN_COUNT = DATA_SHEET_HEADERS.length;

  // --- JOB STATES (Specific to this job) ---
  const STEP = {
    NEW_RUN: 'NEW_RUN',
    PROCESSING: 'PROCESSING',
    FINALIZING: 'FINALIZING'
  };

  executeLocked(() => {

    const START_TIME = new Date().getTime();
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const tempSheetName = 'Market_Data_Temp';
    const finalSheetName = 'Market_Data_Raw';

    let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP) || STEP.NEW_RUN;
    let sheet = ss.getSheetByName(tempSheetName);
    const masterRequests = getMasterBatchFromControlTable(); // Assumes this function reads Market_Control

     // Always ensure triggers for this specific function are cleaned up at the start
     deleteTriggersByName('updateMarketDataSheet');
     deleteTriggersByName('finalizeMarketDataUpdate'); // Also clean up finalizer just in case


    // ----------------------------------------------------------------------
    // --- STATE MACHINE EXECUTION ---
    // ----------------------------------------------------------------------

    // --- Handle call while FINALIZING ---
    if (currentStep === STEP.FINALIZING) {
      console.warn(`State: ${STEP.FINALIZING}. A previous job is awaiting finalization.`);
      console.log("Re-triggering 'finalizeMarketDataUpdate' to ensure completion and resetting overall state to IDLE.");
      SCRIPT_PROP.setProperty(STATE_PROP, JOB_STATE.IDLE); // Reset global state
      ScriptApp.newTrigger('finalizeMarketDataUpdate')
        .timeBased()
        .after(5000) // Trigger sooner if just ensuring completion
        .create();
      return; // Exit this execution
    }

    // --- STEP 1: NEW_RUN ---
    if (currentStep === STEP.NEW_RUN || !sheet || !masterRequests || masterRequests.length === 0) {
      currentStep = STEP.NEW_RUN; // Set state explicitly
      console.log(`State: ${STEP.NEW_RUN}. Preparing for new cycle.`);

      if (!masterRequests || masterRequests.length === 0) {
        console.warn("Master Control Table is empty or unreadable. Cannot start job. Resetting state.");
        resetMarketDataJobState_(); // Use helper to reset state
        return;
      }

      // A. Ensure Temp Sheet exists and is clean
      if (!sheet) {
        console.warn(`Temporary sheet "${tempSheetName}" not found. Creating a new one.`);
        sheet = ss.insertSheet(tempSheetName);
      } else {
         // Clear only if sheet already existed
          sheet.clearContents(); // Clear everything including headers initially
      }

      // B. Set Headers
      sheet.getRange(1, 1, 1, COLUMN_COUNT).setValues([DATA_SHEET_HEADERS]);
      SpreadsheetApp.flush(); // Ensure headers are written before proceeding

      // C. Reset potentially oversized sheet dimensions
       const currentMaxRows = sheet.getMaxRows();
       if (currentMaxRows > 1) { // Only delete if more than header row exists
          // sheet.deleteRows(2, currentMaxRows - 1); // Remove old data rows efficiently - Causes Timeout
       }
       const currentMaxCols = sheet.getMaxColumns();
       if (currentMaxCols > COLUMN_COUNT) {
           sheet.deleteColumns(COLUMN_COUNT + 1, currentMaxCols - COLUMN_COUNT);
       }


      // D. Initialize Indices and Transition
      SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, '0');
      SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, '2'); // Start writing data from row 2
      currentStep = STEP.PROCESSING;
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STEP.PROCESSING);
      SCRIPT_PROP.setProperty(STATE_PROP, JOB_STATE.MARKET_DATA_RUNNING); // Set global state
      console.log(`Initialization complete. Transitioning to ${STEP.PROCESSING}.`);
    }

    // --- STEP 2: PROCESSING (Core Logic) ---
    if (currentStep === STEP.PROCESSING) {
      console.log(`State: ${STEP.PROCESSING}. Running data fetch and write loop.`);

      let requestStartIndex = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_REQUEST_INDEX) || '0');
      let nextWriteRow = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_SHEET_ROW) || '2');
      let batchesProcessedInThisRun = 0;

      sheet = ss.getSheetByName(tempSheetName); // Re-validate sheet reference
      if (!sheet) {
        console.error(`Sheet "${tempSheetName}" disappeared during processing. Resetting job.`);
        resetMarketDataJobState_();
        // No rescheduling here, master orchestrator will pick it up next time
        return;
      }


      // ⭐ CORE WHILE LOOP WITH TIME CHECK
      while (requestStartIndex < masterRequests.length) {
        let currentTime = new Date().getTime();

        // Check time limit at the start of each potential batch
        if (currentTime - START_TIME > TIME_LIMIT_MS) {
          SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
          SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
          // DO NOT set global state back to IDLE here. Job is still running.
          ScriptApp.newTrigger('updateMarketDataSheet').timeBased().after(RESCHEDULE_DELAY_MS).create();
          console.warn(`⚠️ Time limit hit after ${batchesProcessedInThisRun} batches. Job RESCHEDULED.`);
          return; // Exit this execution, keeping global state as RUNNING
        }

        // --- A. Define Current Batch & Fetch Data ---
        const requestEndIndex = Math.min(requestStartIndex + BATCH_SIZE, masterRequests.length);
        const requestsForThisRun = masterRequests.slice(requestStartIndex, requestEndIndex);
        if (requestsForThisRun.length === 0) break;

        const marketData = fuzAPI.getDataForRequests(requestsForThisRun); // Assumes fuzAPI exists
        if (!marketData || marketData.length === 0) {
          console.warn(`API returned no data for requests ${requestStartIndex} to ${requestEndIndex}. Skipping batch.`);
          requestStartIndex = requestEndIndex; // Advance index even if no data
          continue;
        }

        // --- B. Flatten Data & Prepare for Write ---
        let allRowsToWrite = [];
        const currentTimeStamp = new Date(); // Use a single timestamp for the batch
        marketData.forEach(crate => {
          crate.fuzObjects.forEach(item => {
            // Ensure data structure matches DATA_SHEET_HEADERS
            allRowsToWrite.push([
              "", // cacheKey - assuming it's generated/handled elsewhere or not needed here
              item.type_id,
              crate.market_type, // location_type
              crate.market_id,   // location_id
              item.sell.min,
              item.buy.max,
              item.sell.volume,
              item.buy.volume,
              currentTimeStamp // last_updated
            ]);
          });
        });

        if (allRowsToWrite.length > 0) {
           if (allRowsToWrite[0].length !== COLUMN_COUNT) {
               console.error(`Column count mismatch! Expected ${COLUMN_COUNT}, but data row has ${allRowsToWrite[0].length}. Data: ${JSON.stringify(allRowsToWrite[0])}`);
               // Decide how to handle: skip batch, throw error, etc.
               // For now, log error and skip batch to avoid writing bad data.
               requestStartIndex = requestEndIndex;
               continue;
            }


          // --- Check Time AGAIN Before Writing ---
           currentTime = new Date().getTime();
           if (currentTime - START_TIME > TIME_LIMIT_MS) {
               SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString()); // Save index BEFORE potential write
               SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
               ScriptApp.newTrigger('updateMarketDataSheet').timeBased().after(RESCHEDULE_DELAY_MS).create();
               console.warn(`⚠️ Time limit hit BEFORE WRITE of batch ${batchesProcessedInThisRun + 1}. Job RESCHEDULED.`);
               return;
           }

            // --- THE FIX: REMOVE insertRowsAfter ---
            // The getRange().setValues() call below is sufficient and much faster.
            // It will automatically expand the sheet if the range exceeds current bounds.
            /*
            const currentMaxRows = sheet.getMaxRows();
            const requiredRows = nextWriteRow + allRowsToWrite.length -1; // -1 because row index is 1-based
            if (currentMaxRows < requiredRows) {
                const rowsToAdd = requiredRows - currentMaxRows;
                console.log(`Inserting ${rowsToAdd} rows into ${tempSheetName} to accommodate data up to row ${requiredRows}. Current max: ${currentMaxRows}`);
                // THIS IS THE SLOW OPERATION TO REMOVE:
                 sheet.insertRowsAfter(currentMaxRows, rowsToAdd);
                 SpreadsheetApp.flush(); // Make sure rows are added before writing
            }
            */
           // --- END FIX ---


          // --- Write the Data Batch ---
          console.log(`Writing batch ${batchesProcessedInThisRun + 1} (${allRowsToWrite.length} rows) to range ${tempSheetName}!A${nextWriteRow}:${COLUMN_COUNT}`);
          sheet.getRange(nextWriteRow, 1, allRowsToWrite.length, COLUMN_COUNT).setValues(allRowsToWrite);

          // --- Update State AFTER Successful Write ---
          nextWriteRow += allRowsToWrite.length;
          requestStartIndex = requestEndIndex;
          batchesProcessedInThisRun++;
          console.log(`Wrote batch ${batchesProcessedInThisRun}. Next request index: ${requestStartIndex}. Next write row: ${nextWriteRow}`);
          // SpreadsheetApp.flush(); // Optional: force write, but can slow things down

        } else {
          console.log(`No valid data rows constructed for batch starting at ${requestStartIndex}.`);
          requestStartIndex = requestEndIndex; // Still advance index
        }
      } // End of WHILE loop

      // --- TRANSITION TO FINALIZING ---
      if (requestStartIndex >= masterRequests.length) {
        console.log("All batches processed. Scheduling finalization step.");
        SCRIPT_PROP.setProperty(PROP_KEY_STEP, STEP.FINALIZING);
        // Global state remains MARKET_DATA_RUNNING until finalizer completes
        ScriptApp.newTrigger('finalizeMarketDataUpdate')
          .timeBased()
          .after(5000) // Short delay before finalizing
          .create();
        return; // Exit this execution
      }
    } // End of PROCESSING step

  }, "updateMarketDataSheet"); // End executeLocked
}

/**
 * Dedicated function for the final sheet update using Clear & Copy.
 * Triggered by updateMarketDataSheet after all processing is complete.
 */
function finalizeMarketDataUpdate() {
  // --- LOCAL CONSTANTS ---
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_REQUEST_INDEX = 'marketDataRequestIndex';
  const PROP_KEY_SHEET_ROW = 'marketDataNextWriteRow';
  const DATA_SHEET_HEADERS = ["cacheKey", "type_id", "location_type", "location_id", "sell_min", "buy_max", "sell_volume", "buy_volume", "last_updated"];
  const COLUMN_COUNT = DATA_SHEET_HEADERS.length;
  const STEP = { FINALIZING: 'FINALIZING', NEW_RUN: 'NEW_RUN' };
  const tempSheetName = 'Market_Data_Temp';
  const finalSheetName = 'Market_Data_Raw';

  // Note: executeLocked will call deleteTriggersByName('finalizeMarketDataUpdate') on success
  executeLocked(() => {
    const currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP);

    // --- State Check ---
    if (currentStep !== STEP.FINALIZING) {
      console.warn(`finalizeMarketDataUpdate called unexpectedly in state: ${currentStep}. Aborting.`);
      // Clean up any stray triggers just in case
      deleteTriggersByName('finalizeMarketDataUpdate');
      return;
    }

    console.log(`State: ${STEP.FINALIZING}. Starting final sheet update (Clear & Copy).`);
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sourceSheet = ss.getSheetByName(tempSheetName);
    let targetSheet = ss.getSheetByName(finalSheetName);

    if (!sourceSheet) {
      console.error("Cannot finalize: Temporary sheet disappeared!");
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STEP.NEW_RUN); // Reset
      return;
    }

    try {
      SpreadsheetApp.flush(); // Ensure prior writes are complete

      // --- 1. Prepare Target Sheet ---
      if (!targetSheet) {
        console.warn(`Final sheet "${finalSheetName}" not found. Creating it.`);
        targetSheet = ss.insertSheet(finalSheetName);
        targetSheet.getRange(1, 1, 1, COLUMN_COUNT).setValues([DATA_SHEET_HEADERS]); // Set headers if new
      } else {
        console.log(`Clearing existing data from "${finalSheetName}"...`);
        // Clear everything except headers
        const maxRows = targetSheet.getMaxRows();
        if (maxRows > 1) { // Only clear if there's more than a header row
          targetSheet.getRange(2, 1, maxRows - 1, targetSheet.getMaxColumns()).clearContent();
        }
        SpreadsheetApp.flush(); // Ensure clear completes
      }

      // --- 2. Copy Data ---
      const sourceDataRange = sourceSheet.getDataRange();
      const sourceDataHeight = sourceDataRange.getHeight();

      if (sourceDataHeight > 1) { // Check if there's data beyond headers
        const sourceValuesRange = sourceSheet.getRange(2, 1, sourceDataHeight - 1, COLUMN_COUNT);
        console.log(`Copying ${sourceDataHeight - 1} rows from temp to final sheet...`);

        // Ensure target sheet has enough rows (avoids errors if target was smaller)
        const targetMaxRows = targetSheet.getMaxRows();
        if (targetMaxRows < sourceDataHeight) {
          targetSheet.insertRowsAfter(targetMaxRows, sourceDataHeight - targetMaxRows);
        }

        // Copy values 
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

      // --- 4. Clear Properties (Job Complete) ---
      SCRIPT_PROP.deleteProperty(PROP_KEY_STEP);
      SCRIPT_PROP.deleteProperty(PROP_KEY_REQUEST_INDEX);
      SCRIPT_PROP.deleteProperty(PROP_KEY_SHEET_ROW);

      console.log("SUCCESS: Job complete and system reset.");

    } catch (e) {
      console.error(`Finalization (Clear & Copy) failed: ${e.message}\nStack: ${e.stack}`);
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STEP.NEW_RUN); // Reset state on failure
      throw e; // Re-throw to make executeLocked aware of the failure
    } finally {
      // Clean up trigger *just in case* executeLocked fails
      // (though executeLocked should handle this on its own)
      deleteTriggersByName('finalizeMarketDataUpdate');
    }

  }, "finalizeMarketDataUpdate");
}


/**
 * This is the single "master" function you will set on a trigger.
 * It runs every 15 minutes and decides which 30-minute job to start.
 * If a job is skipped due to a lock, it schedules a one-time retry.
 */
function masterOrchestrator() {
  const currentMinute = new Date().getMinutes();
  const RETRY_DELAY_MS = 10 * 60 * 1000; // 10 minutes

  // Window 1 (0-14 min)  -> Run Cache Refresh
  // Window 2 (15-29 min) -> Run Market Update
  // Window 3 (30-44 min) -> Run Cache Refresh
  // Window 4 (45-59 min) -> Run Market Update

  if (currentMinute < 15 || (currentMinute >= 30 && currentMinute < 45)) {
    // --- HH:00 and HH:30 window ---
    const funcToRun = fuzzworkCacheRefresh_TimeGated;
    const funcName = 'fuzzworkCacheRefresh_TimeGated';

    console.log(`Master orchestrator (min ${currentMinute}): Dispatching to ${funcName}`);
    const success = executeLocked(funcToRun, funcName);

    if (!success) {
      // --- NEW LOGIC ---
      console.warn(`Scheduling one-time retry for ${funcName}.`);
      scheduleOneTimeTrigger(funcName, RETRY_DELAY_MS);
    }

  } else {
    // --- HH:15 and HH:45 window ---
    const funcToRun = updateMarketDataSheet;
    const funcName = 'updateMarketDataSheet';

    console.log(`Master orchestrator (min ${currentMinute}): Dispatching to ${funcName}`);
    const success = executeLocked(funcToRun, funcName);

    if (!success) {
      // --- NEW LOGIC ---
      console.warn(`Scheduling one-time retry for ${funcName}.`);
      scheduleOneTimeTrigger(funcName, RETRY_DELAY_MS);
    }
  }
}

/**
 * Helper to create a new one-time "retry" trigger.
 * @param {string} functionName The name of the function to trigger.
 * @param {number} delayMs The milliseconds from now to run the trigger.
 */
function scheduleOneTimeTrigger(functionName, delayMs) {
  // First, delete any OTHER pending retry triggers for this same function
  // to prevent a "retry storm". We only want one retry scheduled at a time.
  deleteTriggersByName(functionName);

  // Create the new one-time trigger
  ScriptApp.newTrigger(functionName)
    .timeBased()
    .after(delayMs)
    .create();
  console.log(`Created one-time trigger for ${functionName} to run in ${delayMs / 60000} minutes.`);
}

/**
 * Run this function ONCE from the editor to create
 * the new 15-minute staggered trigger.
 * This will also force a re-authorization, fixing permission errors.
 */
function setupStaggeredTriggers() {
  // 1. Clean up ALL old triggers for the functions
  console.log("Deleting old triggers...");
  deleteTriggersByName('fuzzworkCacheRefresh_TimeGated');
  deleteTriggersByName('updateMarketDataSheet');
  deleteTriggersByName('masterOrchestrator'); // Clean up self first

  // 2. Create the new 15-minute master trigger
  ScriptApp.newTrigger('masterOrchestrator')
    .timeBased()
    .everyMinutes(15)
    .create();

  console.log('SUCCESS: Created 15-minute trigger for masterOrchestrator.');
}

/**
 * Helper to delete all existing installable triggers for a given function name.
 * @param {string} functionName The name of the function to check for triggers.
 */
function deleteTriggersByName(functionName) {
  const allTriggers = ScriptApp.getProjectTriggers();
  allTriggers.forEach(trigger => {
    // Only delete time-based triggers
    if (trigger.getHandlerFunction() === functionName &&
      trigger.getEventType() === ScriptApp.EventType.CLOCK) {
      try {
        ScriptApp.deleteTrigger(trigger);
        console.log(`Deleted existing clock trigger for ${functionName}.`);
      } catch (e) {
        console.warn(`Could not delete trigger for ${functionName}: ${e.message}`);
      }
    }
  });
}


// NOTE: This assumes you have the 'executeLocked', 'getMasterBatchFromControlTable',
// and 'fuzAPI' functions defined elsewhere in your script.

// --- Add your other global orchestrator functions here ---
// function fuzzPriceDataByHub() { executeLocked(() => { /* ... */ }, "fuzzPriceDataByHub"); }
// function fuzzApiPriceDataJitaSell() { executeLocked(() => { /* ... */ }, "fuzzApiPriceDataJitaSell"); }

/**
 * Dedicated function for the final sheet update using Clear & Copy.
 * Triggered by updateMarketDataSheet after all processing is complete.
 */
function finalizeMarketDataUpdate() {
  // --- LOCAL CONSTANTS ---
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_REQUEST_INDEX = 'marketDataRequestIndex';
  const PROP_KEY_SHEET_ROW = 'marketDataNextWriteRow';
  const DATA_SHEET_HEADERS = ["cacheKey", "type_id", "location_type", "location_id", "sell_min", "buy_max", "sell_volume", "buy_volume", "last_updated"];
  const COLUMN_COUNT = DATA_SHEET_HEADERS.length;
  const STEP = { FINALIZING: 'FINALIZING', NEW_RUN: 'NEW_RUN' };
  const tempSheetName = 'Market_Data_Temp';
  const finalSheetName = 'Market_Data_Raw';

  // Note: executeLocked will call deleteTriggersByName('finalizeMarketDataUpdate') on success
  executeLocked(() => {
    const currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP);

    // --- State Check ---
    if (currentStep !== STEP.FINALIZING) {
      console.warn(`finalizeMarketDataUpdate called unexpectedly in state: ${currentStep}. Aborting.`);
      // Clean up any stray triggers just in case
      deleteTriggersByName('finalizeMarketDataUpdate');
      return;
    }

    console.log(`State: ${STEP.FINALIZING}. Starting final sheet update (Clear & Copy).`);
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sourceSheet = ss.getSheetByName(tempSheetName);
    let targetSheet = ss.getSheetByName(finalSheetName);

    if (!sourceSheet) {
      console.error("Cannot finalize: Temporary sheet disappeared!");
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STEP.NEW_RUN); // Reset
      return;
    }

    try {
      SpreadsheetApp.flush(); // Ensure prior writes are complete

      // --- 1. Prepare Target Sheet ---
      if (!targetSheet) {
        console.warn(`Final sheet "${finalSheetName}" not found. Creating it.`);
        targetSheet = ss.insertSheet(finalSheetName);
        targetSheet.getRange(1, 1, 1, COLUMN_COUNT).setValues([DATA_SHEET_HEADERS]); // Set headers if new
      } else {
        console.log(`Clearing existing data from "${finalSheetName}"...`);
        // Clear everything except headers
        const maxRows = targetSheet.getMaxRows();
        if (maxRows > 1) { // Only clear if there's more than a header row
          targetSheet.getRange(2, 1, maxRows - 1, targetSheet.getMaxColumns()).clearContent();
        }
        SpreadsheetApp.flush(); // Ensure clear completes
      }

      // --- 2. Copy Data ---
      const sourceDataRange = sourceSheet.getDataRange();
      const sourceDataHeight = sourceDataRange.getHeight();

      if (sourceDataHeight > 1) { // Check if there's data beyond headers
        const sourceValuesRange = sourceSheet.getRange(2, 1, sourceDataHeight - 1, COLUMN_COUNT);
        console.log(`Copying ${sourceDataHeight - 1} rows from temp to final sheet...`);

        // Ensure target sheet has enough rows (avoids errors if target was smaller)
        const targetMaxRows = targetSheet.getMaxRows();
        if (targetMaxRows < sourceDataHeight) {
          targetSheet.insertRowsAfter(targetMaxRows, sourceDataHeight - targetMaxRows);
        }

        // Copy values 
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

      // --- 4. Clear Properties (Job Complete) ---
      SCRIPT_PROP.deleteProperty(PROP_KEY_STEP);
      SCRIPT_PROP.deleteProperty(PROP_KEY_REQUEST_INDEX);
      SCRIPT_PROP.deleteProperty(PROP_KEY_SHEET_ROW);

      console.log("SUCCESS: Job complete and system reset.");

    } catch (e) {
      console.error(`Finalization (Clear & Copy) failed: ${e.message}\nStack: ${e.stack}`);
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STEP.NEW_RUN); // Reset state on failure
      throw e; // Re-throw to make executeLocked aware of the failure
    } finally {
      // Clean up trigger *just in case* executeLocked fails
      // (though executeLocked should handle this on its own)
      deleteTriggersByName('finalizeMarketDataUpdate');
    }

  }, "finalizeMarketDataUpdate");
}


/**
 * This is the single "master" function you will set on a trigger.
 * It runs every 15 minutes and decides which 30-minute job to start.
 * If a job is skipped due to a lock, it schedules a one-time retry.
 */
function masterOrchestrator() {
  const currentMinute = new Date().getMinutes();
  const RETRY_DELAY_MS = 10 * 60 * 1000; // 10 minutes

  // Window 1 (0-14 min)  -> Run Cache Refresh
  // Window 2 (15-29 min) -> Run Market Update
  // Window 3 (30-44 min) -> Run Cache Refresh
  // Window 4 (45-59 min) -> Run Market Update

  if (currentMinute < 15 || (currentMinute >= 30 && currentMinute < 45)) {
    // --- HH:00 and HH:30 window ---
    const funcToRun = fuzzworkCacheRefresh_TimeGated;
    const funcName = 'fuzzworkCacheRefresh_TimeGated';

    console.log(`Master orchestrator (min ${currentMinute}): Dispatching to ${funcName}`);
    const success = executeLocked(funcToRun, funcName);

    if (!success) {
      // --- NEW LOGIC ---
      console.warn(`Scheduling one-time retry for ${funcName}.`);
      scheduleOneTimeTrigger(funcName, RETRY_DELAY_MS);
    }

  } else {
    // --- HH:15 and HH:45 window ---
    const funcToRun = updateMarketDataSheet;
    const funcName = 'updateMarketDataSheet';

    console.log(`Master orchestrator (min ${currentMinute}): Dispatching to ${funcName}`);
    const success = executeLocked(funcToRun, funcName);

    if (!success) {
      // --- NEW LOGIC ---
      console.warn(`Scheduling one-time retry for ${funcName}.`);
      scheduleOneTimeTrigger(funcName, RETRY_DELAY_MS);
    }
  }
}

/**
 * Helper to create a new one-time "retry" trigger.
 * @param {string} functionName The name of the function to trigger.
 * @param {number} delayMs The milliseconds from now to run the trigger.
 */
function scheduleOneTimeTrigger(functionName, delayMs) {
  // First, delete any OTHER pending retry triggers for this same function
  // to prevent a "retry storm". We only want one retry scheduled at a time.
  deleteTriggersByName(functionName);

  // Create the new one-time trigger
  ScriptApp.newTrigger(functionName)
    .timeBased()
    .after(delayMs)
    .create();
  console.log(`Created one-time trigger for ${functionName} to run in ${delayMs / 60000} minutes.`);
}

/**
 * Run this function ONCE from the editor to create
 * the new 15-minute staggered trigger.
 * This will also force a re-authorization, fixing permission errors.
 */
function setupStaggeredTriggers() {
  // 1. Clean up ALL old triggers for the functions
  console.log("Deleting old triggers...");
  deleteTriggersByName('fuzzworkCacheRefresh_TimeGated');
  deleteTriggersByName('updateMarketDataSheet');
  deleteTriggersByName('masterOrchestrator'); // Clean up self first

  // 2. Create the new 15-minute master trigger
  ScriptApp.newTrigger('masterOrchestrator')
    .timeBased()
    .everyMinutes(15)
    .create();

  console.log('SUCCESS: Created 15-minute trigger for masterOrchestrator.');
}

/**
 * Helper to delete all existing installable triggers for a given function name.
 * @param {string} functionName The name of the function to check for triggers.
 */
function deleteTriggersByName(functionName) {
  const allTriggers = ScriptApp.getProjectTriggers();
  allTriggers.forEach(trigger => {
    // Only delete time-based triggers
    if (trigger.getHandlerFunction() === functionName &&
      trigger.getEventType() === ScriptApp.EventType.CLOCK) {
      try {
        ScriptApp.deleteTrigger(trigger);
        console.log(`Deleted existing clock trigger for ${functionName}.`);
      } catch (e) {
        console.warn(`Could not delete trigger for ${functionName}: ${e.message}`);
      }
    }
  });
}


// NOTE: This assumes you have the 'executeLocked', 'getMasterBatchFromControlTable',
// and 'fuzAPI' functions defined elsewhere in your script.

// --- Add your other global orchestrator functions here ---
// function fuzzPriceDataByHub() { executeLocked(() => { /* ... */ }, "fuzzPriceDataByHub"); }
// function fuzzApiPriceDataJitaSell() { executeLocked(() => { /* ... */ }, "fuzzApiPriceDataJitaSell"); }