// This global variable tracks the lock depth for a single execution.
var EXECUTION_LOCK_DEPTH = 0;

// State Machine Constants (Internal steps for the Market Data job)
const STATE_FLAGS = {
  NEW_RUN: 'NEW_RUN',
  PROCESSING: 'PROCESSING',
  FINALIZING: 'FINALIZING'
};

/**
 * Locks and executes a function, ensuring single execution.
 * @param {function} func The function to execute.
 * @param {string} funcName Adequate name for the lock.
 * @returns {boolean} True if execution started successfully, false if skipped due to lock.
 */
function executeLocked(func, funcName) {
  const lock = LockService.getScriptLock();
  if (lock.tryLock(30000)) { // Wait up to 30 seconds

    const isOuterLock = (EXECUTION_LOCK_DEPTH === 0);
    EXECUTION_LOCK_DEPTH++;

    try {
      if (isOuterLock) {
        deleteTriggersByName(funcName);
        console.log(`--- Starting Execution: ${funcName} ---`);
      }

      func(); // Run the actual function logic

      if (isOuterLock) {
        console.log(`--- Finished Execution: ${funcName} ---`);
      }
    } catch (e) {
      console.error(`${funcName} failed: ${e.message}\nStack: ${e.stack}`);
      // Only market data functions use the specific reset handler
      if (funcName === 'updateMarketDataSheet' || funcName === 'finalizeMarketDataUpdate') {
        console.warn("Attempting to reset Market Data job state due to failure.");
        _resetMarketDataJobState(e);
      }
      throw e; // Re-throw to mark execution as failed
    } finally {
      EXECUTION_LOCK_DEPTH--;
      try {
        lock.releaseLock();
        if (isOuterLock) {
          console.log(`Lock released for ${funcName}.`);
        }
      } catch (lockError) {
        console.warn(`Failed to release lock for ${funcName}: ${lockError.message}`);
      }
    }
    return true; // Execution started and ran
  } else {
    console.warn(`${funcName} was skipped because another process held the lock.`);
    return false; // Execution was skipped
  }
}

/**
 * This is the single "master" function you will set on a trigger (every 15 min).
 * It runs jobs based on time windows, prioritizing finalization.
 * The lock itself manages all concurrency conflicts.
 */
function masterOrchestrator() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');
  const currentMinute = new Date().getMinutes();

  // --- Priority 1: Market Data Finalization Check (MUST be run first) ---
  if (marketDataStep === STATE_FLAGS.FINALIZING) {
    console.log(`Master orchestrator: Market data state requires cleanup/finalization.`);

    // 1. Attempt to delete old sheet FIRST. This is the SLOW step.
    const cleanupSuccess = executeLocked(cleanupOldSheet, 'cleanupOldSheet');

    const delay = 30 * 1000; // 30 seconds wait before swapping

    if (cleanupSuccess) {
      console.log("Cleanup succeeded or was attempted. Scheduling final swap now.");
      // If cleanup succeeded (or was attempted and completed execution), schedule the fast final swap.
      scheduleOneTimeTrigger("finalizeMarketDataUpdate", delay);
    } else {
      // If cleanup was skipped due to lock, the next master run (in 15 min) will try cleanup again.
      console.warn("Cleanup skipped due to lock. Deferring finalization retry to next master run.");
    }
    return; // Prioritize finalization, then exit.
  }

  console.log(`Master orchestrator (min ${currentMinute}): Checking time window.`);

  // --- Staggering Logic ---
  // Windows 1, 3, & 4 (0-14, 30-44, 45-59 min): Cache Warmer (75% time)
  // Window 2 (15-29 min): Market Update (25% time)

  if (currentMinute < 15 || currentMinute >= 30) { // Covers 0-14, 30-44, and 45-59
    // --- Window 1, 3, & 4: Cache Warmer (High Availability) ---
    console.log(`Master orchestrator (min ${currentMinute}): Dispatching CACHE WARMER wrapper.`);
    executeLocked(triggerCacheWarmerWithRetry, 'triggerCacheWarmerWithRetry');
  } else { // currentMinute >= 15 && currentMinute < 30
    // --- Window 2: Market Data Update (High Priority) ---
    console.log(`Master orchestrator (min ${currentMinute}): Dispatching MARKET DATA UPDATE.`);
    executeLocked(updateMarketDataSheet, 'updateMarketDataSheet');
  }
}

/**
 * Wrapper function for the cache warmer.
 * Attempts to run the cache warmer using executeLocked.
 * If skipped due to lock, it schedules a one-time retry trigger for itself.
 */
function triggerCacheWarmerWithRetry() {
  const funcToRun = fuzzworkCacheRefresh_TimeGated;
  const funcName = 'fuzzworkCacheRefresh_TimeGated';
  const wrapperFuncName = 'triggerCacheWarmerWithRetry';
  const retryDelayMs = 2 * 60 * 1000; // 2 minutes retry delay

  console.log(`Wrapper ${wrapperFuncName} called. Attempting to run ${funcToRun.name}...`);

  // Try to execute the actual cache warmer
  const executionStarted = executeLocked(funcToRun, funcName);

  if (!executionStarted) {
    // Lock was busy (e.g., market update running)
    console.warn(`${funcName} skipped due to lock. Scheduling one-time retry for ${wrapperFuncName}.`);
    scheduleOneTimeTrigger(wrapperFuncName, retryDelayMs);
  }
}


/**
 * Cache refresh function. Called by wrapper.
 */
function fuzzworkCacheRefresh_TimeGated() {
  const SUB_BATCH_SIZE = 250;
  const TIME_LIMIT_MS = 270000; // 4m 30s
  const PROP_KEY = 'cacheRefresh_lastIndex';
  const properties = PropertiesService.getScriptProperties();
  const START_TIME = new Date().getTime();

  try {
    const allRequests = getMasterBatchFromControlTable(); // Assumes exists
    if (!allRequests || allRequests.length === 0) {
      console.log("Cache Refresh: Control Table empty. Resetting index.");
      properties.deleteProperty(PROP_KEY);
      return;
    }

    let startIndex = parseInt(properties.getProperty(PROP_KEY) || '0', 10);
    if (startIndex >= allRequests.length || isNaN(startIndex)) {
      startIndex = 0;
      console.log("Cache refresh: Starting over.");
    }

    let itemsProcessedThisRun = 0;

    while (startIndex < allRequests.length) {
      const currentTime = new Date().getTime();
      if (currentTime - START_TIME > TIME_LIMIT_MS) {
        properties.setProperty(PROP_KEY, startIndex.toString());
        console.warn(`⚠️ Cache refresh time limit hit after ${itemsProcessedThisRun} items. Next run starts at index ${startIndex}. RESCHEDULING SELF.`);
        ScriptApp.newTrigger('fuzzworkCacheRefresh_TimeGated').timeBased().after(30 * 1000).create();
        return; // Exit
      }

      const endIndex = Math.min(startIndex + SUB_BATCH_SIZE, allRequests.length);
      const currentSubBatch = allRequests.slice(startIndex, endIndex);

      if (currentSubBatch.length > 0) {
        console.log(`Processing cache sub-batch: ${startIndex + 1} to ${endIndex}`);
        fuzAPI.getDataForRequests(currentSubBatch); // Assumes exists and handles caching
        itemsProcessedThisRun += currentSubBatch.length;
      }
      startIndex = endIndex;
    }

    properties.setProperty(PROP_KEY, '0'); // Reset index for next cycle
    console.log(`Cache refresh: Finished processing all ${allRequests.length} items this cycle.`);

  } finally {
    console.log("Cache refresh execution block finished.");
  }
}

/**
 * Market data update function.
 */
function updateMarketDataSheet() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_REQUEST_INDEX = 'marketDataRequestIndex';
  const PROP_KEY_SHEET_ROW = 'marketDataNextWriteRow';

  const BATCH_SIZE = 1500;
  const TIME_LIMIT_MS = 227000; // ~3m 47s
  const RESCHEDULE_DELAY_MS = 60 * 1000; // 1 min

  const DATA_SHEET_HEADERS = ["cacheKey", "type_id", "location_type", "location_id", "sell_min", "buy_max", "sell_volume", "buy_volume", "last_updated"];
  const COLUMN_COUNT = DATA_SHEET_HEADERS.length;

  const START_TIME = new Date().getTime();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const tempSheetName = 'Market_Data_Temp';

  let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP) || STATE_FLAGS.NEW_RUN;
  let sheet = ss.getSheetByName(tempSheetName);
  const masterRequests = getMasterBatchFromControlTable(); // Assumes exists

  if (currentStep === STATE_FLAGS.FINALIZING) {
    console.warn(`State is ${STATE_FLAGS.FINALIZING}. Exiting. Master orchestrator will handle finalization.`);
    return;
  }

  if (currentStep === STATE_FLAGS.NEW_RUN || !sheet || !masterRequests || masterRequests.length === 0) {
    currentStep = STATE_FLAGS.NEW_RUN;
    console.log(`State: ${STATE_FLAGS.NEW_RUN}. Preparing cycle.`);
    if (!masterRequests || masterRequests.length === 0) {
      console.warn("Control Table empty. Resetting state.");
      _resetMarketDataJobState(new Error("Control Table empty")); // Use reset helper
      return;
    }
    if (!sheet) {
      console.warn(`Creating temp sheet: ${tempSheetName}`);
      sheet = getOrCreateSheet(ss,tempSheetName,DATA_SHEET_HEADERS);
      if (!sheet) throw new Error(`Failed to create sheet ${tempSheetName}`);
      sheet.hideSheet();
    } else {
      console.log(`Clearing temp sheet: ${tempSheetName}`);
      sheet.clearContents();
    }
/**    sheet.getRange(1, 1, 1, COLUMN_COUNT).setValues([DATA_SHEET_HEADERS]);
    const maxRows = sheet.getMaxRows(); if (maxRows > 1) sheet.getRange(2, 1, maxRows - 1, COLUMN_COUNT).clearContent();
    const maxColumns = sheet.getMaxColumns(); if (maxColumns > COLUMN_COUNT) sheet.deleteColumns(COLUMN_COUNT + 1, maxColumns - COLUMN_COUNT);
    if (sheet.getMaxRows() > 1) { sheet.deleteRows(2, sheet.getMaxRows() - 1); } // Ensure only header
    SpreadsheetApp.flush();*/
    SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, '0'); SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, '2');
    currentStep = STATE_FLAGS.PROCESSING; SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.PROCESSING);
    console.log(`Init complete. To ${STATE_FLAGS.PROCESSING}.`);
  }

  if (currentStep === STATE_FLAGS.PROCESSING) {
    console.log(`State: ${STATE_FLAGS.PROCESSING}. Fetch/write loop.`);
    let requestStartIndex = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_REQUEST_INDEX) || '0');
    let nextWriteRow = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_SHEET_ROW) || '2');
    let batchesProcessedInThisRun = 0;
    sheet = ss.getSheetByName(tempSheetName);
    if (!sheet) {
      _resetMarketDataJobState(new Error(`Sheet ${tempSheetName} disappeared`));
      throw new Error(`Sheet ${tempSheetName} disappeared`);
    }
    while (requestStartIndex < masterRequests.length) {
      let currentTime = new Date().getTime();
      if (currentTime - START_TIME > TIME_LIMIT_MS) {
        SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString());
        SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
        ScriptApp.newTrigger('updateMarketDataSheet').timeBased().after(RESCHEDULE_DELAY_MS).create();
        console.warn(`⚠️ Time limit hit after ${batchesProcessedInThisRun} batches. RESCHEDULED.`);
        return;
      }
      const requestEndIndex = Math.min(requestStartIndex + BATCH_SIZE, masterRequests.length);
      const requestsForThisRun = masterRequests.slice(requestStartIndex, requestEndIndex);
      if (requestsForThisRun.length === 0) break;
      const marketData = fuzAPI.getDataForRequests(requestsForThisRun); // Assumes exists
      if (!marketData || marketData.length === 0) {
        console.warn(`API empty for requests ${requestStartIndex + 1}-${requestEndIndex}. Skipping.`);
        requestStartIndex = requestEndIndex; continue;
      }
      let allRowsToWrite = []; const currentTimeStamp = new Date();
      marketData.forEach(crate => crate.fuzObjects.forEach(item => allRowsToWrite.push(["", item.type_id, crate.market_type, crate.market_id, item.sell.min, item.buy.max, item.sell.volume, item.buy.volume, currentTimeStamp])));
      if (allRowsToWrite.length > 0) {
        if (allRowsToWrite[0].length !== COLUMN_COUNT) throw new Error(`Col count mismatch! ${COLUMN_COUNT} vs ${allRowsToWrite[0].length}`);
        currentTime = new Date().getTime();
        if (currentTime - START_TIME > TIME_LIMIT_MS) {
          SCRIPT_PROP.setProperty(PROP_KEY_REQUEST_INDEX, requestStartIndex.toString()); SCRIPT_PROP.setProperty(PROP_KEY_SHEET_ROW, nextWriteRow.toString());
          ScriptApp.newTrigger('updateMarketDataSheet').timeBased().after(RESCHEDULE_DELAY_MS).create();
          console.warn(`⚠️ Time limit BEFORE WRITE batch ${batchesProcessedInThisRun + 1}. RESCHEDULED.`);
          return;
        }
        // --- Write using setValues (insertRowsAfter is commented out elsewhere) ---
        sheet.getRange(nextWriteRow, 1, allRowsToWrite.length, COLUMN_COUNT).setValues(allRowsToWrite);
        nextWriteRow += allRowsToWrite.length; requestStartIndex = requestEndIndex; batchesProcessedInThisRun++;
        console.log(`Wrote batch ${batchesProcessedInThisRun}. Next index: ${requestStartIndex}. Next row: ${nextWriteRow}`);
        // SpreadsheetApp.flush(); // Optional flush removed for performance
      } else {
        console.log(`No rows for batch starting ${requestStartIndex + 1}.`); requestStartIndex = requestEndIndex;
      }
    } // End while
    if (requestStartIndex >= masterRequests.length) {
      console.log("All batches processed. Setting state to FINALIZING.");
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.FINALIZING);
      return; // Let master orchestrator handle finalization
    }
  } // End if PROCESSING
}

/**
 * Final sheet update using **Rename Swap + Delete Old + Create New Temp**.
 * Called by masterOrchestrator when state is FINALIZING.
 */
function finalizeMarketDataUpdate() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_REQUEST_INDEX = 'marketDataRequestIndex';
  const PROP_KEY_SHEET_ROW = 'marketDataNextWriteRow';
  const tempSheetName = 'Market_Data_Temp';
  const finalSheetName = 'Market_Data_Raw';
  const oldSheetName = 'Market_Data_Old';

  executeLocked(() => {
    const currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP);
    if (currentStep !== STATE_FLAGS.FINALIZING) {
      console.warn(`Finalizer called unexpectedly (state: ${currentStep}). Resetting.`);
      _resetMarketDataJobState(new Error(`Finalizer called in wrong state: ${currentStep}`));
      return;
    }
    console.log(`State: ${STATE_FLAGS.FINALIZING}. Starting update (Rename Swap + Delete Old + Create Temp).`);
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const tempSheet = ss.getSheetByName(tempSheetName);
    let liveSheet = ss.getSheetByName(finalSheetName);
    if (!tempSheet) {
      _resetMarketDataJobState(new Error(`Sheet ${tempSheetName} disappeared`));
      throw new Error(`Sheet ${tempSheetName} disappeared`);
    }
   // SpreadsheetApp.flush();
    try {
      // This is the sequence of the FAST SWAP

      // 2. Rename current "Raw" sheet to "Old" (if it exists)
      if (liveSheet) { console.log(`Renaming '${finalSheetName}' to '${oldSheetName}'.`); liveSheet.setName(oldSheetName);
      // SpreadsheetApp.flush();
        }
      else { console.log(`Sheet '${finalSheetName}' not found.`); }

      // 3. Rename "Temp" sheet (holding new data) to "Raw"
      console.log(`Renaming '${tempSheetName}' to '${finalSheetName}'.`);
      tempSheet.setName(finalSheetName); tempSheet.showSheet();
       SpreadsheetApp.flush();



      console.log("Sheet swap complete.");

    } catch (error) {
      console.error(`ERROR during finalization: ${error.message}\nStack: ${error.stack}`);
      try { // Attempt revert
        const currentRaw = ss.getSheetByName(finalSheetName); const currentOld = ss.getSheetByName(oldSheetName); const currentTemp = ss.getSheetByName(tempSheetName);
        if (currentOld && !currentRaw && currentOld.getName() === oldSheetName) { currentOld.setName(finalSheetName); console.log(`Reverted '${oldSheetName}' to '${finalSheetName}'.`); }
        if (currentTemp && !currentRaw && currentTemp.getName() === tempSheetName) { console.log(`Ensured temp name is still '${tempSheetName}'.`); }
        else if (currentTemp && currentTemp.getName() === finalSheetName) { currentTemp.setName(tempSheetName); console.log(`Reverted '${finalSheetName}' back to '${tempSheetName}'.`); }
      } catch (revertError) { console.error(`Failed to revert names: ${revertError.message}`); }
      _resetMarketDataJobState(error); // Use reset helper on failure
      throw error;
    }
    _resetMarketDataJobState(null); // Pass null for error to log success
    console.log("SUCCESS: Finalization complete (deferred delete). State reset.");
  }, "finalizeMarketDataUpdate");
}

/**
 * Simple function to clean up the old market data sheet. Runs on its own trigger.
 */
function cleanupOldSheet() {
  const oldSheetName = 'Market_Data_Old';
  const funcName = 'cleanupOldSheet';
  executeLocked(() => {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const oldSheet = ss.getSheetByName(oldSheetName);
    if (oldSheet) {
      console.log(`Found old sheet '${oldSheetName}'. Deleting it.`);
      try { ss.deleteSheet(oldSheet); console.log(`Successfully deleted '${oldSheetName}'.`); }
      catch (e) { console.error(`Failed to delete '${oldSheetName}': ${e.message}`); }
    } else { console.log(`No sheet named '${oldSheetName}' found.`); }
  }, funcName);
}


/**
 * Helper to create a new one-time "retry" trigger.
 */
function scheduleOneTimeTrigger(functionName, delayMs) {
  if (typeof functionName !== 'string' || functionName.trim() === '') {
    throw new Error(`scheduleOneTimeTrigger: Invalid function name provided. Must be a non-empty string.`);
  }
  try {
    deleteTriggersByName(functionName); // Prevent duplicates for the *same* function's retry
    ScriptApp.newTrigger(functionName)
      .timeBased()
      .after(delayMs)
      .create();
    console.log(`Created one-time trigger for ${functionName} to run in ${Math.round(delayMs / 60000)} minutes.`);
  } catch (e) {
    console.error(`Failed to create/delete trigger for ${functionName}: ${e.message}`);
  }
}

/**
 * Run this function ONCE from the editor to set up triggers.
 */
function setupStaggeredTriggers() {
  console.log("Setting up/Resetting triggers...");
  deleteTriggersByName('fuzzworkCacheRefresh_TimeGated');
  deleteTriggersByName('triggerCacheWarmerWithRetry');
  deleteTriggersByName('updateMarketDataSheet');
  deleteTriggersByName('finalizeMarketDataUpdate');
  deleteTriggersByName('cleanupOldSheet');
  deleteTriggersByName('masterOrchestrator');
  deleteTriggersByName('runAllLedgerImports'); // Also delete ledger trigger since master manages it

  try {
    // Setup Master Orchestrator Trigger (every 15 min)
    ScriptApp.newTrigger('masterOrchestrator')
      .timeBased().everyMinutes(15).create();
    console.log('SUCCESS: Created 15-minute trigger for masterOrchestrator.');


  } catch (e) { console.error(`Failed to create triggers: ${e.message}`); }
}

/**
 * Helper to delete triggers by name.
 */
function deleteTriggersByName(functionName) {
  let deletedCount = 0;
  try {
    const allTriggers = ScriptApp.getProjectTriggers();
    allTriggers.forEach(trigger => {
      if (trigger.getHandlerFunction() === functionName &&
        trigger.getEventType() === ScriptApp.EventType.CLOCK) {
        try {
          ScriptApp.deleteTrigger(trigger);
          deletedCount++;
        } catch (e) {
          console.warn(`Could not delete a trigger for ${functionName}: ${e.message}`);
        }
      }
    });
    if (deletedCount > 0) {
      console.log(`Deleted ${deletedCount} existing clock trigger(s) for ${functionName}.`);
    }
  } catch (e) {
    console.error(`Error accessing or deleting triggers for ${functionName}: ${e.message}`);
  }
}


/**
 * Manual reset function.
 */
function manualResetMarketDataJob() {
  _resetMarketDataJobState(new Error("Manual reset requested"));
  console.log("MANUAL RESET: Market Data job state has been reset.");
}

/**
 * Internal reset helper.
 */
function _resetMarketDataJobState(error) {
  console.warn(`RESETTING Market Data Job State: ${error ? error.message : 'Resetting state'}.`);
  console.log("Rolling back market data job state...");
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const PROP_KEY_REQUEST_INDEX = 'marketDataRequestIndex';
  const PROP_KEY_SHEET_ROW = 'marketDataNextWriteRow';

  SCRIPT_PROP.deleteProperty(PROP_KEY_STEP);
  SCRIPT_PROP.deleteProperty(PROP_KEY_REQUEST_INDEX);
  SCRIPT_PROP.deleteProperty(PROP_KEY_SHEET_ROW);
  deleteTriggersByName('updateMarketDataSheet');
  deleteTriggersByName('finalizeMarketDataUpdate');
  console.log("Market data job state has been successfully reset.");
}


// NOTE: Assumes getMasterBatchFromControlTable, fuzAPI, runAllLedgerImports exist
