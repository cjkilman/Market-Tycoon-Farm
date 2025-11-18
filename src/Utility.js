/**
 * Get or create a sheet, preserving headers and adjusting dimensions.
 * For new sheets, limits the column count to the header length.
 * @param {SpreadsheetApp.Spreadsheet} ss - Spreadsheet object
 * @param {string} name - Sheet name
 * @param {string[]} headers - Array of header strings
 * @returns {GoogleAppsScript.Spreadsheet.Sheet}
 */
function getOrCreateSheet(ss, name, headers) {
  // --- Input Validation (Retained) ---
  if (!ss || typeof ss.getSheetByName !== 'function') {
    ss = SpreadsheetApp.getActiveSpreadsheet();
    if (!ss) {
      throw new Error("getOrCreateSheet: Could not get active spreadsheet.");
    }
  }
  if (typeof name !== 'string' || name.trim() === '') {
    throw new Error("getOrCreateSheet: 'name' must be a non-empty string.");
  }
  if (!Array.isArray(headers)) {
    throw new Error("getOrCreateSheet: 'headers' must be an array of strings.");
  }
  // --- End Input Validation ---

  const sheetName = name.trim();
  const headerCount = headers.length;
  let sheet = ss.getSheetByName(sheetName);
  let isNewSheet = false;

  if (!sheet) {
    // 1. CREATE NEW SHEET (Expensive but necessary once)
    console.log(`Creating new sheet: '${sheetName}'`);
    sheet = ss.insertSheet(sheetName);
    isNewSheet = true;

    // --- *** SIMPLIFIED LOGIC *** ---
    // DO NOT attempt to adjust columns. This is what causes the timeout.
    // Just set the headers.
    sheet.getRange(1, 1, 1, headerCount).setValues([headers]);
    console.log(`Headers set for new sheet '${sheetName}'.`);

  } else {
    // --- *** CORRECTED LOGIC *** ---
    // EXISTING SHEET: Do *nothing* except log.
    // Do NOT rewrite headers. This was causing the timeout.
    console.log(`Sheet '${sheetName}' exists. Skipping creation and header write.`);
    // --- *** END CORRECTION *** ---
  }

  // NOTE: Clearing contents is now left entirely to Orchestrator._updateMarketDataSheetWorker

  return sheet;
}

/* global SpreadsheetApp, LockService, PropertiesService, LoggerEx, SAFE_CONSOLE_SHIM */

const CHUNK_SIZE_LIMIT = 8500; // Safe limit below the official 9KB threshold
//const CHUNK_INDEX_SUFFIX = ':IDX';

/**
 * Stores a large string by splitting it into multiple chunks in PropertiesService.
 */
function _writeShardedProperty(propService, baseKey, largeJsonString) {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('Sharder') : console);
  const totalLength = largeJsonString.length;
  const numChunks = Math.ceil(totalLength / CHUNK_SIZE_LIMIT);
  const keysToWrite = {};

  for (let i = 0; i < numChunks; i++) {
    const start = i * CHUNK_SIZE_LIMIT;
    const end = Math.min(start + CHUNK_SIZE_LIMIT, totalLength);
    const chunk = largeJsonString.substring(start, end);
    keysToWrite[baseKey + ':' + i] = chunk;
  }
  keysToWrite[baseKey + CHUNK_INDEX_SUFFIX] = String(numChunks);

  // Use setProperties for atomic writing of the chunks
  propService.setProperties(keysToWrite, true); // true = delete other keys
  log.log(`Sharded property '${baseKey}' into ${numChunks} chunks.`);
}

/**
 * Reads a large string property that was split into multiple chunks (shards).
 * @returns {string|null} The reconstructed JSON string, or null if corrupt/missing.
 */
function _readShardedProperty(propService, baseKey) {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('Sharder') : console);

  // 1. Get the index key to find the number of chunks
  const numChunksRaw = propService.getProperty(baseKey + CHUNK_INDEX_SUFFIX);
  if (!numChunksRaw) {
    return null;
  }
  const numChunks = parseInt(numChunksRaw, 10);

  // 2. Build the list of keys to retrieve
  const keysToGet = [];
  for (let i = 0; i < numChunks; i++) {
    keysToGet.push(baseKey + ':' + i);
  }

  // 3. Get all chunks
  const chunksMap = propService.getProperties();

  const result = [];
  for (let i = 0; i < numChunks; i++) {
    const key = baseKey + ':' + i;
    const chunk = chunksMap[key];
    if (chunk == null) {
      // If any chunk is missing, the data is corrupt/expired
      log.error(`Sharded property '${baseKey}' corrupt: Missing chunk ${i}.`);
      return null;
    }
    result.push(chunk);
  }

  return result.join('');
}
/**
 * Deletes all Script Properties that are not recognized as essential configuration
 * or active job resumption markers. This tool is designed to free up space 
 * when the 500 KB quota is exceeded.
 */
function cleanupUnusedScriptProperties() {
  // --- 1. WHITELIST: ESSENTIAL KEYS TO KEEP ---
  // These keys are defined across your module files (e.g., Orchestrator, InventoryManager).
  const ESSENTIAL_KEYS_PREFIX = [
    'SDE_invTypes_TypeMap',  // The large SDE cache key
    'AssetCache_',           // Asset job resumption markers
    'StructName_',           // Structure name persistent cache
    '_CORP_AUTH_CHAR_PROP',  // GESI Auth character persistence
    'CORP_JOURNAL_LAST_ID',  // Ledger resume anchor
    'GLOBAL_SYSTEM_STATE',   // Maintenance mode flag
    'marketDataJobLeaseUntil', // Orchestrator Lease
    'marketDataJobStep',     // Orchestrator state
    'marketDataFinalizeStep', // Orchestrator finalizer step
    'marketDataNextWriteRow', // Orchestrator write position
    'marketDataRequestIndex', // Orchestrator request index
    'marketDataChunkSize',    // Orchestrator chunk size
    'SDE_JOB_RUNNING',        // SDE Job Controller flags
    'SDE_JOB_LIST',
    'SDE_JOB_INDEX',
    'SDE_JOB_CHUNK_INDEX',
    'SDE_LAST_WRITE_MS'
  ];

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('QUOTA_CLEANER') : console);

  log.info('--- Starting Unused Property Cleanup ---');

  try {
    const allProperties = SCRIPT_PROP.getProperties();
    const allKeys = Object.keys(allProperties);
    let keysToDelete = [];
    let keysKept = [];

    // --- 2. IDENTIFY KEYS FOR DELETION ---
    allKeys.forEach(key => {
      const isEssential = ESSENTIAL_KEYS_PREFIX.some(prefix => key.startsWith(prefix));

      if (isEssential) {
        keysKept.push(key);
      } else {
        keysToDelete.push(key);
      }
    });

    // --- 3. EXECUTE DELETION (Using stable individual deletion) ---
    if (keysToDelete.length > 0) {
      log.warn(`Identified ${keysToDelete.length} non-essential keys for deletion. This process is slow.`);

      // ** CRITICAL FIX: Use the stable, individual delete method **
      keysToDelete.forEach(key => {
        // This will execute successfully in all Apps Script environments.
        SCRIPT_PROP.deleteProperty(key);
      });
      // ** END CRITICAL FIX **

      log.info(`✅ Successfully deleted ${keysToDelete.length} keys.`);
    } else {
      log.warn("No unessential keys found. Storage quota should be clear.");
    }

    // --- 4. REPORT STATUS ---
    const finalKeyCount = Object.keys(SCRIPT_PROP.getProperties()).length;
    log.info(`Final Key Count: ${finalKeyCount}. Keys remaining: ${keysKept.join(', ')}`);

  } catch (e) {
    log.error(`FATAL CLEANUP ERROR: ${e.message}. Quota may still be exceeded.`);
    // Note: Individual deletion is slow and may still hit a time limit, but the TypeError is fixed.
    throw e;
  }
}

/**
 * Adds the cleanup tool to the custom menu.
 * (Assumed to be placed in src/Main.js within the onOpen function)
 */
function addCleanupToolToMenu() {
  const ui = SpreadsheetApp.getUi();
  ui.createMenu('Sheet Tools')
    // ... (other menu items) ...
    .addSeparator()
    .addItem('🛠️ DEBUG: Cleanup Property Quota', 'cleanupUnusedScriptProperties')
    .addToUi();
}

/**
 * Executes a single, simple, uncached Sheet API read operation and logs the duration.
 * Used to measure Spreadsheet service latency.
 * NOTE: Assumes there is a Sheet named 'Utility' or 'Sheet1' with content in A1.
 * @returns {number} The duration of the Sheet GET operation (in milliseconds).
 */
function _measureSpreadsheetLatency() {
  const TEST_CELL = 'A1';
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('PERF_LOG') : Logger);
  let duration = 0;

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName('Utility') || ss.getSheets()[0]; // Use first sheet if Utility is missing

    if (!sheet) {
      log.error('Spreadsheet Latency Test FAILED: No sheets found.');
      return 0;
    }

    const startGet = new Date().getTime();
    // Perform a forced read operation
    sheet.getRange(TEST_CELL).getValue();
    duration = new Date().getTime() - startGet;

    log.info(`Spreadsheet Latency: ${duration}ms`);
  } catch (e) {
    log.error('Spreadsheet Latency Test FAILED.', e);
  }

  return duration;
}

/**
 * Executes a function with a Document Lock using a non-blocking TryLock pattern.
 * NOTE: This function returns a complex state object {success, state, error} on completion
 * or throws an error if the internal function throws.
 * * @param {function} fn The function containing critical spreadsheet operations.
 * @param {number} [timeoutMs=5000] The time (in ms) to wait for the lock via tryLock.
 * @returns {Object} A standardized result object {success: boolean, state: any, error: string}.
 */
function guardedSheetTransaction(fn, timeoutMs = 5000) {
  var lock = LockService.getDocumentLock();

  // 1. Attempt TryLock (non-blocking acquisition)
  if (!lock.tryLock(timeoutMs)) {
    // Handles skip/bail gracefully by returning a standardized failure object
    console.warn(`Document Lock busy: Skipping critical sheet transaction.`);
    return { success: false, state: null, error: "Lock Conflict/Busy" };
  }

  try {
    console.log("Document Lock acquired via tryLock.");
    // Execute the critical function and capture its return value
    const state = fn();

    // FIX: Corrected typo from 'sucess' to 'success'
    return { success: true, state: state, error: "" };

  } catch (e) {
    console.error(`CRITICAL ERROR during locked sheet operation: ${e.message}`);
    // Re-throw the error outside the lock release mechanism for the worker to catch.
    return { success: false, state: null, error: e.message };

  } finally {
    // 2. Guaranteed Release
    try {
      lock.releaseLock();
      console.log("Document Lock released.");
    } catch (rlErr) {
      console.error("CRITICAL: Failed to release Document Lock!", rlErr);
    }
  }
}

/**
 * Executes one read and one write operation on PropertyService and logs the duration.
 * This is used to detect PropertyService slowdowns/bottlenecks.
 * NOTE: Uses a dedicated, small property key.
 * @returns {number} The duration of the slowest operation (in milliseconds).
 */
function _measurePropertyService() {
  const TEST_KEY = 'PROP_PERF_TEST';
  const PROP = PropertiesService.getScriptProperties();
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('PERF_LOG') : Logger);
  let maxDuration = 0;

  // 1. Measure SET (Write) operation
  try {
    const startSet = new Date().getTime();
    PROP.setProperty(TEST_KEY, String(startSet));
    const durationSet = new Date().getTime() - startSet;
    maxDuration = Math.max(maxDuration, durationSet);
  } catch (e) {
    log.error('PropertiesService SET Test FAILED.', e);
  }

  // 2. Measure GET (Read) operation
  try {
    const startGet = new Date().getTime();
    PROP.getProperty(TEST_KEY);
    const durationGet = new Date().getTime() - startGet;
    maxDuration = Math.max(maxDuration, durationGet);
  } catch (e) {
    log.error('PropertiesService GET Test FAILED.', e);
  }

  // 3. Clean up and log
  PROP.deleteProperty(TEST_KEY);
  log.info(`PropertiesService Performance: Max Latency ${maxDuration}ms`);

  return maxDuration;
}

// src/Utility.js

// src/Utility.js

/**
 * REPLACEMENT for the original withSheetLock function.
 * Executes a function with a Document Lock using a non-blocking TryLock pattern.
 * * This prevents the script from being killed while waiting for the lock, 
 * eliminating the "stuck lock" problem.
 *
 * @param {function} fn The function containing critical spreadsheet operations.
 * @param {number} [timeoutMs=5000] The time (in ms) to wait for the lock via tryLock.
 * @returns {any} The result of fn, or undefined if execution was skipped due to lock conflict.
 */
function withSheetLock(fn, timeoutMs = 5000) {
  var lock = LockService.getDocumentLock();

  // 1. Attempt TryLock (non-blocking acquisition)
  if (!lock.tryLock(timeoutMs)) {
    console.warn(`Document Lock busy: Skipping critical sheet transaction.`);
    return; // Returns undefined, signaling a skip
  }

  try {
    console.log("Document Lock acquired via tryLock.");
    // Execute the critical function (e.g., deleteSheet, insertSheet, clearContent)
    return fn();

  } catch (e) {
    console.error(`CRITICAL ERROR during locked sheet operation: ${e.message}`);
    // Re-throw the error outside the lock release mechanism for worker to catch.
    throw e;

  } finally {
    // 2. Guaranteed Release (even if the function threw an error)
    try {
      lock.releaseLock();
      console.log("Document Lock released.");
    } catch (rlErr) {
      // This catches the exact platform error that causes original stickiness
      console.error("CRITICAL: Failed to release Document Lock!", rlErr);
    }
  }
}

/**
 * Safely writes a large 2D array of data to a sheet in throttled batches, integrating LockService resilience.
 * This function performs the dynamic chunk size adjustment and checks for the overall job time limit.
 * * NOTE: This utility no longer performs structural clearing or chunk size re-initialization 
 * on its own, relying entirely on the Orchestrator for those steps.
 * * @param {string} sheetName The name of the sheet to write to.
 * @param {Array<Array>} dataArray The 2D array of data to be written.
 * @param {number} startRow The row number where data begins (usually 2).
 * @param {number} startCol The column number where data begins (usually 1).
 * @param {Object} [stateObject] Optional object containing ss, config, and resume metrics.
 * @returns {Object} Status Object: {success: bool, rowsProcessed: num, duration: num, state: stateObject, error: string, bailout_reason: string}
 */
function writeDataToSheet(sheetName, dataArray, startRow, startCol, stateObject) {
  // 1. DEFINE STATE AND CONFIG
  var state = stateObject || {};

  // Micro-Optimization: Use state object's SS reference, fall back if not provided
  var ss = state.ss || SpreadsheetApp.getActiveSpreadsheet();

  var targetSheet;

  // Define constants for the new logic (can be overridden in state.config)
  const TARGET_WRITE_TIME_MS = state.config.TARGET_WRITE_TIME_MS || 3000;
  const MAX_FACTOR = state.config.MAX_FACTOR || 1.5; // Cap the increase to 50% per step

  // --- CRITICAL FIX: Harden variable initialization against null/undefined ---
  // Ensure non-numeric inputs (null, undefined) default to a safe number.
  var docLockTimeoutMs = Number(state.config && state.config.DOC_LOCK_TIMEOUT_MS) || 5000; 
  var THROTTLE_THRESHOLD_MS = Number(state.config && state.config.THROTTLE_THRESHOLD_MS) || 800;
  var THROTTLE_PAUSE_MS = Number(state.config && state.config.THROTTLE_PAUSE_MS) || 200;
  var SOFT_LIMIT_MS = Number(state.config && state.config.SOFT_LIMIT_MS) || 0;

  // Fetch throttling constants for math
  var CHUNK_DECREASE_RATE = Number(state.config && state.config.CHUNK_DECREASE_RATE) || 200;
  var MIN_CHUNK_SIZE = Number(state.config && state.config.MIN_CHUNK_SIZE) || 50;
  var MAX_CHUNK_SIZE = Number(state.config && state.config.MAX_CHUNK_SIZE) || 5000;
  // --- END CRITICAL FIX ---
  
  // Fetch mutable metrics from state or initialize
  var startTime = state.metrics && state.metrics.startTime || 0;
  var currentChunkSize = state.config && state.config.currentChunkSize || MIN_CHUNK_SIZE;
  var previousDuration = state.metrics && state.metrics.previousDuration || 0;
  var rowsProcessed = state.metrics && state.metrics.rowsProcessed || 0;
  var i = state.nextBatchIndex || 0; // Resume point

  // * Conflicting chunk size initialization removed *

  var dataLength = dataArray.length;
  var numCols = 0;
  var docLock = LockService.getDocumentLock();

  try {
    targetSheet = ss.getSheetByName(sheetName);
    if (!targetSheet) {
      throw new Error("Sheet not found: " + sheetName);
    }

    numCols = dataLength > 0 ? dataArray[0].length : 0;
    if (numCols === 0 && dataLength > 0) {
      throw new Error("Data array is corrupted (zero columns).");
    }

    // * Structural clearing logic removed *

    if (state.logInfo) state.logInfo("Starting batch write. Rows: " + dataLength + ", Resume Index: " + i);

    // REDUNDANT CHUNK SIZE INITIALIZATION BLOCK REMOVED.

    // --- START RESILIENT BATCH WRITE LOOP (while loop) ---
    while (i < dataLength) {

      // 2. PREDICTIVE BAILOUT CHECK (Logic commented out for hard timeout resilience)
      // ...

      // 3. DYNAMIC THROTTLING CHECK & ADJUSTMENT
      if (previousDuration > THROTTLE_THRESHOLD_MS) {
        // Throttle Down (The crash-avoidance math)
        currentChunkSize = Math.max(MIN_CHUNK_SIZE, currentChunkSize - CHUNK_DECREASE_RATE);

        if (state.logInfo) state.logInfo("[THROTTLE] Pausing for " + THROTTLE_PAUSE_MS + "ms to yield execution.");
        Utilities.sleep(THROTTLE_PAUSE_MS);
        previousDuration = 0; // Reset duration after pause
      }

      var chunkStartTime = new Date().getTime();
      var batch = dataArray.slice(i, i + currentChunkSize);
      var numRows = batch.length; // Actual number of rows being written
      var targetRow = startRow + i;

      // --- ATOMIC CHUNK WRITE LOGIC (Lock/Release/Yank) ---
      if (!docLock.tryLock(docLockTimeoutMs)) {
        // If lock acquisition fails in the middle of a job, signal a bailout to worker.
        return {
          success: false,
          rowsProcessed: rowsProcessed,
          duration: previousDuration,
          state: state,
          error: "LockAcquisitionFailure: Could not acquire Document Lock.",
          bailout_reason: "LOCK_CONFLICT"
        };
      }

      // 2. PREDICTIVE BAILOUT CHECK (Check total elapsed time for job limit)
      if (startTime && SOFT_LIMIT_MS > 0 && (new Date().getTime() - startTime > SOFT_LIMIT_MS)) {
        var bailoutMsg = "Job reached predictive soft time limit. Reschedule required.";
        if (state.logWarn) state.logWarn(bailoutMsg);

        // Return failure, providing the last duration for worker to assess speed on resume
        // and including the CRITICAL TAG for the Orchestrator to detect.
        return {
          success: false,
          rowsProcessed: rowsProcessed,
          duration: previousDuration, // Provide last known speed
          state: state,
          error: bailoutMsg,
          bailout_reason: "PREDICTIVE_BAILOUT" // CRITICAL TAG
        };

      }

      try {

        // The actual sheet write call
        targetSheet
          .getRange(targetRow, startCol, numRows, numCols)
          .setValues(batch);

        // --- SUCCESS PATH ---
        docLock.releaseLock();
        previousDuration = new Date().getTime() - chunkStartTime;

        // --- DYNAMIC CHUNK SIZE ADJUSTMENT (New Logic) ---
        // Throttle Up (The acceleration math)
        if (previousDuration <= THROTTLE_THRESHOLD_MS && previousDuration > 0) {
          const adjustmentFactor = TARGET_WRITE_TIME_MS / previousDuration;
          const limitedFactor = Math.min(adjustmentFactor, MAX_FACTOR);

          // Adjust the size for the NEXT chunk
          currentChunkSize = Math.round(currentChunkSize * limitedFactor);

          // Enforce Min/Max bounds
          currentChunkSize = Math.min(MAX_CHUNK_SIZE, currentChunkSize);
        }

        // Update metrics in the memory state object
        rowsProcessed += numRows;
        state.metrics.rowsProcessed = rowsProcessed;

        // CRITICAL FIX: Manually advance index 'i' by the actual number of rows written (numRows).
        i += numRows;

        state.nextBatchIndex = i;
        state.config.currentChunkSize = currentChunkSize; // Save new chunk size

      } catch (e) {
        // Service Timeout or other write error (The "Yank" operation)

        docLock.releaseLock();
        var errorMessage = "ServiceTimeoutFailure: Batch Write failed at row " + targetRow + ". Error: " + e.message;
        state.logError(errorMessage);

        // --- CRITICAL FIX: Ensure final state is saved before returning failure ---
        // The index 'i' holds the starting index of the failed batch (the correct checkpoint).
        state.nextBatchIndex = rowsProcessed;
        state.config.currentChunkSize = currentChunkSize;
        // --------------------------------------------------------------------------


        // FAILURE STATUS OBJECT: Returns the final memory state and failure data
        return {
          success: false,
          rowsProcessed: rowsProcessed,
          duration: previousDuration,
          state: state,
          error: errorMessage
        };
      }
    }
    // --- END RESILIENT BATCH WRITE LOOP ---

    // Final success return
    if (state.logInfo) state.logInfo("Write SUCCESS. Total Rows Written: " + rowsProcessed);

    // SUCCESS STATUS OBJECT: Returns the final memory memory state and success data
    return {
      success: true,
      rowsProcessed: rowsProcessed,
      duration: previousDuration,
      state: state,
      error: ""
    };

  } catch (e) {
    // Catches: Sheet Not Found, LockAcquisitionFailure (Uncaught)
    var finalErrorMsg = e.message;

    if (state.logError) state.logError("CRITICAL FAILURE in writeDataToSheet. Error: " + finalErrorMsg);

    // CATASTROPHIC FAILURE STATUS OBJECT: Returns the final memory state before the crash
    return {
      success: false,
      rowsProcessed: rowsProcessed,
      duration: 0,
      state: state,
      error: finalErrorMsg
    };
  }
}

const CONDITIONAL_FLUSH_THRESHOLD_MS = 5000; // 5 seconds: Threshold to trigger flush

/**
 * Executes a simple Sheet API read operation to measure latency.
 * If latency exceeds a threshold, it forces a SpreadsheetApp.flush()
 * to commit pending structural changes.
 * * NOTE: Assumes 'Utility' or 'Sheet1' exists with content in A1.
 * @param {number} [thresholdMs=CONDITIONAL_FLUSH_THRESHOLD_MS] The time (in ms) above which a flush should be performed.
 * @returns {number} The measured latency of the Sheet GET operation (in milliseconds).
 */
function performLatencyCheckAndFlush(thresholdMs = CONDITIONAL_FLUSH_THRESHOLD_MS) {
  const TEST_CELL = 'A1';
  // Use the existing LoggerEx if available
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('PERF_FLUSH') : Logger);
  let duration = 0;

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    // Use first sheet if Utility is missing, as defined in _measureSpreadsheetLatency
    const sheet = ss.getSheetByName('Utility') || ss.getSheets()[0];

    if (!sheet) {
      log.error('Latency Test FAILED: No sheets found.');
      return 0;
    }

    const startGet = new Date().getTime();
    // Perform a forced read operation (uncached, raw I/O)
    sheet.getRange(TEST_CELL).getValue();
    duration = new Date().getTime() - startGet;

    log.info(`Spreadsheet Latency (GET A1): ${duration}ms. Threshold: ${thresholdMs}ms.`);

    if (duration > thresholdMs) {
      log.warn(`Latency ${duration}ms exceeded threshold. Forcing SpreadsheetApp.flush() to commit pending operations.`);
      // This is the core action: flush when performance is slow.
      SpreadsheetApp.flush();
      log.warn(`Forced flush successful.`);
    }

  } catch (e) {
    log.error('Spreadsheet Latency Test FAILED.', e);
  }

  return duration;
}
/**
 * Reads all data rows from a sheet, filters them using a custom function, and returns the resulting 2D array.
 * * @param {string} sheetName The name of the sheet to read from.
 * @param {function} filterFunction A custom function (row, index) => boolean 
 * that returns TRUE if the row should be KEPT (i.e., passed the pruning check).
 * @param {Object} [stateObject] Optional object containing the pre-loaded ss reference and logging hooks.
 * @returns {Array<Array>} The 2D array of filtered rows (including the header row).
 */
function processAndFilterRows(sheetName, filterFunction, stateObject) {
  var state = stateObject || {};

  // Micro-Optimization: Use state object's SS reference, fall back if not provided
  var ss = state.ss || SpreadsheetApp.getActiveSpreadsheet();
  var sourceSheet;

  try {
    sourceSheet = ss.getSheetByName(sheetName);
    if (!sourceSheet) {
      throw new Error("Source sheet not found: " + sheetName);
    }
  } catch (e) {
    if (state.logError) state.logError("CRITICAL: Failed to get sheet reference. Error: " + e.message);
    return [];
  }

  // --- Data Extraction ---
  // *** FIX: Use getDataRange() to get the *actual* last row with data ***
  var dataRange = sourceSheet.getDataRange();
  var allRows = dataRange.getValues();
  var headerRows = 1;

  // Check if there are any data rows to process
  if (allRows.length <= headerRows) {
    if (state.logInfo) state.logInfo("Sheet " + sheetName + " is empty or contains only headers.");
    // Return an array containing just the header, if it exists
    return allRows.length >= 1 ? [allRows[0]] : [];
  }
  // *** END FIX ***

  var header = allRows[0];
  var dataRows = allRows.slice(headerRows); // Remove the header row
  var keptRows = [];
  var initialCount = dataRows.length;

  // --- Filtering Logic ---
  dataRows.forEach(function (row, index) {
    // Apply the custom filter function provided by the worker module
    if (filterFunction(row, index)) {
      keptRows.push(row);
    }
  });

  var rowsRemoved = initialCount - keptRows.length;

  // Re-attach the header to the front of the kept rows
  var finalDataForWrite = [header].concat(keptRows);

  if (state.logInfo) state.logInfo(
    "Filtering complete in " + sheetName + ". Kept: " + keptRows.length + ", Removed: " + rowsRemoved
  );

  // Return the final 2D array, ready for writeDataToSheet
  return finalDataForWrite;
}

/**
  * Performs an Atomic Swap (delete old, rename new) using a non-blocking TryLock.
  * If the lock is busy, the operation is skipped immediately.
  * * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss The Spreadsheet object.
  * @param {string} oldSheetName The name of the sheet to be DELETED and REPLACED.
  * @param {string} newSheetName The name of the sheet containing the NEW data.
  * @returns {Object} Status Object: {success: boolean, duration: number, errorMessage: string}
  */
function atomicSwapAndFlush(ss, oldSheetName, newSheetName) {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('AtomicSwap') : console);
  const startTime = new Date().getTime();
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();

  let swapSuccess = false;
  let errorMessage = "";
  const docLock = LockService.getDocumentLock();

  // 1. Attempt to acquire TryLock
  if (!docLock.tryLock(5000)) {
    errorMessage = `Lock conflict: Could not acquire Document Lock within 5000ms. Atomic swap skipped.`;
    log.warn(errorMessage);
    return {
      success: false,
      duration: new Date().getTime() - startTime,
      errorMessage: errorMessage
    };
  }

  try {
    // 2. Lock Acquired: Execute Critical Swap
    const oldSheet = ss.getSheetByName(oldSheetName);
    const newSheet = ss.getSheetByName(newSheetName);

    if (!newSheet) {
      errorMessage = `CRITICAL SWAP FAILED: New sheet '${newSheetName}' not found.`;
      log.error(errorMessage);
      throw new Error("New sheet for swap is missing.");
    }

    // 2a. Delete the old sheet (The slow I/O operation)
    if (oldSheet) {
      ss.deleteSheet(oldSheet);
      log.info(`Deleted old sheet: ${oldSheetName}`);
    }

    // 2b. Rename the new sheet
    newSheet.setName(oldSheetName);
    newSheet.showSheet(); // Ensure the resulting sheet is visible
    log.info(`SUCCESS: Sheet '${newSheetName}' renamed to '${oldSheetName}'.`);
    swapSuccess = true;

  } catch (e) {
    // Catch errors during the rename/delete process
    if (!errorMessage) {
      errorMessage = `CRITICAL SWAP FAILED. Error: ${e.message}`;
      log.error(errorMessage);
    }
  } finally {
    // 3. Robust Lock Release
    try {
      docLock.releaseLock();
    } catch (rlErr) {
      // Log the critical failure to release the lock but continue the return flow.
      log.error("CRITICAL: Failed to release Document Lock in atomicSwap!", rlErr);
    }
  }

  // 4. Flush (outside lock and only on success)
  if (swapSuccess) {
    SpreadsheetApp.flush();
  }

  const duration = new Date().getTime() - startTime;

  // 5. Final standardized return structure
  return {
    success: swapSuccess,
    duration: duration,
    errorMessage: errorMessage
  };
}

// *** NOTE: The redundant second definition of writeDataToSheet was removed. ***

/**
 * Utility helpers -- generic functions reused across modules.
 * Keep this file focused on non-domain-specific helpers.
 */
var Utility = (function () {
  'use strict';

  /**
   * Median of a numeric array.
   * - Coerces strings to numbers
   * - By default ignores non-positive values (0/negatives) to match our price logic
   * @param {Array} values
   * @param {Object} [opts]
   * @param {boolean} [opts.ignoreNonPositive=true]
   * @returns {number|string} median value, or '' if no usable values
   */
  function median(values, opts) {
    opts = opts || {};
    var ignoreNonPositive = opts.ignoreNonPositive !== false; // default true
    if (!values || !values.length) return '';
    var nums = values.map(function (v) { return (typeof v === 'number' ? v : Number(v)); })
      .filter(function (v) { return Number.isFinite(v) && (!ignoreNonPositive || v > 0); })
      .sort(function (a, b) { return a - b; });
    if (!nums.length) return '';
    var mid = Math.floor(nums.length / 2);
    return (nums.length % 2) ? nums[mid] : (nums[mid - 1] + nums[mid]) / 2;
  }



  // src/Utility.js

  // ... near other constants


  /**
   * Local-tz window check with strict argument validation.
   * @param {Date} now
   * @param {number} startH hour (0-23)
   * @param {number} startM minute (0-59)
   * @param {number} durationMin duration in minutes (>0)
   * @returns {boolean} true if now is within the window
   */
  function inWindow(now, startH, startM, durationMin) {
    if (!(now instanceof Date) || isNaN(now)) {
      throw new Error(`inWindow: "now" must be a valid Date, got ${now}`);
    }
    if (!Number.isInteger(startH) || !Number.isInteger(startM)) {
      throw new Error(`inWindow: startH/startM must be ints, got h=${startH} m=${startM}`);
    }
    if (!Number.isInteger(durationMin) || durationMin <= 0) {
      throw new Error(`inWindow: durationMin must be a positive int, got ${durationMin}`);
    }

    const start = new Date(now);
    start.setHours(startH, startM, 0, 0); // LOCAL tz
    const end = new Date(start.getTime() + durationMin * 60 * 1000);
    return now >= start && now < end;     // inclusive start, exclusive end
  }

  /** HM wrappers that defer to PT.coerceHM, preserving legacy array API */
  function toHM(val) {
    var hm = (typeof PT !== 'undefined' && PT && typeof PT.coerceHM === 'function') ? PT.coerceHM(val) : { h: 0, m: 0 };
    return hm;
  }
  function _toHM(val) {
    var hm = toHM(val);
    return [hm.h | 0, hm.m | 0];
  }
  // Register global legacy _toHM if not already defined
  try { if (typeof globalThis !== 'undefined' && typeof globalThis._toHM !== 'function') { globalThis._toHM = _toHM; } } catch (e) { }

  return {
    median: median,
    toHM: toHM,
    _toHM: _toHM,
    inWindow: inWindow,
    _inWindow_: inWindow
  };
})();