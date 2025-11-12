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

function withSheetLock(fn, timeoutMs) {
  var lock = LockService.getDocumentLock();     // document-scoped: safest for Sheets

  try {
    console.log(`Attempting to acquire Document Lock (wait ${timeoutMs || 30000}ms)...`);
    lock.waitLock(timeoutMs || 30000);           // waits up to timeout
    console.log("Document Lock acquired.");
    return fn(); // Execute the function while holding the lock
  } catch (e) {
    // Log lock timeout specifically
    if (e.message.includes("Lock wait timeout")) {
      console.error(`Failed to acquire Document Lock within ${timeoutMs || 30000}ms.`);
    } else {
      console.error(`Error during locked operation: ${e.message}`);
    }

    throw e; // Re-throw the error
  } finally {
    try {
      lock.releaseLock();                         // releases even if fn throws
      console.log("Document Lock released.");
    } catch (rlErr) {
      console.error("CRITICAL: Failed to release Document Lock!", rlErr);
      // Depending on context, you might want to handle this failure differently
    }
  }
}

/**
 * Safely writes a large 2D array of data to a sheet in throttled batches, integrating LockService resilience.
 * This function performs the dynamic chunk size adjustment and checks for the overall job time limit.
 * * NOTE: This utility does NOT call SpreadsheetApp.flush(); the calling Orchestrator module must do that.
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

  // Fetch critical config from state or use safe defaults
  var docLockTimeoutMs = state.config && state.config.DOC_LOCK_TIMEOUT_MS || 5000;
  var THROTTLE_THRESHOLD_MS = state.config && state.config.THROTTLE_THRESHOLD_MS || 800;
  var THROTTLE_PAUSE_MS = state.config && state.config.THROTTLE_PAUSE_MS || 200;
  var SOFT_LIMIT_MS = state.config && state.config.SOFT_LIMIT_MS || 0;

  // Fetch throttling constants for math
  var CHUNK_INCREASE_RATE = state.config && state.config.CHUNK_INCREASE_RATE || 50;
  var CHUNK_DECREASE_RATE = state.config && state.config.CHUNK_DECREASE_RATE || 200;
  var MIN_CHUNK_SIZE = state.config && state.config.MIN_CHUNK_SIZE || 50;
  var MAX_CHUNK_SIZE = state.config && state.config.MAX_CHUNK_SIZE || 5000;

  // Fetch mutable metrics from state or initialize
  var startTime = state.metrics && state.metrics.startTime || 0;
  var currentChunkSize = state.config && state.config.currentChunkSize || MIN_CHUNK_SIZE;
  var previousDuration = state.metrics && state.metrics.previousDuration || 0;
  var rowsProcessed = state.metrics && state.metrics.rowsProcessed || 0;
  var i = state.nextBatchIndex || 0; // Resume point

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

    // CRITICAL STEP: Clear ONLY the data area below the header (startRow preserves Row 1)
    // *** FIX: Check if startRow is within the sheet's bounds before clearing ***
    if (startRow > 0 && startRow <= targetSheet.getMaxRows()) {
      targetSheet.getRange(startRow, 1, targetSheet.getMaxRows() - startRow + 1, targetSheet.getMaxColumns()).clearContent();
    } else if (startRow > targetSheet.getMaxRows()) {
      // If startRow is beyond the sheet, there's nothing to clear. This is fine.
    }


    if (state.logInfo) state.logInfo("Starting batch write. Rows: " + dataLength + ", Resume Index: " + i);

    // --- START RESILIENT BATCH WRITE LOOP (while loop) ---
    while (i < dataLength) {

      // 2. PREDICTIVE BAILOUT CHECK (Check total elapsed time for job limit)
      if (startTime && SOFT_LIMIT_MS > 0 && (new Date().getTime() - startTime > SOFT_LIMIT_MS)) {
        var bailoutMsg = "Job reached predictive soft time limit. Reschedule required.";
        if (state.logWarn) state.logWarn(bailoutMsg);

        // Return failure, providing the last duration for worker to assess speed on resume
        return {
          success: false,
          rowsProcessed: rowsProcessed,
          duration: previousDuration, // Provide last known speed
          state: state,
          error: bailoutMsg,
          bailout_reason: "PREDICTIVE_BAILOUT" // CRITICAL TAG
        };
      }

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
      var numRows = batch.length;
      var targetRow = startRow + i;

      // --- ATOMIC CHUNK WRITE LOGIC (Lock/Release/Yank) ---
      if (!docLock.tryLock(docLockTimeoutMs)) {
        throw new Error("LockAcquisitionFailure: Could not acquire Document Lock.");
      }

      try {
        // The actual sheet write call
        targetSheet
          .getRange(targetRow, startCol, numRows, numCols)
          .setValues(batch);

        // --- SUCCESS PATH ---
        docLock.releaseLock();
        previousDuration = new Date().getTime() - chunkStartTime;

        // Throttle Up (The acceleration math)
        if (previousDuration <= THROTTLE_THRESHOLD_MS && previousDuration > 0) {
          currentChunkSize = Math.min(MAX_CHUNK_SIZE, currentChunkSize + CHUNK_INCREASE_RATE);
        }

        // Update metrics in the memory state object
        rowsProcessed += numRows;
        if (state.metrics) state.metrics.rowsProcessed = rowsProcessed;

        // CRITICAL: Manually advance index 'i' and update persistent state
        i += currentChunkSize;
        if (state.nextBatchIndex) state.nextBatchIndex = i;
        if (state.config) state.config.currentChunkSize = currentChunkSize; // Save new chunk size

      } catch (e) {
        // Service Timeout or other write error (The "Yank" operation)
        docLock.releaseLock();
        var errorMessage = "ServiceTimeoutFailure: Batch Write failed at row " + targetRow + ". Error: " + e.message;
        if (state.logError) state.logError(errorMessage);

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

    // SUCCESS STATUS OBJECT: Returns the final memory state and success data
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