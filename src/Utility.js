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

    // 2. SET HEADERS AND MINIMALLY ADJUST COLUMNS
    // We only perform heavy pruning/adjusting when NEW, not when existing.
    const maxCols = sheet.getMaxColumns();
    if (maxCols > headerCount) {
      // Delete excess columns added by default (26 -> 9)
      sheet.deleteColumns(headerCount + 1, maxCols - headerCount);
    } else if (maxCols < headerCount) {
      sheet.insertColumnsAfter(maxCols, headerCount - maxCols);
    }

    sheet.getRange(1, 1, 1, headerCount).setValues([headers]);
    console.log(`Headers set for new sheet '${sheetName}'.`);

  } else {
    // EXISTING SHEET: Perform no expensive column/header manipulation.
    // The sheet worker (Orchestrator.js) will handle clearing contents (clearContent) 
    // and resetting headers only if needed, or rely on the final swap logic.
    console.log(`Sheet '${sheetName}' exists. Skipping column/header cleanup.`);

    // --- CRITICAL RE-INSERTION FOR ERROR RECOVERY (Step 2) ---
    // If we're here, it means we are in the ERROR RECOVERY path of Orchestrator (Setup Step 2)
    // The recovery path in Orchestrator *must* succeed, but its sheet manipulation
    // caused the timeout. The only thing we absolutely must ensure is the header is present.
    try {
        // Attempt to just set the headers, ignoring what's already there (fast overwrite)
        sheet.getRange(1, 1, 1, headerCount).setValues([headers]);
        console.log(`Headers overwritten/verified for existing sheet.`);
    } catch (e) {
        // If even this fails, rethrow.
        throw new Error(`Failed during minimal header set on existing sheet: ${e.message}`);
    }
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
 * Utility helpers — generic functions reused across modules.
 * Keep this file focused on non-domain-specific helpers.
 */
var Utility = (function(){
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
    var nums = values.map(function(v){ return (typeof v === 'number' ? v : Number(v)); })
                     .filter(function(v){ return Number.isFinite(v) && (!ignoreNonPositive || v > 0); })
                     .sort(function(a,b){ return a-b; });
    if (!nums.length) return '';
    var mid = Math.floor(nums.length/2);
    return (nums.length % 2) ? nums[mid] : (nums[mid-1] + nums[mid]) / 2;
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
    var hm = (typeof PT !== 'undefined' && PT && typeof PT.coerceHM === 'function') ? PT.coerceHM(val) : {h:0, m:0};
    return hm;
  }
  function _toHM(val) {
    var hm = toHM(val);
    return [hm.h|0, hm.m|0];
  }
  // Register global legacy _toHM if not already defined
  try { if (typeof globalThis !== 'undefined' && typeof globalThis._toHM !== 'function') { globalThis._toHM = _toHM; } } catch (e) {}

  return {
    median: median,
    toHM: toHM,
    _toHM: _toHM,
    inWindow: inWindow,
    _inWindow_: inWindow
  };
})();
