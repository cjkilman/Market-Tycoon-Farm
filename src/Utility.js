/**
 * Get or create a sheet, preserving headers and adjusting dimensions.
 * For new sheets, limits the column count to the header length.
 * Optionally sets the maximum number of rows.
 * @param {SpreadsheetApp.Spreadsheet} ss - Spreadsheet object
 * @param {string} name - Sheet name
 * @param {string[]} headers - Array of header strings
 * @param {number} [maxRows] - Optional. The desired total number of data rows (excluding header). If provided, rows will be added or deleted to match this size + 1 (for header).
 * @returns {GoogleAppsScript.Spreadsheet.Sheet}
 */
function getOrCreateSheet(ss, name, headers) {
  // --- Input Validation ---
  if (!ss || typeof ss.getSheetByName !== 'function') {
    // Attempt to get active spreadsheet if ss is invalid
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

  const sheetName = name.trim(); // Use trimmed name
  const headerCount = headers.length;
  let sheet = ss.getSheetByName(sheetName);
  let isNewSheet = false;

  if (!sheet) {
    // Create new sheet
    console.log(`Creating new sheet: '${sheetName}'`);
    sheet = ss.insertSheet(sheetName);
    isNewSheet = true;

    // Adjust columns to match headers exactly
    const maxCols = sheet.getMaxColumns();
    if (maxCols > headerCount) {
      sheet.deleteColumns(headerCount + 1, maxCols - headerCount);
    } else if (maxCols < headerCount) {
      sheet.insertColumnsAfter(maxCols, headerCount - maxCols);
    }

    // Append headers to the new sheet
    sheet.appendRow(headers);
    console.log(`Headers appended to new sheet '${sheetName}'.`);

  } else {
    // Existing sheet: check and potentially prune/add columns
    const maxCols = sheet.getMaxColumns();
     if (maxCols > headerCount) {
      console.log(`Pruning columns in existing sheet '${sheetName}' from ${maxCols} to ${headerCount}`);
      sheet.deleteColumns(headerCount + 1, maxCols - headerCount);
    } else if (maxCols < headerCount) {
       console.log(`Adding columns in existing sheet '${sheetName}' from ${maxCols} to ${headerCount}`);
      sheet.insertColumnsAfter(maxCols, headerCount - maxCols);
    }

    // Check headers, clear and reset if they don't match
    try {
        const currentHeaders = sheet.getRange(1, 1, 1, headerCount).getValues()[0];
        const same = currentHeaders.every((h, i) => String(h).trim() === String(headers[i]).trim()); // Trim headers for comparison
        if (!same) {
          console.warn(`Headers mismatch in sheet '${sheetName}'. Clearing content below header and resetting headers.`);
          // Clear content below header row
          const lastRow = sheet.getLastRow();
          if (lastRow > 1) {
              sheet.getRange(2, 1, lastRow - 1, sheet.getMaxColumns()).clearContent();
          }
          // Set new headers
          sheet.getRange(1, 1, 1, headerCount).setValues([headers]);
          isNewSheet = true; // Treat as new for row adjustment logic below
          console.log(`Headers reset for sheet '${sheetName}'.`);
        }
    } catch (headerError) {
        console.error(`Error reading headers for sheet '${sheetName}': ${headerError.message}. Clearing and resetting.`);
        sheet.clearContents(); // Clear everything if header read fails
        sheet.getRange(1, 1, 1, headerCount).setValues([headers]); // Set new headers
        isNewSheet = true;
    }
  }

  

  return sheet;
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
