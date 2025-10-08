
/**
 * Get or create a sheet, preserving headers.
 * For new sheets, limits the column count to the header length.
 * @param {SpreadsheetApp.Spreadsheet} ss - Spreadsheet object
 * @param {string} name - Sheet name
 * @param {string[]} headers - Array of header strings
 * @returns {GoogleAppsScript.Spreadsheet.Sheet}
 */
function getOrCreateSheet(ss, name, headers) {
  if (!ss || typeof ss.getSheetByName !== 'function') {
    ss = SpreadsheetApp.getActiveSpreadsheet();
  }
  if (!Array.isArray(headers)) {
    throw new Error("getOrCreateSheet: headers must be an array of strings");
  }

  let sheet = ss.getSheetByName(name);

  if (!sheet) {
    // Create new sheet
    sheet = ss.insertSheet(name);

    // Adjust columns to match headers exactly
    const headerCount = headers.length;
    const maxCols = sheet.getMaxColumns();
    if (maxCols > headerCount) {
      sheet.deleteColumns(headerCount + 1, maxCols - headerCount);
    } else if (maxCols < headerCount) {
      sheet.insertColumnsAfter(maxCols, headerCount - maxCols);
    }

    sheet.appendRow(headers);
  } else {
    // Existing sheet: check headers
    const currentHeaders = sheet.getRange(1, 1, 1, headers.length).getValues()[0];
    const same = currentHeaders.every((h, i) => h === headers[i]);
    if (!same) {
      sheet.clearContents();
      sheet.appendRow(headers);
    }
  }

  return sheet;
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
