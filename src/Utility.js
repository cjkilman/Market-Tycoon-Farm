/* global SpreadsheetApp, LockService, Utilities, LoggerEx, CacheService */

// ======================================================================
// SHARED UTILITY BELT (The Engine Room)
// ======================================================================

// --- GLOBAL CONSTANTS ---
// !!! ADD THESE TWO LINES !!!
const MAX_CACHE_CHUNK_SIZE = 95000; // Safe limit under 100KB
const CHUNK_INDEX_SUFFIX = '_CHUNKS';

// NITRO_CONFIG TUNING FOR ASSETS
// 1. Drop MAX_CHUNK_SIZE: Assets are complex, 8000 is too big. 
// 2. Drop SOFT_LIMIT_MS: Bail out at 4.5 mins (270s) instead of 5.5 mins. 
//    This reserves 90s for the "Ghost Gap" in the NEXT run.

const [MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, SOFT_LIMIT_MS, RESCHEDULE_DELAY_MS]
  = [1000, 100, 280000, 5000];
/**
 * [NEW] SHARED NITRO CONFIGURATION
 * Centralized settings for high-volume sheet writes.
 * Workers can import this and override specific fields (like Chunk Sizes).
 */
var NITRO_CONFIG = {
  // --- Shared Stability Settings ---
  TARGET_WRITE_TIME_MS: 3000,
  MAX_FACTOR: 1.8,             // Conservative growth (don't grow chunks too fast)
  THROTTLE_THRESHOLD_MS: -1,   // Disable standard throttling (rely on adaptive)
  THROTTLE_PAUSE_MS: 30000,     // Long pause if we hit a wall
  LAG_SPIKE_THRESHOLD_MS: 60000,

  // --- Baseline Defaults (Override these in Worker if needed) ---
  MAX_CELLS_PER_CHUNK: 40000,
  SOFT_LIMIT_MS: 280000,       // 4.5 Minutes
  MIN_CHUNK_SIZE: 500,
  MAX_CHUNK_SIZE: 4000
};

function reportSheetBloat() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();
  let report = "--- Sheet Bloat Report ---\n";
  let totalExcess = 0;

  sheets.forEach(sheet => {
    const name = sheet.getName();
    const maxRows = sheet.getMaxRows();
    const lastRow = sheet.getLastRow();

    // Always leave a small buffer of empty rows so scripts can append normally
    const blankRows = maxRows - (lastRow < 1 ? 1 : lastRow);

    // Only report sheets that have more than 100 wasted rows at the bottom
    if (blankRows > 100) {
      report += `[${name}] -> ${blankRows} wasted blank rows (Data ends at ${lastRow}, Sheet ends at ${maxRows})\n`;
      totalExcess += blankRows;
    }
  });

  if (totalExcess === 0) {
    report += "All clear. No significant blank row bloat found.";
  } else {
    report += `\nTOTAL EXCESS BLANK ROWS: ${totalExcess}\n`;
    report += "Note: Array formulas evaluate ALL of these. Trimming them will drastically reduce calculation lag.";
  }

  console.log(report);
}

/**
 * Helper to retrieve indices for Setting/Value columns
 */
function _getColIndexMap(headers, names) {
  // SAFETY CHECK: If headers or names are undefined, throw a clear error
  if (!headers || !Array.isArray(headers)) {
    throw new Error("_getColIndexMap: 'headers' is empty or invalid. Check the sheet structure.");
  }
  if (!names || !Array.isArray(names)) {
    throw new Error("_getColIndexMap: 'names' is empty or invalid.");
  }

  const map = {};
  names.forEach(name => {
    const idx = headers.indexOf(name);
    // If the header isn't found, idx is -1. 
    // Your code throws an error, which is good, but let's make it clearer.
    if (idx === -1) throw new Error("Required column header missing in sheet: " + name);
    map[name] = idx;
  });
  return map;
}

function executeBloatTrim() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheets = ss.getSheets();
  let trimmedCount = 0;

  // Safe buffer - always leave a few rows so appends are fast
  const SAFE_BUFFER = 20;

  sheets.forEach(sheet => {
    const name = sheet.getName();
    const maxRows = sheet.getMaxRows();
    let lastRow = sheet.getLastRow();

    // If a sheet is completely empty, treat row 1 as the last row
    if (lastRow < 1) lastRow = 1;

    // Calculate how many rows are completely unused beyond the buffer
    const rowsToDelete = maxRows - lastRow - SAFE_BUFFER;

    if (rowsToDelete > 0) {
      try {
        // Syntax: deleteRows(rowPosition, howMany)
        // Start deleting right after the last data row + buffer
        sheet.deleteRows(lastRow + SAFE_BUFFER + 1, rowsToDelete);
        console.log(`[TRIMMED] ${name}: Removed ${rowsToDelete} rows.`);
        trimmedCount += rowsToDelete;
      } catch (e) {
        console.error(`[ERROR] Failed to trim ${name}: ${e.message}`);
      }
    }
  });

  console.log(`--- BLOAT TRIM COMPLETE ---`);
  console.log(`Total empty rows removed: ${trimmedCount}`);
  console.log(`Your workbook should calculate significantly faster now.`);
}

/**
 * Maps Market Settings from the Location List sheet using a Named Range.
 * Bulletproof against row/column insertions.
 */
function getMarketSettingsMap(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();

  const range = ss.getRangeByName("g_market_settings");
  const map = new Map();

  if (!range) {
    console.error("Named Range 'g_market_settings' not found!");
    return map;
  }

  const values = range.getValues();
  if (values.length < 2) return map; // Not enough data (headers only or empty)

  try {
    // values[0] is the first row of your Named Range (the headers)
    const headers = values[0];
    const col = _getColIndexMap(headers, ['Setting', 'Value']);

    // Process everything after the header row
    const data = values.slice(1);

    for (const r of data) {
      const key = r[col.Setting];
      if (key && String(key).trim() !== "") {

        let rawVal = r[col.Value];
        let val;

        // Clean values exactly like your previous helper
        if (typeof rawVal === 'number') {
          val = rawVal;
        } else {
          const cleaned = String(rawVal || "0").replace(/[^0-9.-]/g, '');
          val = parseFloat(cleaned) || 0;
        }

        map.set(key, val);
      }
    }
  } catch (e) {
    console.error("Error in getMarketSettingsMap: " + e.message);
  }

  return map;
}


/**
 * [THE RACER] - Optimized Reset Strategy
 * Bypasses heavy sheet.clear() operations to prevent V8 INTERNAL crashes.
 * Uses clearContents() to dump the data array instantly while the sheet is awake.
 * Dynamically strips dead columns to protect the 10M cell limit.
 */
function prepareTempSheet(ss, sheetName, headers) {
  // Trigger-safe check
  if (!ss || typeof ss.getSheetByName !== 'function') {
    ss = SpreadsheetApp.getActiveSpreadsheet();
  }
  
  var sheet = ss.getSheetByName(sheetName);

  if (sheet) {
    try {
      sheet.clearContents();
    } catch (e) {
      return { success: false, state: null, error: "ClearContents failed: " + e.message };
    }
  } else {
    try {
      sheet = ss.insertSheet(sheetName);
    } catch (e) {
      return { success: false, state: null, error: "Insert failed: " + e.message };
    }
  }

  // Set Headers & Trim Bloat
  if (headers && headers.length > 0) {
    try {
      var headerRow = Array.isArray(headers[0]) ? headers[0] : headers;
      var requiredCols = headerRow.length;
      
      sheet.getRange(1, 1, 1, requiredCols).setValues([headerRow]);
      sheet.setFrozenRows(1);
      
      // THE GUILLOTINE: Destroy all unused columns
      var maxCols = sheet.getMaxColumns();
      if (maxCols > requiredCols) {
        sheet.deleteColumns(requiredCols + 1, maxCols - requiredCols);
      }
    } catch (e) {
      console.warn("[prepareTempSheet] Header setup/trim warning: " + e.message);
    }
  }

  return { success: true, state: sheet, error: null };
}



/**
 * [THE BUILDER] - Safe, Non-Destructive Sheet Creator.
 * UPDATED: Includes 'fixHeaders' argument to repair missing/mismatched headers.
 * * @param {Spreadsheet} ss - The spreadsheet object.
 * @param {string} name - The name of the sheet.
 * @param {Array} headers - 1D array of header strings.
 * @param {boolean} [fixHeaders=false] - If true, checks Row 1 for mismatch and inserts headers if needed.
 */
function getOrCreateSheet(ss, name, headers, fixHeaders = false) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(name);

  // 1. Create if missing
  if (!sheet) {
    console.log(`Creating new sheet: '${name}'`);
    sheet = ss.insertSheet(name);
  }

  // 2. Handle Headers
  if (headers && headers.length > 0) {
    const lastRow = sheet.getLastRow();
    const maxCols = sheet.getMaxColumns();

    // Safety: Ensure sheet has enough columns for the headers
    if (maxCols < headers.length) {
      sheet.insertColumnsAfter(maxCols, headers.length - maxCols);
    }

    // Case A: Sheet is empty (Safe to write headers)
    if (lastRow === 0) {
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      console.log(`Headers written to new/empty sheet '${name}'`);
    }
    // Case B: Sheet has data, check for Repair (Only if fixHeaders is TRUE)
    else if (fixHeaders === true) {
      // Read current row 1 to see if it matches
      const currentHeaders = sheet.getRange(1, 1, 1, headers.length).getValues()[0];

      // Compare contents
      const isMismatch = JSON.stringify(currentHeaders) !== JSON.stringify(headers);

      if (isMismatch) {
        console.warn(`[getOrCreateSheet] Header mismatch detected in '${name}'. Repairing...`);

        // CRITICAL: Shift existing data down to Row 2 to prevent overwriting
        sheet.insertRowBefore(1);

        // Write correct headers into the NEW empty Row 1
        sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
      }
    }
  }

  return sheet;
}

/**
 * [ANESTHESIA] - Pauses heavy formulas via Helper Cells.
 * Toggles Utility!B3:D3 to 0.
 */
function pauseSheet(ss) {
  // Guard rail: Ensure we have a spreadsheet object
  if (!ss) {
    try { ss = SpreadsheetApp.getActiveSpreadsheet(); } catch (e) { }
    if (!ss) {
      console.warn("[pauseSheet] No Spreadsheet object found.");
      return false;
    }
  }

  try {
    const sheet = ss.getSheetByName('Utility');
    if (sheet) {
      // 1. Set flags B3:D3 to 0 (Pause)
      // 2. Set E3 to current time (Timestamp)
      const timestamp = new Date();

      sheet.getRange("B3:D3").setValues([[0, 0, 0]]);
      sheet.getRange("E3").setValue(timestamp);

      // Optional: Set format to make it human-readable in the sheet
      sheet.getRange("E3").setNumberFormat("yyyy-mm-dd hh:mm:ss");

      SpreadsheetApp.flush();
      console.log("[Anesthesia] Set Utility flags to 0 (Paused) and updated E3 timestamp.");
      return true;
    } else {
      console.warn("[pauseSheet] 'Utility' sheet not found.");
    }
  } catch (e) {
    console.warn("Failed to set Utility flags: " + e.message);
  }
  return false;
}

/**
 * [WAKE UP] - Resumes heavy formulas via Helper Cells.
 * Toggles Utility!B3:D3 to 1.
 * UPDATED: Handles Trigger Event Object correctly.
 */
function wakeUpSheet(ss) {
  // 1. Sanitize Input
  // If called by a trigger, 'ss' is an Event Object, which is not null but lacks methods.
  // We MUST check if it actually has the getSheetByName method.
  if (!ss || typeof ss.getSheetByName !== 'function') {
    try { ss = SpreadsheetApp.getActiveSpreadsheet(); } catch (e) { }

    // If we still don't have a spreadsheet, we can't proceed.
    if (!ss) {
      console.warn("[wakeUpSheet] Could not find Active Spreadsheet (Trigger context).");
      return;
    }
  }

  try {
    const sheet = ss.getSheetByName('Utility');
    if (sheet) {
      // Set flags to 1 to RESUME formulas
      sheet.getRange("B3:D3").setValues([[1, 1, 1]]);
      console.log("[Anesthesia] Set Utility flags to 1 (Resumed).");
    } else {
      console.warn("[wakeUpSheet] 'Utility' sheet not found.");
    }
  } catch (e) {
    console.error("Failed to wake up sheet: " + e.message);
  }
}




/**
 * Internal check to see if the refresh "Engine" is active.
 * Returns true if the ESI toggle (D3) in the Utility sheet is set to 1.
 */
function isEngineRunning_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const util = ss.getSheetByName("Utility");
  if (!util) return false;

  // Checks cell D3 (TICK.ESI)
  return util.getRange("D3").getValue() === 1;
}

/**
 * UTILITY: EMERGENCY DEFIBRILLATOR (Glitch-Proof Version)
 * Checks for Manual Calculation Mode. 
 * If the script engine is broken (missing Enums), it prompts for a UI check and exits safely.
 */
function forceManualMode_Emergency() {
  const funcName = 'forceManualMode_Emergency';
  console.time(funcName);
  console.log(`[${funcName}] Connecting to Active Spreadsheet...`);

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();

    // 1. SAFETY CHECK: Does the Environment have the Definitions?
    if (!SpreadsheetApp.CalculationMode) {
      console.warn("⚠️ SYSTEM GLITCH DETECTED: 'SpreadsheetApp.CalculationMode' is undefined.");
      console.warn("👉 ACTION REQUIRED: Please verify manually in the UI: File > Settings > Calculation > Recalculation is set to 'OFF'.");
      console.log(`[${funcName}] Skipping script-based mode change to prevent crash.`);
      return;
    }

    // 2. CHECK CURRENT STATE
    const currentMode = ss.getCalculationMode();
    console.log(`[${funcName}] Current Mode: ${currentMode}`);

    if (currentMode === SpreadsheetApp.CalculationMode.MANUAL) {
      console.log(`[${funcName}] Success: Spreadsheet is ALREADY in Manual Mode.`);
      return;
    }

    // 3. FORCE MANUAL MODE
    console.log(`[${funcName}] Attempting to set MANUAL mode...`);
    ss.setCalculationMode(SpreadsheetApp.CalculationMode.MANUAL);
    SpreadsheetApp.flush();

    console.log(`[${funcName}] SUCCESS. Calculation Mode set to MANUAL.`);

  } catch (e) {
    console.error(`[${funcName}] FAILED: ${e.message}`);
  } finally {
    console.timeEnd(funcName);
  }
}

// --- SMART WRITER (Self-Contained Anesthesia Edition) ---
function writeDataToSheet(sheetName, dataArray, startRow, startCol, stateObject) {
  // 1. DEFINE STATE AND CONFIG
  var state = stateObject || { config: {}, metrics: {} };
  if (!state.config) state.config = {};
  if (!state.metrics) state.metrics = {};

  var ss = state.ss || SpreadsheetApp.getActiveSpreadsheet();
  var targetSheet;

  // Defaults
  const TARGET_WRITE_TIME_MS = Number(state.config.TARGET_WRITE_TIME_MS) || 1000;
  const LAG_SPIKE_THRESHOLD_MS = Number(state.config.LAG_SPIKE_THRESHOLD_MS) || 60000;
  const MAX_FACTOR = Number(state.config.MAX_FACTOR) || 1.5;
  const MAX_CELLS_PER_CHUNK = Number(state.config.MAX_CELLS_PER_CHUNK) || 25000;

  var docLockTimeoutMs = Number(state.config.DOC_LOCK_TIMEOUT_MS) || 30000;
  var THROTTLE_THRESHOLD_MS = Number(state.config.THROTTLE_THRESHOLD_MS) || 800;
  var THROTTLE_PAUSE_MS = Number(state.config.THROTTLE_PAUSE_MS) || 200;
  var SOFT_LIMIT_MS = Number(state.config.SOFT_LIMIT_MS) || 280000;


  var CHUNK_DECREASE_RATE = Number(state.config.CHUNK_DECREASE_RATE) || 200;
  var MIN_CHUNK_SIZE = Number(state.config.MIN_CHUNK_SIZE) || 50;
  var MAX_CHUNK_SIZE = Number(state.config.MAX_CHUNK_SIZE) || 5000;

  var startTime = Number(state.metrics.startTime) || 0;
  var currentChunkSize = Number(state.config.currentChunkSize) || MIN_CHUNK_SIZE;
  var previousDuration = Number(state.metrics.previousDuration) || 0;
  var i = Number(state.nextBatchIndex) || 0;

  currentChunkSize = Math.min(MAX_CHUNK_SIZE, Math.max(MIN_CHUNK_SIZE, currentChunkSize));
  var previousChunkSize = 0;
  var dataLength = dataArray.length;
  var numCols = (dataLength > 0) ? dataArray[0].length : 0;

  // --- [NEW] TIME SANITY CHECK ---
  var nowCheck = new Date().getTime();
  var elapsedSoFar = nowCheck - startTime;
  var timeRemaining = SOFT_LIMIT_MS - elapsedSoFar;

  if (state.logWarn) {
    state.logWarn(`[TIME CHECK] Writer Start. 
      > Global Start: ${startTime} 
      > Current Time: ${nowCheck} 
      > Elapsed Pre-Write: ${elapsedSoFar}ms 
      > Budget: ${SOFT_LIMIT_MS}ms 
      > Remaining: ${timeRemaining}ms`);
  }


  // --- PRE-FLIGHT ---
  try {
    targetSheet = ss.getSheetByName(sheetName);
    if (!targetSheet) throw new Error("Sheet not found: " + sheetName);
    if (numCols === 0) return { success: true, rowsProcessed: 0, duration: 0, state: state };

    const MAX_ROWS_BY_COLUMNS = Math.floor(MAX_CELLS_PER_CHUNK / numCols);
    currentChunkSize = Math.min(currentChunkSize, MAX_ROWS_BY_COLUMNS);

    if (state.logInfo) state.logInfo("Starting batch write. Total: " + dataLength + ", Resume: " + i);

    // --- 1. ACQUIRE LOCK (Once) ---
    var docLock = LockService.getDocumentLock();
    if (!docLock.tryLock(docLockTimeoutMs)) {
      return { success: false, rowsProcessed: i, state: state, error: "Lock Failed", bailout_reason: "LOCK_CONFLICT" };
    }

    // --- 2. ENGAGE ANESTHESIA (Manual Mode) ---
    // Moved to external Usage, Callers Now handle this.

    try {
      // --- 3. BATCH LOOP ---
      // Added lock check to loop condition
      while (i < dataLength && (new Date().getTime() - startTime) < SOFT_LIMIT_MS && docLock.hasLock()) {

        if (previousDuration > THROTTLE_THRESHOLD_MS) {
          currentChunkSize = Math.max(MIN_CHUNK_SIZE, currentChunkSize - CHUNK_DECREASE_RATE);
          Utilities.sleep(THROTTLE_PAUSE_MS);
          previousDuration = 0;
        }

        currentChunkSize = Math.min(currentChunkSize, MAX_ROWS_BY_COLUMNS);
        currentChunkSize = Math.max(currentChunkSize, MIN_CHUNK_SIZE);

        var chunkStartTime = new Date().getTime();
        var chunkSizeToUse = Math.min(currentChunkSize, dataLength - i);
        var batch = dataArray.slice(i, i + chunkSizeToUse);
        var numRows = batch.length;
        var targetRow = startRow + i;

        targetSheet.getRange(targetRow, startCol, numRows, numCols).setValues(batch);

        previousDuration = new Date().getTime() - chunkStartTime;

        // --- [NEW] CIRCUIT BREAKER TRIPPED? ---
        if (previousDuration > LAG_SPIKE_THRESHOLD_MS) {
          if (state.logWarn) state.logWarn(`[CRITICAL] Lag Spike Detected (${previousDuration}ms). Bailing out.`);

          // 1. Advance the index because THIS batch did finish (eventually)
          state.nextBatchIndex = i + numRows;

          // 2. set the Chunk Sixe to the last good run
          state.config.currentChunkSize = previousChunkSize;

          // 3. Return PREDICTIVE_BAILOUT so Orchestrator saves state and restarts cleanly
          return { success: false, bailout_reason: "PREDICTIVE_BAILOUT", state: state };
        }
        // save the good chunksize for the next job;
        previousChunkSize = currentChunkSize;

        var ratio = previousDuration / TARGET_WRITE_TIME_MS;

        if (ratio < 0.5) currentChunkSize = Math.ceil(currentChunkSize * ((currentChunkSize < 1000) ? 2.0 : MAX_FACTOR));
        else if (ratio < 0.8) currentChunkSize = Math.ceil(currentChunkSize * 1.05);
        else if (ratio > 1.2) currentChunkSize = Math.floor(currentChunkSize * 0.6);

        currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.min(currentChunkSize, MAX_CHUNK_SIZE));

        if (state.logInfo) state.logInfo(`[Write] Batch: ${numRows} | Time: ${previousDuration}ms | Next: ${currentChunkSize}`);

        i += numRows;

        state.nextBatchIndex = i;

        state.config.currentChunkSize = currentChunkSize;
        state.metrics.previousDuration = previousDuration;
      }

    } catch (loopError) {
      var errorMessage = "ServiceTimeoutFailure: Batch Write failed at row " + (startRow + i) + ". Error: " + loopError.message;
      if (state.logError) state.logError(errorMessage);
      state.config.currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.round(currentChunkSize / 2));
      return { success: false, rowsProcessed: i, state: state, error: errorMessage, bailout_reason: "SERVICE_FAILURE" };
    } finally {
      docLock.releaseLock();
    }

    if (i < dataArray.length) {
      return { success: false, bailout_reason: "PREDICTIVE_BAILOUT", state: state };
    }
    return { success: true, rowsProcessed: i, state: { ...state, nextBatchIndex: 0 } };

  } catch (e) {
    if (state.logError) state.logError("CRITICAL FAILURE in writeDataToSheet: " + e.message);
    return { success: false, rowsProcessed: i, state: state, error: e.message, bailout_reason: "CATASTROPHIC_FAILURE" };
  }
}

// ======================================================================
// CACHE SHARDING HELPERS (Required by InventoryManager)
// ======================================================================

/**
 * NITRO SHARDED CACHE ENGINE: Handles memory-safe 100KB block allocations.
 * Replaces high-overhead PropertiesService with fast, transaction-safe in-memory caching.
 */

/**
 * Splits a large string into 100KB chunks and stores them in ScriptCache.
 * @param {string} key The base cache key.
 * @param {string} content The string content to cache.
 * @param {number} ttlSeconds Expiration time in seconds (Max 21600 / 6 Hours).
 * @returns {boolean} True on success.
 */
function _chunkAndPut(key, content, ttlSeconds) {
  if (!content) return false;

  const cache = CacheService.getScriptCache();
  const MAX_SIZE = 100000; // 100KB safe allocation threshold
  const safeTtl = Math.min(Number(ttlSeconds) || 21600, 21600); // Guard against ESI max limits

  try {
    // Case 1: Payload fits in a single memory slot
    if (content.length <= MAX_SIZE) {
      // Look for old meta markers BEFORE overwriting the primary key slot
      const existingChunksMeta = cache.get(key + "_chunks");

      cache.put(key, content, safeTtl);

      // Clean up orphaned tail shards from a previous larger historical payload write
      if (existingChunksMeta) {
        _deleteShardedData(key, parseInt(existingChunksMeta, 10));
      }
      return true;
    }

    // Case 2: Multi-slot Sharding Needed
    const chunks = [];
    let offset = 0;
    while (offset < content.length) {
      chunks.push(content.substring(offset, offset + MAX_SIZE));
      offset += MAX_SIZE;
    }

    const chunkMap = {};
    chunks.forEach((chunk, index) => {
      chunkMap[`${key}_${index}`] = chunk;
    });
    chunkMap[`${key}_chunks`] = chunks.length.toString();

    // Batch commit memory states in a single atomic payload transaction
    cache.putAll(chunkMap, safeTtl);
    return true;
  } catch (e) {
    console.error(`[CACHE FAULT] _chunkAndPut failed for key ${key}: ${e.message}`);
    return false;
  }
}

function atomicSwapAndFlush(ss, targetName, tempName, repairMap = null, holdAnesthesia = false, requireLock = true) {
  let docLock = null;

  if (requireLock) {
    docLock = LockService.getDocumentLock();
    if (!docLock.tryLock(30000)) return { success: false, errorMessage: "Could not acquire Document Lock." };
  }

  let needsWakeUp = false;

  try {
    // 1. ADMINISTER ANESTHESIA
    if (!holdAnesthesia && typeof pauseSheet === 'function') {
      needsWakeUp = pauseSheet(ss);
    }

    const tempSheet = ss.getSheetByName(tempName);
    if (!tempSheet) throw new Error(`Temp sheet '${tempName}' not found.`);

    // 2. READ DATA IN BULK
    const sourceValues = tempSheet.getDataRange().getValues();
    const numRows = sourceValues.length;
    const numCols = numRows > 0 ? sourceValues[0].length : 0;

    if (numRows === 0 || (numRows === 1 && numCols === 1 && sourceValues[0][0] === "")) {
      throw new Error("Temp sheet contains no data.");
    }

    // 3. TARGET PREP
    let finalSheet = ss.getSheetByName(targetName) || ss.insertSheet(targetName);

    // 4. DETERMINE ANCHOR & CACHE DIMENSIONS
    let pasteRow = 1;
    let pasteCol = 1;
    let anchorRange = null;
    let firstRepairKey = null;

    if (repairMap) {
      const keys = Object.keys(repairMap);
      if (keys.length > 0) {
        firstRepairKey = keys[0];
        anchorRange = finalSheet.getRange(repairMap[firstRepairKey]);
        pasteRow = anchorRange.getRow();
        pasteCol = anchorRange.getColumn();
      }
    }

    const initialMaxRows = finalSheet.getMaxRows();
    const initialMaxCols = finalSheet.getMaxColumns();
    const dataRowsEnd = pasteRow + numRows - 1;
    const dataColsEnd = pasteCol + numCols - 1;

    // 5. TARGETED CLEAR
    if (initialMaxRows >= pasteRow && initialMaxCols >= pasteCol) {
      finalSheet.getRange(
        pasteRow,
        pasteCol,
        initialMaxRows - pasteRow + 1,
        initialMaxCols - pasteCol + 1
      ).clearContent();
    }

    // 6. EXPAND BOUNDARIES (Prevents out-of-bounds crashes)
    if (dataRowsEnd > initialMaxRows) finalSheet.insertRowsAfter(initialMaxRows, dataRowsEnd - initialMaxRows);
    if (dataColsEnd > initialMaxCols) finalSheet.insertColumnsAfter(initialMaxCols, dataColsEnd - initialMaxCols);

    // 7. WRITE DATA
    finalSheet.getRange(pasteRow, pasteCol, numRows, numCols).setValues(sourceValues);

    // 8. AUTO-TRIM EXCESS
    const frozenRows = finalSheet.getFrozenRows();
    const frozenCols = finalSheet.getFrozenColumns();

    const currentMaxRows = finalSheet.getMaxRows();
    const safeTargetRows = Math.max(dataRowsEnd, frozenRows + 1);
    if (currentMaxRows > safeTargetRows) {
      finalSheet.deleteRows(safeTargetRows + 1, currentMaxRows - safeTargetRows);
    }

    const currentMaxCols = finalSheet.getMaxColumns();
    const safeTargetCols = Math.max(dataColsEnd, frozenCols + 1);
    if (currentMaxCols > safeTargetCols) {
      finalSheet.deleteColumns(safeTargetCols + 1, currentMaxCols - safeTargetCols);
    }

    // 9. REWIRE NAMED RANGES (Optimized Local Cache + In-Place Update)
    if (repairMap) {
      // 1. Fetch ALL named ranges in the document exactly once (1 API Call)
      const allNamedRanges = ss.getNamedRanges();

      // 2. Build a local Javascript dictionary for instant, zero-latency lookups
      const nrCache = {};
      for (let i = 0; i < allNamedRanges.length; i++) {
        nrCache[allNamedRanges[i].getName()] = allNamedRanges[i];
      }

      for (const [rangeName, startPos] of Object.entries(repairMap)) {
        if (!startPos) continue;

        try {
          // Fall back to cached anchor to skip another getRange API call when possible
          const providedRange = (rangeName === firstRepairKey) ? anchorRange : finalSheet.getRange(startPos);
          const startRow = providedRange.getRow();
          const startCol = providedRange.getColumn();

          if (dataRowsEnd >= startRow) {
            const exactNumRows = dataRowsEnd - startRow + 1;
            let exactNumCols = providedRange.getNumColumns();

            if (exactNumCols === 1 && String(startPos).indexOf(':') === -1) {
              exactNumCols = Math.max(1, dataColsEnd - startCol + 1);
            }

            const exactRange = finalSheet.getRange(startRow, startCol, exactNumRows, exactNumCols);

            // 3. Instant local check bypasses the ss.getNamedRange() API call
            if (nrCache[rangeName]) {
              nrCache[rangeName].setRange(exactRange);
            } else {
              ss.setNamedRange(rangeName, exactRange); // Only hits the server if creation is required
            }
          }
        } catch (e) {
          console.warn(`[AtomicSwap] Failed to update Named Range '${rangeName}': ${e.message}`);
        }
      }
    }


    // 10. CLEANUP Temp
    tempSheet.clearContents();

    return { success: true, errorMessage: null };

  } catch (e) {
    return { success: false, errorMessage: e.message };
  } finally {
    // 11. WAKE UP & RELEASE LOCK
    if (!holdAnesthesia && needsWakeUp && typeof wakeUpSheet === 'function') {
      wakeUpSheet(ss);
    }
    if (requireLock && docLock) {
      docLock.releaseLock();
    }
  }
}



/**
 * Retrieves and reassembles sharded data from ScriptCache.
 * @param {string} key The base cache key.
 * @returns {string|null} The full string content, or null if missing/incomplete.
 */
function _getAndDechunk(key) {
  const cache = CacheService.getScriptCache();

  try {
    // Check for meta-key indicating if data is sharded
    const countStr = cache.get(key + "_chunks");

    // Case A: Single Entry (Not sharded)
    if (!countStr) {
      return cache.get(key);
    }

    // Case B: Reassemble Chunks
    const count = parseInt(countStr, 10);
    if (isNaN(count) || count <= 0) return null;

    const keys = [];
    for (let i = 0; i < count; i++) {
      keys.push(`${key}_${i}`);
    }

    // High-speed parallel block retrieval (Bypasses sequential cache loop lag)
    const chunks = cache.getAll(keys);
    let fullContentBuffer = "";

    for (let i = 0; i < count; i++) {
      const part = chunks[`${key}_${i}`];
      if (!part) {
        console.warn(`[CACHE CORRUPTION] Missing chunk index ${i} for key ${key}. Incomplete read aborted.`);
        return null;
      }
      fullContentBuffer += part;
    }

    return fullContentBuffer;
  } catch (e) {
    console.error(`[CACHE FAULT] _getAndDechunk failed for key ${key}: ${e.message}`);
    return null;
  }
}

/**
 * UTILITY: Cleans old trailing fragments out of cache cells to prevent data leakage.
 */
function _deleteShardedData(baseKey, totalOldChunks) {
  const cache = CacheService.getScriptCache();
  if (isNaN(totalOldChunks) || totalOldChunks <= 0) return;

  const keysToPurge = [`${baseKey}_chunks`];
  for (let i = 0; i < totalOldChunks; i++) {
    keysToPurge.push(`${baseKey}_${i}`);
  }

  try {
    cache.removeAll(keysToPurge);
  } catch (e) {
    console.warn(`[CACHE CLEANUP WARNING] Could not wipe secondary shards for ${baseKey}: ${e.message}`);
  }
}

function manualEmergencyReset() {
  const sp = PropertiesService.getScriptProperties();
  sp.deleteProperty('marketDataJobLeaseUntil');
  sp.deleteProperty('marketDataJobStep');
  console.log("Locks cleared.");
}

function guardedSheetTransaction(fn, timeoutMs) {
  const lock = LockService.getDocumentLock();

  if (!lock.tryLock(timeoutMs || 5000)) {
    return { success: false, error: "Lock Conflict: Script is currently busy." };
  }

  try {
    return { success: true, state: fn() };
  } catch (e) {
    return { success: false, error: e.message };
  } finally {
    lock.releaseLock();
  }
}

function withSheetLock(fn, timeoutMs) {
  const result = guardedSheetTransaction(fn, timeoutMs);

  if (!result.success) {
    throw new Error(`withSheetLock Failed: ${result.error}`);
  }

  return result.state;
}

var Utility = (function () {
  function median(values, opts) {
    opts = opts || {};
    var ignoreNonPositive = opts.ignoreNonPositive !== false;
    if (!values || !values.length) return '';
    var nums = values.map(function (v) { return (typeof v === 'number' ? v : Number(v)); })
      .filter(function (v) { return Number.isFinite(v) && (!ignoreNonPositive || v > 0); })
      .sort(function (a, b) { return a - b; });
    if (!nums.length) return '';
    var mid = Math.floor(nums.length / 2);
    return (nums.length % 2) ? nums[mid] : (nums[mid - 1] + nums[mid]) / 2;
  }
  return { median: median };
})();