/* global SpreadsheetApp, LockService, Utilities, LoggerEx, CacheService */

// ======================================================================
// SHARED UTILITY BELT (The Engine Room)
// ======================================================================

// --- GLOBAL CONSTANTS ---
var MAX_CACHE_CHUNK_SIZE = 8000;
var CHUNK_INDEX_SUFFIX = ':IDX';

/**
 * [THE RACER] - Destructive, Optimized Temp Sheet Creator.
 */
function prepareTempSheet(ss, sheetName, headers) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(sheetName);

  if (sheet) {
    sheet.clear();
    const currentCols = sheet.getMaxColumns();
    if (currentCols > headers.length) {
      sheet.deleteColumns(headers.length + 1, currentCols - headers.length);
    }
  } else {
    sheet = ss.insertSheet(sheetName);
    const currentCols = sheet.getMaxColumns();
    if (currentCols > headers.length) {
      sheet.deleteColumns(headers.length + 1, currentCols - headers.length);
    }
  }

  if (headers && headers.length > 0) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  }
  sheet.setFrozenRows(1);
  return sheet;
}

/**
 * [THE BUILDER] - Safe, Non-Destructive Sheet Creator.
 */
function getOrCreateSheet(ss, name, headers) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  let sheet = ss.getSheetByName(name);
  if (!sheet) {
    console.log(`Creating new sheet: '${name}'`);
    sheet = ss.insertSheet(name);
    if (headers && headers.length > 0) {
      sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    }
  }
  return sheet;
}

/**
 * Performs a Safe Atomic Swap (Hybrid Safety Version).
 * 1. Locks the script.
 * 2. Attempts to set Manual Mode.
 * 3. Rewires Named Ranges.
 * 4. If Manual Mode worked -> Deletes Old.
 * 5. If Manual Mode failed -> Renames Old (Prevents Timeout).
 */
/**
 * Performs a Safe Atomic Swap (Clean Version).
 * Tries Manual Mode. If it fails, forces Delete (May timeout, but no Trash).
 */
function atomicSwapAndFlush(ss, targetName, tempName, repairMap = null) {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('AtomicSwap') : console);
  const docLock = LockService.getDocumentLock();

  if (!docLock.tryLock(30000)) return { success: false, errorMessage: "Could not acquire Document Lock." };

  let originalCalcMode = null;

  try {
    const targetSheet = ss.getSheetByName(targetName);
    const tempSheet = ss.getSheetByName(tempName);

    if (!tempSheet) return { success: false, errorMessage: `Temp sheet '${tempName}' not found.` };

    // --- PHASE 1: ANESTHESIA (Manual Mode) ---
    try {
      if (ss.getCalculationMode) {
        originalCalcMode = ss.getCalculationMode();
        if (originalCalcMode !== SpreadsheetApp.CalculationMode.MANUAL) {
          ss.setCalculationMode(SpreadsheetApp.CalculationMode.MANUAL);
          log.info("[Swap] Engine silenced (MANUAL mode).");
        }
      }
    } catch (glitch) {
      log.warn(`[Swap] V8 Glitch - Manual Mode Failed. Proceeding with Risk of Timeout.`);
    }

    // --- PHASE 2: REWIRE ---
    if (targetSheet) {
      const namedRanges = ss.getNamedRanges();
      const targetID = targetSheet.getSheetId();
      let rewired = 0;

      namedRanges.forEach(nr => {
        try {
          if (nr.getRange().getSheet().getSheetId() === targetID) {
            nr.setRange(tempSheet.getRange(nr.getRange().getA1Notation()));
            rewired++;
          }
        } catch (e) { /* Optional repair logic */ }
      });
      if (rewired > 0) SpreadsheetApp.flush();

      // --- PHASE 3: EXECUTION (Strict Delete) ---
      if (ss.getNumSheets() === 1) ss.insertSheet();
      ss.deleteSheet(targetSheet);
    }

    // --- PHASE 4: RENAME NEW ---
    tempSheet.setName(targetName);
    return { success: true, errorMessage: null };

  } catch (e) {
    return { success: false, errorMessage: e.message };
  } finally {
    // --- RESTORE STATE ---
    if (originalCalcMode && originalCalcMode !== SpreadsheetApp.CalculationMode.MANUAL) {
        try { ss.setCalculationMode(originalCalcMode); } catch (e) {}
    }
    docLock.releaseLock();
  }
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

  var dataLength = dataArray.length;
  var numCols = (dataLength > 0) ? dataArray[0].length : 0;

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
    var originalCalcMode = null;
    try {
      if (ss.getCalculationMode) {
        originalCalcMode = ss.getCalculationMode();
        if (originalCalcMode !== SpreadsheetApp.CalculationMode.MANUAL) {
          ss.setCalculationMode(SpreadsheetApp.CalculationMode.MANUAL);
          if (state.logInfo) state.logInfo("[Writer] Manual Mode engaged.");
        }
      }
    } catch (glitch) {
      if (state.logWarn) state.logWarn(`[Writer] Manual Mode Glitch: ${glitch.message}. Proceeding in Auto.`);
    }

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
       // --- 4. WAKE UP & UNLOCK ---
       if (originalCalcMode && originalCalcMode !== SpreadsheetApp.CalculationMode.MANUAL) {
          try { ss.setCalculationMode(originalCalcMode); } catch (e) {}
       }
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
 * Splits a large string into 100KB chunks and stores them in ScriptCache.
 * @param {string} key The base cache key.
 * @param {string} content The string content to cache.
 * @param {number} ttlSeconds Expiration time in seconds.
 * @returns {boolean} True on success.
 */
function _chunkAndPut(key, content, ttlSeconds) {
  const cache = CacheService.getScriptCache();
  const MAX_SIZE = 100000; // Safe limit (100KB) per entry
  
  try {
    // Case 1: Fits in single entry
    if (content.length <= MAX_SIZE) {
      cache.put(key, content, ttlSeconds);
      // Clean up any potential old chunks from a previous larger save
      const oldChunkCount = cache.get(key + "_chunks");
      if (oldChunkCount) _deleteShardedData(key); 
      return true;
    }
    
    // Case 2: Needs Sharding
    const chunks = [];
    let offset = 0;
    while (offset < content.length) {
      chunks.push(content.substr(offset, MAX_SIZE));
      offset += MAX_SIZE;
    }
    
    // Batch write chunks to cache
    const chunkMap = {};
    chunks.forEach((c, i) => {
      chunkMap[key + "_" + i] = c;
    });
    chunkMap[key + "_chunks"] = chunks.length.toString();
    
    cache.putAll(chunkMap, ttlSeconds);
    return true;
  } catch (e) {
    console.error(`_chunkAndPut failed for ${key}: ${e.message}`);
    return false;
  }
}

/**
 * Retrieves and reassembles sharded data from ScriptCache.
 * @param {string} key The base cache key.
 * @returns {string|null} The full string content, or null if missing/incomplete.
 */
function _getAndDechunk(key) {
  const cache = CacheService.getScriptCache();
  
  // 1. Check for meta-key indicating chunks
  const countStr = cache.get(key + "_chunks");
  
  // Case A: Single Entry (No chunks)
  if (!countStr) {
    return cache.get(key); 
  }
  
  // Case B: Reassemble Chunks
  const count = parseInt(countStr, 10);
  if (isNaN(count)) return null;

  const keys = [];
  for(let i=0; i<count; i++) keys.push(key + "_" + i);
  
  const chunks = cache.getAll(keys);
  let full = "";
  
  for(let i=0; i<count; i++) {
    const part = chunks[key + "_" + i];
    if (!part) {
      console.warn(`_getAndDechunk: Missing chunk ${i} for ${key}. Cache corrupted.`);
      return null; 
    }
    full += part;
  }
  return full;
}

/**
 * Deletes all shards associated with a cache key.
 * @param {string} key The base cache key.
 */
function _deleteShardedData(key) {
  const cache = CacheService.getScriptCache();
  const countStr = cache.get(key + "_chunks");
  
  if (countStr) {
    const count = parseInt(countStr, 10);
    for(let i=0; i<count; i++) {
      cache.remove(key + "_" + i);
    }
    cache.remove(key + "_chunks");
  }
  // Also remove the base key just in case
  cache.remove(key);
}

function manualEmergencyReset() {
  const sp = PropertiesService.getScriptProperties();
  sp.deleteProperty('marketDataJobLeaseUntil');
  sp.deleteProperty('marketDataJobStep');
  console.log("Locks cleared.");
}

function guardedSheetTransaction(fn, timeoutMs) {
  var lock = LockService.getDocumentLock();
  if (!lock.tryLock(timeoutMs || 5000)) return { success: false, error: "Lock Conflict/Busy" };
  try { return { success: true, state: fn() }; }
  catch (e) { return { success: false, error: e.message }; }
  finally { lock.releaseLock(); }
}

function withSheetLock(fn, timeoutMs) { return guardedSheetTransaction(fn, timeoutMs).state; }

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