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
 * Zero-Downtime "True Sheet Swap".
 * 1. Deletes the Old Sheet (Instant).
 * 2. Renames the Temp Sheet to Live (Instant).
 * * WARNING: This breaks standard formulas like '=Sheet!A1'. 
 * * You MUST use Named Ranges or INDIRECT() in your dashboard.
 */
function atomicSwapAndFlush(ss, targetName, tempName) {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('AtomicSwap') : console);
  const swStart = new Date().getTime();

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const docLock = LockService.getDocumentLock();

  if (!docLock.tryLock(30000)) {
    return { success: false, errorMessage: "Could not acquire Document Lock for Swap." };
  }

  try {
    log.info(`[Swap] Lock Acquired. Time: ${new Date().getTime() - swStart}ms`);

    const targetSheet = ss.getSheetByName(targetName);
    const tempSheet = ss.getSheetByName(tempName);

    if (!tempSheet) return { success: false, errorMessage: `Temp sheet '${tempName}' not found.` };

    // 1. DELETE OLD (The "Drop" Phase)
    if (targetSheet) {
      // Safety: Don't delete if it's the only sheet (rare edge case)
      if (ss.getNumSheets() === 1) {
        ss.insertSheet(); // Create a dummy so we can delete the target
      }
      try {
        ss.deleteSheet(targetSheet);
        log.info(`[Swap] Deleted old '${targetName}'.`);
      } catch (delError) {
        log.error(`[Swap] Failed to delete target: ${delError.message}`);
        return { success: false, errorMessage: delError.message };
      }
    }

    // 2. RENAME NEW (The "Swap" Phase)
    tempSheet.setName(targetName);
    log.info(`[Swap] Renamed temp to '${targetName}'.`);

    const totalTime = new Date().getTime() - swStart;
    log.info(`[Swap] SUCCESS. Total Duration: ${totalTime}ms`);

    return { success: true, errorMessage: null };
  } catch (e) {
    log.error(`[Swap] CRASH: ${e.message}`);
    return { success: false, errorMessage: e.message };
  } finally {
    docLock.releaseLock();
  }
}

// --- SHARED CACHE SHARDING ---
function _chunkAndPut(key, largeString, ttl = 21600) {
  const cache = CacheService.getScriptCache();
  if (!largeString || largeString.length === 0) return false;
  const chunks = [];
  const numChunks = Math.ceil(largeString.length / MAX_CACHE_CHUNK_SIZE);
  for (let i = 0; i < numChunks; i++) {
    const start = i * MAX_CACHE_CHUNK_SIZE;
    const end = start + MAX_CACHE_CHUNK_SIZE;
    chunks.push(largeString.substring(start, end));
  }
  const keysToWrite = {};
  for (let i = 0; i < chunks.length; i++) {
    keysToWrite[key + ':' + i] = chunks[i];
  }
  keysToWrite[key + CHUNK_INDEX_SUFFIX] = String(numChunks);
  try {
    cache.putAll(keysToWrite, ttl);
    return true;
  } catch (e) {
    console.error(`Cache Write Failed: ${e.message}`);
    return false;
  }
}

function _getAndDechunk(key) {
  const cache = CacheService.getScriptCache();
  const numChunksRaw = cache.get(key + CHUNK_INDEX_SUFFIX);
  if (!numChunksRaw) return null;
  const numChunks = parseInt(numChunksRaw, 10);
  const keysToGet = [];
  for (let i = 0; i < numChunks; i++) keysToGet.push(key + ':' + i);
  const chunks = cache.getAll(keysToGet);
  const result = [];
  for (let i = 0; i < numChunks; i++) {
    const chunk = chunks[key + ':' + i];
    if (chunk == null) return null;
    result.push(chunk);
  }
  return result.join('');
}

function _deleteShardedData(key) {
  const cache = CacheService.getScriptCache();
  const numChunksRaw = cache.get(key + CHUNK_INDEX_SUFFIX);
  if (numChunksRaw) {
    const num = parseInt(numChunksRaw, 10);
    const keys = [key + CHUNK_INDEX_SUFFIX];
    for (let i = 0; i < num; i++) keys.push(key + ':' + i);
    cache.removeAll(keys);
  }
}

// --- SMART WRITER ---
function writeDataToSheet(sheetName, dataArray, startRow, startCol, stateObject) {
  // 1. DEFINE STATE AND CONFIG
  var state = stateObject || { config: {}, metrics: {} };
  if (!state.config) state.config = {};
  if (!state.metrics) state.metrics = {};

  // Micro-Optimization: Use state object's SS reference, fall back if not provided
  var ss = state.ss || SpreadsheetApp.getActiveSpreadsheet();
  var targetSheet;

  // Define constants for the new logic (can be overridden in state.config)
  const TARGET_WRITE_TIME_MS = Number(state.config.TARGET_WRITE_TIME_MS) || 100;
  const MAX_FACTOR = Number(state.config.MAX_FACTOR) || 1.5;

  // --- CRITICAL FIX: Harden variable initialization against null/undefined ---
  const MAX_CELLS_PER_CHUNK = Number(state.config.MAX_CELLS_PER_CHUNK) || 25000; // New ceiling
  var docLockTimeoutMs = Number(state.config.DOC_LOCK_TIMEOUT_MS) || 5000;
  var THROTTLE_THRESHOLD_MS = Number(state.config.THROTTLE_THRESHOLD_MS) || 800;
  var THROTTLE_PAUSE_MS = Number(state.config.THROTTLE_PAUSE_MS) || 200;
  var SOFT_LIMIT_MS = Number(state.config.SOFT_LIMIT_MS) || 280000;

  // Fetch throttling constants for math
  var CHUNK_DECREASE_RATE = Number(state.config.CHUNK_DECREASE_RATE) || 200;
  var MIN_CHUNK_SIZE = Number(state.config.MIN_CHUNK_SIZE) || 50;
  var MAX_CHUNK_SIZE = Number(state.config.MAX_CHUNK_SIZE) || 5000;
  // --- END CRITICAL FIX ---

  // Fetch mutable metrics from state or initialize
  var startTime = Number(state.metrics.startTime) || 0;
  var currentChunkSize = Number(state.config.currentChunkSize) || MIN_CHUNK_SIZE;
  var previousDuration = Number(state.metrics.previousDuration) || 0;
  var i = Number(state.nextBatchIndex) || 0; // Resume point
  var rowsProcessed = i; // Initial rows processed equals the start index 'i'

  // Ensure chunk size is within bounds on resume
  currentChunkSize = Math.min(MAX_CHUNK_SIZE, Math.max(MIN_CHUNK_SIZE, currentChunkSize));

  var dataLength = dataArray.length;
  var numCols = 0;
  var docLock = LockService.getDocumentLock();

  try {
    targetSheet = ss.getSheetByName(sheetName);
    if (!targetSheet) {
      throw new Error("Sheet not found: " + sheetName);
    }

    numCols = dataLength > 0 ? dataArray[0].length : 0;
    if (numCols === 0) {
      if (dataLength > 0) throw new Error("Data array is corrupted (zero columns).");
      if (state.logInfo) state.logInfo("Write SUCCESS. Data array is empty.");
      return { success: true, rowsProcessed: 0, duration: 0, state: state, error: "" };
    }

    // --- NEW: Calculate Max Rows allowed by Column Count (Prevents Crash) ---
    const MAX_ROWS_BY_COLUMNS = Math.floor(MAX_CELLS_PER_CHUNK / numCols);
    currentChunkSize = Math.min(currentChunkSize, MAX_ROWS_BY_COLUMNS);


    if (state.logInfo) state.logInfo("Starting batch write. Total Rows: " + dataLength + ", Resume Index: " + i + ". Max Safe Rows: " + MAX_ROWS_BY_COLUMNS);

    // --- START RESILIENT BATCH WRITE LOOP (while loop) ---
    while (i < dataLength && (new Date().getTime() - startTime) < SOFT_LIMIT_MS) {

      // 3. DYNAMIC THROTTLING CHECK & ADJUSTMENT (Throttle Down)
      if (previousDuration > THROTTLE_THRESHOLD_MS) {
        currentChunkSize = Math.max(MIN_CHUNK_SIZE, currentChunkSize - CHUNK_DECREASE_RATE);

        if (state.logInfo) state.logInfo("[THROTTLE] Pausing for " + THROTTLE_PAUSE_MS + "ms to yield execution.");
        Utilities.sleep(THROTTLE_PAUSE_MS);
        previousDuration = 0; // Reset duration after pause
      }

      // --- CRITICAL FIX: Cap currentChunkSize by the Column-based Limit ---
      currentChunkSize = Math.min(currentChunkSize, MAX_ROWS_BY_COLUMNS);
      currentChunkSize = Math.max(currentChunkSize, MIN_CHUNK_SIZE); // Ensure it respects the minimum floor

      var chunkStartTime = new Date().getTime();
      var chunkSizeToUse = Math.min(currentChunkSize, dataLength - i);
      var batch = dataArray.slice(i, i + chunkSizeToUse);
      var numRows = batch.length; // Actual number of rows being written
      var targetRow = startRow + i;

      // --- ATOMIC CHUNK WRITE LOGIC (Lock/Release/Yank) ---
      if (!docLock.tryLock(docLockTimeoutMs)) {
        // Lock acquisition failure: bail out, checkpoint is current index 'i'.
        var lockError = "LockAcquisitionFailure: Could not acquire Document Lock.";
        if (state.logWarn) state.logWarn(lockError + " Index: " + i);

        state.nextBatchIndex = i; // Save checkpoint (start of failed batch)
        state.config.currentChunkSize = currentChunkSize;

        return {
          success: false,
          rowsProcessed: i,
          duration: 0,
          state: state,
          error: lockError,
          bailout_reason: "LOCK_CONFLICT"
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
        var oldChunk = currentChunkSize;
        var ratio = previousDuration / TARGET_WRITE_TIME_MS;

        if (ratio < 0.5) {
          var factor = (currentChunkSize < 1000) ? 2.0 : 1.2;
          currentChunkSize = Math.ceil(currentChunkSize * factor);
        } else if (ratio < 0.8) {
          var factor = 1.05;
          currentChunkSize = Math.ceil(currentChunkSize * factor);
        } else if (ratio > 1.2) {
          currentChunkSize = Math.floor(currentChunkSize * 0.6);
        } else if (ratio > 1.0) {
          currentChunkSize = Math.floor(currentChunkSize * 0.8);
        }
        currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.min(currentChunkSize, MAX_CHUNK_SIZE));
        state.logInfo(`[Write] Batch: ${batch.length} rows | Time: ${previousDuration}ms | Chunk: ${oldChunk} -> ${currentChunkSize}`);

        // Update metrics and advance index
        i += numRows; // CRITICAL: Advance index for the next loop iteration
        rowsProcessed = i; // Update the official processed count

        // Save transient state to the object (for the caller to persist)
        state.nextBatchIndex = i;
        state.config.currentChunkSize = currentChunkSize;
        state.metrics.previousDuration = previousDuration;

      } catch (e) {
        // Service Timeout/Write Error (The "Yank" operation)
        docLock.releaseLock();
        var errorMessage = "ServiceTimeoutFailure: Batch Write failed at row " + targetRow + ". Error: " + e.message;
        if (state.logError) state.logError(errorMessage);

        // Aggressive Chunk Size Reduction
        currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.round(currentChunkSize / 2));

        // Checkpoint remains at 'i' (start of the failed batch)
        state.nextBatchIndex = i;
        state.config.currentChunkSize = currentChunkSize;

        // FAILURE STATUS OBJECT: Signal a schedule for retry
        return {
          success: false,
          rowsProcessed: i,
          duration: previousDuration,
          state: state,
          error: errorMessage,
          bailout_reason: "SERVICE_FAILURE"
        };
      }
    }
    // --- END RESILIENT BATCH WRITE LOOP ---

    if (i < dataArray.length) {
      return { success: false, bailout_reason: "PREDICTIVE_BAILOUT", state: { ...state, nextBatchIndex: i, config: { ...state.config, currentChunkSize } } };
    }
    return { success: true, rowsProcessed: i, state: { ...state, nextBatchIndex: 0 } };

  } catch (e) {
    // CATASTROPHIC FAILURE
    var finalErrorMsg = "CRITICAL FAILURE in writeDataToSheet. Error: " + e.message;
    if (state.logError) state.logError(finalErrorMsg);

    return {
      success: false,
      rowsProcessed: i, // Last known safe checkpoint (or 0)
      duration: 0,
      state: state,
      error: finalErrorMsg,
      bailout_reason: "CATASTROPHIC_FAILURE"
    };
  }
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