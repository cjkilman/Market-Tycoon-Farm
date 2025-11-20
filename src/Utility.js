/* global SpreadsheetApp, LockService, Utilities, LoggerEx, CacheService */

// ======================================================================
// SHARED UTILITY BELT (The Engine Room)
// ======================================================================

// --- GLOBAL CONSTANTS ---
var MAX_CACHE_CHUNK_SIZE = 8000; 
var CHUNK_INDEX_SUFFIX = ':IDX';

/**
 * Get or create a sheet, preserving headers.
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
 * Zero-Downtime "Value Swap".
 */
function atomicSwapAndFlush(ss, targetName, tempName) {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('AtomicSwap') : console);
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const docLock = LockService.getDocumentLock();

  if (!docLock.tryLock(30000)) {
    return { success: false, errorMessage: "Could not acquire Document Lock for Swap." };
  }

  try {
    const targetSheet = ss.getSheetByName(targetName);
    const tempSheet = ss.getSheetByName(tempName);

    if (!tempSheet) return { success: false, errorMessage: `Temp sheet '${tempName}' not found.` };

    if (!targetSheet) {
      tempSheet.setName(targetName);
      log.info(`Target '${targetName}' missing. Renamed temp.`);
    } else {
      const tempRange = tempSheet.getDataRange();
      const newValues = tempRange.getValues();
      const newRows = newValues.length;
      const newCols = newValues[0].length;
      
      const currentMaxRows = targetSheet.getMaxRows();
      const currentMaxCols = targetSheet.getMaxColumns();

      // Expand
      if (currentMaxRows < newRows) targetSheet.insertRowsAfter(currentMaxRows, newRows - currentMaxRows);
      if (currentMaxCols < newCols) targetSheet.insertColumnsAfter(currentMaxCols, newCols - currentMaxCols);
      
      // Overwrite
      if (newRows > 0) targetSheet.getRange(1, 1, newRows, newCols).setValues(newValues);
      
      // Prune
      if (currentMaxRows > newRows) {
         targetSheet.deleteRows(newRows + 1, currentMaxRows - newRows);
      }
      if (currentMaxCols > newCols) {
         targetSheet.deleteColumns(newCols + 1, currentMaxCols - newCols);
      }

      ss.deleteSheet(tempSheet);
      log.info(`Swapped and Trimmed '${targetName}': ${newRows} rows x ${newCols} cols.`);
    }

    SpreadsheetApp.flush();
    return { success: true, errorMessage: null };
  } catch (e) {
    log.error(`Swap Failed: ${e.message}`);
    return { success: false, errorMessage: e.message };
  } finally {
    docLock.releaseLock();
  }
}

// --- SHARED CACHE SHARDING FUNCTIONS ---

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
    if(numChunksRaw) {
        const num = parseInt(numChunksRaw,10);
        const keys = [key + CHUNK_INDEX_SUFFIX];
        for(let i=0; i<num; i++) keys.push(key + ':' + i);
        cache.removeAll(keys);
    }
}

/**
 * Writes data to sheet with UNLOCKED Nitro performance.
 */
function writeDataToSheet(sheetName, dataArray, startRow, startCol, stateObject) {
    var state = stateObject || { config: {}, metrics: {} };
    var ss = state.ss || SpreadsheetApp.getActiveSpreadsheet();
    var logInfo = state.logInfo || console.log;
    
    var MAX_CELLS_PER_CHUNK = state.config.MAX_CELLS_PER_CHUNK || 20000;
    var TARGET_WRITE_TIME_MS = state.config.TARGET_WRITE_TIME_MS || 2000;
    var MIN_CHUNK_SIZE = state.config.MIN_CHUNK_SIZE || 100;
    var MAX_CHUNK_SIZE = state.config.MAX_CHUNK_SIZE || 2000;
    var SOFT_LIMIT_MS = state.config.SOFT_LIMIT_MS || 280000;
    
    // --- NEW: Use config parameters for acceleration ---
    var MAX_FACTOR = state.config.MAX_FACTOR || 1.2; // Default to 1.2 if not set
    var THROTTLE_THRESHOLD_MS = state.config.THROTTLE_THRESHOLD_MS || (TARGET_WRITE_TIME_MS * 0.8);

    var i = state.nextBatchIndex || 0;
    var currentChunkSize = state.config.currentChunkSize || MIN_CHUNK_SIZE;
    var startTime = state.metrics.startTime || new Date().getTime();

    var targetSheet = ss.getSheetByName(sheetName);
    if (!targetSheet) return { success: false, error: "Sheet not found: " + sheetName };

    var numCols = dataArray.length > 0 ? dataArray[0].length : 0;
    if (numCols === 0) return { success: true, rowsProcessed: 0 };

    var maxRowsByCols = Math.floor(MAX_CELLS_PER_CHUNK / numCols);
    currentChunkSize = Math.min(currentChunkSize, maxRowsByCols);

    try {
        while (i < dataArray.length) {
            if (new Date().getTime() - startTime > SOFT_LIMIT_MS) {
                return { 
                    success: false, 
                    bailout_reason: "PREDICTIVE_BAILOUT",
                    state: { ...state, nextBatchIndex: i, config: { ...state.config, currentChunkSize } }
                };
            }

            var chunkSize = Math.min(currentChunkSize, dataArray.length - i);
            var batch = dataArray.slice(i, i + chunkSize);
            var chunkStart = new Date().getTime();

            try {
                targetSheet.getRange(startRow + i, startCol, batch.length, numCols).setValues(batch);
                SpreadsheetApp.flush(); 
                
                var duration = new Date().getTime() - chunkStart;
                var oldChunk = currentChunkSize;
                
                // --- UNLOCKED NITRO LOGIC ---
                if (duration < THROTTLE_THRESHOLD_MS) {
                    // Super Fast? Accelerate by MAX_FACTOR (e.g., 2.0x)
                    currentChunkSize = Math.min(MAX_CHUNK_SIZE, Math.ceil(currentChunkSize * MAX_FACTOR));
                } else if (duration > TARGET_WRITE_TIME_MS) {
                    // Too slow? Pump the brakes
                    currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.floor(currentChunkSize * 0.8));
                }
                // Else: In the sweet spot, maintain speed.

                logInfo(`[Write] Batch: ${batch.length} rows | Time: ${duration}ms | Chunk: ${oldChunk} -> ${currentChunkSize}`);
                
                i += batch.length;

            } catch (e) {
                console.warn(`Write failed at ${i}. Retrying with smaller chunk.`);
                currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.floor(currentChunkSize / 2));
                return { 
                    success: false, 
                    error: e.message,
                    bailout_reason: "SERVICE_FAILURE",
                    state: { ...state, nextBatchIndex: i, config: { ...state.config, currentChunkSize } }
                };
            }
        }
        return { success: true, rowsProcessed: i, state: { ...state, nextBatchIndex: 0 } };
    } catch (e) {
        return { success: false, error: e.message };
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