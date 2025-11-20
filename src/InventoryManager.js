/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, scheduleOneTimeTrigger, executeWithTryLock, getCorpAuthChar, writeDataToSheet, getOrCreateSheet, CacheService 
// Note: Other shared functions like _buildHeaderMap, _buildSdeTypeMap, etc., are assumed to be in this file below.
*/

// ======================================================================
// EVE ONLINE ASSET AND LOCATION MANAGEMENT MODULE
// ======================================================================

// --- 0. MODULE-LEVEL LOGGER ---
// CRITICAL FIX: Wrap the definition to prevent SyntaxError if defined elsewhere
if (typeof SAFE_CONSOLE_SHIM === 'undefined') {
    var SAFE_CONSOLE_SHIM = {
      log: console.log,
      info: console.log, 
      warn: console.warn,
      error: console.error,
      startTimer: function() { return { stamp: function() {} }; } 
    };
}
const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('InventoryManager') : SAFE_CONSOLE_SHIM);

// --- 1. GLOBAL CONSTANTS AND DEPENDENCIES ---

// Keys for storing job state in Script Properties (Resilience)
const ASSET_CACHE_DATA_KEY = 'AssetCache_Data_V2';
const ASSET_CACHE_ROW_INDEX_KEY = 'AssetCache_NextRow';
const ASSET_JOB_STATUS_KEY = 'AssetCache_Status_Key';
const ASSET_WRITE_STATE_KEY = 'ASSET_WRITE_STATE'; 

// Cache Key for SDE Type Map (New Persistent Layer)
const SDE_TYPE_MAP_KEY = 'SDE_invTypes_TypeMap'; 

// Time Constants (in milliseconds)
const SOFT_LIMIT_MS = 280000; // 4m 40s (Safety limit for predictive reschedule)
const DOC_LOCK_TIMEOUT_MS = 5000; // TryLock 5s for chunk writing
const CRIT_LOCK_WAIT_MS = 60000; // WaitLock 60s for sheet clear/prepare
const RESCHEDULE_PAUSE_MS = 10000; // 10 second pause after failure

// --- CHUNKING/THROTTLING CONSTANTS (DEFINED LOCALLY FOR SCOPE FIX) ---
const THROTTLE_THRESHOLD_MS = 800;
const THROTTLE_PAUSE_MS = 200;
const CHUNK_INCREASE_RATE = 50; 
const CHUNK_DECREASE_RATE = 200; 
const MAX_CHUNK_SIZE = 1000;
const MIN_CHUNK_SIZE = 50;
// --- END CHUNKING/THROTTLING CONSTANTS ---


// EVE CONSTANTS
const ASSET_ID_MIN_BOUND = 100000000000; // Used to differentiate asset IDs from type IDs.
const NPC_STATION_ID_MAX = 70000000;      // IDs above this are typically player-owned structures

// Known EVE Bug/Exclusion Lists (Sets for O(1) lookup)
const GHOST_ITEM_IDS = new Set([]);
const EXCLUDED_CONTAINER_TYPE_IDS = new Set([28318]); // Delivery Hangars, Fleet Hangars

// Sheet Names and Headers
const CACHE_SHEET_NAME = 'CorpWarehouseStock';
const LOCATION_MANAGER_SHEET_NAME = 'LocationManager';
const MATERIAL_HANGAR_SHEET_NAME = 'MaterialHangar';
const LOCATION_CACHE_SHEET = "Location_Name_Cache"; // The master cache
const LOCATION_CACHE_HEADERS = ['locationID', 'locationName']; // Standard headers
const ASSET_CACHE_HEADERS = ["is_blueprint_copy", "is_singleton", "item_id", "location_flag", "location_id", "location_type", "quantity", "type_id"];

// --- CRITICAL FIX: Ensure these are defined before use in finalizeAssetCache ---
const NUM_ASSET_COLS = ASSET_CACHE_HEADERS.length; // 8
const CACHE_NAMED_RANGE = 'NR_CORP_ASSETS_CACHE';
// -----------------------------------------------------------------------------

// Global sheet cache map for chunk writing (Required for _prepareCacheSheet)
const _sheetCache = {};

// --- NEW JOB STATUS FLAG ---
const ASSET_JOB_STATUS_FLAG = {
    FETCHED: 'FETCHED',
    SHEET_CLEARED: 'SHEET_CLEARED', // NEW: Sheet is clear, ready for first write
    WRITING: 'WRITING'
};

// ======================================================================
// === CRITICAL SHARDING UTILITIES (Fixes "Argument too large" via CacheService) =======
// ======================================================================

const MAX_SHARD_SIZE_CHARS = 8000; // Max property size is 9KB, use 8KB for safety.
const ASSET_CACHE_TTL_SECONDS = 3600; // 1 hour TTL requested by user

/** Returns the Cache Service instance. */
function _assetCache() {
    return CacheService.getScriptCache();
}

/** Cleans up all shards for a given key prefix (Cache Service). */
function _clearShardedProperty(baseKey) {
    const cache = _assetCache();
    const keysToDelete = [baseKey + '_COUNT'];
    
    // We can't list all keys, so we rely on the count key to estimate how many to delete.
    const shardCountString = cache.get(baseKey + '_COUNT');
    if (shardCountString) {
        const shardCount = parseInt(shardCountString, 10);
        for (let i = 0; i < shardCount; i++) {
            keysToDelete.push(baseKey + '_' + i);
        }
    }
    // Delete all known shard keys and the count key
    cache.remove(keysToDelete);
}

/** Writes a large string (like JSON) by splitting it into smaller chunks (shards) in CacheService. */
function _writeShardedProperty(baseKey, largeString) {
    const cache = _assetCache();
    // Clear old shards first, relying on TTL otherwise
    _clearShardedProperty(baseKey);
    
    let parts = {};
    const totalLength = largeString.length;
    let index = 0;
    let shardCount = 0;

    while (index < totalLength) {
        const end = Math.min(index + MAX_SHARD_SIZE_CHARS, totalLength);
        const chunk = largeString.substring(index, end);
        const shardKey = baseKey + '_' + shardCount;
        
        parts[shardKey] = chunk;
        
        index = end;
        shardCount++;
    }

    // Write all data chunks in a batch operation with 1 hour TTL
    cache.putAll(parts, ASSET_CACHE_TTL_SECONDS);
    
    // Save the count/metadata under a unique key, also with 1 hour TTL
    cache.put(baseKey + '_COUNT', shardCount.toString(), ASSET_CACHE_TTL_SECONDS);
    log.info('[SHARDING] Saved ' + shardCount + ' shards to CacheService for key ' + baseKey);
}

/** Reads a large string from sharded property keys in CacheService and reconstructs the string. */
function _readShardedProperty(baseKey) {
    const cache = _assetCache();
    const countKey = baseKey + '_COUNT';
    const shardCountString = cache.get(countKey); 
    
    if (!shardCountString) return null;

    const shardCount = parseInt(shardCountString, 10);
    
    const keysToFetch = [];
    for (let i = 0; i < shardCount; i++) {
        keysToFetch.push(baseKey + '_' + i);
    }
    
    // Fetch all shards in one go
    const shards = cache.getAll(keysToFetch);
    
    // Check for integrity (ensure all expected keys were returned)
    if (Object.keys(shards).length !== shardCount) {
        log.error('[SHARDING] Data integrity failure: Missing shards. Counted ' + Object.keys(shards).length + ' shards, expected ' + shardCount);
        return null; 
    }
    
    // Reconstruct the full string
    let fullString = '';
    for (let i = 0; i < shardCount; i++) {
        const chunk = shards[baseKey + '_' + i];
        // We already checked the count, so this should be fine, but a final check is safe.
        if (!chunk) { 
            log.error('[SHARDING] Data integrity failure during reconstruction. Missing index ' + i);
            return null;
        }
        fullString += chunk;
    }
    return fullString;
}

// ======================================================================
// ======================================================================


/**
 * Resolves a Structure ID by checking the persistent cache first, then ESI.
 * @param {number} structureId
 * @param {object} ss The Spreadsheet object for fetching auth char.
 * @param {object} structureCacheMap The local Map to update/check.
 * @param {object} SCRIPT_PROP The persistent property service.
 * @returns {string} The resolved structure name.
 */
function _getStructureNameFromCacheOrESI(structureId, ss, structureCacheMap, SCRIPT_PROP) {
    // Check local map (for current run)
    if (structureCacheMap.has(structureId)) {
        return structureCacheMap.get(structureId);
    }

    const structureIdString = structureId.toString();
    const CACHE_KEY = 'StructName_' + structureIdString;
    const structuresClient = getGESIStructuresClient_();
    
    // Check persistent cache
    const cachedName = SCRIPT_PROP.getProperty(CACHE_KEY);
    if (cachedName) {
        structureCacheMap.set(structureId, cachedName);
        return cachedName;
    }
    
    // Fallback: Must call ESI (slow, sequential, authenticated call)
    const mainChar = (typeof getCorpAuthChar === 'function') ? getCorpAuthChar(ss) : GESI.getMainCharacter();
    
    try {
        const structureData = structuresClient.executeRaw({ structure_id: structureId, name: mainChar });
        const name = structureData && structureData.name ? structureData.name : 'Structure (ID: ' + structureIdString + ')';
        
        // Save to persistent cache and local map
        SCRIPT_PROP.setProperty(CACHE_KEY, name);
        structureCacheMap.set(structureId, name);
        Utilities.sleep(50); // Respect the 50ms delay after ESI call
        return name;
    } catch (e) {
        log.error('[ESI_CACHE] WARNING: ESI call for structure ID ' + structureId + ' failed: ' + e.message + '.');
        const fallbackName = 'Structure (ID: ' + structureIdString + ')';
        // Save fallback to prevent hitting ESI again on next run
        SCRIPT_PROP.setProperty(CACHE_KEY, fallbackName);
        structureCacheMap.set(structureId, fallbackName);
        return fallbackName;
    }
}

// --- 2. PERSISTENT CLIENTS AND CORE CLASSES ---

/** Returns a persistent ESIClient instance for corporations_corporation_divisions */
function getGESIDivisionsClient_() {
  return GESI.getClient().setFunction('corporations_corporation_divisions');
}

/** Returns a persistent ESIClient instance for corporations_corporation_assets_names */
function getGESINamesClient_() {
  return GESI.getClient().setFunction('corporations_corporation_assets_names');
}

/** Returns a persistent ESIClient instance for universe_names (resolves stations, etc.) */
function getGESIUniverseNamesClient_() {
  return GESI.getClient().setFunction('universe_names');
}

/** Returns a persistent ESIClient instance for universe_structures_structure (resolves player structures) */
function getGESIStructuresClient_() {
  return GESI.getClient().setFunction('universe_structures_structure');
}

/**
 * A simple class to store Office Folder data (the "Branch") and its
 * parent Station/Structure ID (The "Root").
 * FIX: Added locationName property.
 */
class CorpOffice {
  /**
   * @param {number} itemId The Office Folder's Item ID (The "Branch" ID)
   * @param {number} locationId The Station/Structure ID (The "Root" ID)
   */
  constructor(itemId, locationId) {
    this.itemId = itemId;
    this.locationId = locationId;
    this.locationName = null; // Will be populated after name resolution
  }
}


// --- 3. ESI DATA ACQUISITION AND WRITING HELPERS ---

/**
 * Executes sequential ESI requests to fetch all pages of corporation assets
 * using client.executeRaw().
 * * This guarantees data integrity across all pages, matching GESI's reliability.
 * * This is the final, robust implementation that solves the data loss and size issue.
 */
function _fetchAssetsConcurrently(mainChar) {
  const SCRIPT_NAME = '_fetchAssetsConcurrently';
  const client = GESI.getClient().setFunction('corporations_corporation_assets');

  let maxPages = 1;
  const headerRow = ['is_blueprint_copy', 'is_singleton', 'item_id', 'location_flag', 'location_id', 'location_type', 'quantity', 'type_id'];
  const allAssets = [headerRow]; // Start with header

  try {
    // 1. Fetch Page 1 using executeRaw
    const resultPage1 = client.executeRaw({ page: 1 });
    
    // CRITICAL FIX: Assume GESI executeRaw returns raw array or {data: Array} and extract data array.
    const dataPage1 = Array.isArray(resultPage1) ? resultPage1 : (resultPage1.data || []);
    const metadata = resultPage1.metadata || {}; // Metadata contains X-Pages header

    // Check for data existence
    if (!dataPage1 || dataPage1.length === 0) {
        log.error('[' + SCRIPT_NAME + '] CRITICAL: Page 1 returned no data.');
        throw new Error('Failed to fetch initial asset page. Result was empty or malformed.');
    }

    maxPages = Number(metadata['X-Pages'] || metadata['x-pages']) || 1;
    log.info('[' + SCRIPT_NAME + '] Found ' + maxPages + ' pages of assets. Fetching sequentially via executeRaw...');

    // Process Page 1 Data
    dataPage1.forEach(obj => {
      allAssets.push([
        obj.is_blueprint_copy, obj.is_singleton, obj.item_id, obj.location_flag, 
        obj.location_id, obj.location_type, obj.quantity, obj.type_id
      ]);
    });
    
    // 2. Fetch Subsequent Pages (Sequentially via executeRaw)
    for (let page = 2; page <= maxPages; page++) {
      const result = client.executeRaw({ page: page });
      
      const pageData = Array.isArray(result) ? result : (result.data || []);

      if (pageData.length === 0 && page <= maxPages) {
        log.error('[' + SCRIPT_NAME + '] CRITICAL: Page ' + page + ' returned no data. Expected ' + maxPages + ' pages total.');
        throw new Error('Asset Fetch CRITICAL: Sequential page ' + page + ' failed. Data integrity compromised.');
      }
      
      pageData.forEach(obj => {
        allAssets.push([
          obj.is_blueprint_copy, obj.is_singleton, obj.item_id, obj.location_flag, 
          obj.location_id, obj.location_type, obj.quantity, obj.type_id
        ]);
      });
      Utilities.sleep(50); // ESI recommendation for sequential large volume calls
    }

  } catch (e) {
    // This catches GESI's internal error (like 401) or our manual throw
    log.error('[' + SCRIPT_NAME + '] FATAL ESI ERROR: ' + e.message);
    throw new Error('Asset Fetch CRITICAL: ESI failed during fetch/parse. Reschedule fetch.');
  }

  log.info('[' + SCRIPT_NAME + '] Sequential fetch complete. Total asset rows found: ' + (allAssets.length - 1));
  return allAssets;
}

/**
 * Clears the target sheet and writes the headers. Must be called before Phase 2.
 * NOTE: Headers are intentionally placed in ROW 2 (A2:H2) per user instruction.
 */
function _prepareCacheSheet() {
  const SCRIPT_NAME = '_prepareCacheSheet';

  const docLock = LockService.getDocumentLock();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let cacheSheet = ss.getSheetByName(CACHE_SHEET_NAME);

  if (!cacheSheet) { log.error('[' + SCRIPT_NAME + '] ERROR: Target sheet \'' + CACHE_SHEET_NAME + '\' not found.'); return { success: false, duration: 0 }; }

  // Add to cache for chunk writer
  _sheetCache[CACHE_SHEET_NAME] = cacheSheet;

  const lockStartTime = new Date().getTime();

  // Use a very long waitLock for this critical, one-time operation.
  docLock.waitLock(CRIT_LOCK_WAIT_MS);
  const lockAcquiredTime = new Date().getTime();

  try {
    const writeStartTime = new Date().getTime(); 
    
    // 1. AGGRESSIVE CLEAR: Delete all rows below the header row (Row 2).
    // This physically shrinks the sheet, ensuring no old content remains.
    const lastRow = cacheSheet.getMaxRows();
    if (lastRow > 2) {
      // Delete from Row 3 (the first data row) downwards
      cacheSheet.getRange('A3:H' + lastRow).clearContent();
    }
    
    // 2. HEADER WRITE: Write headers to ROW 2 (A2:H2).
    // This must be done AFTER deletion as deletion preserves the header.
    cacheSheet.getRange('A2:H2').setValues([ASSET_CACHE_HEADERS]);

    const criticalWriteDuration = new Date().getTime() - writeStartTime;
    log.info('[' + SCRIPT_NAME + '] CRIT-WRITE: Deleted old rows and wrote headers in ' + criticalWriteDuration + 'ms. Headers placed in ROW 2.');
    
    return { success: true, duration: lockAcquiredTime - lockStartTime };

  } catch (e) {
    log.error('[' + SCRIPT_NAME + '] CRITICAL ERROR during sheet preparation: ' + e);
    return { success: false, duration: 0 };
  }
  finally {
    docLock.releaseLock();

    log.info('[' + SCRIPT_NAME + '] LOCK STATS: Lock Released after preparation.');
  }
}


// --- 4. RESILIENT ASSET CACHE FUNCTIONS (Worker and Dispatch) ---

/**
 * Set this function as the target of the recurring hourly trigger.
 * It ensures the main recurring trigger is not deleted by the retry logic.
 */
function _dispatchAssetJob() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  // If the job is paused (has a next row index), let the pending retry trigger handle it.
  if (SCRIPT_PROP.getProperty(ASSET_CACHE_ROW_INDEX_KEY)) {
    log.info("Dispatch: Resumable asset job is paused. Bailing out to allow pending retry trigger to resume it.");
    return;
  }

  // If not paused, dispatch the main trigger function.
  if (typeof cacheAllCorporateAssetsTrigger === 'function') {
    log.info("Dispatch: Job not paused. Calling cacheAllCorporateAssetsTrigger to initiate or acquire lock.");
    cacheAllCorporateAssetsTrigger();
  } else {
    log.error("Dispatch: Error: cacheAllCorporateAssetsTrigger function not found.");
  }
}

/**
 * Executes cacheAllCorporateAssets.
 * **This must be the function referenced by scheduleOneTimeTrigger.**
 * This function exists to provide the 'executeWithTryLock' wrapper.
 */
function cacheAllCorporateAssetsTrigger() {
  const funcName = 'cacheAllCorporateAssets';

  // executeWithTryLock acquires the script lock and holds it for the worker's execution.
  const result = executeWithTryLock(cacheAllCorporateAssets, funcName);

  if (result === null) {
    // Action: Log and rely on the higher-level trigger (Dispatcher)
    log.warn(funcName + ' skipped due to Script Lock conflict. Waiting for next scheduled run.');
  }
}

/**
 * Orchestrator function to fetch all corporate assets and write them to a
 * cache sheet using a resumable, stateful batch write process.
 *
 * **CRITICAL FIX:** Now uses sharded CacheService to store large data sets.
 */
function cacheAllCorporateAssets() {
  const SCRIPT_NAME = 'cacheAllCorporateAssets';

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const START_TIME = new Date().getTime();

  // --- PHASE 1: Data Acquisition (Fetch or Resume) ---
  let processedAssets = [];
  
  // CRITICAL: Read sharded data from CacheService
  const cachedAssetDataJson = _readShardedProperty(ASSET_CACHE_DATA_KEY);
  
  let nextWriteRow = parseInt(SCRIPT_PROP.getProperty(ASSET_CACHE_ROW_INDEX_KEY) || '0', 10);
  let jobStatus = SCRIPT_PROP.getProperty(ASSET_JOB_STATUS_KEY);

  if (cachedAssetDataJson) {
    processedAssets = JSON.parse(cachedAssetDataJson);
    log.info('[STATE] Resuming job. Status: ' + jobStatus + '. Loaded ' + processedAssets.length + ' assets from properties.');
  } else {
    // --- NEW JOB START (Scenario 1) ---
    const mainChar = GESI.getMainCharacter();
    const allAssets = _fetchAssetsConcurrently(mainChar); // This function will now THROW on failure.

    if (allAssets.length <= 1) { log.warn('[' + SCRIPT_NAME + '] WARNING: No assets retrieved.'); return; }

    const rawAssetsData = allAssets.slice(1);
    
    // [SANITIZATION STEP]
    const SANITIZATION_ITEM_ID_INDEX = 2; 
    const SANITIZATION_LOCATION_ID_INDEX = 4; 

    const sanitizedAssetsData = rawAssetsData.filter(row => {
      const item_id = Number(row[SANITIZATION_ITEM_ID_INDEX]);
      const location_id = Number(row[SANITIZATION_LOCATION_ID_INDEX]);

      // --- CRITICAL DEBUGGING: FILTERING REMAINS MINIMAL ---
      const is_valid_id = item_id > 0;
      const is_not_ghost = !GHOST_ITEM_IDS.has(item_id);

      return is_valid_id && is_not_ghost; 
    });
    // [END SANITIZATION]

    processedAssets = sanitizedAssetsData;

    // CRITICAL: Save large data using sharding to CacheService
    const processedJson = JSON.stringify(processedAssets);
    _writeShardedProperty(ASSET_CACHE_DATA_KEY, processedJson);

    SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, '0');
    SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, ASSET_JOB_STATUS_FLAG.FETCHED); // Use flag here
    log.info('[STATE] New job started. Assets saved. Status: ' + ASSET_JOB_STATUS_FLAG.FETCHED + '.');

    scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000);
    return;
  }

  // Re-acquire sheet access for Phase 2/3
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!ss.getSheetByName(CACHE_SHEET_NAME)) { log.error('[' + SCRIPT_NAME + '] ERROR: Target sheet not found for write phase.'); return; }

  // --- PHASE 2A: Critical Sheet Preparation ---
  // The job must be in FETCHED state to run this, guaranteeing data is loaded.
  if (jobStatus === ASSET_JOB_STATUS_FLAG.FETCHED) { 
    log.info('[PHASE 2A] Executing critical sheet clear and header write...');
    const result = _prepareCacheSheet();

    if (!result.success) { log.error('[PHASE 2A] Failed to clear sheet. Aborting.'); return; }
    
    // --- Initialize the writeState object ---
    const initialWriteState = {
      nextBatchIndex: 0,
      config: {
        // --- USING EXPLICIT USER-PROVIDED VALUES ---
        TARGET_WRITE_TIME_MS: 3000, 
        MAX_FACTOR: 2, // User's requested aggressive factor
        THROTTLE_THRESHOLD_MS: THROTTLE_THRESHOLD_MS, 
        THROTTLE_PAUSE_MS: THROTTLE_PAUSE_MS,
        // --- USING GLOBALLY DEFINED CHUNKING/THROTTLING PROPERTIES ---
        SOFT_LIMIT_MS: SOFT_LIMIT_MS, 
        CHUNK_DECREASE_RATE: CHUNK_DECREASE_RATE,
        MIN_CHUNK_SIZE: MIN_CHUNK_SIZE, 
        MAX_CHUNK_SIZE: MAX_CHUNK_SIZE,
        currentChunkSize: MIN_CHUNK_SIZE 
      },
      metrics: { startTime: START_TIME, previousDuration: 0, rowsProcessed: 0 }
    };
    
    // ** CRITICAL FIX: Persist state and status immediately after successful sheet clear **
    SCRIPT_PROP.setProperty(ASSET_WRITE_STATE_KEY, JSON.stringify(initialWriteState));
    SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, ASSET_JOB_STATUS_FLAG.WRITING); // Set to WRITING
    SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY); // Remove old index key
    
    log.info('[STATE] Sheet prepared. Initial write-state created. Status: ' + ASSET_JOB_STATUS_FLAG.WRITING + '. Scheduling next run to start chunking.');
    scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000);
    return;
  }


  // --- PHASE 2B: Resumable Chunk Write (Delegated to writeDataToSheet) ---
  if (jobStatus !== ASSET_JOB_STATUS_FLAG.WRITING) { log.warn('[STATE] Job status is not WRITING (' + jobStatus + '). Bailing out.'); return; }

  // CRITICAL: Read sharded data from CacheService
  const writeStateString = SCRIPT_PROP.getProperty(ASSET_WRITE_STATE_KEY);
  if (!writeStateString) { log.error('[CRITICAL] Job status is WRITING but ASSET_WRITE_STATE_KEY is missing. Aborting.'); return; }
  
  let writeState = JSON.parse(writeStateString);

  // Re-attach non-serializable properties (ss, loggers, startTime)
  writeState.ss = ss; 
  writeState.metrics.startTime = START_TIME; 
  
  if (typeof log !== 'undefined' && typeof log.info === 'function') {
    writeState.logInfo = log.info; writeState.logWarn = log.warn; writeState.logError = log.error;
  } else {
    writeState.logInfo = function(msg) { Logger.log(String(msg)); };
    writeState.logWarn = function(msg) { Logger.log(String(msg)); };
    writeState.logError = function(msg) { Logger.log(String(msg)); };
  }

  log.info('[PHASE 2B] Calling writeDataToSheet. Resuming from index: ' + (writeState.nextBatchIndex || 0));
  
  // Call writeDataToSheet ONCE with the FULL data array and the resumable state.
  const result = writeDataToSheet(
    CACHE_SHEET_NAME, processedAssets, 3, 1, writeState
  );

  if (result.success) {
    // IT'S DONE! Schedule the Finalization.
    log.info('[PHASE 2B] writeDataToSheet completed successfully. Total rows: ' + result.rowsProcessed + '. Scheduling finalization.');
    
    scheduleOneTimeTrigger('finalizeAssetCache', 5000);

    // CRITICAL: Clear sharded properties on success
    _clearShardedProperty(ASSET_CACHE_DATA_KEY);
    
    // Clean up transient state keys here.
    SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
    SCRIPT_PROP.deleteProperty(ASSET_WRITE_STATE_KEY); 
    SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
    
    return; // Exit successfully
    
  } else {
    // IT FAILED (Timeout, Lock, etc.) - Reschedule.
    log.warn('[PHASE 2B] writeDataToSheet returned a non-success state. Reason: ' + result.error);
    
    // Save the *new* state returned by the function for the next run.
    const stateToSave = result.state;
    delete stateToSave.ss; delete stateToSave.logInfo; delete stateToSave.logWarn; delete stateToSave.logError;

    // We do NOT save chunk size to a separate key here; it's saved inside ASSET_WRITE_STATE_KEY.
    SCRIPT_PROP.setProperty(ASSET_WRITE_STATE_KEY, JSON.stringify(stateToSave));
    
    log.info('[STATE] Saving write state to resume at index: ' + stateToSave.nextBatchIndex + '. Scheduling next run.');
    scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', RESCHEDULE_PAUSE_MS); // <-- NOW USES THE PAUSE
    return; // Exit this execution.
  }
} // End cacheAllCorporateAssets


/**
 * PHASE 3 WORKER: Performs the heavy SpreadsheetApp.flush() and named range creation
 * after the data is successfully written. Runs in its own scheduled execution context.
 */
function finalizeAssetCache() {
    const SCRIPT_NAME = 'finalizeAssetCache';
    const SCRIPT_PROP = PropertiesService.getScriptProperties();
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    // FIX: Define a higher retry delay (90 seconds)
    const FINALIZE_RETRY_DELAY_MS = 90000; 

    log.info('[' + SCRIPT_NAME + '] Starting final spreadsheet flush and Named Range creation.');

    try {
        // 1. Flush (This is the heavy, risky part)
        SpreadsheetApp.flush();
        log.info('[' + SCRIPT_NAME + '] Flush successful.');

        // 2. Create/Update Named Range for downstream consumers.
        const cacheSheet = ss.getSheetByName(CACHE_SHEET_NAME);
        
        if (!cacheSheet) {
            log.error('[' + SCRIPT_NAME + '] CRITICAL: Cache sheet ' + CACHE_SHEET_NAME + ' not found during finalization. Cannot create Named Range.');
            return;
        }

        // Determine data height from the last written row (Row 3 is the first data row)
        const dataHeight = Math.max(1, cacheSheet.getLastRow() - 2); 
        const NUM_ASSET_COLS = 8; // Assumed 8 columns from Phase 1 logic

        // Start named range at Row 3
        ss.setNamedRange(
            CACHE_NAMED_RANGE,
            cacheSheet.getRange(3, 1, dataHeight, NUM_ASSET_COLS)
        );
        log.info('[' + SCRIPT_NAME + '] Successfully created Named Range (' + CACHE_NAMED_RANGE + ') covering ' + dataHeight + ' rows.');

    } catch (e) {
        log.error('[' + SCRIPT_NAME + '] CRITICAL FAILURE during finalization: ' + e.message + '. Rescheduling retry.');
        // Reschedule using the increased delay
        scheduleOneTimeTrigger('finalizeAssetCache', FINALIZE_RETRY_DELAY_MS); 
        return;
    }

    // FINAL CLEANUP: Clear properties on successful finalization.
    SCRIPT_PROP.deleteProperty(ASSET_CACHE_DATA_KEY);
    SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
    SCRIPT_PROP.deleteProperty(ASSET_WRITE_STATE_KEY); 
    SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);

    log.info('[' + SCRIPT_NAME + '] Job finalized successfully.');
}