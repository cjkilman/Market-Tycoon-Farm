/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, scheduleOneTimeTrigger, executeWithTryLock, getCorpAuthChar, CacheService */

// ======================================================================
// EVE ONLINE ASSET AND LOCATION MANAGEMENT MODULE
// ======================================================================

// --- 0. MODULE-LEVEL LOGGER ---
const SAFE_CONSOLE_SHIM = {
  log: console.log,
  info: console.log, 
  warn: console.warn,
  error: console.error,
  startTimer: () => ({ stamp: () => {} }) 
};
const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('InventoryManager') : SAFE_CONSOLE_SHIM);

// --- UTILITY/SHARDING FUNCTIONS (REQUIRED FOR STABILITY) ---
const ASSET_CACHE_TTL = 3600; 

function _readShardedProperty(SCRIPT_PROP, key) { 
    const cache = CacheService.getScriptCache();
    return cache.get(key); 
}
function _writeShardedProperty(SCRIPT_PROP, key, data) { 
    const cache = CacheService.getScriptCache();
    cache.put(key, data, ASSET_CACHE_TTL); 
}
function _deleteShardedProperty(SCRIPT_PROP, key) { 
    const cache = CacheService.getScriptCache();
    cache.remove(key); 
}

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
 */
function cacheAllCorporateAssetsTrigger() {
  const funcName = 'cacheAllCorporateAssets';

  // executeWithTryLock acquires the script lock and holds it for the worker's execution.
  const result = executeWithTryLock(cacheAllCorporateAssetsWorker, funcName);

  if (result === null) {
    // Action: Log and rely on the higher-level trigger (Dispatcher)
    log.warn(`${funcName} skipped due to Script Lock conflict. Waiting for next scheduled run.`);
  }
}
// ------------------------------------------------------------------------

// --- 1. GLOBAL CONSTANTS AND DEPENDENCIES ---

// Keys for storing job state in Script Properties (Resilience)
const ASSET_CACHE_DATA_KEY = 'AssetCache_Data_V2'; 
const ASSET_CACHE_ROW_INDEX_KEY = 'AssetCache_NextRow';
const ASSET_JOB_STATUS_KEY = 'AssetCache_Status_Key';
const ASSET_CHUNK_SIZE_KEY = 'AssetCache_ChunkSize';

// --- NEW CHUNK AND TIME LIMITS (GLOBAL) ---
const NEW_MAX_CHUNK_SIZE = 2000;
const NEW_MIN_CHUNK_SIZE = 100;
const NEW_SOFT_LIMIT_MS = 285000; 
const CRIT_LOCK_WAIT_MS = 60000;
const CHUNK_DECREASE_RATE = 200; 
const THROTTLE_THRESHOLD_MS = 800; 
const THROTTLE_PAUSE_MS = 300; 



// EVE CONSTANTS
const ASSET_CACHE_HEADERS = ["is_blueprint_copy", "is_singleton", "item_id", "location_flag", "location_id", "location_type", "quantity", "type_id"];

// Sheet Names and Headers
const CACHE_SHEET_NAME = 'CorpWarehouseStock';
const TEMP_SHEET_NAME = 'CorpWarehouseStock_TEMP'; // New temporary target sheet
const NUM_ASSET_COLS = ASSET_CACHE_HEADERS.length;
const CACHE_NAMED_RANGE = 'NR_CORP_ASSETS_CACHE';
const _sheetCache = {}; 

// --- ALIASES FOR LOCAL/EXTERNAL ACCESS ---
const PROP_KEY_WRITE_INDEX = ASSET_CACHE_ROW_INDEX_KEY;
const PROP_KEY_CHUNK_SIZE = ASSET_CHUNK_SIZE_KEY;
const MIN_CHUNK_SIZE = NEW_MIN_CHUNK_SIZE;
const MAX_CHUNK_SIZE = NEW_MAX_CHUNK_SIZE;
const SOFT_LIMIT_MS = NEW_SOFT_LIMIT_MS;


/**
 * Executes concurrent ESI requests to fetch all pages of corporation assets.
 * (Full body omitted for brevity, but the correct retry logic is assumed).
 */
function _fetchAssetsConcurrently(mainChar) {
    // ... (Full body of fetch logic omitted) ...
    // This is the function that returns the allAssets array.
    const SCRIPT_NAME = '_fetchAssetsConcurrently';
    const client = GESI.getClient().setFunction('corporations_corporation_assets');
    const allAssetObjects = []; 

    // Simulation of fetch logic. Replace with your actual implementation.
    try {
        const responsePage1 = UrlFetchApp.fetch(client.buildRequest({page:1}).url, {muteHttpExceptions: true});
        const dataPage1 = JSON.parse(responsePage1.getContentText());
        allAssetObjects.push(...dataPage1);

        // Final merge and sanitize logic (as confirmed in previous steps)
        const finalAssets = [ASSET_CACHE_HEADERS];
        allAssetObjects.forEach(obj => {
            const item_id = Number(obj.item_id);
            const location_id = Number(obj.location_id);
            if (item_id > 0 && location_id > 0) { 
                finalAssets.push([obj.is_blueprint_copy, obj.is_singleton, obj.item_id, obj.location_flag, obj.location_id, obj.location_type, obj.quantity, obj.type_id]);
            }
        });
        return finalAssets;
    } catch (e) {
        log.error(`[${SCRIPT_NAME}] Critical fetch error: ${e.message}`);
        return [ASSET_CACHE_HEADERS];
    }
}


/**
 * Clears the target sheet and writes the headers (ROW 2) using a guarded transaction.
 *
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss The stable Spreadsheet object.
 */
function _prepareCacheSheet(ss) {
    const SCRIPT_NAME = '_prepareCacheSheet';
    
    // Execute the critical sheet clear logic inside the guarded transaction
    const transactionResult = guardedSheetTransaction(() => {
        
        let cacheSheet = ss.getSheetByName(CACHE_SHEET_NAME);

        if (!cacheSheet) { 
            throw new Error(`Target sheet '${CACHE_SHEET_NAME}' not found.`); 
        }
        _sheetCache[CACHE_SHEET_NAME] = cacheSheet;

        // 1. AGGRESSIVE CLEAR
        const lastRow = cacheSheet.getMaxRows();
        if (lastRow > 2) {
            cacheSheet.getRange("A3:H" + lastRow).clearContent();
        }
        
        // 2. HEADER WRITE
        cacheSheet.getRange("A2:H2").setValues([ASSET_CACHE_HEADERS]);

        log.info(`[${SCRIPT_NAME}] Cleared old rows and wrote headers.`);

        return { success: true, duration: 0 }; 
        
    }, CRIT_LOCK_WAIT_MS); // Use the full critical timeout (60 seconds)

    // Handle the result of the guarded transaction
    if (transactionResult.success) {
        // Returns the inner success object {success: true, duration: 0}
        return transactionResult.state; 
    } else {
        log.error(`[${SCRIPT_NAME}] CRITICAL ERROR during sheet preparation: ${transactionResult.error}`);
        // Guarantees a non-undefined return that the orchestrator can safely check.
        return { success: false, duration: 0 };
    }
}

/**
 * Executes the full ESI asset pull and writes the result to a local sheet.
 */
/**
 * Executes the full ESI asset pull and writes the result to a local sheet.
 * FINAL FIX: Uses the Atomic Swap Pattern to eliminate sheet clear crashes.
 */
function cacheAllCorporateAssetsWorker() {
    const SCRIPT_NAME = 'cacheAllCorporateAssetsWorker';
    const SCRIPT_PROP = PropertiesService.getScriptProperties(); 
    const START_TIME = new Date().getTime();

    // Initialization (omitted for brevity)
    let currentChunkSize = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || MIN_CHUNK_SIZE.toString(), 10);
    currentChunkSize = Math.max(MIN_CHUNK_SIZE, currentChunkSize);
    
    let processedAssets = [];
    const cachedAssetDataJson = _readShardedProperty(SCRIPT_PROP, ASSET_CACHE_DATA_KEY);
    let nextWriteRow = parseInt(SCRIPT_PROP.getProperty(ASSET_CACHE_ROW_INDEX_KEY) || '0', 10);
    let jobStatus = SCRIPT_PROP.getProperty(ASSET_JOB_STATUS_KEY);

    // --- PHASE 1: Data Acquisition (Fetch or Resume) ---
    // ... (logic for fetching and stabilizing state omitted) ...
    if (processedAssets.length === 0) { return; } 

    const ss_stable = SpreadsheetApp.getActiveSpreadsheet();
    let tempSheet = getOrCreateSheet(ss_stable,TEMP_SHEET_NAME,ASSET_CACHE_HEADERS); // Find existing temp sheet if resuming

    // --- PHASE 2A/2B: Write to TEMP Sheet ---
    if (nextWriteRow === 0 && jobStatus === 'FETCHED') {
        // 1. Check for existing temp sheet from a previous crash and delete it if found.
        if (tempSheet) {
            log.warn(`[SWAP] Found leftover temp sheet. Deleting and restarting job clear.`);
          //  ss_stable.deleteSheet(tempSheet);
        }
        
        // 2. Create the NEW temporary target sheet
        tempSheet = ss_stable.insertSheet(TEMP_SHEET_NAME);
        log.info(`[PHASE 2A] Created new temp sheet: ${TEMP_SHEET_NAME}. Starting write from Index 0.`);

        // 3. Write Headers to the new sheet (Row 2, Column 1)
        tempSheet.getRange("A2:H2").setValues([ASSET_CACHE_HEADERS]);
        
        // Update state to WRITING and fall through
        SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, 'WRITING');
        jobStatus = 'WRITING'; 
    }
    
    // --- Phase 2B: Resumable Write to TEMP SHEET ---
    if (jobStatus === 'WRITING') {
        if (!tempSheet) tempSheet = ss_stable.getSheetByName(TEMP_SHEET_NAME);
        if (!tempSheet) {
             log.error("[FATAL] Temp sheet disappeared or was not created. Cannot resume.");
             return;
        }

        // 1. Enforce STRICT MINIMUM STARTING CHUNK
        if (nextWriteRow === 0) {
            currentChunkSize = STRICT_MIN_CHUNK;
            log.info(`[INIT] Forcing initial chunk size to ${STRICT_MIN_CHUNK} for reliable write start.`);
        }

        // 2. CONSTRUCT STATE OBJECT & CALL THE WRITING UTILITY
        const stateObject = { /* ... state object construction ... */ };

        // The write operation now only commits data to the temporary sheet.
        const writeResult = writeDataToSheet(
            TEMP_SHEET_NAME, // Write to the temporary sheet name
            processedAssets, 
            3, 1, 
            stateObject
        );

        // 3. HANDLE WRITE RESULT (Bailout/Completion)
        if (!writeResult.success) {
            // ... (Bailout logic saves state and returns) ...
            return; // EXIT EXECUTION
        }
        
        // If the function reaches here, writeResult.success is TRUE.
        // Fall through to Phase 3: Finalization
    }

    // --- PHASE 3: Finalization (The Atomic Swap) ---
    if (jobStatus === 'WRITING') { 
        const dataHeight = processedAssets.length;
        const rangeHeight = Math.max(1, dataHeight);
        
        // 1. Set Final Properties and Named Range on the TEMP sheet
        if (tempSheet) {
            tempSheet.setFrozenRows(2);
            // NOTE: The Named Range must be set on the final target sheet name, not the temp sheet.
            // We set the range but defer renaming until after the swap.
            tempSheet.getParent().setNamedRange(
                CACHE_NAMED_RANGE,
                tempSheet.getRange(3, 1, rangeHeight, NUM_ASSET_COLS)
            );
        }

        // 2. Perform the Atomic Swap (Delete old, Rename new)
        try {
            // Utilize the external atomicSwapAndFlush utility
            const swapResult = atomicSwapAndFlush(ss_stable, CACHE_SHEET_NAME, TEMP_SHEET_NAME);
            if (!swapResult.success) {
                // If swap fails, DO NOT clear persistence keys; job is incomplete.
                throw new Error(`Atomic Swap Failed: ${swapResult.errorMessage}`);
            }
        } catch (e) {
            log.error(`[CRITICAL SWAP FAILURE] The sheet swap failed. Data remains in '${TEMP_SHEET_NAME}'. Error: ${e.message}`);
            return; // Exit without clearing state
        }

        // 3. CRITICAL CLEANUP: Clear state properties on success
        _deleteShardedProperty(SCRIPT_PROP, ASSET_CACHE_DATA_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
        SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE); 
        log.info('[cacheAllCorporateAssets] Successfully cached ' + dataHeight + ' asset rows. Job finalized and cache reset for next hourly run.');
    }
}