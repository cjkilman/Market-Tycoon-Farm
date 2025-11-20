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
const NEW_MAX_CHUNK_SIZE = 5000;
const NEW_MIN_CHUNK_SIZE = 500;
const NEW_SOFT_LIMIT_MS = 285000; 
const CRIT_LOCK_WAIT_MS = 60000;
const CHUNK_DECREASE_RATE = 200; 
const THROTTLE_THRESHOLD_MS = 800; 
const THROTTLE_PAUSE_MS = 200; 



// EVE CONSTANTS
const ASSET_CACHE_HEADERS = ["is_blueprint_copy", "is_singleton", "item_id", "location_flag", "location_id", "location_type", "quantity", "type_id"];

// Sheet Names and Headers
const CACHE_SHEET_NAME = 'CorpWarehouseStock';
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
 * FIX: Guarantees a structured return object is passed to the orchestrator.
 */
function _prepareCacheSheet(ss) {
    const SCRIPT_NAME = '_prepareCacheSheet';
    
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
        return transactionResult.state; // Returns the inner success object
    } else {
        log.error(`[${SCRIPT_NAME}] CRITICAL ERROR during sheet preparation: ${transactionResult.error}`);
        return { success: false, duration: 0 };
    }
}

/**
 * Executes the full ESI asset pull and writes the result to a local sheet.
 */
function cacheAllCorporateAssetsWorker() {
    const ssDoc = SpreadsheetApp.getActiveSpreadsheet();// State { ss: } Anchor
    const SCRIPT_NAME = 'cacheAllCorporateAssetsWorker';
    const SCRIPT_PROP = PropertiesService.getScriptProperties(); 
    const START_TIME = new Date().getTime();

    // 1. Initialization: Load currentChunkSize using PROP_KEY_CHUNK_SIZE
    let currentChunkSize = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || MIN_CHUNK_SIZE.toString(), 10);
    currentChunkSize = Math.max(MIN_CHUNK_SIZE, currentChunkSize);
    
    let processedAssets = [];
    const cachedAssetDataJson = _readShardedProperty(SCRIPT_PROP, ASSET_CACHE_DATA_KEY);
    let nextBatchIndex = parseInt(SCRIPT_PROP.getProperty(ASSET_CACHE_ROW_INDEX_KEY) || '0', 10);
    let jobStatus = SCRIPT_PROP.getProperty(ASSET_JOB_STATUS_KEY);

    // --- PHASE 1: Data Acquisition (Fetch or Resume) ---
    if (cachedAssetDataJson) {
        try {
            processedAssets = JSON.parse(cachedAssetDataJson);
        } catch (e) {
            log.error(`[STATE] Failed to parse sharded asset data from cache. Forcing new ESI fetch. Error: ${e.message}`);
            _deleteShardedProperty(SCRIPT_PROP, ASSET_CACHE_DATA_KEY);
            SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
            SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
        }
    } 
    
    // --- STATE FIX: Stabilize corrupted state before proceeding ---
    if (processedAssets.length > 0 && jobStatus !== 'FETCHED' && jobStatus !== 'WRITING') {
        log.warn(`[STATE FIX] Data loaded but persistent status was ${jobStatus}. Forcing status to FETCHED.`);
        jobStatus = 'FETCHED';
        SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, 'FETCHED');
    }

    // Check if we need to START A NEW JOB
    if (processedAssets.length === 0) {
        // --- NEW JOB START ---
        log.info(`[STATE] Starting new job (Initial/Cache Miss). Fetching all assets from ESI...`);
        const mainChar = GESI.getMainCharacter();
        const allAssets = _fetchAssetsConcurrently(mainChar);

        if (allAssets.length <= 1) { log.warn('[cacheAllCorporateAssets] WARNING: No assets retrieved.'); return; }

        const rawAssetsData = allAssets.slice(1);
        const sanitizedAssetsData = rawAssetsData.filter(row => {
            const item_id = Number(row[2]);
            const location_id = Number(row[4]);
            return item_id > 0 && location_id > 0;
        });
        
        processedAssets = sanitizedAssetsData;

        _writeShardedProperty(SCRIPT_PROP, ASSET_CACHE_DATA_KEY, JSON.stringify(processedAssets));
        
        // Save persistent state
        SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, '0');
        SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, 'FETCHED');
        
        // Final Fix for infinite loop: Force commit and slight delay
        SpreadsheetApp.flush(); 
        Utilities.sleep(100); 

        log.info(`[STATE] New job started. Assets saved to cache. Status: FETCHED.`);

        scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000);
        return;
    }



    // --- PHASE 2A (MODIFIED): Sheet Preparation + IMMEDIATE TRANSITION TO WRITING ---
    if (nextBatchIndex === 0 && jobStatus === 'FETCHED') {
        log.info(`[PHASE 2A] Executing critical sheet clear and header write, then starting chunking...`);
        
        // Pass the stable spreadsheet object
        const result = _prepareCacheSheet(ssDoc); 

        // CRASH FIX: Check for success status (result is guaranteed to be an object)
        if (!result || !result.success) {
            log.error(`[PHASE 2A] Failed to clear sheet. Aborting. Check if lock was acquired.`);
            return;
        }

        // Set status to WRITING
        SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, 'WRITING');
        jobStatus = 'WRITING'; // Update local variable
        log.info(`[STATE] Sheet prepared. Status: WRITING. Starting Phase 2B chunk loop.`);
        // FALL THROUGH to Phase 2B
    }


    // --- PHASE 2B: Resumable Chunk Write (Delegated to writeDataToSheet) ---
    if (jobStatus === 'WRITING') {
         const ss_stable = SpreadsheetApp.getActiveSpreadsheet();
    // --- SHEET ACCESS (ss_stable must be acquired here for the state object) ---

    // --- ENFORCEMENT OF STRICT MINIMUM STARTING CHUNK ---
        const STRICT_MIN_CHUNK = 50;
        if (nextBatchIndex === 0) {
            currentChunkSize = STRICT_MIN_CHUNK;
            log.info(`[INIT] Forcing initial chunk size to ${STRICT_MIN_CHUNK} for reliable write start.`);
        }
        log.info("[PHASE 2B] Delegating chunk writing to writeDataToSheet utility.");

        // 1. CONSTRUCT THE CANONICAL WRITE STATE OBJECT
        const stateObject = {
            logInfo: log.info, logError: log.error, logWarn: log.warn,
            ss: ss_stable,
            metrics: { startTime: START_TIME },
            nextBatchIndex: nextBatchIndex, 
            config: {
                MAX_CELLS_PER_CHUNK: 25000,
                TARGET_WRITE_TIME_MS: 3000, MAX_FACTOR: 2, THROTTLE_THRESHOLD_MS: THROTTLE_THRESHOLD_MS, THROTTLE_PAUSE_MS: THROTTLE_PAUSE_MS,
                currentChunkSize: currentChunkSize, MAX_CHUNK_SIZE: MAX_CHUNK_SIZE, MIN_CHUNK_SIZE: MIN_CHUNK_SIZE, SOFT_LIMIT_MS: SOFT_LIMIT_MS
            }
        };

        // 2. CALL THE WRITING UTILITY
        const writeResult = writeDataToSheet(
            CACHE_SHEET_NAME, 
            processedAssets, 
            3, 1, // Row 3, Col 1
            stateObject
        );

        // 3. HANDLE WRITE RESULT (Bailout/Completion)
        if (!writeResult.success) {
            log.error(`[CRITICAL ABORT] Write failed at index ${writeResult.state.nextBatchIndex}. Reason: ${writeResult.error}`);
            // --- BAILOUT / RE-SCHEDULE REQUIRED ---
            const nextIndex = writeResult.state.nextBatchIndex;
            const nextChunkSize = writeResult.state.config.currentChunkSize;
            
            // Save the *new* persistent state using the correct keys
            SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, nextIndex.toString());
            SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, nextChunkSize.toString()); 

            log.warn(`[STATE] Write bailout reason: ${writeResult.bailout_reason || writeResult.error}. Resuming at row index ${nextIndex}.`);
            scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000);
            return; // EXIT EXECUTION
        }
        
        // Fall through to Phase 3: Finalization
    }

    // --- PHASE 3: Finalization ---
    if (jobStatus === 'WRITING') { 
        const dataHeight = processedAssets.length;
        const rangeHeight = Math.max(1, dataHeight);

    //    SpreadsheetApp.flush();
        log.info('[cacheAllCorporateAssets] Final spreadsheet flush and Named Range creation.');

        let cacheSheetFinal = ssDoc.getSheetByName(CACHE_SHEET_NAME);
        if (cacheSheetFinal) {
            
            // 1. Update Named Range
            cacheSheetFinal.getParent().setNamedRange(
                CACHE_NAMED_RANGE,
                cacheSheetFinal.getRange(3, 1, rangeHeight, NUM_ASSET_COLS)
            );

           
        }
        
        // FINAL CLEANUP: Clear state properties on success
        _deleteShardedProperty(SCRIPT_PROP, ASSET_CACHE_DATA_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
        SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE); 
        log.info('[cacheAllCorporateAssets] Successfully cached ' + dataHeight + ' asset rows. Job finalized and cache reset for next hourly run.');
    }
}