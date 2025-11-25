/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, scheduleOneTimeTrigger, executeWithTryLock, getCorpAuthChar, CacheService, writeDataToSheet, guardedSheetTransaction, atomicSwapAndFlush, deleteTriggersByName, _chunkAndPut, _getAndDechunk, _deleteShardedData */

// ======================================================================
// EVE ONLINE ASSET AND LOCATION MANAGEMENT MODULE
// ======================================================================

const SAFE_CONSOLE_SHIM = {
    log: console.log, info: console.log, warn: console.warn, error: console.error,
    startTimer: () => ({ stamp: () => { } })
};
const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('InventoryManager') : SAFE_CONSOLE_SHIM);

// --- TRIGGER MANAGEMENT ---

function cacheAllCorporateAssetsTrigger() {
    const SCRIPT_PROP = PropertiesService.getScriptProperties();
    const status = SCRIPT_PROP.getProperty(ASSET_JOB_STATUS_KEY);

    if (status === 'FINALIZING') {
        log.info("Trigger: Job is in FINALIZING state. Dispatching finalizer.");
        finalizeAssetCacheJob();
    } else {
        const funcName = 'cacheAllCorporateAssetsWorker';
        executeWithTryLock(cacheAllCorporateAssetsWorker, funcName);
    }
}

// ------------------------------------------------------------------------

// --- GLOBAL CONSTANTS ---
const ASSET_CACHE_DATA_KEY = 'AssetCache_Data_V2';
const ASSET_CACHE_ROW_INDEX_KEY = 'AssetCache_NextRow';
const ASSET_JOB_STATUS_KEY = 'AssetCache_Status_Key';
const ASSET_CHUNK_SIZE_KEY = 'AssetCache_ChunkSize';

// --- TUNED CACHE SETTINGS ---
// 1 Hour TTL for Assets (Matches ESI Cache exactly)
const ASSET_CACHE_TTL = 21600;

// --- CONFIGURATION ---
const STARTING_CHUNK_SIZE = 1500; // Start fast
const NEW_MIN_CHUNK_SIZE = 1000;   // Floor
const NEW_MAX_CHUNK_SIZE = 8000;  // Cap
const NEW_SOFT_LIMIT_MS = 280000; // 4m 35s
const CRIT_LOCK_WAIT_MS = 60000;

// Configuration passed to the shared writeDataToSheet utility
const WRITE_CONFIG = {
    MAX_CELLS_PER_CHUNK: 60000,       // 60k cells per batch (Nitro)
    TARGET_WRITE_TIME_MS: 2500,       // 2.5s target
    MAX_FACTOR: 1.8,
    THROTTLE_THRESHOLD_MS: 200,
    THROTTLE_PAUSE_MS: 100,
    MAX_CHUNK_SIZE: NEW_MAX_CHUNK_SIZE,
    MIN_CHUNK_SIZE: NEW_MIN_CHUNK_SIZE,
    SOFT_LIMIT_MS: NEW_SOFT_LIMIT_MS
};

const ASSET_CACHE_HEADERS = ["is_blueprint_copy", "is_singleton", "item_id", "location_flag", "location_id", "location_type", "quantity", "type_id"];
const CACHE_SHEET_NAME = 'CorpWarehouseStock';
const TEMP_SHEET_NAME = CACHE_SHEET_NAME + '_TEMP';
const NUM_ASSET_COLS = ASSET_CACHE_HEADERS.length;

// *** UPDATED NAMED RANGE ***
const CACHE_NAMED_RANGE = 'warehouse_unfiltered';

const _sheetCache = {};

const PROP_KEY_WRITE_INDEX = ASSET_CACHE_ROW_INDEX_KEY;
const PROP_KEY_CHUNK_SIZE = ASSET_CHUNK_SIZE_KEY;

/**
 * TRUE CONCURRENT FETCHER (The Ferrari Engine)
 */
function _fetchAssetsConcurrently(authName) {
    const SCRIPT_NAME = '_fetchAssetsConcurrently';
    const client = GESI.getClient().setFunction('corporations_corporation_assets');

    let maxPages = 1;
    const headerRow = ASSET_CACHE_HEADERS;
    const allAssets = [headerRow];

    let corpId = 0;
    try {
        const charObj = GESI.getCharacterData ? GESI.getCharacterData(authName) : null;
        if (charObj) corpId = charObj.corporation_id;
    } catch (e) { }

    if (!corpId && GESI.name === authName) {
        const charData = GESI.getCharacterData ? GESI.getCharacterData() : null;
        if (charData) corpId = charData.corporation_id;
    }

    if (!corpId) {
        try {
            const search = GESI.search(['character'], authName);
            if (search && search.character && search.character.length > 0) {
                const charId = search.character[0];
                const pubChar = GESI.characters_character(charId);
                corpId = pubChar.corporation_id;
            }
        } catch (e) { }
    }

    if (!corpId) {
        log.error(`[${SCRIPT_NAME}] Could not resolve Corp ID for '${authName}'.`);
        return [headerRow];
    }

    try {
        // 1. Fetch Page 1
        const req1 = client.buildRequest({ corporation_id: corpId, page: 1, name: authName });
        const options1 = {
            method: req1.method || 'get',
            headers: req1.headers,
            muteHttpExceptions: true
        };
        const resp1 = UrlFetchApp.fetch(req1.url, options1);

        if (resp1.getResponseCode() !== 200) {
            throw new Error(`Page 1 failed: ${resp1.getResponseCode()}`);
        }

        const headers = resp1.getHeaders();
        maxPages = Number(headers['X-Pages'] || headers['x-pages']) || 1;
        log.info(`[${SCRIPT_NAME}] Found ${maxPages} pages of assets. Fetching concurrently...`);

        const dataPage1 = JSON.parse(resp1.getContentText());
        dataPage1.forEach(obj => {
            allAssets.push([
                obj.is_blueprint_copy,
                obj.is_singleton,
                obj.item_id,
                obj.location_flag,
                obj.location_id,
                obj.location_type,
                obj.quantity,
                obj.type_id
            ]);
        });

    } catch (e) {
        log.error(`[${SCRIPT_NAME}] Critical fetch error: ${e.message}`);
        return [headerRow];
    }

    // 2. Fetch Remaining Pages
    if (maxPages > 1) {
        const allRequests = [];
        for (let i = 2; i <= maxPages; i++) {
            const req = client.buildRequest({ corporation_id: corpId, page: i, name: authName });
            allRequests.push({
                url: req.url,
                method: req.method || 'get',
                headers: req.headers,
                muteHttpExceptions: true
            });
        }

        if (allRequests.length > 0) {
            const responses = UrlFetchApp.fetchAll(allRequests);
            responses.forEach((response, index) => {
                if (response.getResponseCode() === 200) {
                    try {
                        const rawData = JSON.parse(response.getContentText());
                        rawData.forEach(obj => {
                            allAssets.push([
                                obj.is_blueprint_copy,
                                obj.is_singleton,
                                obj.item_id,
                                obj.location_flag,
                                obj.location_id,
                                obj.location_type,
                                obj.quantity,
                                obj.type_id
                            ]);
                        });
                    } catch (e) { }
                }
            });
        }
    }
    return allAssets;
}

// ... inside _prepareCacheSheet ...

function _prepareCacheSheet(ss) {
    const SCRIPT_NAME = '_prepareCacheSheet';
    const transactionResult = guardedSheetTransaction(() => {

        // *** USE THE RACER ***
        const cacheSheet = prepareTempSheet(ss, TEMP_SHEET_NAME, ASSET_CACHE_HEADERS);

        _sheetCache[TEMP_SHEET_NAME] = cacheSheet;
        log.info(`[${SCRIPT_NAME}] Prepared TEMP sheet '${TEMP_SHEET_NAME}' (Trimmed).`);
        return { success: true, duration: 0 };
    }, CRIT_LOCK_WAIT_MS);

    if (transactionResult.success) return transactionResult.state;
    else {
        log.error(`[${SCRIPT_NAME}] CRITICAL ERROR: ${transactionResult.error}`);
        return { success: false, duration: 0 };
    }
}

/**
 * Corporate Asset Worker (Mirrored from Orchestrator.js "Nitro" Pattern)
 */
/**
 * Corporate Asset Cache Worker (Nitro Edition)
 * Mirrors the logic of the Market Data worker for robust, resumable execution.
 */
function cacheAllCorporateAssetsWorker() {
    // ... (No changes to this function from previous turn) ...
    const START_TIME = new Date().getTime();
    const SCRIPT_PROP = PropertiesService.getScriptProperties();
    const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ASSET_WORKER') : console);
    
    // State Keys
    const PROP_KEY_STEP = 'AssetCache_JobStatus'; 
    const PROP_KEY_WRITE_INDEX = 'AssetCache_RowIndex';
    const PROP_KEY_CHUNK_SIZE = 'AssetCache_ChunkSize';
    const ASSET_CACHE_DATA_KEY = 'AssetCache_Data_Shard';
    
    // --- NITRO CONFIGURATION ---
    const [MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, SOFT_LIMIT_MS, RESCHEDULE_DELAY_MS] 
        = [8000, 500, 270000, 10000];
        
    const START_ROW = 3; 
    const START_COL = 1; 
    const TEMP_SHEET_NAME = 'CorpWarehouseStock_Temp'; 
    const ASSET_CACHE_HEADERS = [["is_blueprint_copy", "is_singleton", "item_id", "location_flag", "location_id", "location_type", "quantity", "type_id"]];

    var ss_anchor = {};
    
    let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP);

    if (!currentStep) currentStep = 'NEW_RUN';

    // ==========================================================================
    // PHASE 1: FETCH & PREPARE
    // ==========================================================================
    if (currentStep === 'NEW_RUN' || currentStep === 'FETCHED') {
        log.info(`[Worker] State: ${currentStep}. Starting Fetch & Prep.`);

        const authName = (typeof getCorpAuthChar === 'function') ? getCorpAuthChar() : null;
        
        if (!authName) {
            log.warn('[Worker] No authorized character found (getCorpAuthChar). checking GESI main.');
        }
        
        if (typeof _fetchAssetsConcurrently !== 'function') {
             log.error('[Worker] _fetchAssetsConcurrently function missing.');
             return;
        }

        let allAssets = [];
        try {
             allAssets = _fetchAssetsConcurrently(authName);
        } catch (e) {
             log.error(`[Worker] Fetch failed: ${e.message}`);
             return;
        }

        if (!allAssets || allAssets.length <= 1) {
            log.warn('[Worker] No assets retrieved (or only headers). Aborting.');
            return;
        }

        const rawAssetsData = allAssets.slice(1); 
        const processedAssets = rawAssetsData; 

        if (typeof _chunkAndPut === 'function') {
            const saved = _chunkAndPut(ASSET_CACHE_DATA_KEY, JSON.stringify(processedAssets), 21600); 
            if (!saved) {
                log.error('[Worker] Failed to cache asset data. Aborting.');
                return;
            }
        } else {
             log.error('[Worker] _chunkAndPut missing. Cannot persist data.');
             return;
        }

        const setupResult = guardedSheetTransaction(() => {
            ss_anchor = SpreadsheetApp.getActiveSpreadsheet();
            let sheet = ss_anchor.getSheetByName(TEMP_SHEET_NAME);
            if (sheet) {
                 sheet.clear();
            } else {
                 sheet = ss_anchor.insertSheet(TEMP_SHEET_NAME);
            }
            if (ASSET_CACHE_HEADERS && ASSET_CACHE_HEADERS.length > 0) {
                sheet.getRange(1, 1, 1, ASSET_CACHE_HEADERS[0].length).setValues(ASSET_CACHE_HEADERS);
            }
            return true;
        }, 60000);

        if (!setupResult.success) {
            log.warn(`[Worker] Sheet prep failed (${setupResult.error}). Rescheduling.`);
            scheduleOneTimeTrigger('cacheAllCorporateAssetsWorker', RESCHEDULE_DELAY_MS);
            return;
        }

        SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, '0');
        SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
        SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'WRITING');
        
        log.info(`[Worker] Fetch Success (${processedAssets.length} rows). Transitioning to WRITING.`);
        scheduleOneTimeTrigger('cacheAllCorporateAssetsWorker', 1000); 
        return;
    }

    // ==========================================================================
    // PHASE 2: WRITE (Nitro Mode)
    // ==========================================================================
    if (currentStep === 'WRITING') {
        
        let cachedJson = null;
        if (typeof _getAndDechunk === 'function') {
             cachedJson = _getAndDechunk(ASSET_CACHE_DATA_KEY);
        }
        
        if (!cachedJson) {
            log.error(`[Worker] CRITICAL: Cache Loss Detected. Resetting Job.`);
            SCRIPT_PROP.deleteProperty(PROP_KEY_STEP);
            SCRIPT_PROP.deleteProperty(PROP_KEY_WRITE_INDEX);
            return;
        }
        
        let allRowsToWrite = [];
        try {
            allRowsToWrite = JSON.parse(cachedJson);
        } catch (e) {
            log.error(`[Worker] JSON Parse Error. Resetting.`);
            SCRIPT_PROP.deleteProperty(PROP_KEY_STEP);
            return;
        }

        ss_anchor = SpreadsheetApp.getActiveSpreadsheet();

        let writeState = {
            logInfo: log.info, logError: log.error, logWarn: log.warn,
            nextBatchIndex: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_WRITE_INDEX) || '0'),
            ss: ss_anchor, 
            metrics: { startTime: START_TIME },
            config: {
                MAX_CELLS_PER_CHUNK: 60000,
                TARGET_WRITE_TIME_MS: 1000,
                MAX_FACTOR: 1.0,
                THROTTLE_THRESHOLD_MS: -1,
                THROTTLE_PAUSE_MS: 5000, 
                currentChunkSize: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || '1000'),
                MAX_CHUNK_SIZE: MAX_CHUNK_SIZE,
                MIN_CHUNK_SIZE: MIN_CHUNK_SIZE,
                SOFT_LIMIT_MS: SOFT_LIMIT_MS,
                LAG_SPIKE_THRESHOLD_MS: 60000 
            }
        };
        
        if (writeState.nextBatchIndex === 0) {
             writeState.config.currentChunkSize = 1000;
        }

        var needsWakeUp = false;
        if (typeof pauseSheet === 'function') {
             needsWakeUp = pauseSheet(ss_anchor);
        }

        log.info(`[Worker] Writing to '${TEMP_SHEET_NAME}' (Index: ${writeState.nextBatchIndex}).`);

        const writeResult = writeDataToSheet(TEMP_SHEET_NAME, allRowsToWrite, START_ROW, START_COL, writeState);

        if (needsWakeUp) {
           console.log("[Worker] Scheduling 'wakeUpSheet' to restore calculation.");
           scheduleOneTimeTrigger('wakeUpSheet', 30000); 
        }

        if (writeResult.success) {
            log.info("Write SUCCESS. Transitioning to FINALIZING.");
            SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'FINALIZING');
            SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
            SCRIPT_PROP.deleteProperty(PROP_KEY_WRITE_INDEX);
            scheduleOneTimeTrigger('finalizeAssetCacheJob', RESCHEDULE_DELAY_MS);
        }
        else if (writeResult.bailout_reason === "PREDICTIVE_BAILOUT" ||
          (writeResult.error && (
            writeResult.error.includes("ServiceTimeoutFailure") ||
            writeResult.error.includes("Service timed out") ||
            writeResult.error.includes("Exceeded maximum execution time")
          ))) {

          const reason = writeResult.error ? writeResult.error : "Soft Time Limit Reached (Predictive)";
          log.warn(`[Worker] Write phase interrupted. Reason: ${reason}. Rescheduling.`);

          const nextIndex = writeResult.state.nextBatchIndex.toString();
          
          let nextChunkSize = writeResult.state.config.currentChunkSize;
          if (writeResult.error) {
              nextChunkSize = Math.max(MIN_CHUNK_SIZE, Math.floor(nextChunkSize / 2));
          }

          SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, nextIndex);
          SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, nextChunkSize.toString());
          
          Utilities.sleep(1000);
          scheduleOneTimeTrigger('cacheAllCorporateAssetsWorker', 30000);
        }
        else {
          log.error(`[Worker] Fatal Write Failure: ${writeResult.error}`);
        }
    }
}

function finalizeAssetCacheJob() {
    const funcName = 'finalizeAssetCacheJob';
    const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ASSET_FINALIZER') : console);
    
    // [FIX 1] Initialize ss_anchor immediately so we can pass it to pauseSheet
    var ss_anchor = SpreadsheetApp.getActiveSpreadsheet();

    executeWithTryLock(() => {
        const SCRIPT_PROP = PropertiesService.getScriptProperties();
        // Ensure constants match those in cacheAllCorporateAssetsWorker
        const ASSET_JOB_STATUS_KEY = 'AssetCache_JobStatus';
        const CACHE_NAMED_RANGE = 'NR_CORP_ASSETS'; // Or your specific range name
        const CACHE_SHEET_NAME = 'CorpWarehouseStock'; // Or your specific sheet name
        const TEMP_SHEET_NAME = 'CorpWarehouseStock_Temp';
        const ASSET_CACHE_DATA_KEY = 'AssetCache_Data_Shard';
        const ASSET_CACHE_ROW_INDEX_KEY = 'AssetCache_RowIndex';
        const PROP_KEY_CHUNK_SIZE = 'AssetCache_ChunkSize';
        const NUM_ASSET_COLS = 8; 

        const status = SCRIPT_PROP.getProperty(ASSET_JOB_STATUS_KEY);

        if (status !== 'FINALIZING') {
            log.warn(`[Finalizer] Called in wrong state (${status}). Aborting.`);
            return;
        }

        // [2] PAUSE (Anesthesia) - Using your helper, passing ss_anchor
        var needsWakeUp = pauseSheet(ss_anchor);

        log.info('[Finalizer] Performing ATOMIC SWAP.');

        const repairMap = {
             [CACHE_NAMED_RANGE]: `A3:H` 
        };

        const transactionResult = guardedSheetTransaction(() => {
            // Refresh reference inside lock for safety
            ss_anchor = SpreadsheetApp.getActiveSpreadsheet(); 
            return atomicSwapAndFlush(ss_anchor, CACHE_SHEET_NAME, TEMP_SHEET_NAME, repairMap);
        }, 60000);

        let swapResult;
        if (!transactionResult.success) {
            swapResult = { success: false, errorMessage: transactionResult.error };
        } else {
            swapResult = transactionResult.state;
        }

        // 4. HANDLE FAILURE
        if (!swapResult.success) {
            // [3] IMMEDIATE WAKE UP ON FAILURE using ss_anchor
            if (needsWakeUp) wakeUpSheet(ss_anchor);

            if (swapResult.errorMessage && swapResult.errorMessage.includes("not found")) {
                log.error(`[Finalizer] CRITICAL: Temp sheet missing. Clearing state to reset.`);
                if (typeof _deleteShardedData === 'function') _deleteShardedData(ASSET_CACHE_DATA_KEY);
                SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
                SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
                SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
                deleteTriggersByName('cacheAllCorporateAssetsWorker');
                return;
            }
            log.warn(`[Finalizer] Swap Failed: ${swapResult.errorMessage}. Retrying in 120s.`);
            scheduleOneTimeTrigger('cacheAllCorporateAssetsWorker', 120000); // Or trigger the finalizer again? Usually retrying the finalizer is safer
            // However, your logic triggers the worker. If the worker sees 'FINALIZING', it should call the finalizer again.
            // But cacheAllCorporateAssetsWorker doesn't handle 'FINALIZING' state explicitly (it falls through).
            // Better to trigger finalizeAssetCacheJob directly.
            scheduleOneTimeTrigger('finalizeAssetCacheJob', 120000);
            return;
        }

        // 5. POST-SWAP RESIZE
        try {
            ss_anchor = SpreadsheetApp.getActiveSpreadsheet();
            const finalSheet = ss_anchor.getSheetByName(CACHE_SHEET_NAME);
            if (finalSheet) {
                const lastRow = finalSheet.getLastRow();
                const rangeHeight = Math.max(1, lastRow - 2); 
                ss_anchor.setNamedRange(
                    CACHE_NAMED_RANGE,
                    finalSheet.getRange(3, 1, rangeHeight, NUM_ASSET_COLS)
                );
                log.info(`[Finalizer] Resized Named Range '${CACHE_NAMED_RANGE}' to ${lastRow} rows.`);
            }
        } catch (nrError) {
             log.warn(`[Finalizer] Range Resize Warning: ${nrError.message}`);
        }

        // 6. CLEANUP & SUCCESS
        if (typeof _deleteShardedData === 'function') _deleteShardedData(ASSET_CACHE_DATA_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
        SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);

        deleteTriggersByName('cacheAllCorporateAssetsWorker'); 
        deleteTriggersByName('finalizeAssetCacheJob');

        log.info(`[Finalizer] Job Complete. Swap successful.`);

        // [4] SCHEDULE WAKE UP (Success Case)
        if (needsWakeUp) {
             log.info("[Finalizer] Scheduling 'wakeUpSheet' to restore calculation.");
             scheduleOneTimeTrigger('wakeUpSheet', 30000);
        }

    }, funcName);
}