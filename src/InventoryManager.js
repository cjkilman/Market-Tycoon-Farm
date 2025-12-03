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
    const ASSET_JOB_STATUS_KEY = 'AssetCache_JobStatus';
    const status = SCRIPT_PROP.getProperty(ASSET_JOB_STATUS_KEY);

    if (status === 'FINALIZING') {
        log.info("Trigger: Job is in FINALIZING state. Dispatching finalizer.");
        finalizeAssetCacheJob();
        return; // [FIX] Stop here! Do not run the worker.
    }

    // Only run the worker if NOT finalizing
    const funcName = 'cacheAllCorporateAssetsWorker';
    executeWithTryLock(cacheAllCorporateAssetsWorker, funcName);
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




/**
 * Corporate Asset Cache Worker (Nitro Edition - HYBRID)
 * Phase 1: Pauses briefly to create sheet safely.
 * Phase 2: Runs LIVE (Unpaused) to keep dashboard usable.
 */
function cacheAllCorporateAssetsWorker() {
    const START_TIME = new Date().getTime();
    const SCRIPT_PROP = PropertiesService.getScriptProperties();
    // Use the global log instance or create a specific one, ensure consistency
    const workerLog = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ASSET_WORKER') : console);

    const PROP_KEY_STEP = 'AssetCache_JobStatus';
    const PROP_KEY_WRITE_INDEX = 'AssetCache_RowIndex';
    const PROP_KEY_CHUNK_SIZE = 'AssetCache_ChunkSize';
    const ASSET_CACHE_DATA_KEY = 'AssetCache_Data_Shard';


    const START_ROW = 3;
    const START_COL = 1;
    const TEMP_SHEET_NAME = 'CorpWarehouseStock_Temp';
    const ASSET_CACHE_HEADERS = [["is_blueprint_copy", "is_singleton", "item_id", "location_flag", "location_id", "location_type", "quantity", "type_id"]];

    // Load State
    let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP);

    // DEBUG: Log the state to diagnose silent exits
    if (!currentStep) {
        workerLog.info(`[Worker] No job state found. Defaulting to NEW_RUN.`);
        currentStep = 'NEW_RUN';
    } else {
        workerLog.info(`[Worker] Loaded job state: ${currentStep}`);
    }

    var ss_anchor = SpreadsheetApp.getActiveSpreadsheet();

    // ==========================================================================
    // PHASE 1: FETCH & PREPARE (SURGICAL PAUSE)
    // ==========================================================================
    if (currentStep === 'NEW_RUN' || currentStep === 'FETCHED') {
        workerLog.info(`[Worker] State: ${currentStep}. Starting Fetch & Prep.`);

        const authName = (typeof getCorpAuthChar === 'function') ? getCorpAuthChar() : null;
        if (!authName) workerLog.warn('[Worker] No authorized character found.');

        // 1. Fetch Data (Live - No Pause yet)
        if (typeof _fetchAssetsConcurrently !== 'function') { workerLog.error('[Worker] missing _fetchAssetsConcurrently'); return; }
        let allAssets = [];
        try { allAssets = _fetchAssetsConcurrently(authName); } catch (e) { workerLog.error(`[Worker] Fetch failed: ${e.message}`); return; }
        if (!allAssets || allAssets.length <= 1) { workerLog.warn('[Worker] No assets retrieved. Aborting.'); return; }

        const processedAssets = allAssets.slice(1);
        if (typeof _chunkAndPut === 'function') _chunkAndPut(ASSET_CACHE_DATA_KEY, JSON.stringify(processedAssets), 21600);

        // 2. PAUSE (Crucial for Sheet Creation)
        var needsWakeUp = pauseSheet(ss_anchor);

        const setupResult = guardedSheetTransaction(() => {
            // prepareTempSheet now returns { success, state, error }
            const result = prepareTempSheet(ss_anchor, TEMP_SHEET_NAME, ASSET_CACHE_HEADERS[0]);

            if (!result.success) {
                throw new Error(result.error);
            }
            // Return the sheet object so the wrapper puts it in setupResult.state
            return result.state;
        }, 60000);

        // 3. WAKE UP IMMEDIATELY (Do not leave it paused for Phase 2)
        if (needsWakeUp) {
            wakeUpSheet(ss_anchor);
            console.log("[Worker] Surgical Pause complete. Sheet woken up for Write Phase.");
        }

        if (!setupResult.success) {
            workerLog.warn(`[Worker] Sheet prep failed (${setupResult.error}). Rescheduling.`);
            scheduleOneTimeTrigger('cacheAllCorporateAssetsWorker', RESCHEDULE_DELAY_MS);
            return;
        }

        SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, '0');
        SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
        SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'WRITING');

        workerLog.info(`[Worker] Prep Success. Transitioning to WRITING (Live Mode).`);
        scheduleOneTimeTrigger('cacheAllCorporateAssetsWorker', 1000);
        return;
    }

    // ==========================================================================
    // PHASE 2: WRITE (Nitro Mode - LIVE/UNPAUSED)
    // ==========================================================================
    if (currentStep === 'WRITING') {
        let cachedJson = _getAndDechunk(ASSET_CACHE_DATA_KEY);
        if (!cachedJson) {
            workerLog.error(`[Worker] CRITICAL: Cache Loss. Resetting Job.`);
            SCRIPT_PROP.deleteProperty(PROP_KEY_STEP);
            return;
        }
        let allRowsToWrite = JSON.parse(cachedJson);

        ss_anchor = SpreadsheetApp.getActiveSpreadsheet();

        let writeState = {
            logInfo: workerLog.info, logError: workerLog.error, logWarn: workerLog.warn,
            nextBatchIndex: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_WRITE_INDEX) || '0'),
            ss: ss_anchor,
            metrics: { startTime: START_TIME },
            config: {
                // Shared Settings
                ...(typeof NITRO_CONFIG !== 'undefined' ? NITRO_CONFIG : {}),

                // OVERRIDES FOR HEAVY ASSETS
                MAX_CELLS_PER_CHUNK: 30000, // Reduced from standard
                MAX_CHUNK_SIZE: 2000,       // Force smaller bites
                SOFT_LIMIT_MS: 280000,      // 4.5 Minutes (BAIL EARLY)

                // Dynamic State
                currentChunkSize: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || '500')
            }
        };

        if (writeState.nextBatchIndex === 0) writeState.config.currentChunkSize = 500;

        // [NO PAUSE HERE] - Running Live

        workerLog.info(`[Worker] Writing to '${TEMP_SHEET_NAME}' (Index: ${writeState.nextBatchIndex}).`);

        const writeResult = writeDataToSheet(TEMP_SHEET_NAME, allRowsToWrite, START_ROW, START_COL, writeState);

        if (writeResult.success) {
            workerLog.info("Write SUCCESS. Transitioning to FINALIZING.");
            SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'FINALIZING');
            SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
            SCRIPT_PROP.deleteProperty(PROP_KEY_WRITE_INDEX);
            scheduleOneTimeTrigger('finalizeAssetCacheJob', RESCHEDULE_DELAY_MS);
        }
        else if (writeResult.bailout_reason === "PREDICTIVE_BAILOUT" || (writeResult.error && writeResult.error.includes("timed out"))) {
            const reason = writeResult.error ? writeResult.error : "Predictive Bailout";
            workerLog.warn(`[Worker] Interrupted (${reason}). Rescheduling.`);

            const nextIndex = writeResult.state.nextBatchIndex.toString();
            let nextChunkSize = writeResult.state.config.currentChunkSize;
            if (writeResult.error) nextChunkSize = Math.max(MIN_CHUNK_SIZE, Math.floor(nextChunkSize / 2));

            SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, nextIndex);
            SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, nextChunkSize.toString());

            Utilities.sleep(1000);
            scheduleOneTimeTrigger('cacheAllCorporateAssetsWorker', 30000);
        }
        else {
            workerLog.error(`[Worker] Fatal Write Failure: ${writeResult.error}`);
        }
    }

    // Final fallback check
    if (currentStep !== 'NEW_RUN' && currentStep !== 'FETCHED' && currentStep !== 'WRITING') {
        workerLog.warn(`[Worker] Unhandled state encountered: '${currentStep}'. Job may be stuck.`);
    }
}

function finalizeAssetCacheJob() {
    const funcName = 'finalizeAssetCacheJob';
    const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ASSET_FINALIZER') : console);

    var ss_anchor = SpreadsheetApp.getActiveSpreadsheet();

    // Attempt to acquire lock
    const lock = LockService.getScriptLock();
    if (!lock.tryLock(5000)) {
        log.warn(`[Finalizer] Could not acquire lock. Previous job might be active. Rescheduling.`);
        scheduleOneTimeTrigger(funcName, 10000);
        return;
    }

    try {
        const SCRIPT_PROP = PropertiesService.getScriptProperties();
        const ASSET_JOB_STATUS_KEY = 'AssetCache_JobStatus';
        const CACHE_NAMED_RANGE = 'warehouse_unfiltered';
        const CACHE_SHEET_NAME = 'CorpWarehouseStock';
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

        // [ANESTHESIA]
        var needsWakeUp = pauseSheet(ss_anchor);

        // REFRESH CONNECTION
        ss_anchor = SpreadsheetApp.getActiveSpreadsheet();

        log.info('[Finalizer] Performing ATOMIC SWAP.');

        const repairMap = {
            [CACHE_NAMED_RANGE]: `A3:H`
        };

        const transactionResult = guardedSheetTransaction(() => {
            return atomicSwapAndFlush(ss_anchor, CACHE_SHEET_NAME, TEMP_SHEET_NAME, repairMap);
        }, 60000);

        // [WAKE UP] Immediately
        if (needsWakeUp) wakeUpSheet(ss_anchor);

        let swapResult;
        if (!transactionResult.success) {
            swapResult = { success: false, errorMessage: transactionResult.error };
        } else {
            swapResult = transactionResult.state;
        }

        if (!swapResult.success) {
            if (swapResult.errorMessage && swapResult.errorMessage.includes("not found")) {
                log.error(`[Finalizer] CRITICAL: Temp sheet missing. Clearing state.`);
                if (typeof _deleteShardedData === 'function') _deleteShardedData(ASSET_CACHE_DATA_KEY);
                SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
                SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
                SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
                deleteTriggersByName('cacheAllCorporateAssetsWorker');
                return;
            }
            log.warn(`[Finalizer] Swap Failed: ${swapResult.errorMessage}. Retrying.`);
            scheduleOneTimeTrigger('finalizeAssetCacheJob', 120000);
            return;
        }

        // Named Range resizing is handled in atomicSwapAndFlush

        if (typeof _deleteShardedData === 'function') _deleteShardedData(ASSET_CACHE_DATA_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
        SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);

        deleteTriggersByName('cacheAllCorporateAssetsWorker');
        deleteTriggersByName('finalizeAssetCacheJob');

        log.info(`[Finalizer] Job Complete. Swap successful.`);

    } finally {
        lock.releaseLock();
    }
}
