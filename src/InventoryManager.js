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
const STARTING_CHUNK_SIZE = 1000; // Start fast
const NEW_MIN_CHUNK_SIZE = 500;   // Floor
const NEW_MAX_CHUNK_SIZE = 8000;  // Cap
const NEW_SOFT_LIMIT_MS = 280000; // 4m 35s
const CRIT_LOCK_WAIT_MS = 60000;

// Configuration passed to the shared writeDataToSheet utility
const WRITE_CONFIG = {
    MAX_CELLS_PER_CHUNK: 60000,       // 60k cells per batch (Nitro)
    TARGET_WRITE_TIME_MS: 2500,       // 2.5s target
    MAX_FACTOR: 2.0,
    THROTTLE_THRESHOLD_MS: 2000,
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
function cacheAllCorporateAssetsWorker() {
    const START_TIME = new Date().getTime();
    const SCRIPT_PROP = PropertiesService.getScriptProperties();
    
    // State Keys (Matching the "Nitro" pattern logic)
    const PROP_KEY_STEP = ASSET_JOB_STATUS_KEY; 
    const PROP_KEY_WRITE_INDEX = ASSET_CACHE_ROW_INDEX_KEY;
    const PROP_KEY_CHUNK_SIZE = ASSET_CHUNK_SIZE_KEY;
    
    // --- NITRO CONFIGURATION (Mirrored from Orchestrator) ---
    // Soft Limit: 4.5 Minutes
    const [MAX_CHUNK_SIZE, MIN_CHUNK_SIZE, SOFT_LIMIT_MS, RESCHEDULE_DELAY_MS] 
        = [8000, 500, 270000, 10000];
        
    // Columns: 8 
    const START_ROW = 3; // Assets start at row 3 in your sheet
    const START_COL = 1; 

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP);

    // Default to NEW_RUN if null
    if (!currentStep) currentStep = 'NEW_RUN';

    // ==========================================================================
    // PHASE 1: FETCH & PREPARE (Equivalent to NEW_RUN in Market Worker)
    // ==========================================================================
    if (currentStep === 'NEW_RUN' || currentStep === 'FETCHED') {
        log.info(`[Worker] State: ${currentStep}. Starting Fetch & Prep.`);

        // 1. Auth & Fetch
        const authName = getCorpAuthChar();
        if (!authName) {
            log.error('[Worker] No authorized character found. Check GESI settings.');
            return;
        }
        log.info(`[Worker] Fetching assets as: ${authName}`);

        const allAssets = _fetchAssetsConcurrently(authName);

        if (!allAssets || allAssets.length <= 1) {
            log.warn('[Worker] No assets retrieved (or only headers). Aborting.');
            return;
        }

        // 2. Sanitize Data
        const rawAssetsData = allAssets.slice(1);
        const processedAssets = rawAssetsData.filter(row => {
            return Number(row[2]) > 0 && Number(row[4]) > 0;
        });

        // 3. Save to Cache (Persistence)
        const saved = _chunkAndPut(ASSET_CACHE_DATA_KEY, JSON.stringify(processedAssets), ASSET_CACHE_TTL);
        if (!saved) {
            log.error('[Worker] Failed to cache asset data. Aborting.');
            return;
        }

        // 4. Prepare Sheet (Guarded Transaction)
        const setupResult = guardedSheetTransaction(() => {
            const ss_inner = SpreadsheetApp.getActiveSpreadsheet();
            // _prepareCacheSheet logic inline for safety
            const cacheSheet = prepareTempSheet(ss_inner, TEMP_SHEET_NAME, ASSET_CACHE_HEADERS);
            log.info(`[Worker] Prepared TEMP sheet '${TEMP_SHEET_NAME}'.`);
            return true;
        }, 60000);

        if (!setupResult.success) {
            log.warn(`[Worker] Sheet prep failed (${setupResult.error}). Rescheduling.`);
            scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', RESCHEDULE_DELAY_MS);
            return;
        }

        // 5. Transition State
        SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, '0');
        SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
        SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'WRITING');
        
        log.info(`[Worker] Fetch Success (${processedAssets.length} rows). Transitioning to WRITING.`);
        scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 1000); // Instant dispatch
        return;
    }

    // ==========================================================================
    // PHASE 2: WRITE (Nitro Mode - Mirrored Logic)
    // ==========================================================================
    if (currentStep === 'WRITING') {
        
        // 1. Load Data from Cache
        const cachedJson = _getAndDechunk(ASSET_CACHE_DATA_KEY);
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

        // 2. Define FRESH Spreadsheet Reference (The "Working" Pattern)
        const ss_stable = SpreadsheetApp.getActiveSpreadsheet();

        // 3. Setup Write State (Exact mirror of Market Worker config)
        let writeState = {
            logInfo: log.info, logError: log.error, logWarn: log.warn,
            nextBatchIndex: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_WRITE_INDEX) || '0'),
            ss: ss_stable, // <--- PASSED FRESH
            metrics: { startTime: START_TIME },
            config: {
                MAX_CELLS_PER_CHUNK: 60000,
                TARGET_WRITE_TIME_MS: 5000,
                MAX_FACTOR: 2.0,
                THROTTLE_THRESHOLD_MS: 2000,
                THROTTLE_PAUSE_MS: 100,
                currentChunkSize: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || STARTING_CHUNK_SIZE.toString()),
                MAX_CHUNK_SIZE: MAX_CHUNK_SIZE,
                MIN_CHUNK_SIZE: MIN_CHUNK_SIZE,
                SOFT_LIMIT_MS: SOFT_LIMIT_MS
            }
        };
        
        // Initial chunk enforcement
        if (writeState.nextBatchIndex === 0) {
             writeState.config.currentChunkSize = STARTING_CHUNK_SIZE;
        }

        log.info(`[Worker] Writing to '${TEMP_SHEET_NAME}' (Index: ${writeState.nextBatchIndex}, Chunk: ${writeState.config.currentChunkSize}).`);

        // 4. Execute Write
        const writeResult = writeDataToSheet(TEMP_SHEET_NAME, allRowsToWrite, START_ROW, START_COL, writeState);

        // 5. Handle Result (The "Working" Logic)
        if (writeResult.success) {
            log.info("Write SUCCESS. Transitioning to FINALIZING.");
            SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'FINALIZING');
            SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
            SCRIPT_PROP.deleteProperty(PROP_KEY_WRITE_INDEX);
            scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', RESCHEDULE_DELAY_MS);
        }
        // *** FIXED LOGIC: Catch Bailouts and Timeouts ***
        else if (writeResult.bailout_reason === "PREDICTIVE_BAILOUT" ||
          (writeResult.error && (
            writeResult.error.includes("ServiceTimeoutFailure") ||
            writeResult.error.includes("Service timed out") ||
            writeResult.error.includes("Exceeded maximum execution time")
          ))) {

          // Clean Log Message
          const reason = writeResult.error ? writeResult.error : "Soft Time Limit Reached (Predictive)";
          log.warn(`[Worker] Write phase interrupted. Reason: ${reason}. Rescheduling.`);

          const nextIndex = writeResult.state.nextBatchIndex.toString();
          
          // Smart Chunk Adjustment: Slash if crash, maintain if predictive
          let nextChunkSize = writeResult.state.config.currentChunkSize;
          if (writeResult.error) {
              nextChunkSize = Math.max(MIN_CHUNK_SIZE, Math.floor(nextChunkSize / 2));
          }

          SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, nextIndex);
          SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, nextChunkSize.toString());
          
          // Reschedule
          Utilities.sleep(1000);
          scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 30000);
        }
        else {
          log.error(`[Worker] Fatal Write Failure: ${writeResult.error}`);
          // Optional: Reset state here if you want hard failure
        }
    }
}

function finalizeAssetCacheJob() {
    const funcName = 'finalizeAssetCacheJob';
    executeWithTryLock(() => {
        const SCRIPT_PROP = PropertiesService.getScriptProperties();
        const status = SCRIPT_PROP.getProperty(ASSET_JOB_STATUS_KEY);

        if (status !== 'FINALIZING') {
            log.warn(`[Finalizer] Called in wrong state (${status}). Aborting.`);
            return;
        }

        log.info('[Finalizer] Performing ATOMIC SWAP.');

        // 1. DEFINE REPAIR MAP (The "Cure" for #REF ranges)
        // If the range is broken or deleted, this tells the swap where it belongs.
        // We use A3 notation (Open ended or specific columns) to match your logic.
        const repairMap = {
             // Dynamic Key : Value
             [CACHE_NAMED_RANGE]: `A3:E` // <--- UPDATE THIS to match your actual columns (e.g., A to E)
        };

        // 2. EXECUTE GUARDED SWAP
        const transactionResult = guardedSheetTransaction(() => {
            const ss_inner = SpreadsheetApp.getActiveSpreadsheet(); // <--- CRITICAL: Fresh Instance
            
            // Pass the repairMap as the 4th argument!
            return atomicSwapAndFlush(ss_inner, CACHE_SHEET_NAME, TEMP_SHEET_NAME, repairMap);
            
        }, 60000); // 60s Timeout

        // 3. UNWRAP RESULTS
        // guardedSheetTransaction returns { success: boolean, state: any, error: string }
        // atomicSwapAndFlush returns { success: boolean, errorMessage: string }
        let swapResult;

        if (!transactionResult.success) {
            // The Transaction Wrapper failed (Timeout or hard crash)
            swapResult = { success: false, errorMessage: transactionResult.error };
        } else {
            // The Transaction ran, now we check the Swap Logic result
            swapResult = transactionResult.state;
        }

        // 4. HANDLE FAILURE
        if (!swapResult.success) {
            // CASE A: Missing Sheet (Fatal - Do not retry)
            if (swapResult.errorMessage && swapResult.errorMessage.includes("not found")) {
                log.error(`[Finalizer] CRITICAL: Temp sheet missing. Clearing state to reset.`);
                _deleteShardedData(ASSET_CACHE_DATA_KEY);
                SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
                SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
                SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
                return;
            }

            // CASE B: Timeout/Lock (Retry)
            log.warn(`[Finalizer] Swap Failed: ${swapResult.errorMessage}. Retrying in 120s.`);
            scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 120000);
            return;
        }

        // 5. POST-SWAP RESIZE (Optional but Recommended)
        // The Atomic Swap preserves the *Previous* dimensions. 
        // If you want the Named Range to hug the new data perfectly (e.g. Row 5000 -> 6000), 
        // this block is still useful to "Trim" the range.
        try {
            const ss_fresh = SpreadsheetApp.getActiveSpreadsheet();
            const finalSheet = ss_fresh.getSheetByName(CACHE_SHEET_NAME);
            if (finalSheet) {
                const lastRow = finalSheet.getLastRow();
                // Ensure we don't have negative height if sheet is empty
                const rangeHeight = Math.max(1, lastRow - 2); 
                
                ss_fresh.setNamedRange(
                    CACHE_NAMED_RANGE,
                    finalSheet.getRange(3, 1, rangeHeight, NUM_ASSET_COLS)
                );
                log.info(`[Finalizer] Resized Named Range '${CACHE_NAMED_RANGE}' to ${lastRow} rows.`);
            }
        } catch (nrError) {
             log.warn(`[Finalizer] Range Resize Warning: ${nrError.message}`);
        }

        // 6. CLEANUP & SUCCESS
        _deleteShardedData(ASSET_CACHE_DATA_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
        SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);

        log.info(`[Finalizer] Job Complete. Swap successful.`);

    }, funcName);
}