/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, scheduleOneTimeTrigger, executeWithTryLock, getCorpAuthChar, CacheService, writeDataToSheet, guardedSheetTransaction, atomicSwapAndFlush, deleteTriggersByName, _chunkAndPut, _getAndDechunk, _deleteShardedData */

// ======================================================================
// EVE ONLINE ASSET AND LOCATION MANAGEMENT MODULE
// ======================================================================

const SAFE_CONSOLE_SHIM = {
  log: console.log, info: console.log, warn: console.warn, error: console.error,
  startTimer: () => ({ stamp: () => {} }) 
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
const ASSET_CACHE_TTL = 3600; 

// --- CONFIGURATION ---
const STARTING_CHUNK_SIZE = 5000; // Start fast
const NEW_MIN_CHUNK_SIZE = 500;   // Floor
const NEW_MAX_CHUNK_SIZE = 8000;  // Cap
const NEW_SOFT_LIMIT_MS = 275000; // 4m 35s
const CRIT_LOCK_WAIT_MS = 60000;

// Configuration passed to the shared writeDataToSheet utility
const WRITE_CONFIG = {
    MAX_CELLS_PER_CHUNK: 60000,       // 60k cells per batch (Nitro)
    TARGET_WRITE_TIME_MS: 5000,       // 5s target
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
    } catch(e) {}
    
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
         } catch(e) {}
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
              } catch (e) {}
            }
          });
        }
    }
    return allAssets;
}

function _prepareCacheSheet(ss) {
    const SCRIPT_NAME = '_prepareCacheSheet';
    // Uses guardedSheetTransaction from Utility.js
    const transactionResult = guardedSheetTransaction(() => {
        let cacheSheet = ss.getSheetByName(TEMP_SHEET_NAME);
        if (!cacheSheet) { 
             cacheSheet = ss.insertSheet(TEMP_SHEET_NAME);
        }
        _sheetCache[TEMP_SHEET_NAME] = cacheSheet;
        cacheSheet.clear();
        cacheSheet.getRange("A2:H2").setValues([ASSET_CACHE_HEADERS]);
        log.info(`[${SCRIPT_NAME}] Prepared TEMP sheet '${TEMP_SHEET_NAME}'.`);
        return { success: true, duration: 0 }; 
    }, CRIT_LOCK_WAIT_MS); 

    if (transactionResult.success) return transactionResult.state; 
    else {
        log.error(`[${SCRIPT_NAME}] CRITICAL ERROR: ${transactionResult.error}`);
        return { success: false, duration: 0 };
    }
}

/**
 * WORKER
 */
function cacheAllCorporateAssetsWorker() {
    const ssDoc = SpreadsheetApp.getActiveSpreadsheet();
    const SCRIPT_PROP = PropertiesService.getScriptProperties(); 
    const START_TIME = new Date().getTime();

    let currentChunkSize = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || STARTING_CHUNK_SIZE.toString(), 10);
    currentChunkSize = Math.max(NEW_MIN_CHUNK_SIZE, currentChunkSize);
    
    let processedAssets = [];
    
    // Uses _getAndDechunk from Utility.js
    const cachedAssetDataJson = _getAndDechunk(ASSET_CACHE_DATA_KEY); 
    let nextBatchIndex = parseInt(SCRIPT_PROP.getProperty(ASSET_CACHE_ROW_INDEX_KEY) || '0', 10);
    let jobStatus = SCRIPT_PROP.getProperty(ASSET_JOB_STATUS_KEY);

    if (nextBatchIndex > 0 && !cachedAssetDataJson) {
        log.error(`[Worker] CACHE LOSS DETECTED at index ${nextBatchIndex}. Restarting job from 0.`);
        nextBatchIndex = 0;
        jobStatus = null; 
        SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, '0');
    }

    if (cachedAssetDataJson) {
        try {
            processedAssets = JSON.parse(cachedAssetDataJson);
        } catch (e) {
             // Uses _deleteShardedData from Utility.js
             _deleteShardedData(ASSET_CACHE_DATA_KEY);
             processedAssets = [];
        }
    } 
    
    if (processedAssets.length > 0 && jobStatus !== 'FETCHED' && jobStatus !== 'WRITING') {
        jobStatus = 'FETCHED';
        SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, 'FETCHED');
    }

    if (processedAssets.length === 0) {
        log.info(`[Worker] Starting new job. Resolving auth...`);
        
        const authName = getCorpAuthChar();
        if (!authName) {
            log.error('[Worker] No authorized character found. Check GESI settings.');
            return;
        }
        log.info(`[Worker] Fetching assets as: ${authName}`);
        
        const allAssets = _fetchAssetsConcurrently(authName);

        if (!allAssets || allAssets.length <= 1) { 
            log.warn('[Worker] No assets retrieved (or only headers).'); 
            return; 
        }

        const rawAssetsData = allAssets.slice(1);
        const sanitizedAssetsData = rawAssetsData.filter(row => {
            return Number(row[2]) > 0 && Number(row[4]) > 0;
        });
        
        processedAssets = sanitizedAssetsData;

        // Uses _chunkAndPut from Utility.js
        const saved = _chunkAndPut(ASSET_CACHE_DATA_KEY, JSON.stringify(processedAssets), ASSET_CACHE_TTL);
        if (!saved) return; 
        
        SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, '0');
        SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, 'FETCHED');
        
        SpreadsheetApp.flush(); 
        Utilities.sleep(100); 
        log.info(`[Worker] Assets fetched (${processedAssets.length} rows). Saved/Verified.`);
        scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 1000);
        return;
    }

    if (nextBatchIndex === 0 && jobStatus === 'FETCHED') {
        const result = _prepareCacheSheet(ssDoc); 
        if (!result || !result.success) return;

        SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, 'WRITING');
        jobStatus = 'WRITING'; 
        log.info(`[Worker] Sheet prepared. Status: WRITING.`);
    }

    if (jobStatus === 'WRITING') {
        const ss_stable = SpreadsheetApp.getActiveSpreadsheet();
        if (nextBatchIndex === 0) currentChunkSize = STARTING_CHUNK_SIZE;

        log.info(`[Worker] Writing to '${TEMP_SHEET_NAME}' (Index: ${nextBatchIndex}).`);

        const dynamicConfig = { ...WRITE_CONFIG, currentChunkSize: currentChunkSize };
        const stateObject = {
            logInfo: log.info, logError: log.error, logWarn: log.warn,
            ss: ss_stable,
            metrics: { startTime: START_TIME },
            nextBatchIndex: nextBatchIndex, 
            config: dynamicConfig
        };

        // Uses writeDataToSheet from Utility.js
        const writeResult = writeDataToSheet(
            TEMP_SHEET_NAME, 
            processedAssets, 
            3, 1, 
            stateObject
        );

        if (!writeResult.success) {
            log.error(`[Worker] Write Abort at ${writeResult.state.nextBatchIndex}. Rescheduling.`);
            const nextIndex = writeResult.state.nextBatchIndex;
            const nextChunkSize = Math.max(NEW_MIN_CHUNK_SIZE, Math.floor(writeResult.state.config.currentChunkSize / 2));
            
            SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, nextIndex.toString());
            SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, nextChunkSize.toString()); 
            scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000);
            return; 
        }

        log.info(`[Worker] Write Complete. Transitioning to FINALIZING.`);
        SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, 'FINALIZING');
        SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
        SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);

        scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 1000);
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
        const ssDoc = SpreadsheetApp.getActiveSpreadsheet();

        // Uses atomicSwapAndFlush from Utility.js
        const swapResult = atomicSwapAndFlush(ssDoc, CACHE_SHEET_NAME, TEMP_SHEET_NAME);

        if (!swapResult.success) {
             log.error(`[Finalizer] Swap Failed: ${swapResult.errorMessage}. Retrying in 30s.`);
             scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 30000);
             return;
        }

        let finalSheet = ssDoc.getSheetByName(CACHE_SHEET_NAME);
        if (finalSheet) {
            const lastRow = finalSheet.getLastRow();
            const rangeHeight = Math.max(1, lastRow - 2); 
            finalSheet.getParent().setNamedRange(
                CACHE_NAMED_RANGE,
                finalSheet.getRange(3, 1, rangeHeight, NUM_ASSET_COLS)
            );
        }
        
        // Uses _deleteShardedData from Utility.js
        _deleteShardedData(ASSET_CACHE_DATA_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
        SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE); 

        log.info(`[Finalizer] Job Complete. Swap successful.`);

    }, funcName);
}