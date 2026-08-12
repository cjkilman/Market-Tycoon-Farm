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
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ASSET_TRIGGER') : console);

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const ASSET_JOB_STATUS_KEY = 'AssetCache_JobStatus';
  const status = SCRIPT_PROP.getProperty(ASSET_JOB_STATUS_KEY);

  if (status === 'FINALIZING') {
    log.info("Trigger: Job is in FINALIZING state. Dispatching finalizer.");

    // Wrapped in ScriptLock. No parentheses. No trailing comma.
    const finalizerName = 'finalizeAssetCacheJob';
    executeWithTryLock(finalizeAssetCacheJob, finalizerName);

    return; // Stop here! Do not run the worker.
  }

  // Wrapped in ScriptLock.
  const workerName = 'cacheAllCorporateAssetsWorker';
  executeWithTryLock(cacheAllCorporateAssetsWorker, workerName);
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
 * UPGRADED: Corporate Asset Ingestion Engine
 * Now integrates directly with the standard ESI module wrapper.
 * Dynamically handles pagination by parsing sequential pages until empty.
 */
/**
 * UPGRADED: Corporate Asset Ingestion Engine
 * Now integrates directly with the standard ESI module wrapper.
 * Dynamically handles pagination by parsing sequential pages until empty.
 */
function _fetchAssetsConcurrently(authName) {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('CORP_ASSETS') : console);
  const charData = GESI.getCharacterData ? GESI.getCharacterData(authName) : null;

  if (!charData?.corporation_id) {
    log.error(`Could not resolve Corp ID for: ${authName}`);
    return [ASSET_CACHE_HEADERS];
  }

  log.info(`[START] Syncing Assets: Corp ${charData.corporation_id}`);

  const result = ESI.forEndpoint(GESI.getClient(authName), 'corporations_corporation_assets')
    .get({ corporation_id: charData.corporation_id });

  if (result.error) {
    log.error(`Fetch failed: ${result.error}`);
    return [ASSET_CACHE_HEADERS];
  }

  // Mapping the data
  const mappedAssets = result.data.map(obj => [
    obj.is_blueprint_copy, obj.is_singleton, obj.item_id,
    obj.location_flag, obj.location_id, obj.location_type,
    obj.quantity, obj.type_id
  ]);

  log.info(`[SUCCESS] Assets synced: ${mappedAssets.length} items.`);
  return [ASSET_CACHE_HEADERS, ...mappedAssets];
}

/**
 * Corporate Asset Cache Worker (Nitro Edition - HYBRID)
 * Phase 1: Prepares the Temp sheet quietly.
 * Phase 2: Runs LIVE (Unpaused) to keep dashboard usable.
 */
function cacheAllCorporateAssetsWorker(ss) {
  const funcName = 'cacheAllCorporateAssetsWorker';
  const START_TIME = new Date().getTime();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const workerLog = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ASSET_WORKER') : console);

  const PROP_KEY_STEP = 'AssetCache_JobStatus';
  const PROP_KEY_WRITE_INDEX = 'AssetCache_RowIndex';
  const PROP_KEY_CHUNK_SIZE = 'AssetCache_ChunkSize';
  const ASSET_CACHE_DATA_KEY = 'AssetCache_Data_Shard';

  const localDelayMs = (typeof RESCHEDULE_DELAY_MS !== 'undefined') ? RESCHEDULE_DELAY_MS : 120000;

  const START_ROW = 3;
  const START_COL = 1;
  const TEMP_SHEET_NAME = 'CorpWarehouseStock_Temp';
  const ASSET_CACHE_HEADERS = [["is_blueprint_copy", "is_singleton", "item_id", "location_flag", "location_id", "location_type", "quantity", "type_id"]];

  let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP);

  if (currentStep === 'FINALIZING') {
    workerLog.info(`[Worker] Job is currently FINALIZING. Exiting safely.`);
    return;
  }

  if (!currentStep) {
    workerLog.info(`[Worker] No job state found. Defaulting to NEW_RUN.`);
    currentStep = 'NEW_RUN';
  } else {
    workerLog.info(`[Worker] Loaded job state: ${currentStep}`);
  }

  var ss_anchor = ss;
  if (!ss_anchor || typeof ss_anchor.getSheetByName !== 'function') {
    ss_anchor = SpreadsheetApp.getActiveSpreadsheet();
  }

  // ==========================================================================
  // PHASE 1: FETCH & PREPARE
  // ==========================================================================
  if (currentStep === 'NEW_RUN' || currentStep === 'FETCHED') {
    workerLog.info(`[Worker] State: ${currentStep}. Starting Fetch & Prep.`);

    const authName = (typeof getCorpAuthChar === 'function') ? getCorpAuthChar() : null;
    if (!authName) workerLog.warn('[Worker] No authorized character found.');

    if (typeof _fetchAssetsConcurrently !== 'function') { workerLog.error('[Worker] missing _fetchAssetsConcurrently'); return; }
    let allAssets = [];
    try {
      allAssets = _fetchAssetsConcurrently(authName);
    } catch (e) {
      workerLog.error(`[Worker] Fetch failed: ${e.message}`);
      return;
    }

    if (!allAssets || allAssets.length <= 1) {
      workerLog.warn('[Worker] No assets retrieved. Aborting.');
      return;
    }

    const processedAssets = allAssets.slice(1);
    if (typeof _chunkAndPut === 'function') _chunkAndPut(ASSET_CACHE_DATA_KEY, JSON.stringify(processedAssets), 21600);

    const setupResult = guardedSheetTransaction(() => {
      const result = prepareTempSheet(ss_anchor, TEMP_SHEET_NAME, ASSET_CACHE_HEADERS[0]);
      if (!result.success) throw new Error(result.error);
      return result.state;
    }, 60000);

    if (!setupResult.success) {
      workerLog.warn(`[Worker] Sheet prep failed (${setupResult.error}). Resetting state to NEW_RUN and Rescheduling.`);
      SCRIPT_PROP.deleteProperty(PROP_KEY_STEP);
      if (typeof _deleteShardedData === 'function') _deleteShardedData(ASSET_CACHE_DATA_KEY);
      
      deleteTriggersByName(funcName);
      scheduleOneTimeTrigger(funcName, localDelayMs);
      return;
    }

    SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, '0');
    SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
    SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'WRITING');

    workerLog.info(`[Worker] Prep Success. Transitioning to WRITING.`);
    deleteTriggersByName(funcName);
    scheduleOneTimeTrigger(funcName, 1000);
    return;
  }

  // ==========================================================================
  // PHASE 2: WRITE (Nitro Mode)
  // ==========================================================================
  if (currentStep === 'WRITING') {
    let cachedJson = (typeof _getAndDechunk === 'function') ? _getAndDechunk(ASSET_CACHE_DATA_KEY) : null;
    if (!cachedJson) {
      workerLog.error(`[Worker] CRITICAL: Cache Loss. Resetting Job.`);
      SCRIPT_PROP.deleteProperty(PROP_KEY_STEP);
      return;
    }
    let allRowsToWrite = JSON.parse(cachedJson);

    const nitro = typeof NITRO_CONFIG !== 'undefined' ? NITRO_CONFIG : { MIN_CHUNK_SIZE: 500 };

    let writeState = {
      nextBatchIndex: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_WRITE_INDEX) || '0'),
      ss: ss_anchor,
      metrics: { startTime: START_TIME },
      config: {
        ...nitro,
        currentChunkSize: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || nitro.MIN_CHUNK_SIZE)
      }
    };

    if (writeState.nextBatchIndex === 0) {
      writeState.config.currentChunkSize = nitro.MIN_CHUNK_SIZE;
    }

    workerLog.info(`[Worker] Writing to '${TEMP_SHEET_NAME}' (Index: ${writeState.nextBatchIndex}).`);

    const writeResult = writeDataToSheet(TEMP_SHEET_NAME, allRowsToWrite, START_ROW, START_COL, writeState);

    if (writeResult.success) {
      workerLog.info("Write SUCCESS. Transitioning to FINALIZING.");
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'FINALIZING');
      SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
      SCRIPT_PROP.deleteProperty(PROP_KEY_WRITE_INDEX);
      
      deleteTriggersByName('finalizeAssetCacheJob');
      scheduleOneTimeTrigger('finalizeAssetCacheJob', 1000);
    }
    else if (writeResult.bailout_reason === "PREDICTIVE_BAILOUT" || 
             (writeResult.error && (writeResult.error.includes("timed out") || writeResult.error.includes("Lock")))) {
      
      const reason = writeResult.error ? writeResult.error : "Predictive Bailout";
      workerLog.warn(`[Worker] Interrupted (${reason}). Rescheduling to RESUME.`);

      const nextIndex = writeResult.state.nextBatchIndex.toString();
      let nextChunkSize = writeResult.state.config.currentChunkSize;
      
      if (writeResult.error) {
        nextChunkSize = Math.max(nitro.MIN_CHUNK_SIZE, Math.floor(nextChunkSize / 2));
      }

      SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, nextIndex);
      SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, nextChunkSize.toString());

      Utilities.sleep(1000);
      deleteTriggersByName(funcName);
      scheduleOneTimeTrigger(funcName, 30000);
    }
    else {
      workerLog.error(`[Worker] Unrecoverable Write Failure: ${writeResult.error}. Resetting state.`);
      SCRIPT_PROP.deleteProperty(PROP_KEY_STEP);
      SCRIPT_PROP.deleteProperty(PROP_KEY_WRITE_INDEX);
      SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
      if (typeof _deleteShardedData === 'function') _deleteShardedData(ASSET_CACHE_DATA_KEY);
    }
  }

  if (currentStep !== 'NEW_RUN' && currentStep !== 'FETCHED' && currentStep !== 'WRITING' && currentStep !== 'FINALIZING') {
    workerLog.warn(`[Worker] Unhandled state encountered: '${currentStep}'. Job may be stuck.`);
  }
}

function updateHangarNamedRanges(ss) {
  if (!ss || typeof ss.getSheetByName !== 'function') {
    ss = SpreadsheetApp.getActiveSpreadsheet();
  }

  // 1. Fetch once
  const allNamedRanges = ss.getNamedRanges();
  
  // 2. Build local dictionary for instant lookups
  const nrCache = {};
  for (let i = 0; i < allNamedRanges.length; i++) {
    nrCache[allNamedRanges[i].getName()] = allNamedRanges[i];
  }

  // Define all hangars here to keep the engine clean and scalable
  const hangars = [
    { sheetName: "MarketStorage", rangeName: "NR_WAREHOUSE_HANGAR", numColumns: 4 },
    { sheetName: "MaterialHangar", rangeName: "NR_MATERIAL_HANGAR", numColumns: 3 }
  ];

  hangars.forEach(hangar => {
    const sh = ss.getSheetByName(hangar.sheetName);

    // FIX: Safely wipe global caches to ensure fresh reads next time
    if (typeof GLOBALS !== 'undefined') {
      if (GLOBALS.dataCache) GLOBALS.dataCache.delete(hangar.rangeName);
      if (GLOBALS.rangeCache) GLOBALS.rangeCache.delete(hangar.rangeName);
    }

    if (!sh) {
      console.warn(`[WARN] ${hangar.sheetName} sheet not found. Skipping ${hangar.rangeName}.`);
      return;
    }

    // THE FIX: If the sheet is totally wiped, collapse the range to Row 1 
    const lastRow = Math.max(sh.getLastRow(), 1);

    // Build the dynamic range based on the config array
    const newRange = sh.getRange(1, 1, lastRow, hangar.numColumns);

    // 3. Execute zero-latency cache check
    if (nrCache[hangar.rangeName]) {
      nrCache[hangar.rangeName].setRange(newRange);
      console.log(`[UPDATE] ${hangar.rangeName} resized to row ${lastRow}.`);
    } else {
      ss.setNamedRange(hangar.rangeName, newRange);
      console.log(`[CREATE] ${hangar.rangeName} initialized at row ${lastRow}.`);
    }
  });
}

function finalizeAssetCacheJob(ss) {
  const funcName = 'finalizeAssetCacheJob';
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ASSET_FINALIZER') : console);

  // 1. Trigger-Safe Spreadsheet Fetch
  if (!ss || typeof ss.getSheetByName !== 'function') {
    ss = SpreadsheetApp.getActiveSpreadsheet();
  }

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const ASSET_JOB_STATUS_KEY = 'AssetCache_JobStatus';
  const CACHE_NAMED_RANGE = 'warehouse_unfiltered';
  const CACHE_SHEET_NAME = 'CorpWarehouseStock';
  const TEMP_SHEET_NAME = 'CorpWarehouseStock_Temp';
  const ASSET_CACHE_DATA_KEY = 'AssetCache_Data_Shard';
  const ASSET_CACHE_ROW_INDEX_KEY = 'AssetCache_RowIndex';
  const PROP_KEY_CHUNK_SIZE = 'AssetCache_ChunkSize';

  const status = SCRIPT_PROP.getProperty(ASSET_JOB_STATUS_KEY);

  if (status !== 'FINALIZING') {
    log.warn(`[Finalizer] Called in wrong state (${status}). Aborting.`);
    return;
  }

  let needsWakeUp = false;

  try {
    const repairMap = {
      [CACHE_NAMED_RANGE]: 'A1:H'
    };

    // 3. Execute the swap AND dependency updates while calculations are strictly frozen
    const transactionResult = guardedSheetTransaction(() => {
      // [ANESTHESIA] - Put the sheet to sleep ONLY after acquiring the lock
      needsWakeUp = pauseSheet(ss);
      
      log.info('[Finalizer] Performing ATOMIC SWAP.');
      
      // Pass TRUE to holdAnesthesia, telling the swap function we will wake the sheet ourselves
     const swapRes = atomicSwapAndFlush(ss, CACHE_SHEET_NAME, TEMP_SHEET_NAME, repairMap, true, false);

      // [DEPENDENCY INJECTION] - Update Hangars under Anesthesia to prevent calculation timeouts
      if (swapRes && swapRes.success) {
        try {
          log.info(`[Finalizer] Swap successful. Updating Hangar ranges under anesthesia...`);
          updateHangarNamedRanges(ss);
          log.info(`[Finalizer] Hangar ranges updated safely.`);
        } catch (hangarErr) {
          log.warn(`[Finalizer] Hangar update failed, but proceeding with swap success: ${hangarErr.message}`);
        }
      }

      return swapRes;
    }, 60000);

    // 4. [WAKE UP] - Turn calculations back on. The sheet will now calculate ONCE with all new bounds.
    if (needsWakeUp) {
      wakeUpSheet(ss);
      needsWakeUp = false; // reset so the catch block doesn't double-fire
    }

    // --- ERROR HANDLING ---

    if (!transactionResult.success) {
      log.warn(`[Finalizer] Transaction Failed: ${transactionResult.error}. Retrying in 2 minutes.`);
      scheduleOneTimeTrigger(funcName, 120000);
      return;
    }

    const swapState = transactionResult.state || {};

    if (!swapState.success) {
      if (swapState.errorMessage && swapState.errorMessage.includes("not found")) {
        log.error(`[Finalizer] CRITICAL: Temp sheet missing. Clearing state.`);
        if (typeof _deleteShardedData === 'function') _deleteShardedData(ASSET_CACHE_DATA_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
        SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
        SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
        deleteTriggersByName('cacheAllCorporateAssetsWorker');
        return;
      }
      log.warn(`[Finalizer] Swap Failed: ${swapState.errorMessage}. Retrying in 2 minutes.`);
      scheduleOneTimeTrigger(funcName, 120000);
      return;
    }

    // --- CLEANUP ON SUCCESS ---

    if (typeof _deleteShardedData === 'function') _deleteShardedData(ASSET_CACHE_DATA_KEY);
    SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
    SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
    SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);

    deleteTriggersByName('cacheAllCorporateAssetsWorker');
    deleteTriggersByName(funcName);

    log.info(`[Finalizer] Job Complete. Swap and Hangar updates successful.`);

  } catch (e) {
    log.error(`[Finalizer] Unexpected Error: ${e.message}`);
    if (needsWakeUp) wakeUpSheet(ss);
  }
}
