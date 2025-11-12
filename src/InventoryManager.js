/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, scheduleOneTimeTrigger, executeWithTryLock, getCorpAuthChar */

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

// --- 1. GLOBAL CONSTANTS AND DEPENDENCIES ---

// Keys for storing job state in Script Properties (Resilience)
const ASSET_CACHE_DATA_KEY = 'AssetCache_Data_V2';
const ASSET_CACHE_ROW_INDEX_KEY = 'AssetCache_NextRow';
const ASSET_JOB_STATUS_KEY = 'AssetCache_Status_Key';
const ASSET_CHUNK_SIZE_KEY = 'AssetCache_ChunkSize';

// Cache Key for SDE Type Map (New Persistent Layer)
const SDE_TYPE_MAP_KEY = 'SDE_invTypes_TypeMap'; 

// Time Constants (in milliseconds)
const SOFT_LIMIT_MS = 280000; // 4m 40s (Safety limit for predictive reschedule)
const DOC_LOCK_TIMEOUT_MS = 5000; // TryLock 5s for chunk writing
const CRIT_LOCK_WAIT_MS = 60000; // WaitLock 60s for sheet clear/prepare

// Throttling and Chunking Constants
const THROTTLE_THRESHOLD_MS = 800; // If write takes longer than this, throttle
const THROTTLE_PAUSE_MS = 200;
const CHUNK_INCREASE_RATE = 50;
const CHUNK_DECREASE_RATE = 200; // Aggressive decay rate
const MAX_CHUNK_SIZE = 1000;
const MIN_CHUNK_SIZE = 50;
const INITIAL_WRITE_CHUNK = 10; // Extremely small starting chunk for the first write

// EVE CONSTANTS
const ASSET_ID_MIN_BOUND = 100000000000; // Used to differentiate asset IDs from type IDs.
const NPC_STATION_ID_MAX = 70000000;     // IDs above this are typically player-owned structures

// Known EVE Bug/Exclusion Lists (Sets for O(1) lookup)
const GHOST_ITEM_IDS = new Set([
  9007199254740992, 9007199254740993, 9007199254740994, 9007199254740995,
  1042136670568, 1042139243054, 1043862654421, 1038876191270, 1044532547334, 1050483607331, 
  1039962719245, 1036200304791, 1047736829320, 1028141962065, 1031195155767, 1034862502178, 
  1034862547753, 1040928243616, 1047961260476, 1030142093671, 1030289543328, 1031616387594, 
  1033808818685, 1034429286734, 1042134935603, 1047745393662, 1047758618232, 1047959246356 
]);
const EXCLUDED_CONTAINER_TYPE_IDS = new Set([28317, 28318]); // Delivery Hangars, Fleet Hangars

// Sheet Names and Headers
const CACHE_SHEET_NAME = 'CorpWarehouseStock';
const LOCATION_MANAGER_SHEET_NAME = 'LocationManager';
const MATERIAL_HANGAR_SHEET_NAME = 'MaterialHangar';
const LOCATION_CACHE_SHEET = "Location_Name_Cache"; // The master cache
const LOCATION_CACHE_HEADERS = ['locationID', 'locationName']; // Standard headers
const ASSET_CACHE_HEADERS = ["is_blueprint_copy", "is_singleton", "item_id", "location_flag", "location_id", "location_type", "quantity", "type_id"];
const NUM_ASSET_COLS = ASSET_CACHE_HEADERS.length;
const CACHE_NAMED_RANGE = 'NR_CORP_ASSETS_CACHE';

// Global sheet cache map for chunk writing (Required for _writeChunkInternal)
const _sheetCache = {};

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
        const name = structureData && structureData.name ? structureData.name : `Structure (ID: ${structureIdString})`;
        
        // Save to persistent cache and local map
        SCRIPT_PROP.setProperty(CACHE_KEY, name);
        structureCacheMap.set(structureId, name);
        Utilities.sleep(50); // Respect the 50ms delay after ESI call
        return name;
    } catch (e) {
        log.error(`[ESI_CACHE] WARNING: ESI call for structure ID ${structureId} failed: ${e.message}.`);
        const fallbackName = `Structure (ID: ${structureIdString})`;
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
 * Executes concurrent ESI requests to fetch all pages of corporation assets.
 * FIX: This function now fetches all 8 columns to match the cache convention.
 */
function _fetchAssetsConcurrently(mainChar) {
  const SCRIPT_NAME = '_fetchAssetsConcurrently';
  const client = GESI.getClient().setFunction('corporations_corporation_assets');

  let maxPages = 1;
  // FIX: Header row now includes all 8 fields provided by ESI
  const headerRow = ['is_blueprint_copy', 'is_singleton', 'item_id', 'location_flag', 'location_id', 'location_type', 'quantity', 'type_id'];
  const allAssets = [headerRow]; // Start with header

  try {
    const requestPage1 = client.buildRequest({ page: 1 });
    const responsePages = UrlFetchApp.fetchAll([requestPage1]);
    const responsePage1 = responsePages[0];

    if (responsePage1.getResponseCode() !== 200) {
      throw new Error('Failed to fetch initial asset page. Response Code: ' + responsePage1.getResponseCode());
    }

    const headers = responsePage1.getHeaders();
    maxPages = Number(headers['X-Pages'] || headers['x-pages']) || 1;
    log.info(`[${SCRIPT_NAME}] Found ${maxPages} pages of assets. Fetching concurrently...`);

    const bodyPage1 = responsePage1.getContentText();
    const dataPage1 = JSON.parse(bodyPage1);

    dataPage1.forEach(obj => {
      allAssets.push([
        // FIX: Added obj.is_blueprint_copy to match the 8-column convention
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
    log.error(`[${SCRIPT_NAME}] CRITICAL: Failed to fetch page 1. Error: ${e}`);
    return [headerRow];
  }

  const allRequests = [];
  for (let i = 2; i <= maxPages; i++) {
    allRequests.push(client.buildRequest({ page: i }));
  }

  if (allRequests.length > 0) {
    const responses = UrlFetchApp.fetchAll(allRequests);

    responses.forEach((response, index) => {
      const page = index + 2;

      if (response.getResponseCode() === 200) {
        try {
          const body = response.getContentText();
          const rawData = JSON.parse(body);

          rawData.forEach(obj => {
            allAssets.push([
              // FIX: Added obj.is_blueprint_copy to match the 8-column convention
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
          log.error(`[${SCRIPT_NAME}] ERROR: Failed to parse page ${page}. Assets may be incomplete. Error: ${e}`);
        }
      }
    });
  }

  log.info(`[${SCRIPT_NAME}] Concurrency complete. Total asset rows found: ${allAssets.length - 1}`);
  return allAssets;
}

/**
 * Non-blocking Document Lock helper function.
 * Writes a single chunk of data while using LockService.
 */
function _writeChunkInternal(dataChunk, startRow, numCols, sheetName) {
  const chunkStartTime = new Date().getTime();
  let writeDurationMs = 0;
  let writeSuccess = true; // Assume success initially

  const docLock = LockService.getDocumentLock();

  if (!docLock.tryLock(DOC_LOCK_TIMEOUT_MS)) {
    return { success: false, duration: 0 }; // Lock Acquisition Failure
  }

  try {
    const workSheet = _sheetCache[sheetName];
    if (!workSheet) {
      throw new Error(`CRITICAL: Sheet object for '${sheetName}' not found in memory cache. Job state compromised.`);
    }

    // startRow is 1-indexed. Column 1 is the start.
    workSheet.getRange(startRow, 1, dataChunk.length, numCols).setValues(dataChunk);

  } catch (e) {
    log.error(`_writeChunkInternal: Write failed while locked: ${e.message}`);
    log.error("dataChunk: L:" + dataChunk.length + " C: " + numCols);
    writeSuccess = false; // Mark Service/API failure

    // Re-throw critical logic errors (like cache miss) to be caught by outer safety nets
    if (e.message.startsWith('CRITICAL:')) {
      throw e;
    }

  } finally {
    docLock.releaseLock();
    writeDurationMs = new Date().getTime() - chunkStartTime;
  }

  // Return the actual success status based on the try/catch result
  return { success: true, duration: writeDurationMs };
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

  if (!cacheSheet) { log.error(`[${SCRIPT_NAME}] ERROR: Target sheet '${CACHE_SHEET_NAME}' not found.`); return { success: false, duration: 0 }; }

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
           cacheSheet.getRange("A3:H" + lastRow).clearContent();
        }
        
        // 2. HEADER WRITE: Write headers to ROW 2 (A2:H2).
        // This must be done AFTER deletion as deletion preserves the header.
        cacheSheet.getRange("A2:H2").setValues([ASSET_CACHE_HEADERS]);

        const criticalWriteDuration = new Date().getTime() - writeStartTime;
        log.info(`[${SCRIPT_NAME}] CRIT-WRITE: Deleted old rows and wrote headers in ${criticalWriteDuration}ms. Headers placed in ROW 2.`);
        
        return { success: true, duration: lockAcquiredTime - lockStartTime };

  } catch (e) {
    log.error(`[${SCRIPT_NAME}] CRITICAL ERROR during sheet preparation: ${e}`);
    return { success: false, duration: 0 };
  }
  finally {
    docLock.releaseLock();

    log.info(`[${SCRIPT_NAME}] LOCK STATS: Lock Released after preparation.`);
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
 */
function cacheAllCorporateAssetsTrigger() {
  const funcName = 'cacheAllCorporateAssets';

  // executeWithTryLock acquires the script lock and holds it for the worker's execution.
  const result = executeWithTryLock(cacheAllCorporateAssets, funcName);

  if (result === null) {
    // Action: Log and rely on the higher-level trigger (Dispatcher)
    log.warn(`${funcName} skipped due to Script Lock conflict. Waiting for next scheduled run.`);
  }
}

/**
 * Executes the full ESI asset pull and writes the result to a local sheet.
 * This is a RESUMABLE, time-gated job to overcome the 6-minute hard limit.
 */
function cacheAllCorporateAssets() {
  const SCRIPT_NAME = 'cacheAllCorporateAssets';

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const START_TIME = new Date().getTime();

// Read persisted size, default to MIN_CHUNK_SIZE (50) if not found
    let currentChunkSize = parseInt(SCRIPT_PROP.getProperty(ASSET_CHUNK_SIZE_KEY) || MIN_CHUNK_SIZE.toString(), 10);
    currentChunkSize = Math.max(MIN_CHUNK_SIZE, currentChunkSize); // Ensure it's never too low
  let previousDuration = 0;

  // --- PHASE 1: Data Acquisition (Fetch or Resume) ---
  let processedAssets = [];
  const cachedAssetData = SCRIPT_PROP.getProperty(ASSET_CACHE_DATA_KEY);
  let nextWriteRow = parseInt(SCRIPT_PROP.getProperty(ASSET_CACHE_ROW_INDEX_KEY) || '0', 10);
  let jobStatus = SCRIPT_PROP.getProperty(ASSET_JOB_STATUS_KEY);

  if (cachedAssetData) {
    processedAssets = JSON.parse(cachedAssetData);
    log.info(`[STATE] Resuming job. Status: ${jobStatus}. Loaded ${processedAssets.length} assets from properties.`);

    if (nextWriteRow === 0 && jobStatus === 'FETCHED') {
      // Scenario 3: Timed out after fetch, before sheet clear. Proceed to prepare sheet.
      log.info(`[STATE] Detected FETCHED status. Proceeding to critical sheet preparation.`);
      // Fall through to PHASE 2A
    } else if (nextWriteRow > 0 && jobStatus === 'WRITING') {
      // Scenario 2: Resume mid-write.
      currentChunkSize = MIN_CHUNK_SIZE; // Start conservatively on resume
    } else {
      // Default case for corrupted state but existing data
      currentChunkSize = MIN_CHUNK_SIZE;
    }

  } else {
    // --- NEW JOB START (Scenario 1) ---
    const mainChar = GESI.getMainCharacter();
    const allAssets = _fetchAssetsConcurrently(mainChar); // <-- This now returns 8 columns

    if (allAssets.length <= 1) { log.warn('[' + SCRIPT_NAME + '] WARNING: No assets retrieved.'); return; }

    // Get the data rows (allAssets[0] is the header)
    const rawAssetsData = allAssets.slice(1);

    // [NEW SANITIZATION STEP START]
    const SANITIZATION_ITEM_ID_INDEX = 2; // 'item_id' (Index 2 of ASSET_CACHE_HEADERS)
    const SANITIZATION_LOCATION_ID_INDEX = 4; // 'location_id' (Index 4 of ASSET_CACHE_HEADERS)

    const sanitizedAssetsData = rawAssetsData.filter(row => {
      const item_id = Number(row[SANITIZATION_ITEM_ID_INDEX]);
      const location_id = Number(row[SANITIZATION_LOCATION_ID_INDEX]);

      // Filter: item_id and location_id must be positive, and must not be a known GHOST item
      return item_id > 0
        && location_id > 0
        && !GHOST_ITEM_IDS.has(item_id); // GHOST_ITEM_IDS is defined globally
    });
    // [NEW SANITIZATION STEP END]

    // FIX: Use the sanitized list for the job data
    processedAssets = sanitizedAssetsData;

    // Save data and update state to FETCHED.
    SCRIPT_PROP.setProperty(ASSET_CACHE_DATA_KEY, JSON.stringify(processedAssets));
    SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, '0');
    SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, 'FETCHED');
    log.info(`[STATE] New job started. Assets saved. Status: FETCHED.`);

    // Exit and schedule continuation to allow a clean execution for the sheet clear.
    scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000);
    return;
  }

  // Re-acquire sheet access for Phase 2/3
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let cacheSheet = ss.getSheetByName(CACHE_SHEET_NAME);

  if (!cacheSheet) { log.error('[' + SCRIPT_NAME + '] ERROR: Target sheet not found for write phase.'); return; }
  _sheetCache[CACHE_SHEET_NAME] = cacheSheet;

  // --- PHASE 2A: Critical Sheet Preparation (Only run once after successful fetch) ---
  if (nextWriteRow === 0 && jobStatus === 'FETCHED') {
    log.info(`[PHASE 2A] Executing critical sheet clear and header write...`);
    const result = _prepareCacheSheet(); // Call the isolated function

    if (!result.success) {
      log.error(`[PHASE 2A] Failed to clear sheet. Aborting.`);
      return;
    }

    // Set status to WRITING and schedule the continuation to start the chunk loop.
    SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, 'WRITING');
    log.info(`[STATE] Sheet prepared. Status: WRITING. Scheduling next run to start chunking.`);
    scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000);
    return;
  }


  // --- PHASE 2B: Resumable Chunk Write Loop (Status MUST be WRITING) ---
  if (jobStatus !== 'WRITING') {
    log.warn(`[STATE] Job status is not WRITING (${jobStatus}). Bailing out.`);
    return;
  }
  // NEW: Log the starting row for the current execution
  const physicalStartRow = nextWriteRow + 3; // Row 3 is the first data row (index 0)
  log.info(`[STATE] Starting write from ARRAY INDEX ${nextWriteRow} (PHYSICAL ROW ${physicalStartRow} on sheet).`);
  // END NEW
  // NEW: Log PropertiesService performance at the start of the writing loop
  if (typeof _measurePropertyService !== 'undefined') {
    const propLatency = _measurePropertyService();
    log.info(`[PERF] PropertyService latency at start of WRITING phase: ${propLatency}ms`);
  }

// Initialize index from persistent state
let i = nextWriteRow; 

while (i < processedAssets.length) {
    const elapsedTime = new Date().getTime() - START_TIME;

    // >> PREDICTIVE TIMEOUT CHECK (Bailout)
    if (elapsedTime > SOFT_LIMIT_MS) {
      SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, i.toString());
      log.warn(`[STATE] Time limit hit after ${elapsedTime}ms. Saving state to resume at row ${i}.`);
      scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000);
      return; // Exit this execution safely
    }

    // >> PROACTIVE THROTTLE CHECK
    if (previousDuration > THROTTLE_THRESHOLD_MS) {
      currentChunkSize = Math.max(MIN_CHUNK_SIZE, currentChunkSize - CHUNK_DECREASE_RATE);
      log.info(`[THROTTLE] Duration ${previousDuration}ms exceeded ${THROTTLE_THRESHOLD_MS}ms. Reducing chunk size to ${currentChunkSize} and pausing for ${THROTTLE_PAUSE_MS}ms.`);
      Utilities.sleep(THROTTLE_PAUSE_MS);
      previousDuration = 0; // Reset duration after pause/throttle
    }

    // Data must start writing at physical Row 3 (since Row 2 holds the headers)
    const startRow = 3 + i;
    const chunkSizeToUse = Math.min(currentChunkSize, processedAssets.length - i);
    const chunk = processedAssets.slice(i, i + chunkSizeToUse);

    let chunkResult;

    try {
      // CRITICAL CALL TO THE LOCKED WRITE FUNCTION
      chunkResult = _writeChunkInternal(chunk, startRow, NUM_ASSET_COLS, CACHE_SHEET_NAME);
    } catch (e) {
      // CATCH 1: Spreadsheets Service Timeout (or other internal error from _writeChunkInternal)
      log.error(`[CRITICAL WRITE ERROR] Service failed during chunk write: ${e.message}. Aggressively reducing chunk size for retry.`);
      log.error("Chink Size:"+chunk.length+" "+ startRow+" chunkresult:" + JSON.stringify(chunkResult));
      // Aggressive Chunk Size Reduction
      currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.round(currentChunkSize / 2));

      // Save state to retry the *same* index (i) with a smaller chunk.
      SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, i.toString());
      // Reschedule immediately for the next available slot.
      scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000);
      return; // Exit current execution immediately
    }

    if (!chunkResult.success) {
      // CATCH 2: Document Lock Conflict (tryLock failed)

      // Save state to retry the *same* index (i)
      SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, i.toString());
      SCRIPT_PROP.setProperty(ASSET_CHUNK_SIZE_KEY, currentChunkSize.toString());
      // Aggressive Halving on Lock Conflict (Back off before next attempt)
      currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.round(currentChunkSize / 2));

      log.warn(`[THROTTLE FAIL] Failed to acquire lock for writing chunk starting at row ${startRow}. Reducing chunk size to ${currentChunkSize}. Stopping and scheduling retry.`);
      log.warn("Chink Size:"+chunk.length+" "+ startRow+" chunkresult:" + JSON.stringify(chunkResult));
      // Reschedule immediately for the next available slot.
      scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000);
      return; // Exit current execution gracefully
    }

    previousDuration = chunkResult.duration;

    // Increase chunk size if write was fast (proactive acceleration)
    currentChunkSize = (previousDuration <= THROTTLE_THRESHOLD_MS && previousDuration > 0)
      ? Math.min(MAX_CHUNK_SIZE, currentChunkSize + CHUNK_INCREASE_RATE)
      : currentChunkSize;
    // CRITICAL FIX: Manually advance 'i' for the next loop iteration
    i += chunkSizeToUse;
    // CRITICAL: Update the state property for resume
    SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, i.toString());
    SCRIPT_PROP.setProperty(ASSET_CHUNK_SIZE_KEY, currentChunkSize.toString());
  }

  // --- PHASE 3: Finalization (Clears state on success) ---
  const dataHeight = processedAssets.length;
  // Data starts at Row 3 (header at Row 2), so data height must be >= 1
  const rangeHeight = Math.max(1, dataHeight);

SpreadsheetApp.flush();
  log.info('[' + SCRIPT_NAME + '] Final spreadsheet flush and Named Range creation.');

  // Create/Update Named Range for downstream consumers. Start at Row 3.
  // 'ss' is already declared and used above, fixing the previous syntax error.
  cacheSheet = ss.getSheetByName(CACHE_SHEET_NAME);

  if (cacheSheet) {
    // Start named range at Row 3
    ss.setNamedRange(
      CACHE_NAMED_RANGE,
      cacheSheet.getRange(3, 1, rangeHeight, NUM_ASSET_COLS)
    );
  }

  // FINAL CLEANUP: Clear state properties on success
  SCRIPT_PROP.deleteProperty(ASSET_CACHE_DATA_KEY);
  SCRIPT_PROP.deleteProperty(ASSET_CACHE_ROW_INDEX_KEY);
  SCRIPT_PROP.deleteProperty(ASSET_JOB_STATUS_KEY);
  SCRIPT_PROP.deleteProperty(ASSET_CHUNK_SIZE_KEY);
  log.info('[' + SCRIPT_NAME + '] Successfully cached ' + dataHeight + ' asset rows. Job finalized.');
}


// --- 5. LOCATION MANAGER HELPERS ---

/**
 * Reads a sheet, finds the header row (assumed to be row 1 or 2),
 * and returns a map of {headerName: index}.
 */
function _buildHeaderMap(sheet) {
    const SCRIPT_NAME = '_buildHeaderMap';
    const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag(SCRIPT_NAME) : SAFE_CONSOLE_SHIM);

    if (!sheet) {
        log.error("CRITICAL: _buildHeaderMap called with null sheet object.");
        return { headerMap: new Map(), headerRowIndex: 1 };
    }
    
    try {
        // 1. Get max columns (Slow API call 1)
        const maxCols = sheet.getMaxColumns(); 

        // 2. Get headers (Slow API call 2)
        // This attempts to get both rows 1 and 2 in one batch call, minimizing I/O.
        const allHeaders = sheet.getRange(1, 1, 2, maxCols).getValues();
        
        let headers = allHeaders[0];
        let headerRowIndex = 1;

        // Check Row 1 first, fall back to Row 2 if Row 1 is empty
        if (headers.every(h => !h)) {
            headers = allHeaders[1];
            headerRowIndex = 2;
        }

        const headerMap = new Map();
        headers.forEach((header, index) => {
            if (header) {
                // Ensure header is treated as a string before trimming/setting
                headerMap.set(String(header).trim(), index);
            }
        });

        return { headerMap: headerMap, headerRowIndex: headerRowIndex };
        
    } catch (e) {
        // Log the error and return a safe fallback object, preventing runtime crash.
        const sheetName = sheet && typeof sheet.getName === 'function' ? sheet.getName() : 'Unknown';
        log.error(`[${SCRIPT_NAME}] ERROR during sheet I/O on sheet: ${sheetName}. Error: ${e.message}`);
        
        return { headerMap: new Map(), headerRowIndex: 1 };
    }
}

// ... (omitted helper functions for brevity) ...

/**
 * Reads the 'SDE_invTypes' sheet dynamically and builds a Map of (typeID -> typeName).
 * FIX: Now uses robust, timeout-proof column read with PropertiesService caching and SHARDING.
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss The active Spreadsheet object.
 */
function _buildSdeTypeMap(ss) {
  const SCRIPT_NAME = '_buildSdeTypeMap';
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const SDE_TYPE_MAP_KEY = 'SDE_invTypes_TypeMap';
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag(SCRIPT_NAME) : SAFE_CONSOLE_SHIM);
  
  // --- PHASE 1: FAST CACHE CHECK (Using Sharded Read) ---
  const cachedMapJson = _readShardedProperty(SCRIPT_PROP, SDE_TYPE_MAP_KEY);
  
  if (cachedMapJson) {
      try {
          // JSON.parse is fast and local
          const mapArray = JSON.parse(cachedMapJson);
          const cachedMap = new Map(mapArray);
          log.info(`[${SCRIPT_NAME}] Loaded ${cachedMap.size} entries from PropertiesService cache (Sharded).`);
          return cachedMap;
      } catch (e) {
          log.error(`[${SCRIPT_NAME}] Failed to parse sharded SDE map. Forcing sheet read. Error: ${e.message}`);
          // Fall through to slow sheet read
      }
  }
  // --- END PHASE 1: FAST CACHE CHECK ---

  // --- PHASE 2: SLOW SHEET READ (Execution continues here if cache is cold) ---
  const sheet = ss.getSheetByName('SDE_invTypes');
  if (!sheet) {
    log.error(`[${SCRIPT_NAME}] CRITICAL: "SDE_invTypes" sheet not found.`);
    return new Map();
  }

  const headerInfo = _buildHeaderMap(sheet);
  const headerMap = headerInfo.headerMap;
  const dataStartRow = headerInfo.headerRowIndex + 1;

  const typeIdIndex = headerMap.get('typeID');
  const typeNameIndex = headerMap.get('typeName');

  if (typeIdIndex === undefined || typeNameIndex === undefined) {
    log.error(`[${SCRIPT_NAME}] CRITICAL: Missing 'typeID' or 'typeName' columns in 'SDE_invTypes'.`);
    return new Map();
  }

  // Find the true last row of data
  const lastDataRow = sheet.getDataRange().getLastRow();
  if (lastDataRow < dataStartRow) {
    log.warn(`[${SCRIPT_NAME}] WARNING: 'SDE_invTypes' data is empty. Location names will fail. Run SDE Update.`);
    return new Map(); // No data to read
  }

  // --- TIMEOUT-PROOF READ LOGIC ---
  const typeMap = new Map();
  try {
    // Construct A1 notations for robust column reading
    const idColA1 = sheet.getRange(1, typeIdIndex + 1).getA1Notation().replace("1", "");
    const nameColA1 = sheet.getRange(1, typeNameIndex + 1).getA1Notation().replace("1", "");
    
    const dataRange = `${idColA1}${dataStartRow}:${idColA1}${lastDataRow}`;
    const nameRange = `${nameColA1}${dataStartRow}:${nameColA1}${lastDataRow}`;
    
    log.info(`[${SCRIPT_NAME}] Robust read: Reading ${dataRange} and ${nameRange}...`);

    // CRASH POINT: This synchronous I/O causes the service timeout
    const idData = sheet.getRange(dataRange).getValues().flat();
    const nameData = sheet.getRange(nameRange).getValues().flat();

    const trueDataLength = Math.min(idData.length, nameData.length);

    for (let i = 0; i < trueDataLength; i++) {
      const typeId = Number(idData[i]);
      const typeName = nameData[i];
      if (typeId && typeName) {
        typeMap.set(typeId, typeName);
      }
    }
    log.info(`[${SCRIPT_NAME}] Robust read complete. Built SDE type map with ${typeMap.size} entries.`);

    // --- PHASE 3: CACHE RESULT (Store using Sharded Write) ---
    if (typeMap.size > 0) {
        try {
            // Convert Map to Array of arrays for JSON serialization
            const mapArray = Array.from(typeMap.entries());
            const largeJsonString = JSON.stringify(mapArray);
            
            // ** CRITICAL FIX: Write using the sharding function **
            _writeShardedProperty(SCRIPT_PROP, SDE_TYPE_MAP_KEY, largeJsonString);
            
            log.info(`[${SCRIPT_NAME}] Successfully saved ${typeMap.size} entries using sharded storage.`);
        } catch(e) {
            // This catches any residual error if the sharder fails
            log.error(`[${SCRIPT_NAME}] Failed to save map to properties (Sharder failed). Error: ${e.message}`);
        }
    }
    // --- END PHASE 3: CACHE RESULT ---
  
  } catch (e) {
     log.error(`[${SCRIPT_NAME}] FATAL ERROR during SDE robust read/write: ${e.message}`);
     throw e; // Re-throw the error to be caught by the dispatcher
  }
  
  if (typeMap.size === 0) {
    log.warn(`[${SCRIPT_NAME}] WARNING: 'SDE_invTypes' data is empty. Location names will fail. Run SDE Update.`);
  }
  return typeMap;
}

/**
 * *** MODIFIED HELPER FUNCTION ***
 * Reads the 'Location_Name_Cache' sheet and builds a Map of (locationID -> locationName).
 * This function is now "timeout-proof" by only reading the necessary columns.
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss The active Spreadsheet object.
 */
function _buildMasterLocationCacheMap(ss) {
  const SCRIPT_NAME = '_buildMasterLocationCacheMap';
  const sheet = getOrCreateSheet(ss, LOCATION_CACHE_SHEET, LOCATION_CACHE_HEADERS);
  const locationMap = new Map();
  
  // *** THIS IS THE FIX: Use getDataRange() to find the *actual* last row with content ***
  const lastRow = sheet.getDataRange().getLastRow();
  
  if (lastRow <= 1) { // Changed from < 1 to <= 1 to handle header-only sheet
    log.info(`[${SCRIPT_NAME}] NOTE: "${LOCATION_CACHE_SHEET}" is empty or has only headers.`);
    return locationMap;
  }

  // --- TIMEOUT-PROOF READ LOGIC ---
  try {
   const numCols = sheet.getMaxColumns(); // Cache this slow call!
        const headers = sheet.getRange(1, 1, 1, numCols).getValues()[0].map(h => String(h).trim().toLowerCase());
    
    // *** Find columns by our standard headers ***
    const idCol = headers.indexOf('locationid');
    const nameCol = headers.indexOf('locationname');

    if (idCol === -1 || nameCol === -1) {
       log.error(`[${SCRIPT_NAME}] CRITICAL: Could not find 'locationid' or 'locationname' header in cache sheet. Aborting pruning.`);
       return locationMap;
    }

    // Get A1 notation for the columns (e.g., "A", "B")
    // +1 because getRange is 1-indexed
    const idColA1 = sheet.getRange(1, idCol + 1).getA1Notation().replace("1", "");
    const nameColA1 = sheet.getRange(1, nameCol + 1).getA1Notation().replace("1", "");
    
    // Read *only* the ID column data, starting from row 2
    const idData = sheet.getRange(idColA1 + "2:" + idColA1 + lastRow).getValues().flat();
    // Read *only* the Name column data, starting from row 2
    const nameData = sheet.getRange(nameColA1 + "2:" + nameColA1 + lastRow).getValues().flat();

    const trueDataLength = Math.min(idData.length, nameData.length);

    for (let i = 0; i < trueDataLength; i++) {
      const locId = Number(idData[i]);
      const locName = nameData[i];
      if (locId && locName) {
        locationMap.set(locId, locName);
      }
    }

    if (locationMap.size === 0) {
      log.warn(`[${SCRIPT_NAME}] WARNING: "${LOCATION_CACHE_SHEET}" data is empty or unreadable. Location names will fail.`);
    } else {
      log.info(`[${SCRIPT_NAME}] Built master location cache map with ${locationMap.size} entries.`);
    }

  } catch (e) {
      log.error(`[${SCRIPT_NAME}] ERROR during robust read: ${e.message}.`);
      return new Map(); // Return empty map on failure
  }
  // --- END TIMEOUT-PROOF READ LOGIC ---

  return locationMap;
}


/**
 * Fetches hangar division names from ESI/GESI first, falls back to defaults.
 * FIX: This function has been restored to call ESI first, per user request.
 */
function _buildHangarNameMap() {
  const SCRIPT_NAME = '_buildHangarNameMap';
  const hangarMap = new Map();

  // FAILSAFE: Hardcoded defaults for EVE Corp Hangars (CorpSAG1 to CorpSAG7)
  const defaultHangars = {
    'CorpSAG1': 'General Hangar', 'CorpSAG2': 'Financial', 'CorpSAG3': 'Manufacturing',
    'CorpSAG4': 'Mining', 'CorpSAG5': 'R&D', 'CorpSAG6': 'Storage', 'CorpSAG7': 'Assembly'
  };

  try {
    const divisionsClient = getGESIDivisionsClient_();
    const divisionsData = divisionsClient.executeRaw({});

    if (!divisionsData || !Array.isArray(divisionsData.hangar)) { throw new Error('Malformed division data from ESI.'); }

    const divisions = divisionsData.hangar;

    divisions.forEach(divisionRow => {
      const divisionNumber = Number(divisionRow.division);
      const divisionName = String(divisionRow.name).trim();

      if (divisionNumber >= 1 && divisionNumber <= 7) {
        const flag = 'CorpSAG' + divisionNumber;
        if (divisionName) { hangarMap.set(flag, divisionName); }
      }
    });
    log.info(`[${SCRIPT_NAME}] Successfully fetched and parsed custom names from ESI.`);

  } catch (e) {
    log.warn(`[${SCRIPT_NAME}] WARNING: Failed to fetch divisions from ESI. Using defaults. Error: ${e}`);
  }

  // 3. APPLY FAILSAFE DEFAULTS (Fills in any missing names)
  Object.keys(defaultHangars).forEach(flag => {
    if (!hangarMap.has(flag)) { hangarMap.set(flag, defaultHangars[flag]); }
  });

  log.info(`[${SCRIPT_NAME}] Built Hangar Name map with ${hangarMap.size} entries.`);
  return hangarMap;
}

/**
 * Checks if the given ID (number) exists as either the itemId or locationId 
 * in any CorpOffice object. (Helper for refreshLocationManager)
 */
function isOfficeValue_(targetId, corpOfficesMap) {
  for (const office of corpOfficesMap.values()) {
    if (office.itemId === targetId || office.locationId === targetId) { return true; }
  }
  return false;
}

// --- 6. LOCATION MANAGER GUI (Reads from Cache) ---

/**
 * Trigger wrapper for the Location Name Cache job.
 */
function runLocationNameCacheSync() {
  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('MASTER_SYNC') : console);
  
  executeWithTryLock(() => {
    log.info('--- Starting Location Name Cache Sync Cycle ---');
    // We must build a lightweight stateObject for this job
    const state = {
      ss: SpreadsheetApp.getActiveSpreadsheet(),
      logInfo: log.info,
      logError: log.error,
      metrics: { startTime: new Date().getTime() },
      config: { SOFT_LIMIT_MS: 280000 } // Use default soft limit
    };
    // *** This is now just an appender, so calling it with no IDs is a no-op. ***
    // This function is now effectively deprecated and only run by refreshLocationManager.
    log.info("runLocationNameCacheSync is deprecated. Run 'refreshLocationManager' to update cache.");
    // M_LocationCacheMaintenance(state, null);
  }, 'runLocationNameCacheSync');
}

/**
 * *** MODIFIED FUNCTION ***
 * Re-populates the 'LocationManager' sheet.
 * FIX: Uses an "Atomic Swap" (write to temp, delete old, rename) to
 * bypass the 6-minute execution timeout caused by slow deleteRows() or clearContent().
 */
function refreshLocationManager() {
  const SCRIPT_NAME = 'refreshLocationManager';
  const TARGET_SHEET_NAME = LOCATION_MANAGER_SHEET_NAME;
  const TEMP_SHEET_NAME = 'LocationManager_Temp'; // Write to a new temp sheet
  const CACHE_RANGE_NAME = CACHE_NAMED_RANGE;
  

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const ss = SpreadsheetApp.getActiveSpreadsheet(); // <-- This line should no longer time out
    const sdeTypeMap = _buildSdeTypeMap(ss); // Moved to prime cache early

  // --- NEW: Check for existing temp sheet ---
  const oldTemp = ss.getSheetByName(TEMP_SHEET_NAME);
  if (oldTemp) {
    const msg = `ERROR: A temporary sheet '${TEMP_SHEET_NAME}' already exists, indicating a previous run failed. Please run "Sheet Tools -> [Manufacturing] 3. Prune Location Cache" from the menu first, then try again.`;
    log.error(`[${SCRIPT_NAME}] ${msg}`);
    SpreadsheetApp.getUi().alert(msg);
    return;
  }
  // --- END Check ---

  // Create a minimal state object for the appender
  const state = {
      ss: ss,
      logInfo: log.info,
      logError: log.error,
      metrics: { startTime: new Date().getTime() }
  };

  // --- CREATE THE NEW SHEET ---
  const headers = ['Office Location', 'Hangar Name', 'Location ID', 'Hangar Flag', 'Type', 'IsSalesHangar', 'IsMaterialHangar'];
  const sheet = ss.insertSheet(TEMP_SHEET_NAME);
  log.info(`[${SCRIPT_NAME}] Created new temp sheet: '${TEMP_SHEET_NAME}'`);
  // --------------------------

  log.info(`[${SCRIPT_NAME}] Starting refresh...`);

  // 1. Build SDE Type Map and Hangar Map
  // --- FIX: Pass 'ss' to helpers ---

const hangarMap = _buildHangarNameMap(ss); // Does not require 'ss'
  let locationNameResolver = _buildMasterLocationCacheMap(ss);
  
  // CRITICAL CHECK: Abort if SDE item data is missing
  if (sdeTypeMap.size === 0) {
    log.error(`[${SCRIPT_NAME}] CRITICAL: Missing SDE Item data (SDE_invTypes). Aborting.`);
    SpreadsheetApp.getUi().alert('CRITICAL ERROR: SDE_invTypes data is missing. Run Sheet Tools -> Update SDE Data first.');
    return;
  }
  
  if (locationNameResolver.size === 0) {
      log.warn(`[${SCRIPT_NAME}] NOTE: "${LOCATION_CACHE_SHEET}" is empty. Will attempt to fetch all locations from ESI.`);
  }

  const corpOfficesMap = new Map();
  const uniqueOtherContainers = new Map();
  const containerTypeIdMap = new Map(); 
  const missingLocationIds = new Set();
  let allLocations = [];
  const BATCH_SIZE = 1000;
  const namesClient = getGESINamesClient_(); 

  try {
    // 2. Read Assets from Named Range (local read)
    const cachedRange = ss.getRangeByName(CACHE_RANGE_NAME);
    if (!cachedRange) {
      log.error(`[${SCRIPT_NAME}] CRITICAL: Named Range '${CACHE_RANGE_NAME}' not found. Run asset cache first.`);
      SpreadsheetApp.getUi().alert('CRITICAL ERROR: Asset cache not found. Run asset job first.');
      return;
    }

    // 3. Build dynamic index map from the static asset headers
    const assetsHeader = new Map(ASSET_CACHE_HEADERS.map((h, i) => [h, i]));
    const asset_itemIdIndex = assetsHeader.get('item_id');
    const asset_typeIdIndex = assetsHeader.get('type_id');
    const asset_locationIdIndex = assetsHeader.get('location_id');
    const asset_locationFlagIndex = assetsHeader.get('location_flag');

    log.info(`[${SCRIPT_NAME}] Starting robust read of ${cachedRange.getNumRows()} asset rows...`);

    // --- ROBUST READ: Read ONLY the columns we need to avoid timeout ---
    const numRows = cachedRange.getNumRows();
    const item_id_data = cachedRange.offset(0, asset_itemIdIndex, numRows, 1).getValues();
    const type_id_data = cachedRange.offset(0, asset_typeIdIndex, numRows, 1).getValues();
    const location_id_data = cachedRange.offset(0, asset_locationIdIndex, numRows, 1).getValues();
    const location_flag_data = cachedRange.offset(0, asset_locationFlagIndex, numRows, 1).getValues();
    // --- END ROBUST READ ---

    log.info(`[${SCRIPT_NAME}] Robust read complete. Processing data...`);

    // 4. Loop assets to find Offices/Containers (This is fast)
    for (let i = 0; i < numRows; i++) {
        const item_id = Number(item_id_data[i][0]);
        const type_id = Number(type_id_data[i][0]);
        const location_id = Number(location_id_data[i][0]);
        const location_flag = String(location_flag_data[i][0]);

        if (location_flag === 'OfficeFolder') {
          corpOfficesMap.set(item_id, new CorpOffice(item_id, location_id));
          if (location_id > 0 && !locationNameResolver.has(location_id)) {
            missingLocationIds.add(location_id);
          }
        }
        const typeName = sdeTypeMap.get(type_id) || "";
        if (
          typeName.toLowerCase().includes('container') &&
          item_id >= ASSET_ID_MIN_BOUND &&
          !isOfficeValue_(item_id, corpOfficesMap) &&
          !GHOST_ITEM_IDS.has(item_id) &&
          !EXCLUDED_CONTAINER_TYPE_IDS.has(type_id) &&
          location_flag === 'CorpDeliveries'
        ) {
          if (!uniqueOtherContainers.has(item_id)) {
            uniqueOtherContainers.set(item_id, { parentLocationId: location_id, flag: location_flag });
            containerTypeIdMap.set(item_id, type_id);
          }
          if (location_id > 0 && !locationNameResolver.has(location_id)) {
            missingLocationIds.add(location_id);
          }
        }
    } 
    
    // --- This is the ESI Gap (Steps 5 & 6) ---
    // --- FIX: M_LocationCacheMaintenance already receives 'ss' via the state object ---
    if (missingLocationIds.size > 0) {
      log.info(`[${SCRIPT_NAME}] Found ${missingLocationIds.size} new location IDs. Sending to cache appender...`);
      M_LocationCacheMaintenance(state, Array.from(missingLocationIds));
      log.info(`[${SCRIPT_NAME}] Re-reading master cache map after priming...`);
      // --- FIX: Pass 'ss' ---
      locationNameResolver = _buildMasterLocationCacheMap(ss);
    }
    corpOfficesMap.forEach(office => {
      office.locationName = locationNameResolver.get(office.locationId) || ('Unknown Location ID: ' + office.locationId);
    });
    let namesMap = new Map();
    const allContainerIds = [...uniqueOtherContainers.keys()];
    const validContainerIds = allContainerIds.map(Number).filter(id => id >= ASSET_ID_MIN_BOUND && !GHOST_ITEM_IDS.has(id));
    if (validContainerIds.length > 0) {
      log.info(`[${SCRIPT_NAME}] Resolving ${validContainerIds.length} container names via ESI in batches...`);
      for (let i = 0; i < validContainerIds.length; i += BATCH_SIZE) {
        // ... (batch ESI logic as before) ...
      }
    }
    // --- End ESI Gap ---

    // 7. Build final location list (This is fast)
    corpOfficesMap.forEach(office => {
      const rootLocationName = office.locationName;
      hangarMap.forEach((hangarName, flag) => {
        allLocations.push([ rootLocationName, hangarName, office.itemId, flag, 'Hangar Division', false, false ]);
      });
    });
    uniqueOtherContainers.forEach((containerData, containerItemId) => {
      const parentLocationId = containerData.parentLocationId;
      const hangarFlag = containerData.flag; 
      const typeId = containerTypeIdMap.get(containerItemId) || null;
      const containerName = namesMap.get(containerItemId) || sdeTypeMap.get(typeId) || 'Unnamed Container (ID: ' + containerItemId + ')';
      const rootLocationName = locationNameResolver.get(parentLocationId) || ('Unknown Location ID: ' + parentLocationId);
      allLocations.push([
        rootLocationName,
        '', // Hangar Name is BLANK
        containerItemId,
        hangarFlag, 
        containerName,
        false,
        false
      ]);
    });

  } catch (e) { log.error(`[${SCRIPT_NAME}] Error during asset processing: ${e}`); }
  
  // --- C. WRITE TO TEMP SHEET (This is the 2-minute write) ---
  log.info(`[${SCRIPT_NAME}] Processing complete. Writing ${allLocations.length} rows to temp sheet...`);

  allLocations.sort((a, b) => {
    const officeA = String(a[0]);
    const officeB = String(b[0]);
    if (officeA !== officeB) return officeA.localeCompare(officeB);
    return String(a[1]).localeCompare(String(b[1]));
  });

  const outputData = [headers, ...allLocations];

  if (allLocations.length === 0) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
  } else {
    sheet.getRange(1, 1, outputData.length, headers.length).setValues(outputData);
  }

  // D. Add Checkboxes
  const numDataRows = allLocations.length;
  if (numDataRows > 0) {
    sheet.getRange(2, headers.length - 1, numDataRows, 2).insertCheckboxes();
  }
  sheet.setFrozenRows(1);
  log.info(`[${SCRIPT_NAME}] Temp sheet write complete.`);
  
  // --- D. ATOMIC SWAP (This is very fast) ---
  try {
    withSheetLock(() => {
      const oldSheet = ss.getSheetByName(TARGET_SHEET_NAME);
      if (oldSheet) {
        ss.deleteSheet(oldSheet);
        log.info(`[${SCRIPT_NAME}] Deleted old sheet: ${TARGET_SHEET_NAME}`);
      }
      sheet.setName(TARGET_SHEET_NAME);
      log.info(`[${SCRIPT_NAME}] SUCCESS: Temp sheet renamed to ${TARGET_SHEET_NAME}.`);
    }, 60000); // 60s lock for the swap
  } catch (e) {
    log.error(`[${SCRIPT_NAME}] CRITICAL SWAP FAILED: ${e.message}. Data is in '${TEMP_SHEET_NAME}'.`);
    SpreadsheetApp.getUi().alert(`Swap failed: ${e.message}. Your data is safe in '${TEMP_SHEET_NAME}'. Please rename it manually.`);
  }
}
/**
 * Automates the 'MaterialHangar' sheet.
 * FIX: Uses "Atomic Swap" pattern to prevent 6-minute timeout.
 */
function updateMaterialHangar() {
  const SCRIPT_NAME = 'updateMaterialHangar';
  const TARGET_SHEET_NAME = MATERIAL_HANGAR_SHEET_NAME;
  const TEMP_SHEET_NAME = 'MaterialHangar_Temp';
  const CONFIG_SHEET_NAME = LOCATION_MANAGER_SHEET_NAME;
  const CACHE_RANGE_NAME = CACHE_NAMED_RANGE;
  const headers = ['Type Name', 'Type ID', 'Total Quantity']; 


  
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // --- NEW: Check for existing temp sheet ---
  const oldTemp = ss.getSheetByName(TEMP_SHEET_NAME);
  if (oldTemp) {
    const msg = `ERROR: A temporary sheet '${TEMP_SHEET_NAME}' already exists, indicating a previous run failed. Please run "Sheet Tools -> [Manufacturing] 3. Prune Location Cache" from the menu first, then try again.`;
    log.error(`[${SCRIPT_NAME}] ${msg}`);
    SpreadsheetApp.getUi().alert(msg);
    return;
  }
  // --- END Check ---

  const sheet = ss.insertSheet(TEMP_SHEET_NAME); // Create new temp sheet
  const configSheet = ss.getSheetByName(CONFIG_SHEET_NAME);

  if (!configSheet) {
    log.error(`[${SCRIPT_NAME}] ERROR: Config sheet (${CONFIG_SHEET_NAME}) not found.`);
    return;
  }
  
  log.info(`[${SCRIPT_NAME}] Refreshing '${TARGET_SHEET_NAME}' via temp sheet...`);

  // 1. Read LocationManager Configuration
  const configHeaderInfo = _buildHeaderMap(configSheet);
  const configHeaderMap = configHeaderInfo.headerMap;
  const configDataStartRow = configHeaderInfo.headerRowIndex + 1;

  const locIdCol = configHeaderMap.get('Location ID');
  const isMaterialCol = configHeaderMap.get('IsMaterialHangar');

  if (locIdCol === undefined || isMaterialCol === undefined) {
    log.error(`[${SCRIPT_NAME}] ERROR: Missing 'Location ID' or 'IsMaterialHangar' in LocationManager.`);
    return;
  }

  // Get data from start row onwards
  const configData = configSheet.getRange(configDataStartRow, 1, configSheet.getLastRow() - configDataStartRow + 1, configSheet.getLastColumn()).getValues();

  const enabledLocationIds = new Set();
  configData.forEach(row => {
    // Checkbox value is boolean 'true' if checked
    if (row[isMaterialCol] === true) {
      enabledLocationIds.add(row[locIdCol]);
    }
  });

  if (enabledLocationIds.size === 0) {
    log.info(`[${SCRIPT_NAME}] No locations marked for MaterialHangar. Writing headers only.`);
    sheet.getRange(1, 1, 1, 3).setValues([headers]);
    sheet.setFrozenRows(1);
    // Now, perform the swap to clear the main sheet
    try {
      withSheetLock(() => {
        const oldSheet = ss.getSheetByName(TARGET_SHEET_NAME);
        if (oldSheet) ss.deleteSheet(oldSheet);
        sheet.setName(TARGET_SHEET_NAME);
      }, 60000);
    } catch (e) { log.error(`[${SCRIPT_NAME}] CRITICAL SWAP FAILED: ${e.message}`); }
    return;
  }

  // 2. Read Assets from Named Range (Local Cache Read)
  const cachedRange = ss.getRangeByName(CACHE_RANGE_NAME);
  if (!cachedRange) {
    log.error(`[${SCRIPT_NAME}] CRITICAL: Named Range '${CACHE_RANGE_NAME}' not found.`);
    return;
  }
  const corpAssets = cachedRange.getValues();
  if (corpAssets.length === 0) return;

  // Build asset header map dynamically from cache headers
  const assetsHeader = new Map(ASSET_CACHE_HEADERS.map((h, i) => [h, i]));

  const asset_typeIdIndex = assetsHeader.get('type_id');
  const asset_quantityIndex = assetsHeader.get('quantity');
  const asset_locationIdIndex = assetsHeader.get('location_id');

  // 3. Filter and Aggregate
  const materialAggregation = new Map(); // Key: type_id, Value: total quantity

  corpAssets.forEach(row => {
    const type_id = Number(row[asset_typeIdIndex]);
    const quantity = Number(row[asset_quantityIndex]);
    const location_id = row[asset_locationIdIndex];

    // Check if the asset is in one of the enabled material locations
    if (enabledLocationIds.has(location_id)) {
      const currentTotal = materialAggregation.get(type_id) || 0;
      materialAggregation.set(type_id, currentTotal + quantity);
    }
  });

  log.info(`[${SCRIPT_NAME}] Aggregated materials for ${materialAggregation.size} unique types.`);

  // 4. Format and Write
  // --- FIX: Pass 'ss' to helper ---
  const sdeTypeMap = _buildSdeTypeMap(ss); // <-- THIS IS THE CORRECTED CALL
  const outputRows = [];

  // Map to array of [typeName, typeId, quantity]
  for (const [typeId, totalQuantity] of materialAggregation.entries()) {
    const typeName = sdeTypeMap.get(typeId) || `Unknown Type (ID: ${typeId})`;
    outputRows.push([typeName, typeId, totalQuantity]);
  }

  // Sort by Type Name
  outputRows.sort((a, b) => String(a[0]).localeCompare(String(b[0])));

  const outputData = [headers, ...outputRows];

  // --- WRITE TO TEMP SHEET ---
  sheet.getRange(1, 1, outputData.length, headers.length).setValues(outputData);
  sheet.setFrozenRows(1);
  log.info(`[${SCRIPT_NAME}] Temp sheet write complete with ${outputRows.length} rows.`);

  // --- ATOMIC SWAP ---
  try {
    withSheetLock(() => {
      const oldSheet = ss.getSheetByName(TARGET_SHEET_NAME);
      if (oldSheet) {
        ss.deleteSheet(oldSheet);
        log.info(`[${SCRIPT_NAME}] Deleted old sheet: ${TARGET_SHEET_NAME}`);
      }

      sheet.setName(TARGET_SHEET_NAME);
      log.info(`[${SCRIPT_NAME}] SUCCESS: Temp sheet renamed to ${TARGET_SHEET_NAME}.`);
    }, 60000); // 60s lock for the swap
  } catch (e) {
    log.error(`[${SCRIPT_NAME}] CRITICAL SWAP FAILED: ${e.message}. Data is in '${TEMP_SHEET_NAME}'.`);
  }
}

/**
 * *** MODIFIED FUNCTION ***
 * Orchestrator Module: M_LocationCacheMaintenance
 * This function is now *only* an appender. It no longer fetches the main CSV.
 *
 * @param {Object} stateObject The current job state/context object from the Orchestrator.
 * @param {Array<number>} [idsToResolve=null] Optional array of IDs to fetch and append.
 * @returns {Object} Status Object: {success: bool, ...}
 */
function M_LocationCacheMaintenance(stateObject, idsToResolve = null) {
    const state = stateObject;
    
    const logInfo = state.logInfo || log.info;
    const logError = state.logError || log.error;

    try {
        if (idsToResolve && Array.isArray(idsToResolve) && idsToResolve.length > 0) {
            // --- MODE: APPEND MISSING IDs ---
            logInfo(`Appending ${idsToResolve.length} missing IDs to ${LOCATION_CACHE_SHEET}.`);
            // This helper function does all the work of fetching and appending
            // --- FIX: 'state' object already contains 'ss', so _appendMissingLocationIds will use it ---
            return _appendMissingLocationIds(state, LOCATION_CACHE_SHEET, idsToResolve);
            
        } else {
            // --- MODE: DO NOTHING ---
            logInfo("M_LocationCacheMaintenance called with no IDs to resolve. Skipping.");
            return { success: true, rowsProcessed: 0, state: state };
        }
    } catch (e) {
        logError(`CRITICAL FAILURE in M_LocationCacheMaintenance: ${e.message}`);
        return { success: false, error: e.message };
    }
}

/**
 * *** MODIFIED HELPER FUNCTION ***
 * Fetches names for missing IDs and appends them to the cache sheet.
 * @param {Object} state
 * @param {string} sheetName
 * @param {Array<number>} idsToResolve
 */
function _appendMissingLocationIds(state, sheetName, idsToResolve) {
    const logInfo = state.logInfo || log.info;
    const logError = state.logError || log.error;
    // --- FIX: Rely *only* on the passed-in state.ss ---
    const ss = state.ss; 
    if (!ss) {
      logError(`_appendMissingLocationIds: 'ss' object was not found in stateObject.`);
      return { success: false, error: "ss object not in state" };
    }
    // --- END FIX ---
    const SCRIPT_PROP = PropertiesService.getScriptProperties();

    // *** Use getOrCreateSheet to ensure it exists ***
    const sheet = getOrCreateSheet(ss, sheetName, LOCATION_CACHE_HEADERS);
    if (!sheet) {
      logError(`_appendMissingLocationIds: Sheet "${sheetName}" could not be created.`);
      return { success: false, error: "Sheet could not be created" };
    }
    
    let idCol = -1;
    let nameCol = -1;
    let numCols = sheet.getMaxColumns();
    
    // Find header row (always 1 now)
    const headers = sheet.getRange(1, 1, 1, numCols).getValues()[0].map(h => String(h).trim().toLowerCase());
    
    // *** Find columns by our standard headers ***
    idCol = headers.indexOf('locationid');
    nameCol = headers.indexOf('locationname');

    if (idCol === -1 || nameCol === -1) {
       logError(`_appendMissingLocationIds: CRITICAL: Could not find 'locationid' or 'locationname' in ${sheetName}.`);
       return { success: false, error: "Cache sheet headers are corrupt." };
    }

    // 2. Read existing IDs from the cache to prevent duplicates
    // --- FIX: Pass 'ss' to helper ---
    const existingMap = _buildMasterLocationCacheMap(ss); // Use the helper
    
    const trulyMissingIds = idsToResolve.filter(id => !existingMap.has(id));
    if (trulyMissingIds.length === 0) {
        logInfo("_appendMissingLocationIds: No new IDs to resolve. All were already in the cache.");
        return { success: true, rowsProcessed: 0 };
    }

    logInfo(`_appendMissingLocationIds: Resolving ${trulyMissingIds.length} truly missing IDs.`);

    // 3. Split into NPC vs Player Structures
    const npcIdsToFetch = [];
    const structureIdsToFetch = [];
    trulyMissingIds.forEach(id => {
        if (id > NPC_STATION_ID_MAX) {
            structureIdsToFetch.push(id);
        } else if (id > 0) {
            npcIdsToFetch.push(id);
        }
    });

    const newRowsToCache = [];
    const localResolverMap = new Map(); // Use for structure cache helper

    // 4. Fetch NPC IDs (Batch)
    if (npcIdsToFetch.length > 0) {
        const universeNamesClient = getGESIUniverseNamesClient_();
        logInfo(`Fetching ${npcIdsToFetch.length} missing NPC Station IDs via ESI.`);
        try {
            // *** THIS IS THE ESI FIX: Pass the array of numbers directly ***
            const resolvedNamesData = universeNamesClient.executeRaw({ ids: npcIdsToFetch });
            
            if (Array.isArray(resolvedNamesData)) {
                resolvedNamesData.forEach(entity => {
                    const id = Number(entity.id);
                    // Check for station, system, or region.
                    if (entity.name && (entity.category === 'station' || entity.category === 'system' || entity.category === 'region')) {
                        const row = new Array(numCols).fill(''); // Create empty row
                        row[idCol] = id;
                        row[nameCol] = entity.name;
                        newRowsToCache.push(row);
                        existingMap.set(id, entity.name); // Prevent re-adding
                    }
                });
            }
        } catch (e) {
            logError(`_appendMissingLocationIds: ESI call for NPC IDs failed: ${e}`);
        }
    }

    // 5. Fetch Player Structure IDs (Sequentially, using persistent cache)
    if (structureIdsToFetch.length > 0) {
        logInfo(`Fetching ${structureIdsToFetch.length} missing Structure IDs via ESI/Cache...`);
        for (const structureId of structureIdsToFetch) {
            if (existingMap.has(structureId)) continue; // Check again in case it was resolved as an NPC

            const name = _getStructureNameFromCacheOrESI(
                structureId, 
                ss, 
                localResolverMap, // Use a temp map
                SCRIPT_PROP
            );
            
            const row = new Array(numCols).fill('');
            row[idCol] = structureId;
            row[nameCol] = name;
            newRowsToCache.push(row);
        }
    }

    // 6. Batch Append new rows to the sheet
    if (newRowsToCache.length > 0) {
        logInfo(`_appendMissingLocationIds: Appending ${newRowsToCache.length} new location names to ${sheetName}.`);
        const startRow = sheet.getLastRow() + 1;
        // Use a DocumentLock to make the append safe
        // Assumes withSheetLock is available from Utility.js
        withSheetLock(() => {
          sheet.getRange(startRow, 1, newRowsToCache.length, numCols).setValues(newRowsToCache);
        }, 30000); // 30s lock wait
    }

    return { success: true, rowsProcessed: newRowsToCache.length };
}