/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, scheduleOneTimeTrigger, executeWithTryLock, getCorpAuthChar */

// ======================================================================
// EVE ONLINE ASSET AND LOCATION MANAGEMENT MODULE (FINAL, CORRECTED)
// NOTE: Headers are explicitly set to Row 2 (A2:H2) per user instruction.
// ======================================================================

// --- 1. GLOBAL CONSTANTS AND DEPENDENCIES ---

// Keys for storing job state in Script Properties (Resilience)
const ASSET_CACHE_DATA_KEY = 'AssetCache_Data_V2';
const ASSET_CACHE_ROW_INDEX_KEY = 'AssetCache_NextRow';
const ASSET_JOB_STATUS_KEY = 'AssetCache_Status_Key';

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
// FIX: Merged the 4 known bad IDs with the 24 "dead leaf" IDs
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
// THIS IS THE 8-COLUMN CONVENTION
const ASSET_CACHE_HEADERS = ["is_blueprint_copy", "is_singleton", "item_id", "location_flag", "location_id", "location_type", "quantity", "type_id"];
const NUM_ASSET_COLS = ASSET_CACHE_HEADERS.length;
const CACHE_NAMED_RANGE = 'NR_CORP_ASSETS_CACHE';

// Global sheet cache map for chunk writing (Required for _writeChunkInternal)
const _sheetCache = {};


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
    Logger.log(`[${SCRIPT_NAME}] Found ${maxPages} pages of assets. Fetching concurrently...`);

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
    Logger.log(`[${SCRIPT_NAME}] CRITICAL: Failed to fetch page 1. Error: ${e}`);
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
          Logger.log(`[${SCRIPT_NAME}] ERROR: Failed to parse page ${page}. Assets may be incomplete. Error: ${e}`);
        }
      }
    });
  }

  Logger.log(`[${SCRIPT_NAME}] Concurrency complete. Total asset rows found: ${allAssets.length - 1}`);
  return allAssets;
}

/**
 * Non-blocking Document Lock helper function.
 * Writes a single chunk of data while using LockService.
 */
function _writeChunkInternal(dataChunk, startRow, numCols, sheetName) {
  const chunkStartTime = new Date().getTime();
  let writeDurationMs = 0;
  
  const docLock = LockService.getDocumentLock();

  if (!docLock.tryLock(DOC_LOCK_TIMEOUT_MS)) {
    return { success: false, duration: 0 };
  }

  try {
    const workSheet = _sheetCache[sheetName];
    if (!workSheet) {
      throw new Error(`CRITICAL: Sheet object for '${sheetName}' not found in memory cache. Job state compromised.`);
    }

    // startRow is 1-indexed. Column 1 is the start.
    workSheet.getRange(startRow, 1, dataChunk.length, numCols).setValues(dataChunk);

  } catch (e) {
    Logger.log(`_writeChunkInternal: Write failed while locked: ${e.message}`);
    throw e;
  } finally {
    docLock.releaseLock();
    writeDurationMs = new Date().getTime() - chunkStartTime;
  }

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
    
    if (!cacheSheet) { Logger.log(`[${SCRIPT_NAME}] ERROR: Target sheet '${CACHE_SHEET_NAME}' not found.`); return { success: false, duration: 0 }; }
    
    // Add to cache for chunk writer
    _sheetCache[CACHE_SHEET_NAME] = cacheSheet; 
    
    const lockStartTime = new Date().getTime();
    
    // Use a very long waitLock for this critical, one-time operation.
    docLock.waitLock(CRIT_LOCK_WAIT_MS); 
    const lockAcquiredTime = new Date().getTime();
    
    try {
        const writeStartTime = new Date().getTime(); 
        
        // 1. CLEAR CONTENT: Clear content from ROW 3 downwards (A3:H...).
        const lastRow = cacheSheet.getMaxRows();
        if (lastRow > 2) {
            // Clears all rows from A3 down to the maximum row, columns A through H.
            cacheSheet.getRange("A3:H" + lastRow).clearContent();
        }
        
        // 2. HEADER WRITE: Write headers to ROW 2 (A2:H2) as explicitly instructed.
        cacheSheet.getRange("A2:H2").setValues([ASSET_CACHE_HEADERS]);

        const criticalWriteDuration = new Date().getTime() - writeStartTime;
        Logger.log(`[${SCRIPT_NAME}] CRIT-WRITE: Cleared/Wrote headers in ${criticalWriteDuration}ms. Headers placed in ROW 2.`);
        
        return { success: true, duration: lockAcquiredTime - lockStartTime };
        
    } catch (e) { 
        Logger.log(`[${SCRIPT_NAME}] CRITICAL ERROR during sheet preparation: ${e}`);
        return { success: false, duration: 0 };
    } 
    finally {
        docLock.releaseLock();
        SpreadsheetApp.flush();
        Logger.log(`[${SCRIPT_NAME}] LOCK STATS: Lock Released after preparation.`);
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
      Logger.log("Dispatch: Resumable asset job is paused. Bailing out to allow pending retry trigger to resume it.");
      return; 
  }

  // If not paused, dispatch the main trigger function.
  if (typeof cacheAllCorporateAssetsTrigger === 'function') {
      Logger.log("Dispatch: Job not paused. Calling cacheAllCorporateAssetsTrigger to initiate or acquire lock.");
      cacheAllCorporateAssetsTrigger();
  } else {
      Logger.error("Dispatch: Error: cacheAllCorporateAssetsTrigger function not found."); 
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
    Logger.log(`${funcName} skipped due to Script Lock conflict. Waiting for next scheduled run.`);
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

    // Start current chunk size at MIN_CHUNK_SIZE, but overridden for the first iteration
    let currentChunkSize = MIN_CHUNK_SIZE; 
    let previousDuration = 0; 
    
    // --- PHASE 1: Data Acquisition (Fetch or Resume) ---
    let processedAssets = [];
    const cachedAssetData = SCRIPT_PROP.getProperty(ASSET_CACHE_DATA_KEY);
    let nextWriteRow = parseInt(SCRIPT_PROP.getProperty(ASSET_CACHE_ROW_INDEX_KEY) || '0', 10);
    let jobStatus = SCRIPT_PROP.getProperty(ASSET_JOB_STATUS_KEY);

    if (cachedAssetData) {
        processedAssets = JSON.parse(cachedAssetData);
        Logger.log(`[STATE] Resuming job. Status: ${jobStatus}. Loaded ${processedAssets.length} assets from properties.`);
        
        if (nextWriteRow === 0 && jobStatus === 'FETCHED') {
            // Scenario 3: Timed out after fetch, before sheet clear. Proceed to prepare sheet.
            Logger.log(`[STATE] Detected FETCHED status. Proceeding to critical sheet preparation.`);
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

        if (allAssets.length <= 1) { Logger.log('[' + SCRIPT_NAME + '] WARNING: No assets retrieved.'); return; }
        
        // Get the data rows (allAssets[0] is the header)
        const rawAssetsData = allAssets.slice(1);
        
        // FIX: No transformation needed. The data from _fetchAssetsConcurrently is now
        // already in the correct 8-column format.
        processedAssets = rawAssetsData;

        // Save data and update state to FETCHED.
        SCRIPT_PROP.setProperty(ASSET_CACHE_DATA_KEY, JSON.stringify(processedAssets));
        SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, '0');
        SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, 'FETCHED');
        Logger.log(`[STATE] New job started. Assets saved. Status: FETCHED.`);

        // Exit and schedule continuation to allow a clean execution for the sheet clear.
        scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000); 
        return; 
    }
    
    // Re-acquire sheet access for Phase 2/3
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let cacheSheet = ss.getSheetByName(CACHE_SHEET_NAME);
    
    if (!cacheSheet) { Logger.log('[' + SCRIPT_NAME + '] ERROR: Target sheet not found for write phase.'); return; }
    _sheetCache[CACHE_SHEET_NAME] = cacheSheet; 

    // --- PHASE 2A: Critical Sheet Preparation (Only run once after successful fetch) ---
    if (nextWriteRow === 0 && jobStatus === 'FETCHED') {
        Logger.log(`[PHASE 2A] Executing critical sheet clear and header write...`);
        const result = _prepareCacheSheet(); // Call the isolated function
        
        if (!result.success) {
            Logger.error(`[PHASE 2A] Failed to clear sheet. Aborting.`);
            return;
        }

        // Set status to WRITING and schedule the continuation to start the chunk loop.
        SCRIPT_PROP.setProperty(ASSET_JOB_STATUS_KEY, 'WRITING');
        Logger.log(`[STATE] Sheet prepared. Status: WRITING. Scheduling next run to start chunking.`);
        scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000);
        return; 
    }


    // --- PHASE 2B: Resumable Chunk Write Loop (Status MUST be WRITING) ---
    if (jobStatus !== 'WRITING') {
         Logger.log(`[STATE] Job status is not WRITING (${jobStatus}). Bailing out.`);
         return;
    }
    
    // Loop continues from nextWriteRow
    for (let i = nextWriteRow; i < processedAssets.length; i += currentChunkSize) {
        const elapsedTime = new Date().getTime() - START_TIME;

        // >> PREDICTIVE TIMEOUT CHECK (Bailout)
        if (elapsedTime > SOFT_LIMIT_MS) {
            SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, i.toString());
            Logger.warn(`[STATE] Time limit hit after ${elapsedTime}ms. Saving state to resume at row ${i}.`);
            scheduleOneTimeTrigger('cacheAllCorporateAssetsTrigger', 5000); 
            return; // Exit this execution safely
        }
        
        // >> PROACTIVE THROTTLE CHECK
        if (previousDuration > THROTTLE_THRESHOLD_MS) {
            currentChunkSize = Math.max(MIN_CHUNK_SIZE, currentChunkSize - CHUNK_DECREASE_RATE);
            Logger.log(`[THROTTLE] Duration ${previousDuration}ms exceeded ${THROTTLE_THRESHOLD_MS}ms. Reducing chunk size to ${currentChunkSize} and pausing for ${THROTTLE_PAUSE_MS}ms.`);
            Utilities.sleep(THROTTLE_PAUSE_MS); 
            previousDuration = 0; // Reset duration after pause/throttle
        }

        // Data must start writing at physical Row 3 (since Row 2 holds the headers)
        const startRow = 3 + i; 
        const chunkSizeToUse = Math.min(currentChunkSize, processedAssets.length - i);
        const chunk = processedAssets.slice(i, i + chunkSizeToUse);
        
        let chunkResult = _writeChunkInternal(chunk, startRow, NUM_ASSET_COLS, CACHE_SHEET_NAME);

        if (!chunkResult.success) {
            // Failure to acquire document lock means a concurrency issue, stop job and wait for next scheduled run.
            Logger.log(`[THROTTLE FAIL] Failed to acquire lock for writing chunk starting at row ${startRow}. Stopping.`);
            throw new Error('Lock acquisition failed during chunk write.');
        }
        
        previousDuration = chunkResult.duration; 
        
        // Increase chunk size if write was fast (proactive acceleration)
        currentChunkSize = (previousDuration <= THROTTLE_THRESHOLD_MS && previousDuration > 0) 
                            ? Math.min(MAX_CHUNK_SIZE, currentChunkSize + CHUNK_INCREASE_RATE)
                            : currentChunkSize;

        // CRITICAL: Update the state property for resume
        SCRIPT_PROP.setProperty(ASSET_CACHE_ROW_INDEX_KEY, (i + chunkSizeToUse).toString()); 
    }

    // --- PHASE 3: Finalization (Clears state on success) ---
    const dataHeight = processedAssets.length;
    // Data starts at Row 3 (header at Row 2), so data height must be >= 1
    const rangeHeight = Math.max(1, dataHeight); 

    SpreadsheetApp.flush();
    Logger.log('[' + SCRIPT_NAME + '] Final spreadsheet flush and Named Range creation.');

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
    Logger.log('[' + SCRIPT_NAME + '] Successfully cached ' + dataHeight + ' asset rows. Job finalized.');
}


// --- 5. LOCATION MANAGER HELPERS ---

/**
 * Reads a sheet, finds the header row (assumed to be row 1 or 2),
 * and returns a map of {headerName: index}.
 */
function _buildHeaderMap(sheet) {
  if (!sheet) return new Map();
  // Check Row 1 first, fall back to Row 2 if Row 1 is empty (for robust SDE sheets)
  let headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  let headerRowIndex = 1;

  // If Row 1 headers are empty, assume headers are on Row 2 (like the Cache Sheet)
  if (headers.every(h => !h)) {
    headers = sheet.getRange(2, 1, 1, sheet.getLastColumn()).getValues()[0];
    headerRowIndex = 2;
  }

  const headerMap = new Map();
  headers.forEach((header, index) => {
    if (header) {
      headerMap.set(String(header).trim(), index);
    }
  });
  
  return { headerMap: headerMap, headerRowIndex: headerRowIndex };
}

/**
 * Reads the 'SDE_invTypes' sheet dynamically and builds a Map of (typeID -> typeName).
 */
function _buildSdeTypeMap() {
  const SCRIPT_NAME = '_buildSdeTypeMap';
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('SDE_invTypes');
  if (!sheet) {
    Logger.error(`[${SCRIPT_NAME}] CRITICAL: "SDE_invTypes" sheet not found.`);
    return new Map();
  }

  const headerInfo = _buildHeaderMap(sheet);
  const headerMap = headerInfo.headerMap;
  const dataStartRow = headerInfo.headerRowIndex + 1;

  const typeIdIndex = headerMap.get('typeID');
  const typeNameIndex = headerMap.get('typeName');

  if (typeIdIndex === undefined || typeNameIndex === undefined) {
    Logger.error(`[${SCRIPT_NAME}] CRITICAL: Missing 'typeID' or 'typeName' columns in 'SDE_invTypes'.`);
    return new Map();
  }

  const data = sheet.getRange(dataStartRow, 1, sheet.getLastRow() - dataStartRow + 1, sheet.getLastColumn()).getValues();
  const typeMap = new Map();
  for (const row of data) {
    const typeId = row[typeIdIndex];
    const typeName = row[typeNameIndex];
    if (typeId && typeName) {
      typeMap.set(Number(typeId), typeName);
    }
  }
  
  if (typeMap.size === 0) {
    Logger.warn(`[${SCRIPT_NAME}] WARNING: 'SDE_invTypes' data is empty. Location names will fail. Run SDE Update.`);
  }
  return typeMap;
}

/**
 * Reads 'SDE_staStations' to build a map of (stationID -> stationName).
 */
function _buildStationMap() {
  const SCRIPT_NAME = '_buildStationMap';
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('SDE_staStations');
  if (!sheet) {
    Logger.error(`[${SCRIPT_NAME}] CRITICAL: "SDE_staStations" sheet not found.`);
    return new Map();
  }

  const headerInfo = _buildHeaderMap(sheet);
  const headerMap = headerInfo.headerMap;
  const dataStartRow = headerInfo.headerRowIndex + 1;

  const stationIdIndex = headerMap.get('stationID');
  const stationNameIndex = headerMap.get('stationName');

  if (stationIdIndex === undefined || stationNameIndex === undefined) {
    Logger.error(`[${SCRIPT_NAME}] CRITICAL: Missing 'stationID' or 'stationName' columns in 'SDE_staStations'.`);
    return new Map();
  }

  const data = sheet.getRange(dataStartRow, 1, sheet.getLastRow() - dataStartRow + 1, sheet.getLastColumn()).getValues();
  const stationMap = new Map();
  for (const row of data) {
    const stationId = row[stationIdIndex];
    const stationName = row[stationNameIndex];
    if (stationId && stationName) {
      stationMap.set(Number(stationId), stationName);
    }
  }
  
  if (stationMap.size === 0) {
    Logger.warn(`[${SCRIPT_NAME}] WARNING: 'SDE_staStations' data is empty. Location names will fail. Run SDE Update.`);
  }
  return stationMap;
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
    Logger.log(`[${SCRIPT_NAME}] Successfully fetched and parsed custom names from ESI.`);

  } catch (e) { 
    Logger.log(`[${SCRIPT_NAME}] WARNING: Failed to fetch divisions from ESI. Using defaults. Error: ${e}`); 
  }

  // 3. APPLY FAILSAFE DEFAULTS (Fills in any missing names)
  Object.keys(defaultHangars).forEach(flag => {
    if (!hangarMap.has(flag)) { hangarMap.set(flag, defaultHangars[flag]); }
  });

  Logger.log(`[${SCRIPT_NAME}] Built Hangar Name map with ${hangarMap.size} entries.`);
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
 * Re-populates the 'LocationManager' sheet with all available
 * corp hangars and all top-level containers (named or unnamed),
 * linked to their parent station.
 */
function refreshLocationManager() {
  const SCRIPT_NAME = 'refreshLocationManager';
  const TARGET_SHEET_NAME = LOCATION_MANAGER_SHEET_NAME;
  const CACHE_RANGE_NAME = CACHE_NAMED_RANGE; 
  
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  if (!sheet) { Logger.log(`[${SCRIPT_NAME}] Target sheet '${TARGET_SHEET_NAME}' not found.`); return; }

  Logger.log(`[${SCRIPT_NAME}] Starting refresh of '${TARGET_SHEET_NAME}'...`);

  // 1. Build all necessary lookup maps
  const sdeTypeMap = _buildSdeTypeMap();
  const stationMap = _buildStationMap();
  const hangarMap = _buildHangarNameMap();

  // CRITICAL CHECK: Abort if SDE data is missing (prevents all names from failing)
  if (sdeTypeMap.size === 0 || stationMap.size === 0) {
    Logger.error(`[${SCRIPT_NAME}] CRITICAL: Missing SDE Item or Station data. Run SDE Update first. Aborting.`);
    SpreadsheetApp.getUi().alert('CRITICAL ERROR: SDE Data is missing. Run Sheet Tools -> Update SDE Data first.');
    return;
  }

  /** @type {Map<number, CorpOffice>} */
  const corpOfficesMap = new Map();
  // Key: container_item_id, Value: { parentLocationId, flag }
  const uniqueOtherContainers = new Map();
  const locationNameResolver = new Map(stationMap);
  const missingLocationIds = new Set();
  let allLocations = [];

  const BATCH_SIZE = 1000;
  
  try {
    // 2. Read Assets from Named Range (local read)
    const cachedRange = ss.getRangeByName(CACHE_RANGE_NAME);
    if (!cachedRange) { 
      Logger.log(`[${SCRIPT_NAME}] CRITICAL: Named Range '${CACHE_RANGE_NAME}' not found. Run asset cache first.`); 
      SpreadsheetApp.getUi().alert('CRITICAL ERROR: Asset cache not found. Run asset job first.');
      return; 
    }

    const allAssetsData = cachedRange.getValues(); // <<--- Reads asset data rows from Named Range

    // 3. Build dynamic index map from the static asset headers (required for consistent indexing)
    const assetsHeader = new Map(ASSET_CACHE_HEADERS.map((h, i) => [h, i]));

    const asset_itemIdIndex = assetsHeader.get('item_id');
    const asset_typeIdIndex = assetsHeader.get('type_id');
    const asset_locationIdIndex = assetsHeader.get('location_id');
    const asset_locationFlagIndex = assetsHeader.get('location_flag');
    const asset_locationTypeIndex = assetsHeader.get('location_type');

    // 4. Loop assets to find Offices/Containers
    allAssetsData.forEach(row => { 
        const item_id = Number(row[asset_itemIdIndex]);
        const type_id = Number(row[asset_typeIdIndex]);
        const location_id = Number(row[asset_locationIdIndex]);
        const location_flag = String(row[asset_locationFlagIndex]);
        const location_type = String(row[asset_locationTypeIndex]).toLowerCase();
        
        // 4a. Find Corp Office Folders (Branch -> Root mapping)
        // FIX: This now correctly identifies offices using 'location_type'
        if (location_type === 'office') {
            // item_id is the Office Folder ID (the "Branch"), location_id is the Station/Structure ID (the "Root")
            corpOfficesMap.set(item_id, new CorpOffice(item_id, location_id));
            
            // If the root is a structure (player-owned), add it for name resolution later
            if (location_id > NPC_STATION_ID_MAX && !locationNameResolver.has(location_id)) {
                missingLocationIds.add(location_id);
            }
        }

        // 4b. Find Other Top-Level Containers
        // CRITICAL BUG FIX: Added check to see if the item is a container
        const typeName = sdeTypeMap.get(type_id) || "";
        
        if (
            typeName.toLowerCase().includes('container') && // <-- THIS IS THE FIX
            item_id >= ASSET_ID_MIN_BOUND && 
            !isOfficeValue_(item_id, corpOfficesMap) && 
            !GHOST_ITEM_IDS.has(item_id) && 
            !EXCLUDED_CONTAINER_TYPE_IDS.has(type_id) &&
            location_flag === 'CorpDeliveries' // Common flag for containers/loot in space/structures
            ) { 
            
            // We use the container's item_id as the key. 
            // FIX: Store an object with parentLocationId AND the flag
            if (!uniqueOtherContainers.has(item_id)) {
                uniqueOtherContainers.set(item_id, { parentLocationId: location_id, flag: location_flag });
            }
            
            // Collect the root location ID if it's a structure
            if (location_id > NPC_STATION_ID_MAX && !locationNameResolver.has(location_id)) {
                missingLocationIds.add(location_id);
            }
        }
    });

    // 5. Resolve missing Root IDs (Structures and Stations) via ESI
    // FIX: This block now separates player structures from NPC stations
    const structureIdsToResolve = [];
    const npcIdsToResolve = [];

    missingLocationIds.forEach(id => {
      if (id > NPC_STATION_ID_MAX) {
        structureIdsToResolve.push(id);
      } else if (id > 0) { // Ensure we don't try to resolve ID 0
        npcIdsToResolve.push(id);
      }
    });

    // 5a. Resolve NPC Stations (using POST /universe/ids/)
    if (npcIdsToResolve.length > 0) {
      const universeNamesClient = getGESIUniverseNamesClient_();
      Logger.log(`[${SCRIPT_NAME}] Resolving ${npcIdsToResolve.length} missing NPC Station IDs via ESI.`);
      try {
          // FIX: This now uses the correct GESI executeRaw method with an object payload
          const resolvedNamesData = universeNamesClient.executeRaw({ ids: npcIdsToResolve.map(String) });
          // FIX: This now correctly parses the categorized response from /universe/ids
          if (resolvedNamesData) {
              // Check all categories ESI might return for a station/system ID
              const categories = ['stations', 'systems', 'regions', 'alliances', 'corporations', 'characters'];
              categories.forEach(category => {
                  if (resolvedNamesData[category] && Array.isArray(resolvedNamesData[category])) {
                      resolvedNamesData[category].forEach(entity => {
                          const id = Number(entity.id);
                          if (entity.name) {
                              locationNameResolver.set(id, entity.name);
                          }
                      });
                  }
              });
          }
      } catch (e) {
          Logger.log(`[${SCRIPT_NAME}] WARNING: ESI call to resolve missing NPC station IDs failed: ${e}`);
      }
    }

    // 5b. Resolve Player Structures (using GET /universe/structures/{structure_id}/)
    if (structureIdsToResolve.length > 0) {
      const structuresClient = getGESIStructuresClient_(); // Uses the new helper
      // FIX: Use the correct getCorpAuthChar function to get the Director token
      // This assumes getCorpAuthChar is available in another file (e.g., GESI Extentions.js)
      const mainChar = (typeof getCorpAuthChar === 'function') ? getCorpAuthChar(ss) : GESI.getMainCharacter();
      Logger.log(`[${SCRIPT_NAME}] Resolving ${structureIdsToResolve.length} Structure IDs via ESI using char: ${mainChar}`);
      
      for (const structureId of structureIdsToResolve) {
        try {
          // This endpoint must be called one-by-one and is authenticated
          const structureData = structuresClient.executeRaw({ structure_id: structureId, name: mainChar });
          if (structureData && structureData.name) {
            locationNameResolver.set(structureId, structureData.name);
          } else {
            locationNameResolver.set(structureId, `Structure (ID: ${structureId})`);
          }
          Utilities.sleep(50); // Add a small sleep to avoid hammering the endpoint
        } catch (e) {
            // Log the "Forbidden" error here
            Logger.log(`[${SCRIPT_NAME}] WARNING: ESI call for structure ID ${structureId} failed: ${e.message}`);
            locationNameResolver.set(structureId, `Structure (ID: ${structureId})`);
        }
      }
    }
    
    // 5.5 FIX: Populate locationName in CorpOffice objects *after* all names are resolved
    corpOfficesMap.forEach(office => {
        office.locationName = locationNameResolver.get(office.locationId) || ('Unknown Location ID: ' + office.locationId);
    });


    // 6. Get custom names for Containers via ESI (concurrently)
    let namesMap = new Map();
    const allContainerIds = [...uniqueOtherContainers.keys()];
    // FIX: This filter now uses the complete GHOST_ITEM_IDS list
    const validContainerIds = allContainerIds.map(Number).filter(id => id >= ASSET_ID_MIN_BOUND && !GHOST_ITEM_IDS.has(id));
    
    const namesClient = getGESINamesClient_();

    if (validContainerIds.length > 0) { 
        Logger.log(`[${SCRIPT_NAME}] Resolving ${validContainerIds.length} container names via ESI in batches.`);
        
        for (let i = 0; i < validContainerIds.length; i += BATCH_SIZE) {
            const batchIds = validContainerIds.slice(i, i + BATCH_SIZE);
            try {
                // FIX: This also uses executeRaw for consistency
                const rawNames = namesClient.executeRaw({ item_ids: batchIds });
                if (Array.isArray(rawNames)) {
                    rawNames.forEach(namedAsset => {
                        const itemId = Number(namedAsset.item_id);
                        if (namedAsset.name) {
                            namesMap.set(itemId, namedAsset.name);
                        }
                    });
                }
            } catch (e) {
                Logger.log(`[${SCRIPT_NAME}] WARNING: ESI batch call for container names failed at batch ${i}: ${e}`);
                // FIX: If a batch fails (e.g., "Invalid IDs"), stop looping to prevent timeout
                Logger.log(`[${SCRIPT_NAME}] Stopping name resolution loop due to batch failure.`);
                break; 
            }
        }
    }

    // 7. Build final location list (Hangars and Containers)

    // 7a. Hangar divisions (Flags CorpSAG1-7)
    corpOfficesMap.forEach(office => {
        // FIX: Use the pre-resolved name from the object
        const rootLocationName = office.locationName; 
        
        hangarMap.forEach((hangarName, flag) => {
            // office.itemId is the ID of the office folder, which becomes the location_id for items inside it.
            allLocations.push([
                rootLocationName, 
                hangarName, 
                office.itemId, 
                flag, 
                'Hangar Division', 
                false, // IsSalesHangar (Checkbox column)
                false  // IsMaterialHangar (Checkbox column)
            ]);
        });
    });

    // 7b. Other Containers (jetcans, secure containers, etc.)
    // FIX: This loop now correctly extracts the stored flag and parentLocationId
    uniqueOtherContainers.forEach((containerData, containerItemId) => {
        const parentLocationId = containerData.parentLocationId;
        const hangarFlag = containerData.flag; // <-- This is the real flag

        // Find the typeId for this container item from the asset cache data
        const assetRow = allAssetsData.find(row => row[asset_itemIdIndex] === containerItemId);
        const typeId = assetRow ? assetRow[asset_typeIdIndex] : null;
        
        // 1. Try custom name 2. Try SDE name 3. Default
        const containerName = namesMap.get(containerItemId) || sdeTypeMap.get(typeId) || 'Unnamed Container (ID: ' + containerItemId + ')';
        const rootLocationName = locationNameResolver.get(parentLocationId) || ('Unknown Location ID: ' + parentLocationId);
        
        // This container's item_id is its own location ID
        allLocations.push([
            rootLocationName, 
            '', // <-- Hangar Name is BLANK
            containerItemId, 
            hangarFlag, // <-- FIX: Use the real flag (e.g., 'CorpDeliveries')
            containerName, // <-- Container name (e.g., "Militants") goes in Type col
            false,
            false
        ]);
    });

  } catch (e) { Logger.log(`[${SCRIPT_NAME}] Error during asset processing: ${e}`); }

  // C. Clear and Write to Sheet
  sheet.clear();
  const headers = [ 'Office Location', 'Hangar Name', 'Location ID', 'Hangar Flag', 'Type', 'IsSalesHangar', 'IsMaterialHangar' ];

  // Sort the final list (sort by Office Location, then Hangar Name)
  allLocations.sort((a, b) => {
    const officeA = String(a[0]);
    const officeB = String(b[0]);
    if (officeA !== officeB) return officeA.localeCompare(officeB);
    return String(a[1]).localeCompare(String(b[1]));
  });
  
  const outputData = [headers, ...allLocations];

  if (allLocations.length === 0) {
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    return;
  }

  // Write all data (LocationManager headers are standard, starting on Row 1)
  sheet.getRange(1, 1, outputData.length, headers.length).setValues(outputData);

  // D. Add Checkboxes
  const numDataRows = allLocations.length;
  // Columns for IsSalesHangar (Col 6) and IsMaterialHangar (Col 7)
  if (numDataRows > 0) {
    // Data starts on row 2 for LocationManager
    sheet.getRange(2, headers.length - 1, numDataRows, 2).insertCheckboxes();
  }

  sheet.setFrozenRows(1);
  Logger.log(`[${SCRIPT_NAME}] '${TARGET_SHEET_NAME}' has been populated with ${allLocations.length} locations.`);
}

// --- 7. MATERIAL WORKER (Reads from Cache) ---

/**
 * Automates the 'MaterialHangar' sheet.
 * Reads its configuration from the 'LocationManager' sheet and fetches materials
 * from the local asset cache.
 */
function updateMaterialHangar() {
  const SCRIPT_NAME = 'updateMaterialHangar';
  const TARGET_SHEET_NAME = MATERIAL_HANGAR_SHEET_NAME;
  const CONFIG_SHEET_NAME = LOCATION_MANAGER_SHEET_NAME;
  const CACHE_RANGE_NAME = CACHE_NAMED_RANGE;

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  const configSheet = ss.getSheetByName(CONFIG_SHEET_NAME);

  if (!sheet || !configSheet) {
    Logger.log(`[${SCRIPT_NAME}] ERROR: Target sheet (${TARGET_SHEET_NAME}) or config sheet (${CONFIG_SHEET_NAME}) not found.`);
    return;
  }

  // 1. Read LocationManager Configuration
  const configHeaderInfo = _buildHeaderMap(configSheet);
  const configHeaderMap = configHeaderInfo.headerMap;
  const configDataStartRow = configHeaderInfo.headerRowIndex + 1;

  const locIdCol = configHeaderMap.get('Location ID');
  const isMaterialCol = configHeaderMap.get('IsMaterialHangar');

  if (locIdCol === undefined || isMaterialCol === undefined) {
    Logger.log(`[${SCRIPT_NAME}] ERROR: Missing 'Location ID' or 'IsMaterialHangar' in LocationManager.`);
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
    Logger.log(`[${SCRIPT_NAME}] No locations marked for MaterialHangar. Clearing sheet.`);
    sheet.clearContents();
    sheet.getRange(1, 1, 1, 3).setValues([['Type Name', 'Type ID', 'Total Quantity']]);
    sheet.setFrozenRows(1);
    return;
  }

  // 2. Read Assets from Named Range (Local Cache Read)
  const cachedRange = ss.getRangeByName(CACHE_RANGE_NAME);
  if (!cachedRange) {
    Logger.log(`[${SCRIPT_NAME}] CRITICAL: Named Range '${CACHE_RANGE_NAME}' not found. Asset cache not run or failed.`);
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

  Logger.log(`[${SCRIPT_NAME}] Aggregated materials for ${materialAggregation.size} unique types.`);

  // 4. Format and Write
  const sdeTypeMap = _buildSdeTypeMap();
  const outputRows = [];

  // Map to array of [typeName, typeId, quantity]
  for (const [typeId, totalQuantity] of materialAggregation.entries()) {
    const typeName = sdeTypeMap.get(typeId) || `Unknown Type (ID: ${typeId})`;
    outputRows.push([typeName, typeId, totalQuantity]);
  }

  // Sort by Type Name
  outputRows.sort((a, b) => String(a[0]).localeCompare(String(b[0])));

  const headers = ['Type Name', 'Type ID', 'Total Quantity'];
  const outputData = [headers, ...outputRows];

  sheet.clearContents();
  sheet.getRange(1, 1, outputData.length, headers.length).setValues(outputData);

  sheet.setFrozenRows(1);
  Logger.log(`[${SCRIPT_NAME}] MaterialHangar updated with ${outputRows.length} unique material rows.`);
}