// ======================================================================
// EVE ONLINE ASSET AND LOCATION MANAGEMENT MODULE (FINAL COMPLETE FILE)
// Includes: ESI Helpers, Concurrent Fetch, Cache Write, Location GUI Logic.
// ======================================================================

/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService */

// --- 1. PERSISTENT CLIENT HELPERS (for ESI connections) ---

/**
 * Returns a persistent ESIClient instance for corporations_corporation_divisions
 */
function getGESIDivisionsClient_() {
  return GESI.getClient().setFunction('corporations_corporation_divisions');
}

/**
 * Returns a persistent ESIClient instance for corporations_corporation_assets_names
 */
function getGESINamesClient_() {
  return GESI.getClient().setFunction('corporations_corporation_assets_names');
}

/**
 * Returns a persistent ESIClient instance for universe_names
 */
function getGESIUniverseNamesClient_() {
  return GESI.getClient().setFunction('universe_names');
}
/**
 * Executes cacheAllCorporateAssets, enforcing a script lock to prevent
 * concurrency issues and spreadsheet service timeouts.
 * This function should be set as the hourly trigger handler.
 * * NOTE: This relies on the external function 'executeWithTryLock' 
 * which manages the Script Lock service.
 */
function cacheAllCorporateAssetsTrigger() {
  const funcName = 'cacheAllCorporateAssets';

  // This wrapper enforces the Script Lock check for every call, including dynamic triggers.
  // We assume 'executeWithTryLock' is available in your Apps Script project.
  const result = executeWithTryLock(cacheAllCorporateAssets, funcName); 

  if (result === null) {
    // Job was skipped due to lock, so we exit silently. The orchestrator or
    // a subsequent trigger will pick it up.
    Logger.log(`${funcName} skipped execution due to a concurrency lock. Will be picked up by next trigger.`);
  }
  // If it executed, the result is handled by the worker's internal reschedule/state change.
}

// ----------------------------------------------------------------------
// >> CorpOffice Class to manage the "Branch" -> "Root" hierarchy
// ----------------------------------------------------------------------
/**
 * A simple class to store Office Folder data (the "Branch") and its
 * parent Station/Structure ID (the "Root").
 */
class CorpOffice {
  /**
   * @param {number} itemId The Office Folder's Item ID (The "Branch" ID)
   * @param {number} locationId The Station/Structure ID (The "Root" ID)
   */
  constructor(itemId, locationId) {
    this.itemId = itemId;
    this.locationId = locationId;
  }
}
// ----------------------------------------------------------------------


/**
 * Executes concurrent ESI requests to fetch all pages of corporation assets.
 */
function _fetchAssetsConcurrently(mainChar) {
  const SCRIPT_NAME = '_fetchAssetsConcurrently';
  const client = GESI.getClient().setFunction('corporations_corporation_assets');

  let maxPages = 1;
  let headerRow = ['item_id', 'is_singleton', 'location_flag', 'location_id', 'location_type', 'quantity', 'type_id'];
  let allAssets = [];
  allAssets.push(headerRow);

  try {
    const requestPage1 = client.buildRequest({ page: 1 });
    const responsePages = UrlFetchApp.fetchAll([requestPage1]);
    const responsePage1 = responsePages[0];

    if (responsePage1.getResponseCode() < 200 || responsePage1.getResponseCode() >= 300) {
      throw new Error('Failed to fetch initial asset page. Response Code: ' + responsePage1.getResponseCode());
    }

    const headers = responsePage1.getHeaders();
    maxPages = Number(headers['X-Pages'] || headers['x-pages']) || 1;
    Logger.log('[' + SCRIPT_NAME + '] Found ' + maxPages + ' pages of assets. Fetching concurrently...');

    const bodyPage1 = responsePage1.getContentText();
    const dataPage1 = JSON.parse(bodyPage1);

    dataPage1.forEach(obj => {
      allAssets.push([
        obj.item_id, obj.is_singleton, obj.location_flag, obj.location_id,
        obj.location_type, obj.quantity, obj.type_id
      ]);
    });

  } catch (e) {
    Logger.log('[' + SCRIPT_NAME + '] CRITICAL: Failed to fetch page 1. Error: ' + e);
    return [headerRow];
  }

  let allRequests = [];
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
              obj.item_id, obj.is_singleton, obj.location_flag, obj.location_id,
              obj.location_type, obj.quantity, obj.type_id
            ]);
          });
        } catch (e) {
          Logger.log('[' + SCRIPT_NAME + '] ERROR: Failed to parse page ' + page + '. Assets may be incomplete. Error: ' + e);
        }
      }
    });
  }

  Logger.log('[' + SCRIPT_NAME + '] Concurrency complete. Total asset rows found: ' + (allAssets.length - 1));
  return allAssets;
}

// NOTE: cacheAllCorporateAssetsTrigger needs an external executeWithTryLock() 
// function to run as intended.

// --- 3. GLOBAL CONSTANTS AND INTERNAL WRITER (for Caching) ---
const _sheetCache = {};
const WRITE_CHUNK_SIZE = 500;
const THROTTLE_THRESHOLD_MS = 800; // Trigger point for slow writes
const THROTTLE_PAUSE_MS = 200;     // Pause duration after a slow write
const INITIAL_CHUNK_SIZE = 500;
const CHUNK_INCREASE_RATE = 50;
const CHUNK_DECREASE_RATE = 100;
const MAX_CHUNK_SIZE = 1000;
const MIN_CHUNK_SIZE = 100;

/**
 * Non-blocking Document Lock helper function.
 * Writes a single chunk of data while using LockService.
 */
function _writeChunkInternal(dataChunk, startRow, numCols, sheetName) {
  const chunkStartTime = new Date().getTime();
  let writeDurationMs = 0;
  const DOC_LOCK_TIMEOUT = 5000; // TryLock 5s 

  const docLock = LockService.getDocumentLock();

  if (!docLock.tryLock(DOC_LOCK_TIMEOUT)) {
    return { success: false, duration: 0 };
  }

  try {
    let workSheet = _sheetCache[sheetName];
    if (!workSheet) {
      throw new Error(`CRITICAL: Sheet object for '${sheetName}' not found in memory cache. Job state compromised.`);
    }

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

// --- 4. CORE ASSET CACHING FUNCTION (Write-Protected & Throttled) ---

/**
 * Executes the full ESI asset pull and writes the result to a local sheet.
 */
function cacheAllCorporateAssets() {
    const SCRIPT_NAME = 'cacheAllCorporateAssets';
    const CACHE_SHEET_NAME = 'CorpWarehouseStock'; // Target Sheet
    
    const CORP_HEADERS =["is_blueprint_copy", "is_singleton", "item_id", "location_flag", "location_id", "location_type", "quantity", "type_id"];
    const numDataCols = CORP_HEADERS.length; 

    const docLock = LockService.getDocumentLock();

    let currentChunkSize = INITIAL_CHUNK_SIZE;
    const thresholdToUse = THROTTLE_THRESHOLD_MS; 
    let previousDuration = 0; // Tracks duration of the previous op (Lock Wait or Chunk Write)
    
    try {
        Logger.log('[' + SCRIPT_NAME + '] Starting full asset cache refresh.');
        
        // 1. Fetch ALL data concurrently
        const mainChar = GESI.getMainCharacter();
        const allAssets = _fetchAssetsConcurrently(mainChar); 
        
        if (allAssets.length <= 1) {
            Logger.log('[' + SCRIPT_NAME + '] WARNING: No assets retrieved.');
            return;
        }

        // 2. Map ESI Data to the New Column Order
        const esiHeadersMap = new Map(allAssets[0].map((h, i) => [String(h).trim(), i]));
        const rawAssetsData = allAssets.slice(1);
        const processedAssets = [];
        
        const idx = {
            item_id: esiHeadersMap.get('item_id'), is_singleton: esiHeadersMap.get('is_singleton'),
            location_flag: esiHeadersMap.get('location_flag'), location_id: esiHeadersMap.get('location_id'),
            location_type: esiHeadersMap.get('location_type'), quantity: esiHeadersMap.get('quantity'),
            type_id: esiHeadersMap.get('type_id')
        }; 
        rawAssetsData.forEach(row => {
            processedAssets.push([
                "", row[idx.is_singleton], row[idx.item_id], row[idx.location_flag], 
                row[idx.location_id], row[idx.location_type], row[idx.quantity], row[idx.type_id]
            ]);
        });


        // 3. Prepare the target sheet and variables
        const ss = SpreadsheetApp.getActiveSpreadsheet();
        let cacheSheet = ss.getSheetByName(CACHE_SHEET_NAME);
        
        if (!cacheSheet) {
            Logger.log('[' + SCRIPT_NAME + '] ERROR: Target sheet "' + CACHE_SHEET_NAME + '" not found.');
            return;
        }
        
        _sheetCache[CACHE_SHEET_NAME] = cacheSheet; 
        const desiredWidth = numDataCols; 
        
        // 4. Clear and Write Headers (Row 2) - CRITICAL SECTION
        const lockStartTime = new Date().getTime();
        docLock.waitLock(30000); 
        const lockAcquiredTime = new Date().getTime();
        
        let criticalWriteDuration = 0;
        try {
            const writeStartTime = new Date().getTime(); 
            
            const startRow = 2;
            const rowsToClear = cacheSheet.getMaxRows() - startRow + 1;
            
            cacheSheet.getRange(startRow, 1, rowsToClear, desiredWidth).clearContent();
            cacheSheet.getRange(2, 1, 1, numDataCols).setValues([CORP_HEADERS]);

            criticalWriteDuration = new Date().getTime() - writeStartTime;
            Logger.log(`[${SCRIPT_NAME}] CRIT-WRITE: Cleared/Wrote headers in ${criticalWriteDuration}ms.`);

        } catch (e) {
            Logger.log('[' + SCRIPT_NAME + '] CRITICAL FAILURE during sheet modification: ' + e);
            throw e;
        } finally {
            const lockWaitDuration = lockAcquiredTime - lockStartTime;
            // Set previousDuration to the Lock Wait Duration for the very first throttle check
            previousDuration = lockWaitDuration; 
            
            docLock.releaseLock();
            Logger.log(`[${SCRIPT_NAME}] LOCK STATS: Wait time: ${lockWaitDuration}ms. Lock Released.`);
        }
        // --- End Critical Section ---

        // >> POST-CRIT FLUSH: Stabilize the sheet before beginning the strenuous chunk write loop.
        SpreadsheetApp.flush();
        Logger.log('[' + SCRIPT_NAME + '] POST-CRIT FLUSH: Stabilizing sheet before chunk writes.');


        // 5. Write data in chunks (starting from Row 3)
        for (let i = 0; i < processedAssets.length; i += currentChunkSize) {
            
            // ----------------------------------------------------------------------
            // >> PROACTIVE THROTTLE CHECK
            // ----------------------------------------------------------------------
            if (previousDuration > thresholdToUse) {
                currentChunkSize = Math.max(MIN_CHUNK_SIZE, currentChunkSize - CHUNK_DECREASE_RATE);
                Logger.log(`[PROACTIVE THROTTLE] Prev duration ${previousDuration}ms exceeded ${thresholdToUse}ms. Reducing chunk size to ${currentChunkSize} and pausing for ${THROTTLE_PAUSE_MS}ms.`);
                Utilities.sleep(THROTTLE_PAUSE_MS); 
                previousDuration = 0; 
            }
            // ----------------------------------------------------------------------

            const chunkSizeToUse = Math.min(currentChunkSize, processedAssets.length - i);
            const chunk = processedAssets.slice(i, i + chunkSizeToUse);
            const startRow = 3 + i; 
            
            let chunkResult = _writeChunkInternal(chunk, startRow, numDataCols, CACHE_SHEET_NAME);

            if (!chunkResult.success) {
                Logger.log(`[THROTTLE FAIL] Failed to acquire lock for writing chunk starting at row ${startRow}. Stopping.`);
                throw new Error('Lock acquisition failed during chunk write.');
            }
            
            previousDuration = chunkResult.duration; 

            // >> FAST WRITE: Increase chunk size slightly for the NEXT iteration
            if (previousDuration <= thresholdToUse && previousDuration > 0) {
                 currentChunkSize = Math.min(MAX_CHUNK_SIZE, currentChunkSize + CHUNK_INCREASE_RATE);
            }
        }

        // 6. Define the Named Range over the exact A3:H range.
        const dataHeight = processedAssets.length;
        const rangeHeight = Math.max(1, dataHeight); 

        // Flush queued writes before updating the Named Range
        SpreadsheetApp.flush();
        Logger.log('[' + SCRIPT_NAME + '] Forced spreadsheet flush to commit all changes.');

        SpreadsheetApp.getActive().setNamedRange(
            'NR_CORP_ASSETS_CACHE', 
            cacheSheet.getRange(3, 1, rangeHeight, desiredWidth)
        );
        
        Logger.log('[' + SCRIPT_NAME + '] Successfully cached ' + dataHeight + ' asset rows to ' + CACHE_SHEET_NAME);

    } catch (e) {
        Logger.log('[' + SCRIPT_NAME + '] CRITICAL FAILURE: Asset caching failed: ' + e);
        throw e;
    }
}


// --- 5. LOCATION MANAGER HELPERS (For GUI Building) ---

/**
 * Reads a sheet, finds the header row (assumed to be row 1),
 * and returns a map of {headerName: index}.
 */
function _buildHeaderMap(sheet) {
  if (!sheet) return new Map();
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const headerMap = new Map();
  headers.forEach((header, index) => {
    if (header) {
      headerMap.set(String(header).trim(), index);
    }
  });
  return headerMap;
}

/**
 * Reads the 'SDE_invTypes' sheet dynamically and builds a Map of (typeID -> typeName).
 */
function _buildSdeTypeMap() {
  const SCRIPT_NAME = '_buildSdeTypeMap';
  const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('SDE_invTypes');
  if (!sheet) {
    Logger.log('[' + SCRIPT_NAME + '] ERROR: "SDE_invTypes" sheet not found.');
    return new Map();
  }

  const headerMap = _buildHeaderMap(sheet);
  const typeIdIndex = headerMap.get('typeID');
  const typeNameIndex = headerMap.get('typeName');

  if (typeIdIndex === undefined || typeNameIndex === undefined) {
    Logger.log('[' + SCRIPT_NAME + '] ERROR: Could not find \'typeID\' or \'typeName\' columns in \'SDE_invTypes\'.');
    return new Map();
  }

  const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
  const typeMap = new Map();
  for (const row of data) {
    const typeId = row[typeIdIndex];
    const typeName = row[typeNameIndex];
    if (typeId && typeName) {
      typeMap.set(Number(typeId), typeName);
    }
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
    Logger.log('[' + SCRIPT_NAME + '] ERROR: "SDE_staStations" sheet not found.');
    return new Map();
  }

  const headerMap = _buildHeaderMap(sheet);
  const stationIdIndex = headerMap.get('stationID');
  const stationNameIndex = headerMap.get('stationName');

  if (stationIdIndex === undefined || stationNameIndex === undefined) {
    Logger.log('[' + SCRIPT_NAME + '] ERROR: Could not find \'stationID\' or \'stationName\' columns in \'SDE_staStations\'.');
    return new Map();
  }

  const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
  const stationMap = new Map();
  for (const row of data) {
    const stationId = row[stationIdIndex];
    const stationName = row[stationNameIndex];
    if (stationId && stationName) {
      stationMap.set(Number(stationId), stationName);
    }
  }
  return stationMap;
}

/**
 * Fetches hangar division names from ESI/GESI first.
 */
function _buildHangarNameMap() {
  const SCRIPT_NAME = '_buildHangarNameMap';
  const hangarMap = new Map();

  // FAILSAFE: Hardcoded defaults for EVE Corp Hangars (CorpSAG1 to CorpSAG7)
  const defaultHangars = {
    'CorpSAG1': 'General Hangar',
    'CorpSAG2': 'Financial',
    'CorpSAG3': 'Manufacturing',
    'CorpSAG4': 'Mining',
    'CorpSAG5': 'R&D',
    'CorpSAG6': 'Storage',
    'CorpSAG7': 'Assembly'
  };

  try {
    // 1. ATTEMPT ESI/GESI FETCH for custom names
    const divisionsClient = getGESIDivisionsClient_();
    const divisionsData = divisionsClient.executeRaw({});

    if (!divisionsData || !Array.isArray(divisionsData.hangar)) {
      throw new Error('Malformed division data from ESI.');
    }

    const divisions = divisionsData.hangar;

    divisions.forEach(divisionRow => {
      const divisionNumber = Number(divisionRow.division);
      const divisionName = String(divisionRow.name).trim();

      if (divisionNumber >= 1 && divisionNumber <= 7) {
        const flag = 'CorpSAG' + divisionNumber;
        if (divisionName) {
          hangarMap.set(flag, divisionName);
        }
      }
    });
    Logger.log('[' + SCRIPT_NAME + '] Successfully fetched and parsed custom names from ESI.');

  } catch (e) {
    Logger.log('[' + SCRIPT_NAME + '] WARNING: Failed to fetch divisions from ESI. Trying local sheet. Error: ' + e);
  }

  // 2. ATTEMPT LOCAL SHEET READ (as backup to ESI)
  if (hangarMap.size === 0 || hangarMap.size < 7) {
    const sheet = SpreadsheetApp.getActiveSpreadsheet().getSheetByName('hangar_names');
    if (sheet) {
      try {
        const headerMap = _buildHeaderMap(sheet);
        const nameIndex = headerMap.get('Name');
        const flagIndex = headerMap.get('Flag');

        if (nameIndex !== undefined && flagIndex !== undefined) {
          const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
          for (const row of data) {
            const name = row[nameIndex];
            const flag = row[flagIndex];
            if (name && flag && !hangarMap.has(flag)) {
              hangarMap.set(flag, name);
            }
          }
        }
      } catch (e) {
        Logger.log('[' + SCRIPT_NAME + '] WARNING: Failed to read \'hangar_names\' sheet.');
      }
    }
  }

  // 3. APPLY FAILSAFE DEFAULTS
  Object.keys(defaultHangars).forEach(flag => {
    if (!hangarMap.has(flag)) {
      hangarMap.set(flag, defaultHangars[flag]);
    }
  });

  Logger.log('[' + SCRIPT_NAME + '] Built Hangar Name map with ' + hangarMap.size + ' entries (guaranteed 7).');
  return hangarMap;
}

/**
 * Checks if the given ID (number) exists as either the itemId or locationId 
 * in any CorpOffice object. (Helper for refreshLocationManager)
 */
function isOfficeValue_(targetId, corpOfficesMap) {
  for (const office of corpOfficesMap.values()) {
    if (office.itemId === targetId || office.locationId === targetId) {
      return true;
    }
  }
  return false;
}

// --- 6. LOCATION MANAGER GUI (Reads from Cache) ---

/**
 * Re-populates the 'LocationManager' sheet with all available
 * corp hangars and all top-level containers (named or unnamed),
 * linked to their parent station.
 * This function now reads exclusively from the local asset cache (NR_CORP_ASSETS_CACHE).
 */
function refreshLocationManager() {
  const SCRIPT_NAME = 'refreshLocationManager';
  const TARGET_SHEET_NAME = 'LocationManager';
  const CACHE_RANGE_NAME = 'NR_CORP_ASSETS_CACHE'; // Read from this named range
  const mainChar = GESI.getMainCharacter();

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  if (!sheet) {
    Logger.log('[' + SCRIPT_NAME + '] Target sheet "' + TARGET_SHEET_NAME + '" not found. Please create it.');
    SpreadsheetApp.getUi().alert('Sheet "' + TARGET_SHEET_NAME + '" not found. Please create it first.');
    return;
  }

  Logger.log('[' + SCRIPT_NAME + '] Starting refresh of \'' + TARGET_SHEET_NAME + '\'...');

  // 1. Build all necessary lookup maps
  const sdeTypeMap = _buildSdeTypeMap();
  const stationMap = _buildStationMap();
  const hangarMap = _buildHangarNameMap();

  /** @type {Map<number, CorpOffice>} */
  const corpOfficesMap = new Map();
  const uniqueOtherContainers = new Map();
  const locationNameResolver = new Map(stationMap);
  const missingLocationIds = new Set();
  let allLocations = [];

  const GHOST_ITEM_IDS = new Set([
     1042136670568, 1042139243054, 1043862654421, 1038876191270, 1044532547334, 1050483607331,
     1039962719245, 1036200304791, 1047736829320, 1028141962065, 1031195155767, 1034862502178,
     1034862547753, 1040928243616, 1047961260476, 1030142093671, 1030289543328, 1031616387594,
     1033808818685, 1034429286734, 1042134935603, 1047745393662, 1047758618232, 1047959246356
  ]);
  const EXCLUDED_CONTAINER_TYPE_IDS = new Set([28317, 28318]);
  const ASSET_ID_MIN_BOUND = 100000000000;
  const NPC_STATION_ID_MAX = 70000000;

  try {
    // 2. Read Assets from Named Range (local read)
    const cachedRange = ss.getRangeByName(CACHE_RANGE_NAME);
    if (!cachedRange) {
        Logger.log(`[${SCRIPT_NAME}] CRITICAL: Named Range '${CACHE_RANGE_NAME}' not found. Run 'cacheAllCorporateAssets' first.`);
        SpreadsheetApp.getUi().alert(`Asset data not found. Run 'cacheAllCorporateAssets' first.`);
        return;
    }

    const allAssetsData = cachedRange.getValues();

    const ESI_CACHE_HEADERS = ["is_blueprint_copy", "is_singleton", "item_id", "location_flag", "location_id", "location_type", "quantity", "type_id"];
    const assetsHeader = new Map(ESI_CACHE_HEADERS.map((h, i) => [h, i]));

    const asset_itemIdIndex = assetsHeader.get('item_id');
    const asset_typeIdIndex = assetsHeader.get('type_id');
    const asset_locationIdIndex = assetsHeader.get('location_id');
    const asset_locationFlagIndex = assetsHeader.get('location_flag');

    Logger.log('[' + SCRIPT_NAME + '] Processing ' + allAssetsData.length + ' asset rows from local cache...');

    // 3. Loop assets to find Offices/Containers
    allAssetsData.forEach(row => {
      const locationId = Number(row[asset_locationIdIndex]);
      const locationFlag = row[asset_locationFlagIndex];
      const typeId = Number(row[asset_typeIdIndex]);
      const itemId = Number(row[asset_itemIdIndex]);

      if (locationFlag === 'OfficeFolder' && locationId > 60000000) {
        if (!corpOfficesMap.has(itemId)) {
          // itemId is the "Branch", locationId is the "Root"
          corpOfficesMap.set(itemId, new CorpOffice(itemId, locationId));
        }
        if (!locationNameResolver.has(locationId)) {
          missingLocationIds.add(locationId);
        }
        return;
      }

      if (itemId < ASSET_ID_MIN_BOUND) return;

      if (!corpOfficesMap.has(locationId) && !locationNameResolver.has(locationId) && locationId > 60000000) {
        missingLocationIds.add(locationId);
      }

      const typeName = sdeTypeMap.get(typeId) || "";

      if (typeName.toLowerCase().includes('container') &&
        locationId > 60000000 &&
        !EXCLUDED_CONTAINER_TYPE_IDS.has(typeId)) {
        if (!uniqueOtherContainers.has(itemId)) {
          uniqueOtherContainers.set(itemId, {
            itemId: itemId, genericName: typeName,
            locationId: locationId, locationFlag: locationFlag
          });
        }
      }
    });

    Logger.log('[' + SCRIPT_NAME + '] Found ' + corpOfficesMap.size + ' unique Office Folders.');
    Logger.log('[' + SCRIPT_NAME + '] Found ' + uniqueOtherContainers.size + ' unique top-level Containers.');

    // 4. Resolve missing Root IDs (Structures) via ESI (Still necessary for new structures)
    const universeNamesClient = getGESIUniverseNamesClient_();
    if (missingLocationIds.size > 0) {
      const missingIdsArray = Array.from(missingLocationIds).map(Number).filter(id => id < NPC_STATION_ID_MAX && id > 60000000); 
      if (missingIdsArray.length > 0) {
        try {
          const names = universeNamesClient.execute(missingIdsArray); 
          names.forEach(obj => { locationNameResolver.set(obj.id, obj.name); });
        } catch (e) {
          Logger.log('[' + SCRIPT_NAME + '] WARNING: Failed to resolve some NPC station names via ESI. Error: ' + e);
        }
      }
    }

    // 5. Get custom names for Containers via ESI (concurrently)
    let namesMap = new Map();
    const allContainerIds = [...uniqueOtherContainers.keys()];
    const validContainerIds = allContainerIds.map(Number).filter(id => !GHOST_ITEM_IDS.has(id));
    const BATCH_SIZE = 20;
    const namesClient = getGESINamesClient_();

    if (validContainerIds.length > 0) {
        let allNameRequests = [];
        for (let i = 0; i < validContainerIds.length; i += BATCH_SIZE) {
          const batch = validContainerIds.slice(i, i + BATCH_SIZE);
          if (batch.length > 0) {
            allNameRequests.push(namesClient.buildRequest({ item_ids: batch }));
          }
        }
        if (allNameRequests.length > 0) {
          const responses = UrlFetchApp.fetchAll(allNameRequests);
          responses.forEach(response => {
             if (response.getResponseCode() === 200) {
                const names = JSON.parse(response.getContentText());
                if (names && names.length > 0) {
                  names.forEach(row => { namesMap.set(row.item_id, row.name); });
                }
             }
          });
        }
    }

    Logger.log('[' + SCRIPT_NAME + '] Found ' + namesMap.size + ' *named* corp assets.');

    // 6. Build final location list (Hangars and Containers)
    corpOfficesMap.forEach(office => {
        const locationId = office.locationId;
        const stationName = locationNameResolver.get(locationId) || 'Structure ' + locationId;
        
        // Add all 7 Hangar Divisions
        hangarMap.forEach((name, flag) => {
            allLocations.push([stationName, name, office.itemId, flag, 'Hangar', '', '']);
        });
    });

    uniqueOtherContainers.forEach(container => {
        const customName = namesMap.get(container.itemId);
        const locationId = container.locationId; // Parent ID (Office or Structure)
        let stationName = "";

        // FINAL HIERARCHY FIX: Resolve parent structure name
        if (corpOfficesMap.has(locationId)) {
            const parentOffice = corpOfficesMap.get(locationId);
            const rootLocationId = parentOffice.locationId;
            stationName = locationNameResolver.get(rootLocationId) || 'Structure ' + rootLocationId;
        } else {
            stationName = locationNameResolver.get(locationId) || 'Structure ' + locationId;
        }

        const displayName = customName || container.genericName;
        // Ensure 7 columns: [Office Location, Hangar Name (Container Display), Item ID, Flag, Type, Sales?, Mat?]
        allLocations.push([stationName, '(' + displayName + ')', container.itemId, container.locationFlag, 'Container', '', '']);
    });

  } catch (e) {
    Logger.log('[' + SCRIPT_NAME + '] Error during asset processing: ' + e);
  }

  // C. Clear and Write to Sheet
  sheet.clear();
  const headers = [ 'Office Location', 'Hangar Name', 'Location ID', 'Hangar Flag', 'Type', 'IsSalesHangar', 'IsMaterialHangar' ];

  // Sort the final list
  allLocations.sort((a, b) => {
     if (a[0] < b[0]) return -1; if (a[0] > b[0]) return 1;
     if (a[1] < b[1]) return -1; if (a[1] > b[1]) return 1;
     return 0;
  });

  const outputData = [headers, ...allLocations];

  if (allLocations.length === 0) {
    Logger.log('[' + SCRIPT_NAME + '] No locations found. Sheet will be blank except for headers.');
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    return;
  }

  // Write all data
  sheet.getRange(1, 1, outputData.length, headers.length)
    .setValues(outputData);

  // D. Add Checkboxes
  const numDataRows = allLocations.length;
  if (numDataRows > 0) {
    sheet.getRange(2, headers.length - 1, numDataRows, 2)
      .insertCheckboxes();
  }

  sheet.setFrozenRows(1);
  Logger.log('[' + SCRIPT_NAME + '] Formatting skipped to prevent timeout.');
  Logger.log('[' + SCRIPT_NAME + '] \'' + TARGET_SHEET_NAME + '\' has been populated with ' + allLocations.length + ' locations.');
  SpreadsheetApp.getUi().alert('Location Manager has been refreshed! Please tag your hangars.');
}