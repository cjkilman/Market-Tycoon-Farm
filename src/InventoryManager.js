// Global placeholder for the required module-level cache
const _sheetCache = {}; 
const WRITE_CHUNK_SIZE = 500; // Define chunk size for writing

// ... (getGESIDivisionsClient_, getGESINamesClient_, getGESIUniverseNamesClient_, and CorpOffice Class are here) ...

// --- 1. PERSISTENT CLIENT HELPERS (for performance) ---

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

// ----------------------------------------------------------------------
// >> NEW: CorpOffice Class to manage the "Branch" -> "Root" hierarchy
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
 * This is the ultimate fix for the 'Exceeded maximum execution time' error,
 * using the persistent client pattern.
 */
function _fetchAssetsConcurrently(mainChar) {
    const SCRIPT_NAME = '_fetchAssetsConcurrently';
    
    // 1. Get persistent client (avoids overhead)
    const client = GESI.getClient().setFunction('corporations_corporation_assets');
    
    // ----------------------------------------------------------------------
    // >> FINAL FIX: Use UrlFetchApp.fetchAll (plural) on an array
    //    containing the single Page 1 request object.
    // ----------------------------------------------------------------------
    let maxPages = 1;
    let headerRow = ['item_id', 'is_singleton', 'location_flag', 'location_id', 'location_type', 'quantity', 'type_id']; // Define the header manually
    let allAssets = [];
    allAssets.push(headerRow); // Add header row first
    
    try {
        // 1.A. Build and Fetch Page 1
        const requestPage1 = client.buildRequest({ page: 1 });
        const responsePages = UrlFetchApp.fetchAll([requestPage1]); // Returns [HTTPResponse]
        const responsePage1 = responsePages[0]; // Get the first response from the array

        if (responsePage1.getResponseCode() < 200 || responsePage1.getResponseCode() >= 300) {
            throw new Error('Failed to fetch initial asset page. Response Code: ' + responsePage1.getResponseCode());
        }

        // 1.B. Get headers from the HTTPResponse object
        const headers = responsePage1.getHeaders();
        maxPages = Number(headers['X-Pages'] || headers['x-pages']) || 1;
        Logger.log('[' + SCRIPT_NAME + '] Found ' + maxPages + ' pages of assets. Fetching concurrently...');

        // 1.C. Process Page 1 data (Array of Objects)
        const bodyPage1 = responsePage1.getContentText();
        const dataPage1 = JSON.parse(bodyPage1);
        
        // Convert array of objects to array of arrays
        dataPage1.forEach(obj => {
            allAssets.push([
                obj.item_id, 
                obj.is_singleton, 
                obj.location_flag, 
                obj.location_id, 
                obj.location_type, 
                obj.quantity, 
                obj.type_id
            ]);
        });

    } catch (e) {
        Logger.log('[' + SCRIPT_NAME + '] CRITICAL: Failed to fetch page 1. Error: ' + e);
        // Return a blank array with an assumed header to prevent downstream crashes
        return [headerRow]; 
    }
    
    // ----------------------------------------------------------------------
    // 2. Build requests for all REMAINING pages (if any)
    // ----------------------------------------------------------------------
    let allRequests = [];
    for (let i = 2; i <= maxPages; i++) { // Start loop at Page 2
        allRequests.push(client.buildRequest({ page: i }));
    }

    // 3. Execute all remaining requests concurrently.
    if (allRequests.length > 0) {
        const responses = UrlFetchApp.fetchAll(allRequests);

        // 4. Process all responses.
        responses.forEach((response, index) => {
            const page = index + 2; // (since we started at page 2)
            
            if (response.getResponseCode() === 200) {
                try {
                    const body = response.getContentText();
                    const rawData = JSON.parse(body); // This is an Array of Objects

                    // Convert array of objects to array of arrays
                    rawData.forEach(obj => {
                        allAssets.push([
                            obj.item_id, 
                            obj.is_singleton, 
                            obj.location_flag, 
                            obj.location_id, 
                            obj.location_type, 
                            obj.quantity, 
                            obj.type_id
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

function cacheAllCorporateAssetsTrigger() {
  const funcName = 'cacheAllCorporateAssets';

  // This wrapper enforces the Script Lock check for every call, including dynamic triggers.
  const result = executeWithTryLock(cacheAllCorporateAssets, funcName);

  if (result === null) {
    // Job was skipped due to lock, so we exit silently. The orchestrator or
    // a subsequent trigger will pick it up.
    console.warn(`${funcName} skipped execution due to a concurrency lock. Will be picked up by next trigger.`);
  }
  // If it executed, the result is handled by the worker's internal reschedule/state change.
}




/**
 * NEW: Non-blocking Document Lock helper function.
 * This function writes a single chunk of data while using LockService.
 * NOTE: This relies on the global _sheetCache being populated.
 */
function _writeChunkInternal(dataChunk, startRow, numCols, sheetName) {
  const chunkStartTime = new Date().getTime();
  let writeDurationMs = 0;
  const DOC_LOCK_TIMEOUT = 5000; // TryLock 5s 

  const docLock = LockService.getDocumentLock();

  // Attempt non-blocking lock
  if (!docLock.tryLock(DOC_LOCK_TIMEOUT)) {
    return { success: false, duration: 0 };
  }

  try {
    // Now retrieving the Sheet object from the module-level map
    let workSheet = _sheetCache[sheetName];
    if (!workSheet) {
      // CRITICAL FAILURE: Cache must be hot by this point.
      throw new Error(`CRITICAL: Sheet object for '${sheetName}' not found in memory cache. Job state compromised.`);
    }

    workSheet.getRange(startRow, 1, dataChunk.length, numCols).setValues(dataChunk);


  } catch (e) {
    // Using Logger.log since console.error is not standard in Apps Script
    Logger.log(`_writeChunkInternal: Write failed while locked: ${e.message}`);
    throw e;
  } finally {
    docLock.releaseLock();
    writeDurationMs = new Date().getTime() - chunkStartTime;
  }

  return { success: true, duration: writeDurationMs };
}


/**
 * Executes the full ESI asset pull and writes the result to a local sheet.
 * This is designed to run on a background trigger (e.g., hourly).
 */
function cacheAllCorporateAssets() {
    const SCRIPT_NAME = 'cacheAllCorporateAssets';
    const CACHE_SHEET_NAME = 'CorpWarehouseStock'; // Target Sheet
    
    // Define the exact column order requested by the user
    const CORP_HEADERS =["is_blueprint_copy", "is_singleton", "item_id", "location_flag", "location_id", "location_type", "quantity", "type_id"];
    const numDataCols = CORP_HEADERS.length; // 8 columns (A:H)
    
    try {
        Logger.log('[' + SCRIPT_NAME + '] Starting full asset cache refresh.');
        
        // 1. Fetch ALL data concurrently (our anti-timeout solution)
        const mainChar = GESI.getMainCharacter();
        const allAssets = _fetchAssetsConcurrently(mainChar); // Fetches with raw ESI headers
        
        if (allAssets.length <= 1) {
            Logger.log('[' + SCRIPT_NAME + '] WARNING: No assets retrieved.');
            return;
        }

        // 2. Map ESI Data to the New Column Order
        const esiHeadersMap = new Map(allAssets[0].map((h, i) => [String(h).trim(), i]));
        const rawAssetsData = allAssets.slice(1);
        const processedAssets = [];

        // Define source indices from ESI's standard response
        const idx = {
            item_id: esiHeadersMap.get('item_id'),
            is_singleton: esiHeadersMap.get('is_singleton'),
            location_flag: esiHeadersMap.get('location_flag'),
            location_id: esiHeadersMap.get('location_id'),
            location_type: esiHeadersMap.get('location_type'),
            quantity: esiHeadersMap.get('quantity'),
            type_id: esiHeadersMap.get('type_id'),
            // 'is_blueprint_copy' is often undefined, so we check for it safely
            is_blueprint_copy: esiHeadersMap.get('is_blueprint_copy')
        };
        
        // Error check for essential columns
        if ([idx.item_id, idx.location_id, idx.type_id].some(i => i === undefined)) {
            throw new Error('CRITICAL: ESI response missing essential column headers.');
        }

        // Reorder the data
        rawAssetsData.forEach(row => {
            processedAssets.push([
                row[idx.is_blueprint_copy] || "", // Ensure this column is always present (even if blank)
                row[idx.is_singleton], 
                row[idx.item_id], 
                row[idx.location_flag], 
                row[idx.location_id], 
                row[idx.location_type], 
                row[idx.quantity], 
                row[idx.type_id]
            ]);
        });


        // 3. Prepare the target sheet and write data
        const ss = SpreadsheetApp.getActiveSpreadsheet();
        let cacheSheet = ss.getSheetByName(CACHE_SHEET_NAME);
        
        if (!cacheSheet) {
            Logger.log('[' + SCRIPT_NAME + '] ERROR: Target sheet "' + CACHE_SHEET_NAME + '" not found.');
            return;
        }
        
        // 3a. Setup the sheet cache and variables
        _sheetCache[CACHE_SHEET_NAME] = cacheSheet; // Populate the global cache
        const totalRows = processedAssets.length + 1; // Total rows including header
        
        // 4. Clear and Write Headers (Row 1)
        const desiredWidth = 8; // A through H
        cacheSheet.getRange(1, 1, cacheSheet.getMaxRows(), desiredWidth).clearContent();
        
        let headerWriteResult = _writeChunkInternal([CORP_HEADERS], 1, numDataCols, CACHE_SHEET_NAME);
        if (!headerWriteResult.success) {
             throw new Error('CRITICAL: Failed to acquire lock/write headers.');
        }

        // 5. Write data in chunks (starting from Row 2)
        for (let i = 0; i < processedAssets.length; i += WRITE_CHUNK_SIZE) {
            const chunk = processedAssets.slice(i, i + WRITE_CHUNK_SIZE);
            const startRow = 2 + i; // Data starts at Row 2
            
            let chunkResult = _writeChunkInternal(chunk, startRow, numDataCols, CACHE_SHEET_NAME);

            if (!chunkResult.success) {
                Logger.log(`[CACHE FAIL] Failed to acquire lock for writing chunk starting at row ${startRow}. Stopping.`);
                throw new Error('Lock acquisition failed during chunk write.');
            }
        }

        // 6. Define the Named Range over the exact A2:H range.
        const dataHeight = Math.max(1, totalRows - 1);

        SpreadsheetApp.getActive().setNamedRange(
            'NR_CORP_ASSETS_CACHE', 
            cacheSheet.getRange(2, 1, dataHeight, desiredWidth)
        );
        
        Logger.log('[' + SCRIPT_NAME + '] Successfully cached ' + (totalRows - 1) + ' asset rows to ' + CACHE_SHEET_NAME);

    } catch (e) {
        Logger.log('[' + SCRIPT_NAME + '] CRITICAL FAILURE: Asset caching failed: ' + e);
        throw e;
    }
}


// --------------------------------------------------------------------------------------
// THE REST OF THE LOCATION MANAGER SCRIPT CONTINUES BELOW (Unchanged)
// --------------------------------------------------------------------------------------


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
    
    // >> FIX: GESI executeRaw() for this endpoint returns the final JSON object, not a table.
    const divisionsData = divisionsClient.executeRaw({});

    // CRITICAL FIX: Check if the returned object has the 'hangar' property
    if (!divisionsData || !Array.isArray(divisionsData.hangar)) {
        Logger.log('[' + SCRIPT_NAME + '] Data structure error: divisionsData object is malformed or missing "hangar" array.');
        throw new Error('Malformed division data from ESI.');
    }

    // --- Parsing Now Safe ---
    
    // The data is already a parsed array of objects, access it directly.
    const divisions = divisionsData.hangar;

    // Process the parsed data
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
    // Rely on GESI to throw the error; catch it here and fall back.
    Logger.log('[' + SCRIPT_NAME + '] WARNING: Failed to fetch and/or parse divisions from ESI. Trying local sheet. Error: ' + e);
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

  // 3. APPLY FAILSAFE DEFAULTS to ensure all 7 divisions exist
  Object.keys(defaultHangars).forEach(flag => {
    if (!hangarMap.has(flag)) {
      hangarMap.set(flag, defaultHangars[flag]);
    }
  });

  Logger.log('[' + SCRIPT_NAME + '] Built Hangar Name map with ' + hangarMap.size + ' entries (guaranteed 7).');
  return hangarMap;
}


// --- 2. GUI-BUILDING SCRIPT (Run from Menu) ---

/**
 * Re-populates the 'LocationManager' sheet with all available
 * corp hangars and all top-level containers (named or unnamed),
 * linked to their parent station.
 * This should be run from a custom menu.
 */
function refreshLocationManager() {
  const SCRIPT_NAME = 'refreshLocationManager';
  const TARGET_SHEET_NAME = 'LocationManager';
  const mainChar = GESI.getMainCharacter();

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  if (!sheet) {
    Logger.log('[' + SCRIPT_NAME + '] Target sheet "' + TARGET_SHEET_NAME + '" not found. Please create it.');
    SpreadsheetApp.getUi().alert('Sheet "' + TARGET_SHEET_NAME + '" not found. Please create it first.');
    return;
  }

  Logger.log('[' + SCRIPT_NAME + '] Starting refresh of \'' + TARGET_SHEET_NAME + '\'...');

  // --- 1. Build all necessary lookup maps ---
  const sdeTypeMap = _buildSdeTypeMap();
  const stationMap = _buildStationMap();
  const hangarMap = _buildHangarNameMap();

  let allLocations = [];

  // --------------------------------------------------------------------------------------
  // >> REFACTOR: Use CorpOffice class and a single location name map
  // --------------------------------------------------------------------------------------
  /** @type {Map<number, CorpOffice>} */
  const corpOfficesMap = new Map(); // Key: Office "Branch" ID, Value: CorpOffice object
  const uniqueOtherContainers = new Map();
  const locationNameResolver = new Map(stationMap); // Pre-load with all local SDE stations
  const missingLocationIds = new Set();
  // --------------------------------------------------------------------------------------
  
  // This filter is for "Dead Leaf" containers that 404 the name endpoint
  const GHOST_ITEM_IDS = new Set([
    1042136670568, 1042139243054, 1043862654421, 1038876191270, 1044532547334, 1050483607331, 
    1039962719245, 1036200304791, 1047736829320, 1028141962065, 1031195155767, 1034862502178, 
    1034862547753, 1040928243616, 1047961260476, 1030142093671, 1030289543328, 1031616387594, 
    1033808818685, 1034429286734, 1042134935603, 1047745393662, 1047758618232, 1047959246356 
  ]);

  // Define items that are *not* intended for continuous storage (repackaged/transport/safety)
  const EXCLUDED_CONTAINER_TYPE_IDS = new Set([
      28317, // Small Asset Safety Wrap
      28318, // Large Asset Safety Wrap
      // Add other known Type IDs for unusable wraps/transport containers here if necessary
  ]);

  // ----------------------------------------------------------------------
  // >> NEW HELPER: Function to check if a value exists in any Office object
  // ----------------------------------------------------------------------
  /**
   * Checks if the given ID (number) exists as either the itemId or locationId 
   * in any CorpOffice object.
   */
  function isOfficeValue_(targetId) {
    for (const office of corpOfficesMap.values()) {
      if (office.itemId === targetId) {
        Logger.log(`[ORPHAN CHECK] Match: ID ${targetId} IS an Office Item ID (Branch).`);
        return true;
      }
      if (office.locationId === targetId) {
        Logger.log(`[ORPHAN CHECK] Match: ID ${targetId} IS an Office Location ID (Root).`);
        return true;
      }
    }
    return false;
  }
  // ----------------------------------------------------------------------


  try {
    // 2. Fetch ALL corp assets (CONCURRENT FIX)
    const allAssets = _fetchAssetsConcurrently(mainChar);
    if (allAssets.length <= 1) {
        Logger.log('[' + SCRIPT_NAME + '] CRITICAL: No assets returned from concurrent fetch. Aborting refresh.');
        return;
    }
    
    const assetsHeader = new Map(allAssets[0].map((h, i) => [String(h).trim(), i]));
    const allAssetsData = allAssets.slice(1);

    // Get column indices by name
    const asset_itemIdIndex = assetsHeader.get('item_id');
    const asset_typeIdIndex = assetsHeader.get('type_id');
    const asset_locationIdIndex = assetsHeader.get('location_id');
    const asset_locationFlagIndex = assetsHeader.get('location_flag');

    Logger.log('[' + SCRIPT_NAME + '] Processing ' + allAssetsData.length + ' asset rows...');

    // 3. Loop all assets to find UNIQUE top-level Offices and Containers
    const ASSET_ID_MIN_BOUND = 100000000000; // 100 Billion

    allAssetsData.forEach(row => {
      const locationId = Number(row[asset_locationIdIndex]);
      const locationFlag = row[asset_locationFlagIndex];
      const typeId = Number(row[asset_typeIdIndex]);
      const itemId = Number(row[asset_itemIdIndex]);

      // Find Office Folders (These host the 7 hangar divisions)
      if (locationFlag === 'OfficeFolder' && locationId > 60000000) {
        if (!corpOfficesMap.has(itemId)) {
          // This is a "Branch". Store it with its "Root" ID.
          corpOfficesMap.set(itemId, new CorpOffice(itemId, locationId));
        }
        // Check if the "Root" ID needs to be resolved
        if (!locationNameResolver.has(locationId)) {
             missingLocationIds.add(locationId);
        }
        return; // This is an office, we are done with this row.
      }
      
      // CRITICAL CHECK: Filter small/invalid IDs for everything ELSE
      if (itemId < ASSET_ID_MIN_BOUND) return;

      // Check if the Parent Location ID is missing from local SDE
      // This handles containers in locations that are NOT offices (e.g., player structures)
      if (!corpOfficesMap.has(locationId) && !locationNameResolver.has(locationId) && locationId > 60000000) {
          missingLocationIds.add(locationId);
      }

      // Find other top-level, usable Containers in a station/structure
      const typeName = sdeTypeMap.get(typeId) || "";

      // Check 1: Does the item name contain 'container'?
      // Check 2: Is the location a station or structure (ID > 60000000)?
      // Check 3: Is it NOT an OfficeFolder? (Already handled above)
      // Check 4: Is its Type ID *NOT* on our excluded list?
      if (typeName.toLowerCase().includes('container') &&
          locationId > 60000000 &&
          !EXCLUDED_CONTAINER_TYPE_IDS.has(typeId))
      {
        if (!uniqueOtherContainers.has(itemId)) {
          uniqueOtherContainers.set(itemId, {
            itemId: itemId,
            genericName: typeName,
            locationId: locationId,
            locationFlag: locationFlag
          });
        }
      }
    });

    Logger.log('[' + SCRIPT_NAME + '] Found ' + corpOfficesMap.size + ' unique Office Folders.');
    Logger.log('[' + SCRIPT_NAME + '] Found ' + uniqueOtherContainers.size + ' unique top-level Containers.');

    // ----------------------------------------------------------------------
    // >> NEW DIAGNOSTIC STEP: Check Ghost IDs against the live Office Map
    // ----------------------------------------------------------------------
    let trulyOrphanedCount = 0;
    Logger.log('--- DIAGNOSTIC: Checking Ghost IDs against Active Offices ---');
    for (const ghostId of GHOST_ITEM_IDS) {
      if (isOfficeValue_(ghostId)) {
        Logger.log(`[FILTER ERROR] ID ${ghostId} IS an active Office component and should NOT be filtered!`);
      } else {
        Logger.log(`[FILTER SUCCESS] ID ${ghostId} is NOT an active Office component.`);
        trulyOrphanedCount++;
      }
    }
    Logger.log(`--- DIAGNOSTIC: ${GHOST_ITEM_IDS.size - trulyOrphanedCount} IDs are potentially valid Office components. ---`);
    // ----------------------------------------------------------------------

    // --------------------------------------------------------------------------------------
    // >> UNIVERSE ID FALLBACK: Resolve all "Root" IDs (Stations/Structures)
    // --------------------------------------------------------------------------------------
    const NPC_STATION_ID_MAX = 70000000;
    const universeNamesClient = getGESIUniverseNamesClient_();

    if (missingLocationIds.size > 0) {
        const missingIdsArray = Array.from(missingLocationIds)
            .map(Number)
            .filter(id => id < NPC_STATION_ID_MAX && id > 60000000); 
            
        if (missingIdsArray.length > 0) {
            try {
                const names = universeNamesClient.execute([missingIdsArray]);
                names.forEach(obj => {
                    if (obj.name) {
                        locationNameResolver.set(obj.id, obj.name); // Add to our master name list
                    }
                });
                Logger.log('[' + SCRIPT_NAME + '] Successfully resolved ' + names.length + ' missing NPC station names via ESI.');
            } catch (e) {
                Logger.log('[' + SCRIPT_NAME + '] WARNING: Failed to resolve some NPC station names via ESI. Error: ' + e);
            }
        }
    }
    // --------------------------------------------------------------------------------------


    // 4. Get the custom names for ALL unique "Leaf" Containers
    let namesMap = new Map();

    const allContainerIds = [...uniqueOtherContainers.keys()];

    // >> FINAL FIX: Filter out the corrupt "Dead Leaf" container IDs
    const validContainerIds = allContainerIds
      .map(Number)
      .filter(id => !GHOST_ITEM_IDS.has(id));

    const BATCH_SIZE = 20;
    const namesClient = getGESINamesClient_();
    
    let allNameRequests = [];

    // Build all requests in batches
    for (let i = 0; i < validContainerIds.length; i += BATCH_SIZE) {
        const batch = validContainerIds.slice(i, i + BATCH_SIZE);
        if (batch.length > 0) {
            allNameRequests.push(namesClient.buildRequest({ item_ids: batch }));
        }
    }

    // Execute all name requests concurrently
    if (allNameRequests.length > 0) {
        try {
            const responses = UrlFetchApp.fetchAll(allNameRequests);
            
            responses.forEach(response => {
                if (response.getResponseCode() === 200) {
                    
                    const names = JSON.parse(response.getContentText()); // This is [ {item_id, name}, ... ]
                    
                    if (names && names.length > 0) {
                        // Iterate directly over the objects. No header. No slice(1).
                        names.forEach(row => {
                            namesMap.set(row.item_id, row.name);
                        });
                    }
                } else {
                    Logger.log('[' + SCRIPT_NAME + '] WARNING: A name lookup batch failed with code: ' + response.getResponseCode());
                }
            });
        } catch (e) {
            Logger.log('[' + SCRIPT_NAME + '] CRITICAL: UrlFetchApp.fetchAll for names failed. Error: ' + e);
        }
    }
    // --------------------------------------------------------------------------------------


    Logger.log('[' + SCRIPT_NAME + '] Found ' + namesMap.size + ' *named* corp assets.');

    // 5. Add ALL Hangars (N x 7 expected) to the final list
    corpOfficesMap.forEach(office => {

      if (!office || !office.itemId) {
        Logger.log('[refreshLocationManager] WARNING: Skipping office due to missing data: ' + JSON.stringify(office));
        return; // Skip this corrupt office entry
      }
      
      // Resolve name: Use Fallback, then SDE/local, then default to "Structure ID"
      const locationId = office.locationId;
      const stationName = locationNameResolver.get(locationId) || 'Structure ' + locationId;

      // Now, for EACH office, add all 7 Hangar Divisions under it (FIX: Guaranteed 7 divisions)
      hangarMap.forEach((name, flag) => {
        // [Office Location, Hangar Name, Location ID (Office Item ID), Hangar Flag, Type, IsSalesHangar, IsMaterialHangar]
        allLocations.push([stationName, name, office.itemId, flag, 'Hangar', '', '']);
      });
    });

    // 6. Add all unique Other Containers
    uniqueOtherContainers.forEach(container => {
        const customName = namesMap.get(container.itemId);
        const locationId = container.locationId; // This is the container's *parent* ID (the "Branch" ID).
        
        let stationName = "";
        
        // --------------------------------------------------------------------------------------
        // >> FINAL HIERARCHY FIX: Check if the container's location is an Office Folder
        // --------------------------------------------------------------------------------------
        if (corpOfficesMap.has(locationId)) {
            // This container is inside an Office Folder. Get the Office's "Root" ID.
            const parentOffice = corpOfficesMap.get(locationId);
            const rootLocationId = parentOffice.locationId;
            // Resolve the "Root" name from our master list
            stationName = locationNameResolver.get(rootLocationId) || stationMap.get(rootLocationId) || 'Structure ' + rootLocationId;
        } else {
            // This container is in a root hangar (or player structure). Look up its "Branch" ID directly.
            stationName = locationNameResolver.get(locationId) || stationMap.get(locationId) || 'Structure ' + locationId;
        }
        // --------------------------------------------------------------------------------------

        // Use custom name if available, otherwise use the generic item type name
        const displayName = customName || container.genericName;
        const hangarName = hangarMap.get(container.locationFlag) || container.locationFlag;

        // Ensure 7 columns for consistency with headers
        allLocations.push([stationName, '(' + displayName + ')', container.itemId, container.locationFlag, 'Container', '', '']);
    });

  } catch (e) {
    Logger.log('[' + SCRIPT_NAME + '] Error during asset processing: ' + e);
    // CRASH SAFEGUARD: Ensure headers are written even on error to prevent a total blank sheet
    sheet.getRange(1, 1, 1, 7).setValues([['Office Location', 'Hangar Name', 'Location ID', 'Hangar Flag', 'Type', 'IsSalesHangar', 'IsMaterialHangar']]);
  }

  // --- C. Clear and Write to Sheet ---
  sheet.clear();
  const headers = [
    'Office Location', 'Hangar Name', 'Location ID', 'Hangar Flag', 'Type', 'IsSalesHangar', 'IsMaterialHangar'
  ];

  // Sort the final list by Office, then by Hangar Name
  allLocations.sort((a, b) => {
    if (a[0] < b[0]) return -1; // Sort by Office Location
    if (a[0] > b[0]) return 1;
    if (a[1] < b[1]) return -1; // Sort by Hangar Name
    if (a[1] > b[1]) return 1;
    return 0;
  });

  const outputData = [headers, ...allLocations];

  if (allLocations.length === 0) {
    Logger.log('[' + SCRIPT_NAME + '] No locations found. Sheet will be blank except for headers.');
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);
    return;
  }

  // Use the fixed headers.length (7) for column count
  sheet.getRange(1, 1, outputData.length, headers.length)
    .setValues(outputData);

  // --- D. Add Checkboxes ---
  const numDataRows = allLocations.length;
  if (numDataRows > 0) {
    // Add checkboxes to the last 2 columns
    sheet.getRange(2, headers.length - 1, numDataRows, 2)
      .insertCheckboxes();
  }

  // Formatting
  sheet.setFrozenRows(1);
  Logger.log('[' + SCRIPT_NAME + '] Formatting (banding, resize) skipped to prevent timeout.');

  Logger.log('[' + SCRIPT_NAME + '] \'' + TARGET_SHEET_NAME + '\' has been populated with ' + allLocations.length + ' locations.');

  // FIX 3: Comment out the disruptive alert/toast pop-up.
  // SpreadsheetApp.getUi().alert('Location Manager has been refreshed! Please tag your hangars.');
}