/**
 * Main.js
 * * Entry point for custom menus and trigger setup.
 * All core logic is in other modules.
 */

/* global SpreadsheetApp, Logger, GESI, UrlFetchApp, PropertiesService, 
   refreshData, forceAuthorization, Full_Recalculate_Cycle, 
   sde_job_START, sde_job_FINALIZE, isSdeJobRunning, 
   runIndustryLedgerUpdate */

// --- SDE Job Control Globals ---

/**
 * NEW: Helper to check the lock (Logic is in SDE_Job_Controller.gs, check uses this property)
 */
function isSdeJobRunning() {
  // Checks the master lock property defined in the SDE_Job_Controller.gs file
  return SCRIPT_PROPS.getProperty('SDE_JOB_RUNNING') === 'true';
}

/**
 * Executes the complex Restock List logic in the background.
 * This final version completely stabilizes the QUERY string by using fixed ColN indices 
 * and removing the slow sqlFromHeaderNamesEx function calls.
 * Target: Need To Buy!C4
 */
function generateRestockQuery() {
  const SCRIPT_NAME = 'generateRestockQuery';
  const TARGET_SHEET_NAME = 'Need To Buy'; 
  const TARGET_CELL = 'C4'; 
  
  // *** FINAL STABILIZED COLUMN INDICES ***
  // These are the fixed, unbreakable references from MarketOverviewData!B3:BA687.
  const COL = {
    ITEM_NAME: 'Col2',
    TARGET_QTY: 'Col4',
    WAREHOUSE_QTY: 'Col28',
    BUY_ORDER_QTY: 'Col15',
    MARGIN: 'Col45',
    DAYS_OF_INV: 'Col29',
    TOTAL_MARKET_QTY: 'Col33',
    VOLUME: 'Col20',           
    MARKET_VOLUME: 'Col21',    
    GROUP: 'Col3'              
  };

  // NOTE: This removes all calls to the custom function.
  executeWithWaitLock(() => {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
    if (!sheet) {
      Logger.log(`Target sheet ${TARGET_SHEET_NAME} not found.`);
      return;
    }

    // --- 1. READ DYNAMIC FILTERS ---
    const filterValues = sheet.getRange('B5:B18').getValues(); 
    const filterDaysTarget = filterValues[0][0];
    const filterMargin = filterValues[2][0];
    const filterGroup = filterValues[4][0]; // B9 (The Group Filter)
    const sortDirection = filterValues[6][0];
    const sortColumnHeader = filterValues[8][0]; 
    const limitNum = filterValues[13][0];
    
    // --- 2. STABILIZE GROUP FILTER LOGIC (Handles B9 empty case) ---
    let groupWhereClause = '';
    if (filterGroup) {
      // Escape single quotes and convert to lower case for the SQL LIKE/CONTAINS
      const safeFilterGroup = filterGroup.toString().toLowerCase().replace(/'/g, `''`);
      
      // If B9 has a value, include the filter
      groupWhereClause = ` AND LOWER(${COL.GROUP}) Contains '${safeFilterGroup}'`;
    }
    // If B9 is blank, groupWhereClause remains empty, correctly filtering ALL groups.
    
    // --- 3. STABILIZE SORT COLUMN ---
    let sortCol = "Col2"; 
    switch (sortColumnHeader.toString().trim()) {
      case 'Quantity': 
      case 'TARGET_QTY': 
        sortCol = "Col2"; break;
      case 'Total Market Quantity': sortCol = "Col3"; break;
      case '30-day traded volume': 
      case 'Volume': 
        sortCol = "Col4"; break;
      case 'Listed Volume (Feed Sell)': 
      case 'Market_Volume': 
        sortCol = "Col5"; break;
      case 'Warehouse Qty': sortCol = "Col6"; break;
      case 'Margin': sortCol = "Col7"; break;
      case 'Item Name': sortCol = "Col1"; break;
      default: sortCol = "Col2"; break;
    }
    
    // --- 4. CONSTRUCT FINAL QUERY STRING ---
    
    // SELECT: Fixed ColN indices
    const sqlSelect = `SELECT ${COL.ITEM_NAME}, ${COL.TARGET_QTY}-${COL.WAREHOUSE_QTY}, ${COL.TOTAL_MARKET_QTY}, ${COL.VOLUME}, ${COL.MARKET_VOLUME}, ${COL.WAREHOUSE_QTY}, ${COL.MARGIN}`;

    // WHERE: Combined fixed ColN and dynamic group filter logic
    const sqlWhere = `WHERE (${COL.BUY_ORDER_QTY} is null AND ${COL.DAYS_OF_INV} < ${filterDaysTarget} AND ${COL.MARGIN} >= ${filterMargin} ${groupWhereClause} AND ${COL.ITEM_NAME} IS NOT NULL)`;

    const sqlLabel = `LABEL ${COL.TARGET_QTY}-${COL.WAREHOUSE_QTY} 'Quantity'`;
    
    const orderBySql = `ORDER BY ${sortCol} ${sortDirection}`;
    const limitSql = (limitNum == "No Limit" || !limitNum || limitNum == 0) ? "" : `LIMIT ${limitNum}`;
    
    const dataRangeRef = 'MarketOverviewData!B3:BA687';
    
  // The final constructed string is assembled using an Array.join(' ') for guaranteed spacing.
const queryClauses = [
    sqlSelect, 
    sqlWhere, 
    orderBySql, 
    limitSql,
    sqlLabel
];

// 1. Join all clauses with a single space.
const finalQueryString = queryClauses.join(' '); 

const finalFormula = `=IF(Utility!B3<>1,, QUERY(${dataRangeRef}, "${finalQueryString.trim()}", 1))`;

// 3. WRITE TO SHEET (Target: Need To Buy!C4)
sheet.getRange(TARGET_CELL).setFormula(finalFormula);
    
    
    Logger.log(`Successfully updated restock query in ${TARGET_CELL}.`);

  }, SCRIPT_NAME); 
}

function onOpen() {
  var ui = SpreadsheetApp.getUi();
  // Or DocumentApp or FormApp.
  ui.createMenu('Sheet Tools')
    .addItem('Refresh All Data', 'refreshData')
    .addItem('Update SDE Data', 'sde_job_START')        // <-- NEW: Starts the robust stateful job
    .addItem('CANCEL SDE Update', 'sde_job_FINALIZE')    // <-- NEW: Manual cleanup
    .addItem('Authorize Script (First Run)', 'forceAuthorization')
    .addItem("Recalculate/Refresh", "Full_Recalculate_Cycle")
    .addSeparator() 
    .addItem('Reset All Job History', 'resetIndustryLedgerProperties')
    .addItem('Run Industry Ledger Update', '_runIndustryLedgerUpdate_MENU') 
    .addToUi();
}

/**
 * NEW: Wrapper for menu item to run the Industry Ledger update.
 */
function _runIndustryLedgerUpdate_MENU() {
  if (isSdeJobRunning()) { // <-- MASTER LOCK CHECK
    SpreadsheetApp.getUi().alert('Cannot run: SDE Update is in progress. Please wait.');
    return;
  }
  if (typeof runIndustryLedgerUpdate === 'function') {
    runIndustryLedgerUpdate();
    SpreadsheetApp.getUi().alert('Industry Ledger update complete. Check Material_Ledger for new entries.');
  } else {
    SpreadsheetApp.getUi().alert('Error: runIndustryLedgerUpdate function not found. Make sure IndustryLedger.gs.js is saved.');
  }
}

function getStructureNames(structureIDs) {
  if (!(Array.isArray(structureIDs))) { structureIDs = [[structureIDs]] };
  var output = [];
  for (var i = 0; i < structureIDs.length; i++) {
    var data = GESI.universe_structures_structure(structureIDs[i][0], GESI.getMainCharacter(), false);
    output.push(data[0][0]);
  }
  return output;
}



/**
 * Rebuilds the 'Market_Control' sheet.
 */
function updateControlSheet() {
  if (isSdeJobRunning()) { 
    Logger.log("updateControlSheet skipped: SDE Job is running.");
    return;
  }

  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('CONTROL_GEN') : console);
  log.info('Starting Market_Control sheet rebuild with last_updated column...');

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const itemMasterSheetName = 'Item List';
  const sdeSheetName = 'SDE_invTypes';
  const locationListSheetName = 'Location List';
  const controlSheetName = 'Market_Control';

  const itemSheet = ss.getSheetByName(itemMasterSheetName);
  const sdeSheet = ss.getSheetByName(sdeSheetName);
  const locationSheet = ss.getSheetByName(locationListSheetName);
  const controlSheet = ss.getSheetByName(controlSheetName);

  if (!itemSheet || !sdeSheet || !locationSheet || !controlSheet) {
    throw new Error(`One or more required sheets are missing.`);
  }

  // 1. Create a lookup map from the SDE (typeName -> typeID)
  const sdeData = sdeSheet.getRange('A2:C' + sdeSheet.getLastRow()).getValues();
  const typeIdMap = new Map(sdeData.map(row => [String(row[2]).trim().toLowerCase(), row[0]]));

  // 2. Read Item Names from the master 'Item List' sheet
  const itemNamesRange = itemSheet.getRange('B2:B' + itemSheet.getLastRow());
  const itemIds = itemNamesRange.getValues()
    .flat()
    .map(name => name ? typeIdMap.get(name.trim().toLowerCase()) : null)
    .filter(id => Number.isFinite(id) && id > 0);

  const uniqueItemIds = Array.from(new Set(itemIds));
  log.info(`Found ${uniqueItemIds.length} unique item IDs from '${itemMasterSheetName}'.`);

  // 3. Read and Deduplicate Location IDs
  const locHeaders = locationSheet.getRange('A5:G5').getValues()[0];
  const stationColIndex = locHeaders.indexOf('Station');
  const systemColIndex = locHeaders.indexOf('System');
  const regionColIndex = locHeaders.indexOf('Region');

  const locData = locationSheet.getRange(6, 1, locationSheet.getLastRow() - 5, locHeaders.length).getValues();
  const uniqueLocationStrings = new Set();
  locData.forEach(row => {
    if (Number(row[stationColIndex]) > 0) uniqueLocationStrings.add(`station_${row[stationColIndex]}`);
    if (Number(row[systemColIndex]) > 0) uniqueLocationStrings.add(`system_${row[systemColIndex]}`);
    if (Number(row[regionColIndex]) > 0) uniqueLocationStrings.add(`region_${row[regionColIndex]}`);
  });

  const locations = Array.from(uniqueLocationStrings).map(locString => {
    const [type, id] = locString.split('_');
    return { type, id: Number(id) };
  });
  log.info(`Found ${locations.length} unique market locations.`);

  // 4. Generate and Write the Control Table Data
  // Assuming withSheetLock is defined elsewhere (e.g., Orchestrator.gs.js)
  // If not, replace with: const docLock = LockService.getDocumentLock(); docLock.waitLock(30000); try { ... } finally { docLock.releaseLock(); }
  withSheetLock(function () {
    controlSheet.clear();
    const headers = [['type_id', 'location_type', 'location_id', 'last_updated']];
    controlSheet.getRange(1, 1, 1, 4).setValues(headers);

    const outputRows = [];

    // REVERSED LOOP ORDER: Iterate over locations first, then items.
    for (const loc of locations) {
      for (const item_id of uniqueItemIds) {
        // Add a blank placeholder for the 'last_updated' timestamp
        outputRows.push([item_id, loc.type, loc.id, '']);
      }
    }

    if (outputRows.length > 0) {
      controlSheet.getRange(2, 1, outputRows.length, 4).setValues(outputRows);
      log.info(`Successfully wrote ${outputRows.length} control rows.`);
    }
  });
  SpreadsheetApp.getUi().alert(`'${controlSheetName}' has been updated successfully.`);
}

/**
 * Function to run manually to force the authorization prompt.
 */
function forceAuthorization() {
  // This function runs a service that requires authorization (UrlFetchApp)
  // and is accessible via the custom menu. Running it guarantees the prompt appears.
  try {

    // _deleteExistingTriggers(); // This function is in main.js from your other project, not this one.
    // Let's call a similar function or just skip it if it's not defined.
    if (typeof _deleteExistingTriggers === 'function') {
      _deleteExistingTriggers();
    }
    UrlFetchApp.fetch("https://google.com");
    SpreadsheetApp.getUi().alert('Authorization granted successfully!');
  } catch (e) {
    if (e.message.includes('Authorization is required')) {
      SpreadsheetApp.getUi().alert('Authorization failed. Please follow the prompt in the editor after running this function.');
    } else {
      // Check if the script needs permissions beyond basic Spreadsheet access
      const propertiesService = PropertiesService.getUserProperties();
      propertiesService.setProperty('AUTH_CHECK', 'RUNNING');
      propertiesService.deleteProperty('AUTH_CHECK');
      SpreadsheetApp.getUi().alert('Authorization check failed. Please run this function again and check the console/editor for prompts.');
    }
  }
}


/**
 *Get Character Names from thier ID
 */
function getChacterNameFromID(charIds, show_column_headings = true) {
  if (!charIds) throw "undefined charIds";
  if (!Array.isArray(charIds)) charIds = [charIds];
  charIds = charIds.filter(Number);

  let chars = [];
  if (show_column_headings) chars = chars.concat("chacacter_name");
  const rowIdx = show_column_headings ? 1 : 0;

  for (I = 0; I < charIds.length; I++) {
    try {
      const char = GESI.characters_character(Number(charIds[I]), show_column_headings);
      chars = chars.concat(char[rowIdx][7]);
    }
    catch (e) {
      throw e;
    }
  }
  Logger.log(chars);
  return chars;
}


/**
 * Replace [Header Name] tokens in a QUERY-like SQL with ColN,
 */
function sqlFromHeaderNamesEx(rangeName, queryString, useColNums) {
  
  // --- NEW: MAINTENANCE MODE CHECK ---
  // SCRIPT_PROPS must be globally defined in Main.js
  if (typeof SCRIPT_PROPS !== 'undefined' && typeof GLOBAL_STATE_KEY !== 'undefined') {
    const systemState = SCRIPT_PROPS.getProperty(GLOBAL_STATE_KEY) || 'RUNNING';
    if (systemState === 'MAINTENANCE') {
      return; // Return blank immediately
    }
  }
  // --- END: MAINTENANCE MODE CHECK ---

  if (typeof rangeName !== 'string' || !rangeName) {
      throw new Error(`sqlFromHeaderNamesEx: First argument must be the name of a Named Range (as a string) or an A1 notation string.`);
  }

  // Get the script cache
  const cache = CacheService.getScriptCache();
  const cacheKey = `headerMap_${rangeName}`;
  let map = {};

  // Try to get the map from cache
  const cachedMap = cache.get(cacheKey);

  if (cachedMap) {
    // Found it in the cache, parse it and use it
    map = JSON.parse(cachedMap);
  } else {
    // Not in cache, so we must build it
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let range = null;

    // 1. Try to resolve as a Named Range first
    range = ss.getRangeByName(rangeName); // This returns null if not found, doesn't throw.

    // 2. If not a Named Range, try to resolve as A1 notation
    if (!range) {
      try {
        range = ss.getRange(rangeName); // This *will* throw an error if invalid
      } catch (e) {
        // If both fail, throw a clear error.
        throw new Error(`sqlFromHeaderNamesEx: Could not resolve range. "${rangeName}" is not a valid Named Range or A1 notation range.`);
      }
    }
    
    // --- Build the Header Map ---
    const headerWidth = range.getNumColumns();
    // This is the expensive API call we want to run only once
    const headerRow = range.offset(0, 0, 1, headerWidth).getValues()[0];

    for (let i = 0; i < headerRow.length; i++) {
      const raw = headerRow[i];
      if (raw == null) continue;
      const h = String(raw).trim();
      if (!h) continue;

      const replacement = `Col${i + 1}`;
      map[h] = replacement;
    }
    
    // Store the newly built map in the cache for 5 minutes (300 seconds)
    cache.put(cacheKey, JSON.stringify(map), 300);
  }

  // --- Header Replacement Logic ---
  // (This part is unchanged)
  // ...
  
  // --- Replace Tokens in Query String ---
  
  // Find all [Header] tokens and replace them with their ColN equivalent
  const rewritten = String(queryString).replace(/\[([^\]]+)\]/g, (m, label) => {
    const key = String(label || "").trim();
    if (map.hasOwnProperty(key)) return map[key];
    
    // Fallback to case-insensitive check
    const found = Object.keys(map).find(k => k.toLowerCase() === key.toLowerCase());
    return found ? map[found] : m; // leave untouched if no match
  });

  return rewritten;
}

function updateControlSheet() {
    // Check master lock (assuming isSdeJobRunning is defined globally and works)
    if (isSdeJobRunning()) { 
        Logger.log("updateControlSheet skipped: SDE Job is running.");
        return;
    }
    
    const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('CONTROL_GEN') : console);
    log.info('Starting Market_Control sheet rebuild with last_updated column...');

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    
    // Define Sheet Names
    const itemSourceSheetName = "MarketOverviewData"; 
    const locationListSheetName = 'Location List';
    const controlSheetName = 'Market_Control';

    // Retrieve Sheets
    const itemSheet = ss.getSheetByName(itemSourceSheetName); 
    const locationSheet = ss.getSheetByName(locationListSheetName);
    const controlSheet = ss.getSheetByName(controlSheetName);

    if (!itemSheet || !locationSheet || !controlSheet) {
        throw new Error(`One or more required sheets are missing (MarketOverviewData, Location List, Market_Control).`);
    }

    // 1. Read Item IDs directly from the new source sheet (MarketOverviewData!B4:B)
    const itemIdsRange = itemSheet.getRange('B4:B' + itemSheet.getLastRow());
    const uniqueItemIds = Array.from(new Set(
        itemIdsRange.getValues()
        .flat()
        .map(Number) 
        .filter(id => Number.isFinite(id) && id > 0)
    ));
    
    log.info(`Found ${uniqueItemIds.length} unique item IDs from '${itemSourceSheetName}'.`);
    
    // 2. Read and Deduplicate Location IDs using DYNAMIC HEADERS
    const locHeaders = locationSheet.getRange('A5:G5').getValues()[0];
    
    // Define the target headers (These match the sheet's column names)
    const STATION_HEADER = 'Station';
    const SYSTEM_HEADER = 'System';
    const REGION_HEADER = 'Region';

    const stationColIndex = locHeaders.indexOf(STATION_HEADER);
    const systemColIndex = locHeaders.indexOf(SYSTEM_HEADER);
    const regionColIndex = locHeaders.indexOf(REGION_HEADER);

    const locData = locationSheet.getRange(6, 1, locationSheet.getLastRow() - 5, locHeaders.length).getValues();
    const locationMap = new Map(); // Used to enforce uniqueness (type_id_number)
    
    // --- HIERARCHY-ENFORCED PROCESSING BLOCK (Fixed Logic) ---
    locData.forEach(row => {
        let type = '';
        let id = 0;

        // 1. Check for the most specific ID: STATION
        if (stationColIndex !== -1 && Number(row[stationColIndex]) > 0) {
            type = 'station';
            id = Number(row[stationColIndex]);
        
        // 2. ONLY if no Station ID was found, check for the next most specific: SYSTEM
        } else if (systemColIndex !== -1 && Number(row[systemColIndex]) > 0) {
            type = 'system';
            id = Number(row[systemColIndex]);
        
        // 3. ONLY if neither a Station nor a System ID was found, check for REGION
        } else if (regionColIndex !== -1 && Number(row[regionColIndex]) > 0) {
            type = 'region';
            id = Number(row[regionColIndex]);
        }
        
        // If a valid ID was found, store it in the Map.
        if (id > 0) {
            // Key is 'type_id' to guarantee uniqueness across types (e.g., station_60001 != system_60001)
            locationMap.set(`${type}_${id}`, { type, id });
        }
    });
    // --- END HIERARCHY-ENFORCED PROCESSING BLOCK ---

    // Convert the Map values into the final array of objects
    const locations = Array.from(locationMap.values());
    log.info(`Found ${locations.length} unique market locations.`);

    // 3. Generate and Write the Control Table Data
    withSheetLock(function() {
        controlSheet.clear();
        const headers = [['type_id', 'location_type', 'location_id', 'last_updated']];
        controlSheet.getRange(1, 1, 1, 4).setValues(headers);

        const outputRows = [];
        
        // Generate all rows
        for (const item_id of uniqueItemIds) {
            for (const loc of locations) {
                outputRows.push([item_id, loc.type, loc.id, '']);
            }
        }
        
        // Sort the entire outputRows array by Location ID (index 2)
        outputRows.sort((a, b) => a[2] - b[2]);

        if (outputRows.length > 0) {
            controlSheet.getRange(2, 1, outputRows.length, 4).setValues(outputRows);
            log.info(`Successfully wrote ${outputRows.length} control rows.`);
        }
    });
    SpreadsheetApp.getUi().alert(`'${controlSheetName}' has been updated successfully.`);
}