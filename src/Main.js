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
// This constant MUST be declared here once for the entire project to prevent
// the "already been declared" error in other files (like SDE_Job_Controller.gs).
const SCRIPT_PROPS = PropertiesService.getScriptProperties();

/**
 * NEW: Helper to check the lock (Logic is in SDE_Job_Controller.gs, check uses this property)
 */
function isSdeJobRunning() {
  // Checks the master lock property defined in the SDE_Job_Controller.gs file
  return SCRIPT_PROPS.getProperty('SDE_JOB_RUNNING') === 'true';
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
    .addItem('Run Industry Ledger Update', '_runIndustryLedgerUpdate_MENU') 
    .addToUi();
}

/**
 * Installable onEdit trigger.
 * Calls generateRestockQuery() if a relevant filter cell is changed.
 * @param {Object} e The event object.
 */
function respondToEdit(e) { // <-- RENAMED from onEdit
  // Exit if no event object (e.g., running from script editor)
  if (!e) {
    return;
  }

  const sheet = e.range.getSheet();
  const sheetName = sheet.getName();
  const cellA1 = e.range.getA1Notation();
  
  // --- Define the sheet and cells that should trigger the query ---
  const TRIGGER_SHEET = 'Need To Buy';
  
  // All filter cells that generateRestockQuery reads
  const TRIGGER_CELLS = [
    'B5',  // Min Days
    'B7',  // Target Days
    'B9',  // Margin
    'B11', // Group Filter
    'B13', // Sort Direction
    'B14', // Sort Column
    'B19', // Limit
    'B21', // Volume Type
    'B22', // Volume Value
    'B26'  // Ignore Groups
  ];

  // --- Check if the edit was in the right place ---
  if (sheetName === TRIGGER_SHEET && TRIGGER_CELLS.includes(cellA1)) {
    // If we edit a trigger cell on the correct sheet, run the function.
    generateRestockQuery();
  }
}

/**
 * Executes the complex Restock List logic.
 * This is a lightweight function called by the onEdit trigger.
 * It reads all inputs from their *correct* new cell locations,
 * reads FEE_RATE and TAX_RATE from named ranges,
 * builds the stable QUERY string, and writes the final formula to 'Need To Buy'!C4.
 */
function generateRestockQuery() {
  const SCRIPT_NAME = 'generateRestockQuery';
  const TARGET_SHEET_NAME = 'Need To Buy';
  const DATA_SHEET_NAME = 'MarketOverviewData'; // Name of the source data sheet
  const TARGET_CELL = 'C4';

  // --- STABILIZED COLUMN INDICES (UPDATED FOR CURRENT CSV) ---
  const COL = {
    ITEM_NAME: 'Col2',
    GROUP: 'Col3',
    QUANTITY_LEFT: 'Col6',        // "Quantity Left"
    BUY_ORDER_QTY: 'Col18',       
    VOLUME: 'Col22',              // "30-day traded volume"
    MARKET_VOLUME: 'Col23',       // "Listed Volume (Feed Sell)"
    EFFECTIVE_VELOCITY: 'Col30',  // "Effective Daily Velocity"
    WAREHOUSE_QTY: 'Col31',       // "Warehouse Qty"
    DAYS_OF_INV: 'Col32',         // "Days of Inventory"
    TOTAL_MARKET_QTY: 'Col36',    // "Total Market Quantity"
    MEDIAN_BUY: 'Col40',          // "Hub Median Buy"
    MARGIN: 'Col47',              // "Margin"
    BUY_ACTION: 'Col53'           // "Buy Action"
  };

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  const dataSheet = ss.getSheetByName(DATA_SHEET_NAME);

  if (!sheet) {
    Logger.log(`Target sheet ${TARGET_SHEET_NAME} not found.`);
    return;
  }
  if (!dataSheet) {
    Logger.log(`Data sheet ${DATA_SHEET_NAME} not found.`);
    return;
  }
  
  // --- 1. READ DYNAMIC FILTERS from 'Need To Buy' (Extended Layout) ---
  const filterValues = sheet.getRange('B5:B26').getValues();

  const filterMinDays = parseFloat(filterValues[0][0]) || 0;      // B5
  const filterTargetDays = parseFloat(filterValues[2][0]) || 0;   // B7
  const filterMargin = parseFloat(filterValues[4][0]) || 0;       // B9
  const filterGroup = filterValues[6][0];        // B11
  const sortDirection = filterValues[8][0];      // B13
  const sortColumnHeader = filterValues[9][0];   // B14
  const limitNum = filterValues[14][0];          // B19
  const filterVolumeType = filterValues[16][0];  // B21
  const filterVolumeValue = filterValues[17][0]; // B22
  const filterIgnoreGroups = filterValues[21][0]; // B26

  // --- 1.5. READ NAMED RANGES for Fee and Tax Rates ---
  let FEE_RATE = 0;
  try {
    const feeRange = ss.getRangeByName("FEE_RATE");
    FEE_RATE = parseFloat(feeRange ? feeRange.getValue() : 0) || 0;
  } catch (e) {
    Logger.log(`Named Range FEE_RATE not found or invalid: ${e}`);
  }
  
  let TAX_RATE = 0;
  try {
    const taxRange = ss.getRangeByName("TAX_RATE");
    TAX_RATE = parseFloat(taxRange ? taxRange.getValue() : 0) || 0;
  } catch (e) {
    Logger.log(`Named Range TAX_RATE not found or invalid: ${e}`);
  }

  const rateMultiplier = (1 + FEE_RATE + TAX_RATE);

  // --- 2. BUILD THE SQL STRING (STABILIZED) ---

  const restockQuantityCalc = `(${COL.EFFECTIVE_VELOCITY}*${filterTargetDays})-(${COL.WAREHOUSE_QTY}+${COL.QUANTITY_LEFT})`;
  const orderCostCalc = `(${restockQuantityCalc})*${COL.MEDIAN_BUY}*${rateMultiplier}`;

  // SELECT Clause
  const sqlSelect = `SELECT ${COL.ITEM_NAME}, ${restockQuantityCalc}, ${COL.MEDIAN_BUY}, ${orderCostCalc}, ${COL.TOTAL_MARKET_QTY}, ${COL.VOLUME}, ${COL.MARKET_VOLUME}, ${COL.WAREHOUSE_QTY}, ${COL.MARGIN}, ${COL.BUY_ACTION}`;

  // WHERE Clause
  let sqlWhere = `WHERE (${COL.DAYS_OF_INV}<${filterMinDays} AND ${COL.MARGIN}>=${filterMargin} AND ${COL.ITEM_NAME} IS NOT NULL AND ${restockQuantityCalc} > 0 AND NOT ${COL.BUY_ACTION} CONTAINS 'SKIP'`;

  // Ignore Group Filter
  if (filterIgnoreGroups && filterIgnoreGroups.toString().trim() !== "") {
    const groupsToExclude = filterIgnoreGroups.toString()
      .split(',')
      .map(g => `'${g.trim().toLowerCase().replace(/'/g, `''`)}'`)
      .join('|');
    sqlWhere += ` AND NOT LOWER(${COL.GROUP}) MATCHES (${groupsToExclude})`;
  }
  
  // Volume Filters
  const numVolumeValue = parseFloat(filterVolumeValue);
  switch (filterVolumeType) {
    case "30 Day Active": sqlWhere += ` AND ${COL.VOLUME}>0`; break;
    case "Nonactive Sellers": sqlWhere += ` AND ${COL.VOLUME}=0`; break;
    case "30 Top Sellers": if (!isNaN(numVolumeValue)) sqlWhere += ` AND ${COL.VOLUME}<${numVolumeValue}`; break;
    case "30 Low Sellers": if (!isNaN(numVolumeValue)) sqlWhere += ` AND ${COL.VOLUME}>${numVolumeValue}`; break;
  }

  // Group Filter
  if (filterGroup && filterGroup.toString().trim() !== "") {
    const safeFilterGroup = filterGroup.toString().toLowerCase().replace(/'/g, `''`);
    sqlWhere += ` AND LOWER(${COL.GROUP}) CONTAINS '${safeFilterGroup}'`;
  }
  
  sqlWhere += `)`; 

  // SORT COLUMN
  let sortCol = "Col2"; 
  switch (sortColumnHeader.toString().trim()) {
    case 'Item Name': sortCol = "Col1"; break;
    case 'Quantity': sortCol = "Col2"; break;
    case 'Median Buy Price': sortCol = "Col3"; break; 
    case 'Order Cost': sortCol = "Col4"; break;
    case 'Total Market Quantity': sortCol = "Col5"; break;
    case '30-day traded volume':
    case 'Volume': sortCol = "Col6"; break;
    case 'Listed Volume (Feed Sell)':
    case 'Market_Volume': sortCol = "Col7"; break;
    case 'Warehouse Qty': sortCol = "Col8"; break;
    case 'Margin': sortCol = "Col9"; break;
    case 'Buy Action': sortCol = "Col10"; break; 
  }

  const orderBySql = `ORDER BY ${sortCol} ${sortDirection}`;
  const limitSql = (limitNum == "No Limit" || !limitNum || !limitNum == 0) ? "" : `LIMIT ${limitNum}`;

  // LABEL Clause
  const sqlLabel = `LABEL ${restockQuantityCalc} 'Quantity', ${COL.MEDIAN_BUY} 'Median Buy Price', ${orderCostCalc} 'Order Cost', ${COL.BUY_ACTION} 'Buy Action'`;
  
  // --- DYNAMIC DATA RANGE CALCULATION ---
  // Get the last row with data from the MarketOverviewData sheet
  const lastRow = dataSheet.getLastRow();
  // Ensure we include the full width (B to BB) and the full height (3 to lastRow)
  const dataRangeRef = `'${DATA_SHEET_NAME}'!B3:BB${lastRow}`;

  const finalQueryString = [sqlSelect, sqlWhere, orderBySql, limitSql, sqlLabel].join(' ');
  const finalFormula = `=IF(Utility!B3<>1,, QUERY(${dataRangeRef}, "${finalQueryString.trim()}", 1))`;

  sheet.getRange(TARGET_CELL).setFormula(finalFormula);
  Logger.log(`Successfully updated restock query in ${TARGET_CELL} with range ${dataRangeRef}.`);
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

function updateControlSheet() {
  if (typeof isSdeJobRunning !== 'undefined' && isSdeJobRunning()) { 
    Logger.log("updateControlSheet skipped: SDE Job is running.");
    return;
  }

  // --- CONFIGURATION ---
  const CONFIG = {
    ITEM_SHEET_NAME: 'MarketOverviewData', 
    LOCATION_SHEET_NAME: 'Location List',
    CONTROL_SHEET_NAME: 'Market_Control',
    SDE_SHEET_NAME: 'SDE_invTypes',
    ITEM_NAME_HEADERS: ['Item Name', 'TypeName', 'Type Name', 'Name', 'Item'],
    ITEM_ID_HEADERS: ['TypeID', 'Type ID', 'Item ID'], 
    LOC_HEADERS: ['Station', 'System', 'Region'] 
  };

  const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('CONTROL_GEN') : console);
  log.info(`Starting Rebuild with Sort & Dedupe. Source: ${CONFIG.ITEM_SHEET_NAME} & ${CONFIG.LOCATION_SHEET_NAME}`);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const itemSheet = ss.getSheetByName(CONFIG.ITEM_SHEET_NAME);
  const locationSheet = ss.getSheetByName(CONFIG.LOCATION_SHEET_NAME);
  const controlSheet = ss.getSheetByName(CONFIG.CONTROL_SHEET_NAME);
  const sdeSheet = ss.getSheetByName(CONFIG.SDE_SHEET_NAME);

  if (!itemSheet || !locationSheet || !controlSheet) throw new Error("Missing required sheets.");

  // =================================================================
  // 1. READ ITEMS
  // =================================================================
  let uniqueItemIds = [];
  const itemDataRaw = itemSheet.getDataRange().getValues();
  if (itemDataRaw.length < 2) throw new Error("Item sheet is empty.");

  let itemHeaderRowIdx = -1, nameColIdx = -1, idColIdx = -1;

  for (let r = 0; r < Math.min(5, itemDataRaw.length); r++) {
    const row = itemDataRaw[r].map(c => String(c).trim().toLowerCase());
    CONFIG.ITEM_NAME_HEADERS.forEach(h => { if (row.indexOf(h.toLowerCase()) > -1) { nameColIdx = row.indexOf(h.toLowerCase()); itemHeaderRowIdx = r; } });
    CONFIG.ITEM_ID_HEADERS.forEach(h => { if (row.indexOf(h.toLowerCase()) > -1) { idColIdx = row.indexOf(h.toLowerCase()); itemHeaderRowIdx = r; } });
    if (itemHeaderRowIdx > -1) break;
  }

  if (itemHeaderRowIdx === -1) throw new Error(`Could not find Item headers in '${CONFIG.ITEM_SHEET_NAME}'.`);

  const rawItems = [];
  if (idColIdx > -1) {
    for (let i = itemHeaderRowIdx + 1; i < itemDataRaw.length; i++) {
      const val = itemDataRaw[i][idColIdx];
      if (Number(val) > 0) rawItems.push(Number(val));
    }
  } else if (nameColIdx > -1 && sdeSheet) {
    const sdeVals = sdeSheet.getRange('A2:C' + sdeSheet.getLastRow()).getValues();
    const typeMap = new Map(sdeVals.map(r => [String(r[2]).trim().toLowerCase(), r[0]]));
    for (let i = itemHeaderRowIdx + 1; i < itemDataRaw.length; i++) {
      const name = String(itemDataRaw[i][nameColIdx]).trim().toLowerCase();
      if (name && typeMap.has(name)) rawItems.push(typeMap.get(name));
    }
  }

  uniqueItemIds = Array.from(new Set(rawItems));
  log.info(`Found ${uniqueItemIds.length} unique items.`);

  // =================================================================
  // 2. READ LOCATIONS
  // =================================================================
  const locHeaderRowIdx = 4; // Row 5 (based on your provided CSV structure)
  const locDataRaw = locationSheet.getDataRange().getValues();
  if (locDataRaw.length <= locHeaderRowIdx) throw new Error("Location sheet too short.");

  const headerRow = locDataRaw[locHeaderRowIdx].map(c => String(c).trim().toLowerCase());
  const colMap = {};
  CONFIG.LOC_HEADERS.forEach(h => colMap[h] = headerRow.indexOf(h.toLowerCase()));

  const locSet = new Set();
  for (let i = locHeaderRowIdx + 1; i < locDataRaw.length; i++) {
    const row = locDataRaw[i];
    CONFIG.LOC_HEADERS.forEach(type => {
      const idx = colMap[type];
      if (idx > -1) {
        const val = row[idx];
        if (val && !isNaN(val) && Number(val) > 0) locSet.add(`${type}|${val}`);
      }
    });
  }

  const locations = Array.from(locSet).map(s => {
    const parts = s.split('|');
    return { type: parts[0], id: Number(parts[1]) };
  });
  log.info(`Found ${locations.length} unique locations.`);

  // =================================================================
  // 3. GENERATE, DEDUPE, & SORT
  // =================================================================
  
  const runLocked = (typeof withSheetLock !== 'undefined') ? withSheetLock : (cb) => cb();

  runLocked(function () {
    controlSheet.clear();
    const headers = [['type_id', 'location_type', 'location_id', 'last_updated']];
    controlSheet.getRange(1, 1, 1, 4).setValues(headers);

    let output = [];
    
    // Generate Rows
    for (const loc of locations) {
      for (const itemId of uniqueItemIds) {
        output.push([itemId, loc.type, loc.id, '']);
      }
    }

    // --- DEDUPLICATION ---
    const seen = new Set();
    const dedupedOutput = [];
    for (const row of output) {
      const key = `${row[0]}_${row[1]}_${row[2]}`; // type_id_loctype_locid
      if (!seen.has(key)) {
        seen.add(key);
        dedupedOutput.push(row);
      }
    }
    
    log.info(`Deduplication: Removed ${output.length - dedupedOutput.length} duplicates.`);

    // --- SORTING (GROUP BY LOCATION) ---
    // Order: Location ID -> Location Type -> Type ID
    dedupedOutput.sort((a, b) => {
      // 1. Location ID (Index 2)
      if (a[2] !== b[2]) return a[2] - b[2]; 
      
      // 2. Location Type (Index 1) - Tie breaker for ID
      if (a[1] !== b[1]) return a[1].localeCompare(b[1]);
      
      // 3. Type ID (Index 0) - Sort items within location
      return a[0] - b[0]; 
    });

    if (dedupedOutput.length > 0) {
      controlSheet.getRange(2, 1, dedupedOutput.length, 4).setValues(dedupedOutput);
      
      const maxRows = controlSheet.getMaxRows();
      if (maxRows > dedupedOutput.length + 1) {
        controlSheet.deleteRows(dedupedOutput.length + 2, maxRows - (dedupedOutput.length + 1));
      }
    }
    log.info(`Successfully wrote ${dedupedOutput.length} sorted rows.`);
  });
  
  SpreadsheetApp.getUi().alert(`Done. Rebuilt Control Sheet (Grouped by Location).`);
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

