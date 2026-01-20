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
 * Watches for filter changes on Dashboard sheets and triggers the appropriate static generator.
 * @param {Object} e The event object.
 */
function respondToEdit(e) {
  // 1. Exit if no event object (e.g., running from script editor) or no range
  if (!e || !e.range) return;

  const sheet = e.range.getSheet();
  const sheetName = sheet.getName();
  const cellA1 = e.range.getA1Notation();

  // --- CONFIGURATION ---

  // Sheet 1: Need To Buy
  const SHEET_BUY = 'Need To Buy';
  const CELLS_BUY = [
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

  // Sheet 2: Restock Items On Hand
  const SHEET_RESTOCK = 'Restock Items On Hand';
  const CELLS_RESTOCK = [
    'B6',  // Market Stock Level
    'B8',  // Min ROI
    'B10', // Boost %
    'B12', // Group Filter
    'B14', // Sort Direction
    'B15', // Sort Column
    'B17', 'B18', // Delta Sell Min/Max
    'B20', 'B21', // Delta Buy Min/Max
    'B23', // Filter Volume (Checkbox)
    'B25', // Filter Loot (Checkbox)
    'B27', 'B28', // Warehouse Level (Checkbox + Value)
    'B33', // Limit
    'B36', // Min Vol 30
    'B39', // Min Warehouse Qty
    'B42'  // Max Feed Days
  ];

  // --- LOGIC ---

  // Case A: User edited "Need To Buy"
  if (sheetName === SHEET_BUY && CELLS_BUY.includes(cellA1)) {
    console.log(`[Trigger] Filter changed on ${SHEET_BUY} (${cellA1}). updating...`);
    generateRestockQuery();
  }

  // Case B: User edited "Restock Items On Hand"
  else if (sheetName === SHEET_RESTOCK && CELLS_RESTOCK.includes(cellA1)) {
    console.log(`[Trigger] Filter changed on ${SHEET_RESTOCK} (${cellA1}). updating...`);
    generateRestockItemsOnHand();
  }
}

/**
 * Executes the Restock List logic using a JS Worker.
 * FIX: Uses Dynamic Column Mapping to prevent index errors.
 */
function generateRestockQuery() {
  const TARGET_SHEET_NAME = 'Need To Buy';
  const DATA_SHEET_NAME = 'MarketOverviewData';

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  const dataSheet = ss.getSheetByName(DATA_SHEET_NAME);

  if (!sheet || !dataSheet) {
    console.error(`Target or Data sheet not found.`);
    return;
  }

  // --- 1. DYNAMIC COLUMN MAPPING ---
  // Read headers from Row 3 of MarketOverviewData
  const lastCol = dataSheet.getLastColumn();
  const headers = dataSheet.getRange(3, 1, 1, lastCol).getValues()[0];
  
  // Helper to find index (0-based relative to the range, but range starts at Col A=0)
  const getIdx = (name) => {
    const idx = headers.indexOf(name);
    if (idx === -1) console.warn(`Warning: Column "${name}" not found in ${DATA_SHEET_NAME}`);
    return idx;
  };

  const colMap = {
    itemName: getIdx("Item Name"),
    group: getIdx("Group"),
    qtyLeft: getIdx("Quantity Left"), // "Quantity Left" usually implies Sell Orders
    activeJobs: getIdx("Active Jobs"),
    deliveries: getIdx("Deliveries"),
    volume: getIdx("30-day traded volume"),
    buyOrderQty: getIdx("Listed Volume (Feed Buy)"),
    effectiveVel: getIdx("Effective Daily Velocity (u/d)"),
    warehouseQty: getIdx("Warehouse Qty"),
    daysOfInv: getIdx("Days of Inventory"),
    totalMarketQty: getIdx("Total Market Quantity"),
    medianBuy: getIdx("Hub Median Buy"),
    margin: getIdx("Margin"),
    buyAction: getIdx("Buy Action")
  };

  if (colMap.itemName === -1) return; // Critical fail if no Item Name

  // --- 2. READ FILTERS ---
  const filterValues = sheet.getRange('B5:B26').getValues();
  const filterMinDays = parseFloat(filterValues[0][0]) || 0;
  const filterTargetDays = parseFloat(filterValues[2][0]) || 0;
  const filterMargin = parseFloat(filterValues[4][0]) || 0;
  const filterGroup = (filterValues[6][0] || "").toString().toLowerCase().trim();
  const sortDirection = (filterValues[8][0] || "ASC").toString().toUpperCase();
  const sortColumnHeader = (filterValues[9][0] || "Item Name").toString().trim();
  let limitNum = filterValues[14][0];
  if (limitNum === "No Limit" || limitNum === "" || limitNum === 0) limitNum = 999999;
  const filterVolumeType = filterValues[16][0];
  const filterVolumeValue = parseFloat(filterValues[17][0]) || 0;
  const filterIgnoreGroups = (filterValues[21][0] || "").toString().toLowerCase().split(',').map(g => g.trim()).filter(g => g);

  // --- 3. READ TAX RATES ---
  let rateMultiplier = 1.0;
  try {
    const fee = ss.getRangeByName("FEE_RATE")?.getValue() || 0;
    const tax = ss.getRangeByName("TAX_RATE")?.getValue() || 0;
    rateMultiplier = (1 + Number(fee) + Number(tax));
  } catch (e) {}

  // --- 4. FETCH DATA ---
  const lastRow = dataSheet.getLastRow();
  if (lastRow < 4) return;
  
  // Get data starting Row 4
  const rawData = dataSheet.getRange(4, 1, lastRow - 3, lastCol).getValues();

  // --- 5. PROCESS DATA ---
  let processedData = [];

  for (let i = 0; i < rawData.length; i++) {
    const row = rawData[i];
    const itemName = row[colMap.itemName];
    if (!itemName) continue;

    // Safe Number Parsing
    const val = (idx) => (idx > -1) ? (Number(row[idx]) || 0) : 0;
    const txt = (idx) => (idx > -1) ? (String(row[idx]) || "") : "";

    const group = txt(colMap.group);
    const buyAction = txt(colMap.buyAction);
    
    // Inventory Calculation
    const stock = val(colMap.warehouseQty) + val(colMap.qtyLeft) + val(colMap.activeJobs) + val(colMap.buyOrderQty) + val(colMap.deliveries);
    
    const velocity = val(colMap.effectiveVel);
    const targetQty = velocity * filterTargetDays;
    const restockNeed = Math.round(targetQty - stock);
    
    const medianBuyPrice = val(colMap.medianBuy);
    const orderCost = restockNeed * medianBuyPrice * rateMultiplier;
    
    const margin = val(colMap.margin); // Often a decimal (0.20 = 20%)
    const daysInv = val(colMap.daysOfInv);
    const volume = val(colMap.volume);

    // -- FILTERS --
    if (restockNeed <= 0) continue;
    if (buyAction.includes("SKIP") || buyAction.includes("HOLD")) continue;
    
    // Check Filters
    if (margin < filterMargin) continue;
    if (daysInv >= filterMinDays) continue;
    if (filterGroup && !group.toLowerCase().includes(filterGroup)) continue;
    if (filterIgnoreGroups.some(ig => group.toLowerCase().includes(ig))) continue;

    // Volume Filter
    if (filterVolumeType === "30 Day Active" && volume <= 0) continue;
    if (filterVolumeType === "Nonactive Sellers" && volume !== 0) continue;
    if (filterVolumeType === "30 Top Sellers" && volume >= filterVolumeValue) continue;
    if (filterVolumeType === "30 Low Sellers" && volume <= filterVolumeValue) continue;

    processedData.push({
      row: [
        itemName, 
        restockNeed, 
        medianBuyPrice, 
        orderCost, 
        val(colMap.totalMarketQty), 
        volume, 
        0, // Placeholder column "0" from your headers
        val(colMap.warehouseQty), 
        margin, 
        buyAction
      ],
      sortObj: { 
        itemName, restockNeed, medianBuy: medianBuyPrice, orderCost, 
        volume, margin, buyAction 
      }
    });
  }

  // --- 6. SORT ---
  processedData.sort((a, b) => {
    let vA = a.sortObj.itemName, vB = b.sortObj.itemName;
    
    // Map header name to sort key
    if (sortColumnHeader === 'Quantity') { vA = a.sortObj.restockNeed; vB = b.sortObj.restockNeed; }
    else if (sortColumnHeader === 'Order Cost') { vA = a.sortObj.orderCost; vB = b.sortObj.orderCost; }
    else if (sortColumnHeader === 'Margin') { vA = a.sortObj.margin; vB = b.sortObj.margin; }
    else if (sortColumnHeader === 'Volume') { vA = a.sortObj.volume; vB = b.sortObj.volume; }
    
    if (vA < vB) return sortDirection === 'ASC' ? -1 : 1;
    if (vA > vB) return sortDirection === 'ASC' ? 1 : -1;
    return 0;
  });

  // --- 7. LIMIT & WRITE ---
  if (limitNum && processedData.length > limitNum) processedData = processedData.slice(0, limitNum);

  const HEADER_ROW = 4;
  const DATA_START_ROW = 5;
  const START_COL = 3; // Col C

  // Headers
  const HEADERS = ["Item Name", "Quantity", "Median Buy Price", "Order Cost", "Total Market Quantity", "Volume", "0", "Warehouse Qty", "Margin", "Buy Action"];
  
  // Clear & Write
  sheet.getRange(HEADER_ROW, START_COL, sheet.getMaxRows(), HEADERS.length).clearContent();
  sheet.getRange(HEADER_ROW, START_COL, 1, HEADERS.length).setValues([HEADERS]).setFontWeight("bold");
  
  if (processedData.length > 0) {
    const out = processedData.map(p => p.row);
    sheet.getRange(DATA_START_ROW, START_COL, out.length, out[0].length).setValues(out);
  }
  console.log(`Need To Buy updated: ${processedData.length} rows.`);
}

/**
 * GENERATE RESTOCK ITEMS ON HAND (Fixed Column Mapping)
 */
function generateRestockItemsOnHand() {
  const TARGET_SHEET = 'Restock Items On Hand';
  const DATA_SHEET = 'MarketOverviewData';
  const SETTINGS_SHEET = 'Market Overview';

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET);
  const dataSheet = ss.getSheetByName(DATA_SHEET);
  const settingsSheet = ss.getSheetByName(SETTINGS_SHEET);

  if (!sheet || !dataSheet) return;

  // --- 1. CONFIGURATION ---
  const OUT_HEADERS = [
    "Item Name", "Posting Price", "Hub Sell Price", "Seed Qty (units)",
    "Seed Posting Cost (ISK)", "Delta Sell", "Delta Buy", "Total Value",
    "Quantity", "Warehouse Level", "Pending Orders", "Projected Buy",
    "Projected Value", "Total Market Quantity", "Warehouse Qty",
    "Acquisition (30d)", "Effective Daily Velocity (u/d)",
    "30-day traded volume", "Listed Volume (Feed Sell)", "Feed Days of Book",
    "Hub Median Buy", "Effective Cost", "Sell Action", "Buy Action", "Sell Quantity"
  ];

  // --- 2. FILTERS (Column B) ---
  const bCol = sheet.getRange("B1:B45").getValues();
  const getFilter = (r, type='string') => {
    const val = bCol[r-1][0]; // r is 1-based row index
    if (type === 'num') return parseFloat(val) || 0;
    if (type === 'bool') return val === true;
    return String(val);
  };

  const filterStockLvl = getFilter(6, 'num') || 999999;
  const minROI = getFilter(8, 'num');
  const boostPct = getFilter(10, 'num');
  const boost = (boostPct > 1) ? boostPct/100 : boostPct;
  const filterGroup = getFilter(12).toLowerCase();
  const sortDir = getFilter(14).toUpperCase() || "ASC";
  const sortColName = getFilter(15);
  
  const deltaSellMax = bCol[15][0]; // Row 16
  const deltaSellMin = bCol[16][0]; // Row 17
  
  const enableFallback = settingsSheet ? (settingsSheet.getRange("E8").getValue() === true) : false;

  // --- 3. DYNAMIC COLUMN MAPPING ---
  const sourceLastCol = dataSheet.getLastColumn();
  const sourceHeaders = dataSheet.getRange(3, 1, 1, sourceLastCol).getValues()[0];
  
  const getIdx = (name) => {
    const i = sourceHeaders.indexOf(name);
    // Flexible matching for "Quantity Left" vs "Posted Sell Quantuty"
    if (i === -1 && name === "Posted Sell Quantuty") return sourceHeaders.indexOf("Quantity Left");
    return i;
  };

  const colMap = {
    item: getIdx("Item Name"),
    target: getIdx("Target"),
    hubSell: getIdx("Hub Sell Price"),
    mfgCost: getIdx("Manufacturing Unit Cost"),
    effCost: getIdx("Effective Cost"),
    sellQty: getIdx("Posted Sell Quantuty"), // or Quantity Left
    seedQty: getIdx("Seed Qty (units)"),
    seedCost: getIdx("Seed Posting Cost (ISK)"),
    deltaSell: getIdx("Delta Sell"),
    deltaBuy: getIdx("Delta Buy"),
    pending: getIdx("Pending Orders"),
    whLevel: getIdx("Warehouse Level"),
    hubBuy: getIdx("Hub Buy Price"),
    mktQty: getIdx("Total Market Quantity"),
    whQty: getIdx("Warehouse Qty"),
    buyVol: getIdx("Buy Vol"),
    mfgComp: getIdx("Manufactured Completed"),
    lootTrans: getIdx("Loot Transfered"),
    charCont: getIdx("Char Contracts"),
    effVel: getIdx("Effective Daily Velocity (u/d)"),
    vol30: getIdx("30-day traded volume"),
    listVol: getIdx("Listed Volume (Feed Sell)"),
    feedDays: getIdx("Feed Days of Book"),
    hubMedBuy: getIdx("Hub Median Buy"),
    sellAct: getIdx("Sell Action"),
    buyAct: getIdx("Buy Action"),
    group: getIdx("Group"),
    stockLvl: getIdx("Market Stock Level"),
    medROI: getIdx("Median ROI"),
    feedGate: getIdx("Feed Gate"),
    hubGate: getIdx("Hub Gate")
  };

  if (colMap.item === -1) {
    console.error("Critical headers missing in MarketOverviewData");
    return;
  }

  // --- 4. PROCESS DATA ---
  const lastRow = dataSheet.getLastRow();
  if (lastRow < 4) return;
  const rawData = dataSheet.getRange(4, 1, lastRow - 3, sourceLastCol).getValues();
  
  let resultRows = [];

  const cleanNum = (v) => (typeof v === 'number') ? v : parseFloat(String(v).replace(/[^0-9.-]/g, '')) || 0;

  for (let r of rawData) {
    if (!r[colMap.item]) continue;

    // Extract Values
    const hubSell = cleanNum(r[colMap.hubSell]);
    const mfgCost = cleanNum(r[colMap.mfgCost]);
    const effCost = cleanNum(r[colMap.effCost]);
    const postedQty = cleanNum(r[colMap.sellQty]);
    const target = cleanNum(r[colMap.target]);
    const pending = cleanNum(r[colMap.pending]);
    const seedQty = cleanNum(r[colMap.seedQty]);
    
    // ROI Calculation Logic
    const baseCost = (mfgCost > 0) ? mfgCost : effCost;
    const floorPrice = baseCost * (1 + minROI);
    // Post at Hub Price if profitable, else Floor
    const postPrice = (hubSell >= floorPrice) ? hubSell : floorPrice; 

    // Quantities
    const qtyNeed = Math.max(0, target - postedQty);
    const projBuy = Math.max(0, (target * (1 + boost)) - pending);
    
    // Acquisition Total
    const acq30 = cleanNum(r[colMap.buyVol]) + cleanNum(r[colMap.mfgComp]) + cleanNum(r[colMap.lootTrans]) + cleanNum(r[colMap.charCont]);

    // Filtering
    const sellAction = String(r[colMap.sellAct]);
    if (sellAction.includes("HOLD") || sellAction.includes("IGNORE") || sellAction.includes("SKIP")) continue;
    
    const roiVal = (typeof r[colMap.medROI] === 'string') ? parseFloat(r[colMap.medROI])/100 : r[colMap.medROI];
    if (roiVal < minROI) continue;

    if (filterGroup && !String(r[colMap.group]).toLowerCase().includes(filterGroup)) continue;

    // --- BUILD ROW (Order matches OUT_HEADERS) ---
    const rowOut = [
      r[colMap.item],                 // Item Name
      postPrice,                      // Posting Price
      hubSell,                        // Hub Sell
      seedQty,                        // Seed Qty
      r[colMap.seedCost],             // Seed Cost
      r[colMap.deltaSell],            // Delta Sell
      r[colMap.deltaBuy],             // Delta Buy
      (qtyNeed * postPrice),          // Total Value
      qtyNeed,                        // Quantity (To List)
      r[colMap.whLevel],              // WH Level
      pending,                        // Pending
      projBuy,                        // Projected Buy
      (projBuy * cleanNum(r[colMap.hubBuy])), // Projected Value
      r[colMap.mktQty],               // Total Mkt Qty
      r[colMap.whQty],                // Warehouse Qty
      acq30,                          // Acquisition 30d
      r[colMap.effVel],               // Velocity
      r[colMap.vol30],                // Vol 30d
      r[colMap.listVol],              // Listed Vol
      r[colMap.feedDays],             // Feed Days
      r[colMap.hubMedBuy],            // Hub Med Buy
      effCost,                        // Effective Cost
      sellAction,                     // Sell Action
      r[colMap.buyAct],               // Buy Action
      postedQty                       // Sell Qty
    ];

    resultRows.push({
      row: rowOut,
      sortVal: (sortColName === 'Margin' || sortColName === 'Median ROI') ? roiVal : r[colMap.item] 
    });
  }

  // --- 5. SORT ---
  // If specific column selected, map index. Else default Item Name.
  let sortIdx = OUT_HEADERS.indexOf(sortColName);
  if (sortIdx === -1 && sortColName !== 'Median ROI') sortIdx = 0;

  resultRows.sort((a, b) => {
    let valA = (sortIdx > -1) ? a.row[sortIdx] : a.sortVal;
    let valB = (sortIdx > -1) ? b.row[sortIdx] : b.sortVal;
    
    if (typeof valA === 'string') valA = valA.toLowerCase();
    if (typeof valB === 'string') valB = valB.toLowerCase();

    if (valA < valB) return (sortDir === 'ASC') ? -1 : 1;
    if (valA > valB) return (sortDir === 'ASC') ? 1 : -1;
    return 0;
  });

  // --- 6. WRITE ---
  const writeRange = sheet.getRange(3, 3, sheet.getMaxRows(), OUT_HEADERS.length);
  writeRange.clearContent();
  
  // Write Headers
  sheet.getRange(3, 3, 1, OUT_HEADERS.length).setValues([OUT_HEADERS]).setFontWeight("bold");

  // Write Data
  if (resultRows.length > 0) {
    const finalData = resultRows.map(o => o.row);
    sheet.getRange(4, 3, finalData.length, finalData[0].length).setValues(finalData);
  } else {
    sheet.getRange(4, 3).setValue("No items found.");
  }
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
    ITEM_ID_HEADERS: ['type_id', 'TypeID', 'Type ID', 'Item ID'],
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

  // --- FIX: Use toast instead of alert to avoid timeouts ---
  SpreadsheetApp.getActiveSpreadsheet().toast(`Rebuilt Control Sheet. Rows: ${dedupedOutput ? dedupedOutput.length : 0}`, "Success");
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

