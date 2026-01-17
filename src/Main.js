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
 * Executes the Restock List logic using a JS Worker (No Formulas).
 * NOW INCLUDES HEADERS.
 */
function generateRestockQuery() {
  const TARGET_SHEET_NAME = 'Need To Buy';
  const DATA_SHEET_NAME = 'MarketOverviewData';

  // HEADERS go in Row 4
  const HEADER_ROW = 4;
  const DATA_START_ROW = 5;
  const START_COL_WRITE = 3; // Col C

  const HEADERS = [
    "Item Name",
    "Quantity",
    "Median Buy Price",
    "Order Cost",
    "Total Market Quantity",
    "Volume",
    "0",
    "Warehouse Qty",
    "Margin",
    "Buy Action"
  ];

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  const dataSheet = ss.getSheetByName(DATA_SHEET_NAME);

  if (!sheet || !dataSheet) {
    console.error(`Target or Data sheet not found.`);
    return;
  }

  // --- 1. READ FILTERS ---
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

  const filterIgnoreGroupsRaw = filterValues[21][0];
  const filterIgnoreGroups = filterIgnoreGroupsRaw
    ? filterIgnoreGroupsRaw.toString().toLowerCase().split(',').map(g => g.trim())
    : [];

  // --- 2. READ TAX RATES ---
  let FEE_RATE = 0;
  let TAX_RATE = 0;
  try {
    const feeRange = ss.getRangeByName("FEE_RATE");
    if (feeRange) FEE_RATE = parseFloat(feeRange.getValue()) || 0;
    const taxRange = ss.getRangeByName("TAX_RATE");
    if (taxRange) TAX_RATE = parseFloat(taxRange.getValue()) || 0;
  } catch (e) { }
  const rateMultiplier = (1 + FEE_RATE + TAX_RATE);

  // --- 3. FETCH RAW DATA ---
  const lastRow = dataSheet.getLastRow();
  if (lastRow < 3) return;

  const rawData = dataSheet.getRange(3, 2, lastRow - 2, 57).getValues();

  // --- 4. PROCESS DATA ---
  let processedData = [];

  for (let i = 0; i < rawData.length; i++) {
    const row = rawData[i];

    // --- CORRECTED COLUMN MAPPING (Relative to Col B = Index 0) ---
    // Col B = 0, C = 1, ...

    const itemName = row[1]; // Col C
    if (!itemName) continue;

    const group = (row[2] || "").toString(); // Col D
    const qtyLeft = Number(row[5]) || 0;     // Col G (Quantity Left / Sell Orders)
    const activeJobs = Number(row[10]) || 0; // Col L (Active Jobs)
    
    // ADDED: Deliveries (Col Q / Index 15)
    const deliveries = Number(row[15]) || 0; 

    // Volume: 30-day traded volume (Col W / Index 21) - Was reading V (Avg Price)
    const volume = Number(row[21]) || 0;

    // Buy Order Qty: Listed Volume Feed Buy (Col Y / Index 23)
    const buyOrderQty = Number(row[23]) || 0;

    // Velocity: Effective Daily Velocity (Col AE / Index 29) - Was reading AF
    const effectiveVel = Number(row[29]) || 0;

    // Warehouse: Warehouse Qty (Col AF / Index 30) - Was reading AG
    const warehouseQty = Number(row[30]) || 0;

    // Days Inv: Days of Inventory (Col AI / Index 33) - Was reading AJ
    const daysOfInv = Number(row[33]);

    // Total Market: Total Market Quantity (Col AM / Index 37) - Was reading AN
    const totalMarketQty = Number(row[37]) || 0;

    const medianBuy = Number(row[41]) || 0; // Col AQ (OK)

    // Margin: (Col AX / Index 48) - Was reading AY
    const margin = Number(row[48]) || 0;

    // Buy Action: (Col BD / Index 54) - Was reading BE (Total Value Left)
    const buyAction = (row[54] || "").toString();

    // -- CALCULATE --
    const targetQty = effectiveVel * filterTargetDays;

    // FIX: Count Warehouse + Sell Orders + Active Jobs + Buy Orders + DELIVERIES
    const existingStock = warehouseQty + qtyLeft + activeJobs + buyOrderQty + deliveries;

    const restockNeed = Math.round(targetQty - existingStock);
    const orderCost = restockNeed * medianBuy * rateMultiplier;

    // -- FILTERS --
    if (restockNeed <= 0) continue;
    if (buyAction.includes("SKIP")) continue;
    if (buyAction.includes("HOLD")) continue;
    if (margin < filterMargin) continue;
    if (daysOfInv >= filterMinDays) continue;

    if (filterGroup && !group.toLowerCase().includes(filterGroup)) continue;

    if (filterIgnoreGroups.length > 0) {
      const groupLower = group.toLowerCase();
      if (filterIgnoreGroups.some(ignored => groupLower.includes(ignored))) continue;
    }

    if (filterVolumeType === "30 Day Active" && volume <= 0) continue;
    if (filterVolumeType === "Nonactive Sellers" && volume !== 0) continue;
    if (filterVolumeType === "30 Top Sellers" && volume >= filterVolumeValue) continue;
    if (filterVolumeType === "30 Low Sellers" && volume <= filterVolumeValue) continue;

    processedData.push({
      row: [itemName, restockNeed, medianBuy, orderCost, totalMarketQty, volume, 0, warehouseQty, margin, buyAction],
      sortObj: { itemName, restockNeed, medianBuy, orderCost, totalMarketQty, volume, warehouseQty, margin, buyAction }
    });
  }

  // --- 5. SORT ---
  processedData.sort((a, b) => {
    let valA, valB;
    switch (sortColumnHeader) {
      case 'Item Name': valA = a.sortObj.itemName; valB = b.sortObj.itemName; break;
      case 'Quantity': valA = a.sortObj.restockNeed; valB = b.sortObj.restockNeed; break;
      case 'Median Buy Price': valA = a.sortObj.medianBuy; valB = b.sortObj.medianBuy; break;
      case 'Order Cost': valA = a.sortObj.orderCost; valB = b.sortObj.orderCost; break;
      case 'Total Market Quantity': valA = a.sortObj.totalMarketQty; valB = b.sortObj.totalMarketQty; break;
      case 'Volume':
      case '30-day traded volume': valA = a.sortObj.volume; valB = b.sortObj.volume; break;
      case 'Warehouse Qty': valA = a.sortObj.warehouseQty; valB = b.sortObj.warehouseQty; break;
      case 'Margin': valA = a.sortObj.margin; valB = b.sortObj.margin; break;
      case 'Buy Action': valA = a.sortObj.buyAction; valB = b.sortObj.buyAction; break;
      default: valA = a.sortObj.itemName; valB = b.sortObj.itemName;
    }
    if (valA < valB) return sortDirection === 'ASC' ? -1 : 1;
    if (valA > valB) return sortDirection === 'ASC' ? 1 : -1;
    return 0;
  });

  // --- 6. LIMIT ---
  if (limitNum && processedData.length > limitNum) {
    processedData = processedData.slice(0, limitNum);
  }

  // --- 7. WRITE TO SHEET ---
  const maxRows = sheet.getMaxRows();

  // Clear everything from Header Row down
  const rangeToClear = sheet.getRange(HEADER_ROW, START_COL_WRITE, maxRows - HEADER_ROW + 1, HEADERS.length);
  rangeToClear.clearContent();

  // Write Headers
  sheet.getRange(HEADER_ROW, START_COL_WRITE, 1, HEADERS.length).setValues([HEADERS]);

  // Write Data (if any)
  if (processedData.length > 0) {
    const outputValues = processedData.map(item => item.row);
    sheet.getRange(DATA_START_ROW, START_COL_WRITE, outputValues.length, outputValues[0].length).setValues(outputValues);
    console.log(`Generated Restock List: ${outputValues.length} items.`);
  } else {
    console.log("Restock List Empty based on current filters.");
  }
}

/**
 * GENERATE RESTOCK ITEMS ON HAND (Static Version - Final Polish)
 * Output Range: C3:AA (Headers on Row 3, Data on Row 4)
 * Features: Dynamic Sorting (Matches B15), Header Formatting, Error Handling
 */
function generateRestockItemsOnHand() {
  const TARGET_SHEET_NAME = 'Restock Items On Hand';
  const DATA_SHEET_NAME = 'MarketOverviewData';
  const SETTINGS_SHEET_NAME = 'Market Overview';

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  const dataSheet = ss.getSheetByName(DATA_SHEET_NAME);
  const settingsSheet = ss.getSheetByName(SETTINGS_SHEET_NAME);

  if (!sheet || !dataSheet) {
    console.error("Required sheets not found.");
    return;
  }

  // --- 1. CONFIGURATION ---
  const OUTPUT_HEADER_ROW = 3;
  const OUTPUT_DATA_ROW = 4;
  const OUTPUT_START_COL = 3; // Column C

  // The Exact Headers (25 Columns)
  const outputHeaders = [
    "Item Name", "Posting Price", "Hub Sell Price", "Seed Qty (units)",
    "Seed Posting Cost (ISK)", "Delta Sell", "Delta Buy", "Total Value",
    "Quantity", "Warehouse Level", "Pending Orders", "Projected Buy",
    "Projected Value", "Total Market Quantity", "Warehouse Qty",
    "Acquisition (30d)", "Effective Daily Velocity (u/d)",
    "30-day traded volume", "Listed Volume (Feed Sell)", "Feed Days of Book",
    "Hub Median Buy", "Effective Cost", "Sell Action", "Buy Action", "Sell Quantity"
  ];

  // --- 2. READ FILTERS (Column B) ---
  const bCol = sheet.getRange("B1:B45").getValues();

  const filterStockLevel = parseFloat(bCol[5][0]) || 999999;
  const minROI = parseFloat(bCol[7][0]) || 0;
  const boostPct = parseFloat(bCol[9][0]) || 0;
  const filterGroup = (bCol[11][0] || "").toString().toLowerCase();
  const sortDirection = (bCol[13][0] || "ASC").toString().toUpperCase();
  const sortColumn = (bCol[14][0] || "").toString(); // This is what we sort by
  const deltaSellMax = bCol[16][0];
  const deltaSellMin = bCol[17][0];
  const deltaBuyMax = bCol[19][0];
  const deltaBuyMin = bCol[20][0];
  const filterVol = bCol[22][0] === true;
  const filterLoot = bCol[24][0] === true;
  const filterWhLevel = bCol[26][0] === true;
  const filterWhLvlVal = parseFloat(bCol[27][0]) || 0;
  const limitNum = bCol[32][0];
  const minVol30 = bCol[35][0];
  const minWarehouse = bCol[38][0];
  const maxFeedDays = bCol[41][0];

  let enableFallback = false;
  if (settingsSheet) {
    enableFallback = settingsSheet.getRange("E8").getValue() === true;
  }
  const boost = (boostPct > 1) ? boostPct / 100 : boostPct;

  // --- 3. FETCH DATA ---
  const lastRow = dataSheet.getLastRow();
  const lastCol = dataSheet.getLastColumn();

  const SOURCE_HEADER_ROW = 3; // Headers in Data Sheet
  if (lastRow <= SOURCE_HEADER_ROW) return;

  const headers = dataSheet.getRange(SOURCE_HEADER_ROW, 1, 1, lastCol).getValues()[0];
  const data = dataSheet.getRange(SOURCE_HEADER_ROW + 1, 1, lastRow - SOURCE_HEADER_ROW, lastCol).getValues();

  const getIdx = (name) => headers.indexOf(name);
  const parseMoney = (val) => {
    if (typeof val === 'number') return val;
    if (!val) return 0;
    return parseFloat(String(val).replace(/[^0-9.-]+/g, "")) || 0;
  };

  // Map Data Columns
  const cols = {
    itemName: getIdx("Item Name"),
    target: getIdx("Target"),
    hubSell: getIdx("Hub Sell Price"),
    mfgCost: getIdx("Manufacturing Unit Cost"),
    effCost: getIdx("Effective Cost"),
    sellQty: getIdx("Posted Sell Quantuty"),
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
    listedVol: getIdx("Listed Volume (Feed Sell)"),
    feedDays: getIdx("Feed Days of Book"),
    hubMedianBuy: getIdx("Hub Median Buy"),
    sellAction: getIdx("Sell Action"),
    buyAction: getIdx("Buy Action"),
    group: getIdx("Group"),
    mktStockLvl: getIdx("Market Stock Level"),
    medianROI: getIdx("Median ROI"),
    feedGate: getIdx("Feed Gate"),
    hubGate: getIdx("Hub Gate"),
  };

  if (cols.itemName === -1) return console.error("Headers not found.");

  let processedData = [];

  // --- 4. PROCESS ROW BY ROW ---
  for (let i = 0; i < data.length; i++) {
    const row = data[i];
    if (!row[cols.itemName]) continue;

    const val_HubSell = parseMoney(row[cols.hubSell]);
    const val_Mfg = parseMoney(row[cols.mfgCost]);
    const val_Eff = parseMoney(row[cols.effCost]);
    const val_SellQty = parseMoney(row[cols.sellQty]);

    const target = Number(row[cols.target]) || 0;
    const seedQty = Number(row[cols.seedQty]) || 0;
    const pending = Number(row[cols.pending]) || 0;
    const mktQty = Number(row[cols.mktQty]) || 0;
    const whQty = Number(row[cols.whQty]) || 0;
    const whLevel = Number(row[cols.whLevel]) || 0;
    const vol30 = Number(row[cols.vol30]) || 0;
    const lootTrans = Number(row[cols.lootTrans]) || 0;
    const deltaSell = Number(row[cols.deltaSell]);
    const deltaBuy = Number(row[cols.deltaBuy]);

    const medianROI = (typeof row[cols.medianROI] === 'string')
      ? parseFloat(row[cols.medianROI].replace('%', '')) / 100
      : Number(row[cols.medianROI]);

    const feedDays = Number(row[cols.feedDays]);
    const feedGate = String(row[cols.feedGate]);
    const hubGate = String(row[cols.hubGate]);
    const sellAction = String(row[cols.sellAction]);
    const group = String(row[cols.group]);

    // Pricing Logic
    const val_Base = (val_Mfg > 0) ? val_Mfg : val_Eff;
    const val_Floor = val_Base * (1 + minROI);
    const val_Post = (val_HubSell >= val_Floor) ? val_HubSell : val_Floor;

    const qtyNeeded = target - val_SellQty;
    const projBuy = (target * (1 + boost)) - pending;

    const acq30d = (Number(row[cols.buyVol]) || 0) +
      (Number(row[cols.mfgComp]) || 0) +
      lootTrans +
      (Number(row[cols.charCont]) || 0);

    const totalValue = qtyNeeded * val_Post;
    const projectedValue = projBuy * parseMoney(row[cols.hubBuy]);

    // --- FILTERS ---
    if (target < 0) continue;
    if (mktQty < 0) continue;
    if (pending < 0) continue;
    if (seedQty <= 0) continue;

    if (minWarehouse !== "" && whQty < minWarehouse) continue;
    if (Number(row[cols.mktStockLvl]) > filterStockLevel) continue;
    if (filterWhLevel && whLevel < filterWhLvlVal) continue;
    if (filterVol && vol30 <= 0) continue;
    if (filterLoot && lootTrans <= 0) continue;

    if (deltaSellMax !== "" && deltaSell > deltaSellMax) continue;
    if (deltaSellMin !== "" && deltaSell < deltaSellMin) continue;
    if (deltaBuyMax !== "" && deltaBuy > deltaBuyMax) continue;
    if (deltaBuyMin !== "" && deltaBuy < deltaBuyMin) continue;

    if (medianROI < minROI) continue;
    if (filterGroup && !group.toLowerCase().includes(filterGroup)) continue;
    if (minVol30 !== "" && vol30 < minVol30) continue;
    if (maxFeedDays !== "" && feedDays > maxFeedDays) continue;
    if (target <= val_SellQty) continue;

    if (enableFallback) {
      if (feedGate !== 'OK' && hubGate !== 'OK') continue;
    } else {
      if (feedGate !== 'OK') continue;
    }
    if (sellAction.includes("HOLD")) continue;
    if (sellAction.includes("IGNORE")) continue;
    if (sellAction.includes('SKIP')) continue;

    // --- BUILD ROW ---
    // This array order MUST match outputHeaders exactly
    const outputRow = [
      row[cols.itemName],         // 1. Item Name
      val_Post,                   // 2. Posting Price
      row[cols.hubSell],          // 3. Hub Sell Price
      row[cols.seedQty],          // 4. Seed Qty
      row[cols.seedCost],         // 5. Seed Posting Cost
      row[cols.deltaSell],        // 6. Delta Sell
      row[cols.deltaBuy],         // 7. Delta Buy
      totalValue,                 // 8. Total Value
      qtyNeeded,                  // 9. Quantity
      row[cols.whLevel],          // 10. Warehouse Level
      row[cols.pending],          // 11. Pending Orders
      projBuy,                    // 12. Projected Buy
      projectedValue,             // 13. Projected Value
      row[cols.mktQty],           // 14. Total Market Qty
      row[cols.whQty],            // 15. Warehouse Qty
      acq30d,                     // 16. Acquisition (30d)
      row[cols.effVel],           // 17. Effective Daily Velocity
      row[cols.vol30],            // 18. 30-day traded volume
      row[cols.listedVol],        // 19. Listed Volume
      row[cols.feedDays],         // 20. Feed Days
      row[cols.hubMedianBuy],     // 21. Hub Median Buy
      row[cols.effCost],          // 22. Effective Cost
      row[cols.sellAction],       // 23. Sell Action
      row[cols.buyAction],        // 24. Buy Action
      val_SellQty                 // 25. Sell Quantity
    ];

    processedData.push({
      row: outputRow,
      sortCriteria: { roi: medianROI } // Keep ROI here in case we need it specifically
    });
  }

  // --- 5. DYNAMIC SORTING ---
  // Find which column index matches the header name in Cell B15
  let sortColIndex = outputHeaders.indexOf(sortColumn);

  // Fallback: If "Median ROI" is selected (which isn't in output), handle specifically
  // Otherwise, if not found, default to Item Name (Index 0)

  processedData.sort((a, b) => {
    let valA, valB;

    if (sortColumn === 'Median ROI' || sortColumn === 'Margin') {
      valA = a.sortCriteria.roi;
      valB = b.sortCriteria.roi;
    } else {
      // If column name matches a header, sort by that column
      // If mismatch, sort by Item Name (Index 0)
      const idx = (sortColIndex > -1) ? sortColIndex : 0;
      valA = a.row[idx];
      valB = b.row[idx];
    }

    // Handle strings case-insensitive
    if (typeof valA === 'string') valA = valA.toLowerCase();
    if (typeof valB === 'string') valB = valB.toLowerCase();

    if (valA < valB) return sortDirection === 'ASC' ? -1 : 1;
    if (valA > valB) return sortDirection === 'ASC' ? 1 : -1;
    return 0;
  });

  // --- 6. LIMIT ---
  if (limitNum !== "No Limit" && limitNum > 0) {
    processedData = processedData.slice(0, limitNum);
  }

  // --- 7. WRITE OUTPUT ---
  const maxRows = sheet.getMaxRows();
  sheet.getRange(OUTPUT_HEADER_ROW, OUTPUT_START_COL, maxRows - OUTPUT_HEADER_ROW + 1, outputHeaders.length).clearContent();

  // Write Headers
  const headerRange = sheet.getRange(OUTPUT_HEADER_ROW, OUTPUT_START_COL, 1, outputHeaders.length);
  headerRange.setValues([outputHeaders]);
  headerRange.setFontWeight("bold").setHorizontalAlignment("center"); // <-- VISUAL FIX

  // Write Data
  if (processedData.length > 0) {
    const output = processedData.map(d => d.row);
    sheet.getRange(OUTPUT_DATA_ROW, OUTPUT_START_COL, output.length, output[0].length).setValues(output);
    console.log(`Restock Items On Hand: Updated ${output.length} rows.`);
  } else {
    sheet.getRange(OUTPUT_DATA_ROW, OUTPUT_START_COL).setValue("No items match filter criteria.");
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

