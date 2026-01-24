// Global Property Service
const SCRIPT_PROPS = PropertiesService.getScriptProperties();

// --- CONFIGURATION SECTION ---

/**
 * CONFIG 1: SDE Tables (Tycoon Version)
 * Defines the massive datasets needed for Market & Industry.
 */
function GET_SDE_CONFIG() {
  return [
    // The Basics
    { name: "SDE_invTypes", file: "invTypes.csv", cols: ["typeID", "groupID", "typeName", "volume", "marketGroupID", "basePrice"] },
    { name: "SDE_invGroups", file: "invGroups.csv", cols: null },
    { name: "SDE_staStations", file: "staStations.csv", cols: null },
      
    // Industry Data (For Manufacturing)
    { name: "SDE_industryActivityMaterials", file: "industryActivityMaterials.csv", cols: null },
    { name: "SDE_industryActivityProducts", file: "industryActivityProducts.csv", cols: null }
  ];
}

/**
 * CONFIG 2: Utility Sheet Settings
 * Single source of truth for the 'Utility' sheet name and range.
 */
function GET_UTILITY_CONFIG() {
  return {
    sheetName: "Utility", 
    range: "B3:C3" // The cells that control the formulas
  };
}

// --- MENU & UI ---

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Market Sheet Tools')
    .addItem('Refresh All Data', 'refreshData')
    .addItem('Update SDE Data', 'sde_job_START') // <--- Pointing to the NEW Engine
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
 * GENERATE RESTOCK ITEMS ON HAND (Fixed Warehouse Logic & Name Formatting)
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
    const val = bCol[r-1][0]; 
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
  
  // --- 3. DYNAMIC COLUMN MAPPING ---
  const sourceLastCol = dataSheet.getLastColumn();
  const sourceHeaders = dataSheet.getRange(3, 1, 1, sourceLastCol).getValues()[0];
  
  const getIdx = (name) => {
    const i = sourceHeaders.indexOf(name);
    if (i === -1 && name === "Posted Sell Quantuty") return sourceHeaders.indexOf("Quantity Left");
    return i;
  };

  const colMap = {
    item: getIdx("Item Name"),
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
    listVol: getIdx("Listed Volume (Feed Sell)"),
    feedDays: getIdx("Feed Days of Book"),
    hubMedBuy: getIdx("Hub Median Buy"),
    sellAct: getIdx("Sell Action"),
    buyAct: getIdx("Buy Action"),
    group: getIdx("Group"),
    medROI: getIdx("Median ROI")
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
    let rawName = r[colMap.item];
    if (!rawName) continue;

    // --- FIX: Formatting Fix for Meta Items ---
    // Detects items starting with ' and removes the surrounding quotes (e.g., 'Arbalest' -> Arbalest)
    let itemName = String(rawName).replace(/^'([^']+)'/, '$1');

    // Extract Values
    const hubSell = cleanNum(r[colMap.hubSell]);
    const mfgCost = cleanNum(r[colMap.mfgCost]);
    const effCost = cleanNum(r[colMap.effCost]);
    const postedQty = cleanNum(r[colMap.sellQty]);
    const target = cleanNum(r[colMap.target]);
    const pending = cleanNum(r[colMap.pending]);
    const seedQty = cleanNum(r[colMap.seedQty]);
    const whQty = cleanNum(r[colMap.whQty]); 
    
    // Filter: Ignore if No Stock
    if (whQty <= 0) continue; 

    // ROI Calculation
    const baseCost = (mfgCost > 0) ? mfgCost : effCost;
    const floorPrice = baseCost * (1 + minROI);
    const postPrice = (hubSell >= floorPrice) ? hubSell : floorPrice; 

    // Logic: Calculate List Quantity
    const gap = Math.max(0, target - postedQty);
    let qtyToList = Math.min(gap, whQty); 

    if (qtyToList <= 0) continue; 

    const projBuy = Math.max(0, (target * (1 + boost)) - pending);
    const acq30 = cleanNum(r[colMap.buyVol]) + cleanNum(r[colMap.mfgComp]) + cleanNum(r[colMap.lootTrans]) + cleanNum(r[colMap.charCont]);

    // Filtering
    const sellAction = String(r[colMap.sellAct]);
    if (sellAction.includes("HOLD") || sellAction.includes("IGNORE") || sellAction.includes("SKIP")) continue;
    
    const roiVal = (typeof r[colMap.medROI] === 'string') ? parseFloat(r[colMap.medROI])/100 : r[colMap.medROI];
    if (roiVal < minROI) continue;

    if (filterGroup && !String(r[colMap.group]).toLowerCase().includes(filterGroup)) continue;

    // --- BUILD ROW ---
    const rowOut = [
      itemName,                       // Fixed Item Name
      postPrice,                      // Posting Price
      hubSell,                        // Hub Sell
      seedQty,                        // Seed Qty
      r[colMap.seedCost],             // Seed Cost
      r[colMap.deltaSell],            // Delta Sell
      r[colMap.deltaBuy],             // Delta Buy
      (qtyToList * postPrice),        // Total Value 
      qtyToList,                      // Quantity
      r[colMap.whLevel],              // WH Level
      pending,                        // Pending
      projBuy,                        // Projected Buy
      (projBuy * cleanNum(r[colMap.hubBuy])), // Projected Value
      r[colMap.mktQty],               // Total Mkt Qty
      whQty,                          // Warehouse Qty
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
      sortVal: (sortColName === 'Margin' || sortColName === 'Median ROI') ? roiVal : itemName 
    });
  }

  // --- 5. SORT ---
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
  sheet.getRange(3, 3, 1, OUT_HEADERS.length).setValues([OUT_HEADERS]).setFontWeight("bold");

  if (resultRows.length > 0) {
    const finalData = resultRows.map(o => o.row);
    sheet.getRange(4, 3, finalData.length, finalData[0].length).setValues(finalData);
  } else {
    sheet.getRange(4, 3).setValue("No items found.");
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
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const GLOBAL_STATE_KEY = 'GLOBAL_SYSTEM_STATE';

  // 1. SAFETY CHECK
  if (typeof isSdeJobRunning !== 'undefined' && isSdeJobRunning()) {
    console.warn("updateControlSheet skipped: SDE Job is running.");
    return;
  }

  try {
    // 2. MAINTENANCE MODE
    SCRIPT_PROP.setProperty(GLOBAL_STATE_KEY, 'MAINTENANCE');
    SpreadsheetApp.getActiveSpreadsheet().toast("Orchestrator Paused (Maintenance Mode)", "System Status");
    console.log("System entered MAINTENANCE mode.");

    // --- CONFIGURATION ---
    const CONFIG = {
      ITEM_SHEET_NAME: 'MarketOverviewData',
      LOCATION_SHEET_NAME: 'Location List',
      CONTROL_SHEET_NAME: 'Market_Control',
      SDE_SHEET_NAME: 'SDE_invTypes',
      ITEM_ID_HEADERS: ['type_id', 'TypeID', 'Type ID', 'Item ID'], 
      ITEM_NAME_HEADERS: ['Item Name', 'TypeName', 'Type Name', 'Name'],
      LOC_HEADERS: ['Station', 'System', 'Region']
    };

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const itemSheet = ss.getSheetByName(CONFIG.ITEM_SHEET_NAME);
    const locationSheet = ss.getSheetByName(CONFIG.LOCATION_SHEET_NAME);
    const controlSheet = ss.getSheetByName(CONFIG.CONTROL_SHEET_NAME);
    const sdeSheet = ss.getSheetByName(CONFIG.SDE_SHEET_NAME);

    if (!itemSheet || !locationSheet || !controlSheet) throw new Error("Missing required sheets.");

    // =================================================================
    // 3. READ ITEMS (Fixed Logic)
    // =================================================================
    const itemDataRaw = itemSheet.getDataRange().getValues();
    if (itemDataRaw.length < 2) throw new Error("Item sheet is empty.");

    let itemHeaderRowIdx = -1, nameColIdx = -1, idColIdx = -1;
    
    // Scan first 10 rows for headers
    for (let r = 0; r < Math.min(10, itemDataRaw.length); r++) {
      const row = itemDataRaw[r].map(c => String(c).trim().toLowerCase());
      CONFIG.ITEM_ID_HEADERS.forEach(h => { if (row.indexOf(h.toLowerCase()) > -1) { idColIdx = row.indexOf(h.toLowerCase()); itemHeaderRowIdx = r; } });
      CONFIG.ITEM_NAME_HEADERS.forEach(h => { if (row.indexOf(h.toLowerCase()) > -1) { nameColIdx = row.indexOf(h.toLowerCase()); } });
      if (idColIdx > -1) break; 
    }

    if (itemHeaderRowIdx === -1 && nameColIdx > -1) {
       for (let r = 0; r < Math.min(10, itemDataRaw.length); r++) {
         const row = itemDataRaw[r].map(c => String(c).trim().toLowerCase());
         if (row.indexOf(CONFIG.ITEM_NAME_HEADERS[0].toLowerCase()) > -1 || row.indexOf('item name') > -1) {
           itemHeaderRowIdx = r;
           break;
         }
       }
    }

    if (itemHeaderRowIdx === -1) throw new Error("Could not find headers in MarketOverviewData.");

    const rawItems = [];
    if (idColIdx > -1) {
      for (let i = itemHeaderRowIdx + 1; i < itemDataRaw.length; i++) {
        const val = Number(itemDataRaw[i][idColIdx]);
        if (val > 0) rawItems.push(val);
      }
    }

    if (rawItems.length === 0 && nameColIdx > -1 && sdeSheet) {
      console.log("Using SDE Name Lookup...");
      const sdeVals = sdeSheet.getRange('A2:C' + sdeSheet.getLastRow()).getValues();
      const typeMap = new Map(sdeVals.map(r => [String(r[2]).trim().toLowerCase(), r[0]]));
      
      for (let i = itemHeaderRowIdx + 1; i < itemDataRaw.length; i++) {
        let name = String(itemDataRaw[i][nameColIdx]).trim().toLowerCase().replace(/^'|'$/g, '');
        if (name && typeMap.has(name)) rawItems.push(typeMap.get(name));
      }
    }

    const uniqueItemIds = Array.from(new Set(rawItems));
    console.log(`Found ${uniqueItemIds.length} unique items.`);

    if (uniqueItemIds.length === 0) {
      ss.toast("Error: No items found. Check 'MarketOverviewData' headers.", "Failed");
      return; 
    }

    // =================================================================
    // 4. READ LOCATIONS
    // =================================================================
    const locDataRaw = locationSheet.getDataRange().getValues();
    const locHeaderRowIdx = 4; // Row 5
    const locSet = new Set();
    
    if (locDataRaw.length > locHeaderRowIdx) {
      const headerRow = locDataRaw[locHeaderRowIdx].map(c => String(c).trim().toLowerCase());
      const colMap = {};
      CONFIG.LOC_HEADERS.forEach(h => colMap[h] = headerRow.indexOf(h.toLowerCase()));

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
    }

    const locations = Array.from(locSet).map(s => {
      const parts = s.split('|');
      return { type: parts[0], id: Number(parts[1]) };
    });
    
    // --- [NEW LOG] Show Location Count ---
    console.log(`Found ${locations.length} unique locations (Markets).`);

    // =================================================================
    // 5. PREPARE DATA
    // =================================================================
    const dedupedOutput = [];
    for (const loc of locations) {
      for (const itemId of uniqueItemIds) {
        dedupedOutput.push([itemId, loc.type, loc.id, '']);
      }
    }

    dedupedOutput.sort((a, b) => (a[2] !== b[2]) ? a[2] - b[2] : (a[1] !== b[1]) ? a[1].localeCompare(b[1]) : a[0] - b[0]);

    controlSheet.clear();
    controlSheet.getRange(1, 1, 1, 4).setValues([['type_id', 'location_type', 'location_id', 'last_updated']]);

    // =================================================================
    // 6. EXECUTE WRITE (LOCAL FUNCTION)
    // =================================================================
    const safeBatchWrite = (targetSheet, data, startRow, startCol) => {
        const SOFT_LIMIT_MS = 280000;
        const startTime = new Date().getTime();
        const CHUNK_SIZE = 5000;
        let rowsWritten = 0;

        for (let i = 0; i < data.length; i += CHUNK_SIZE) {
            if ((new Date().getTime() - startTime) > SOFT_LIMIT_MS) {
                return { success: false, rows: rowsWritten, reason: "TIME_LIMIT" };
            }
            const chunk = data.slice(i, i + CHUNK_SIZE);
            if (chunk.length > 0) {
                targetSheet.getRange(startRow + i, startCol, chunk.length, chunk[0].length).setValues(chunk);
                rowsWritten += chunk.length;
                SpreadsheetApp.flush(); 
                Utilities.sleep(50);    
            }
        }
        return { success: true, rows: rowsWritten };
    };

    const writeResult = safeBatchWrite(controlSheet, dedupedOutput, 2, 1);

    if (writeResult.success) {
       // --- [UPDATED TOAST/LOG] ---
       ss.toast(`Success! Rebuilt ${dedupedOutput.length} rows.`, "Control Sheet");
       console.log(`Rebuild Complete. Total Rows: ${dedupedOutput.length} | Items: ${uniqueItemIds.length} | Locations: ${locations.length}`);
    } else {
       ss.toast(`Partial Write: ${writeResult.rows} rows. Reason: ${writeResult.reason}`, "Warning");
    }

  } catch (e) {
    console.error("Control Sheet Rebuild Failed: " + e.message);
    SpreadsheetApp.getActiveSpreadsheet().toast("Rebuild Failed. See Logs.", "Error");
  } finally {
    // 7. RESTORE ORCHESTRATOR
    SCRIPT_PROP.setProperty(GLOBAL_STATE_KEY, 'RUNNING');
    console.log("System State restored to RUNNING.");
  }
}

/**
 * HOOK: Called BEFORE SDE Start
 * Returns TRUE to continue, FALSE to cancel.
 */
function ON_SDE_START() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert(
    '⚠️ Update SDE Database?',
    'This will download fresh data from GitHub.\n\n' +
    '• Formulas will be paused.\n' +
    '• The sheet will be locked for ~3 minutes.\n' +
    '• ORCHESTRATOR will be PAUSED.\n\n' +
    'Do you want to proceed?',
    ui.ButtonSet.YES_NO
  );

  if (response == ui.Button.NO || response == ui.Button.CLOSE) {
    return false; // Tells Controller to ABORT
  }

  SpreadsheetApp.getActiveSpreadsheet().toast("Pausing Orchestrator & Initializing Update...", "System Status", 10);
  
  // TYCOON SPECIFIC: Pause the business logic
  _manageOrchestrator(false);
  
  return true; // Tells Controller to PROCEED
}

/**
 * HOOK: Called when the job is 100% done
 */
function ON_SDE_COMPLETE() {
  // TYCOON SPECIFIC: Restart the business logic
  _manageOrchestrator(true);

  SpreadsheetApp.getActiveSpreadsheet().toast("SDE Update Complete. Orchestrator Resumed.", "System Status", -1);
}

// --- HELPER: ORCHESTRATOR MANAGER ---

/**
 * Pauses or Restarts the main Market Orchestrator trigger.
 * @param {boolean} turnOn - True to create trigger, False to delete it.
 */
function _manageOrchestrator(turnOn) {
  const FUNCTION_NAME = 'masterOrchestrator'; // <--- Ensure this matches your actual function name
  
  // 1. Always delete existing triggers first (to avoid duplicates or to pause)
  const allTriggers = ScriptApp.getProjectTriggers();
  allTriggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === FUNCTION_NAME) {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  // 2. If turning ON, create a new one
  if (turnOn) {
    ScriptApp.newTrigger(FUNCTION_NAME)
      .timeBased()
      .everyMinutes(10) // <--- Adjust frequency as needed
      .create();
    console.log("Orchestrator Trigger RESTARTED.");
  } else {
    console.log("Orchestrator Trigger PAUSED.");
  }
}

// --- REFRESH TOOLS (Unified) ---

const TIME_DELAY = 2000;

function refreshData() {
  SpreadsheetApp.flush();
  refreshAllData();
  refreshDynamicData();
  refreshStaticData();
}

function refreshAllData() {
  const conf = GET_UTILITY_CONFIG();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(conf.sheetName);
  if (sheet) {
    sheet.getRange(conf.sheetName + '!' + conf.range).setValues([[0, 0]]);
  }
}

function refreshDynamicData() {
  Utilities.sleep(TIME_DELAY);
  const conf = GET_UTILITY_CONFIG();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(conf.sheetName);
  if (sheet) {
    sheet.getRange(conf.sheetName + '!B3').setValue(1);
  }
}

function refreshStaticData() {
  Utilities.sleep(TIME_DELAY);
  const conf = GET_UTILITY_CONFIG();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(conf.sheetName);
  if (sheet) {
    sheet.getRange(conf.sheetName + '!C3').setValue(1);
  }
}

/**
 * Helper: Query Enhancer
 */
function sqlFromHeaderNames(rangeName, queryString, useColNums) {
  let ss = SpreadsheetApp.getActiveSpreadsheet();
  let range;
  try {
    range = ss.getRange(rangeName);
  } catch (e) {
    range = ss.getRangeByName(rangeName);
  }
  let headers = range.getValues()[0];
  for (var i = 0; i < headers.length; i++) {
    if (headers[i].length < 1) continue;
    var re = new RegExp("\\b" + headers[i] + "\\b", "gm");
    if (useColNums) {
      var columnName = "Col" + Math.floor(i + 1);
      queryString = queryString.replace(re, columnName);
    } else {
      var columnLetter = range.getCell(1, i + 1).getA1Notation().split(/[0-9]/)[0];
      queryString = queryString.replace(re, columnLetter);
    }
  }
  return queryString;
}