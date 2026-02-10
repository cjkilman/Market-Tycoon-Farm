// Global Property Service
const SCRIPT_PROPS = PropertiesService.getScriptProperties();

// --- CONFIGURATION SECTION ---

function GET_SDE_CONFIG() {
  return [
    { name: "SDE_invTypes", file: "invTypes.csv", cols: ["typeID", "groupID", "typeName", "volume", "marketGroupID", "basePrice"] },
    { name: "SDE_invGroups", file: "invGroups.csv", cols: null },
    { name: "SDE_staStations", file: "staStations.csv", cols: null },
    { name: "SDE_industryActivityMaterials", file: "industryActivityMaterials.csv", cols: null },
    { name: "SDE_industryActivityProducts", file: "industryActivityProducts.csv", cols: null }
  ];
}

function GET_UTILITY_CONFIG() {
  return {
    sheetName: "Utility",
    range: "B3:C3"
  };
}

// --- MENU & UI ---

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('⚓ Engine Room')
    .addItem('🔄 Sync Restock & NeedToBuy', 'triggerRestockSync')
    .addItem('♻️ Refresh Formula Flags', 'refreshData')
    .addSeparator()
    .addItem('📊 Update SDE Database', 'sde_job_START')
    .addItem('🛠️ Rebuild Control Sheet', 'updateControlSheet')
    .addItem('Generate Projected Build Costs', 'generateProjectedCostTable')
    .addToUi();
}

/**
 * Calculates instant "Dump to Buy" profitability for manufacturing items.
 * Compares Projected Build Cost vs Hub Median Buy Price.
 */
function getDumpToBuyAnalysis() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const overviewSheet = ss.getSheetByName("Market Overview");
  const data = overviewSheet.getDataRange().getValues();

  // Column Mapping based on Tycoon 2.0 structure
  const COL_ITEM_NAME = 10; // Col K
  const COL_PROJECTED_COST = 20; // Col U (Manufacturing Projected Unit Cost)
  const COL_MEDIAN_BUY = 43; // Col AR (Hub Median Buy)
  const SALES_TAX = 0.036; // Adjusted for your current skills/standing

  let profitableDumps = [];

  // Start after header row
  for (let i = 8; i < data.length; i++) {
    let itemName = data[i][COL_ITEM_NAME];
    let projectedCost = parseFloat(data[i][COL_PROJECTED_COST]);
    let medianBuy = parseFloat(data[i][COL_MEDIAN_BUY]);

    if (itemName && !isNaN(projectedCost) && !isNaN(medianBuy) && projectedCost > 0) {
      let netProceeds = medianBuy * (1 - SALES_TAX);
      let dumpProfit = netProceeds - projectedCost;
      let dumpROI = (dumpProfit / projectedCost) * 100;

      if (dumpProfit > 0) {
        profitableDumps.push({
          name: itemName,
          profit: dumpProfit,
          roi: dumpROI,
          buyPrice: medianBuy
        });
      }
    }
  }

  // Sort by highest ROI
  profitableDumps.sort((a, b) => b.roi - a.roi);

  // Log results in ASCII for quick review
  if (profitableDumps.length > 0) {
    console.log("+= DUMP TO BUY PROFITABLE ITEMS =========================+");
    console.log("| Item Name                | Profit (ISK) | ROI (%)    |");
    console.log("+--------------------------+--------------+------------+");
    profitableDumps.forEach(item => {
      let nameStr = item.name.padEnd(24).substring(0, 24);
      let profitStr = item.profit.toLocaleString().padStart(12);
      let roiStr = item.roi.toFixed(2).padStart(8) + "%";
      console.log(`| ${nameStr} | ${profitStr} | ${roiStr}  |`);
    });
    console.log("+========================================================+");
  } else {
    console.log("No profitable dump opportunities found.");
  }
}

function triggerRestockSync() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  ss.toast("Initializing specialized sync...", "Engine Room", 5);
  try {
    generateRestockQuery(ss);
    generateRestockItemsOnHand(ss);
    generateDumpToBuyOrder(ss);
    ss.toast("✅ Restock & NeedToBuy synced successfully.", "Engine Room", 3);
  } catch (e) {
    ss.toast("❌ Sync failed: " + e.message, "Engine Room Error");
    console.error(e);
  }
}

function respondToEdit(e) {
  if (!e || !e.range) return;
  const sheet = e.range.getSheet();
  const sheetName = sheet.getName();
  const cellA1 = e.range.getA1Notation();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const SHEET_BUY = 'Need To Buy';
  const CELLS_BUY = ['B5', 'B7', 'B9', 'B11', 'B13', 'B14', 'B19', 'B21', 'B22', 'B26'];

  const SHEET_RESTOCK = 'Restock Items On Hand';
  const CELLS_RESTOCK = ['B6', 'B8', 'B10', 'B12', 'B14', 'B15', 'B17', 'B18', 'B20', 'B21', 'B23', 'B25', 'B27', 'B28', 'B33', 'B36', 'B39', 'B42', 'A39'];

  if (sheetName === SHEET_BUY && CELLS_BUY.includes(cellA1)) {
    generateRestockQuery(ss);
  } else if (sheetName === SHEET_RESTOCK && CELLS_RESTOCK.includes(cellA1)) {
    generateRestockItemsOnHand(ss);
  }
}

function generateRestockQuery(ss) {
  const TARGET_SHEET_NAME = 'Need To Buy';
  const DATA_SHEET_NAME = 'MarketOverviewData';
  const AUDIT_SHEET_NAME = 'Audit items';

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  const dataSheet = ss.getSheetByName(DATA_SHEET_NAME);
  const auditSheet = ss.getSheetByName(AUDIT_SHEET_NAME);

  if (!sheet || !dataSheet || !auditSheet) return;

  // --- BATCH DATA LOAD (Speed Move #1) ---
  // Load everything into memory at once to avoid repeated API calls
  const rawDataValues = dataSheet.getDataRange().getValues();
  const headers = rawDataValues[2]; // Headers are on Row 3 (index 2)
  const marketRows = rawDataValues.slice(3); // Data starts on Row 4

  const auditValues = auditSheet.getDataRange().getValues();
  const auditMap = new Map(auditValues.slice(1).map(r => [String(r[0]), r[1]]));

  // --- PRE-PROCESS FILTERS (Speed Move #2) ---
  const filters = sheet.getRange('B5:B26').getValues();
  const cfg = {
    minDays: parseFloat(filters[0][0]) || 0,
    targetDays: parseFloat(filters[2][0]) || 0,
    minMargin: parseFloat(filters[4][0]) || 0,
    groups: (filters[6][0] || "").toLowerCase().split(',').map(s => s.trim()).filter(s => s),
    sortDir: (filters[8][0] || "ASC").toUpperCase(),
    sortCol: (filters[9][0] || "Item Name").trim(),
    limit: filters[14][0] === "No Limit" ? 5000 : parseInt(filters[14][0]) || 5000,
    ignoreGroups: (filters[21][0] || "").toLowerCase().split(',').map(s => s.trim()).filter(s => s)
  };

  const getIdx = (name) => headers.indexOf(name);
  const col = {
    name: getIdx("Item Name"),
    group: getIdx("Group"),
    warehouse: getIdx("Warehouse Qty"),
    action: getIdx("Buy Action"),
    vel: getIdx("Effective Daily Velocity (u/d)"),
    margin: getIdx("Margin"),
    effCost: getIdx("Effective Cost"),
    mfgCost: getIdx("Manufacturing Unit Cost"),
    hubBuy: getIdx("Hub Median Buy"),
    totalMkt: getIdx("Total Market Quantity"),
    vol30: getIdx("30-day traded volume"),
    // Inventory components
    qLeft: getIdx("Quantity Left"),
    jobs: getIdx("Active Jobs"),
    buyQty: getIdx("Listed Volume (Feed Buy)"),
    deliv: getIdx("Deliveries")
  };

  // --- PROCESS DATA IN-MEMORY ---
  let results = [];
  marketRows.forEach(row => {
    const name = String(row[col.name] || "");
    if (!name || auditMap.get(name) !== true) return; // The Audit Gate

    const action = String(row[col.action] || "");
    if (action === "" || /SKIP|HOLD/i.test(action)) return;

    const group = String(row[col.group] || "").toLowerCase().trim();
    if (cfg.ignoreGroups.includes(group)) return;
    if (cfg.groups.length > 0 && !cfg.groups.includes(group)) return;

    const velocity = Number(row[col.vel]) || 0;
    const currentStock = (Number(row[col.warehouse]) || 0) +
      (Number(row[col.qLeft]) || 0) +
      (Number(row[col.jobs]) || 0) +
      (Number(row[col.buyQty]) || 0) +
      (Number(row[col.deliv]) || 0);

    const restockNeed = Math.round((velocity * cfg.targetDays) - currentStock);
    if (restockNeed <= 0) return;

    const margin = Number(row[col.margin]) || 0;
    if (margin < cfg.minMargin) return;

    // Price Selection (LCM)
    const cost = Number(row[col.effCost]) || Number(row[col.mfgCost]) || Number(row[col.hubBuy]) || 0;

    results.push({
      data: [name, restockNeed, cost, restockNeed * cost, Number(row[col.totalMkt]) || 0, Number(row[col.vol30]) || 0, 0, Number(row[col.warehouse]) || 0, margin, action],
      sortKey: name // Default sort
    });
  });

  // --- SORTING ---
  results.sort((a, b) => {
    const modifier = cfg.sortDir === "ASC" ? 1 : -1;
    return a.sortKey.localeCompare(b.sortKey) * modifier;
  });

  // --- BATCH WRITE (Speed Move #3) ---
  const output = results.slice(0, cfg.limit).map(r => r.data);
  const clearRange = sheet.getRange(5, 3, Math.max(1, sheet.getLastRow()), 10);
  clearRange.clearContent();

  if (output.length > 0) {
    sheet.getRange(5, 3, output.length, 10).setValues(output);
  }
}

/**
 * OPTIMIZED: generateDumpToBuyOrder(ss)
 * Purpose: Strategic Liquidation with Column D "Replacement NOW", Header Fix, and Zero-Masking.
 * Layout: C: Item | D: Build Now | E: Eff Cost | F: Median Buy | G: Margin | H: Qty | I: Total ISK
 */
function generateDumpToBuyOrder(ss) {
  const TARGET_SHEET = 'Dump to Buy';
  const DATA_SHEET = 'MarketOverviewData'; 
  const CORP_ORDERS_SHEET = 'CorpOrdersCalc'; 

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET);
  const dataSheet = ss.getSheetByName(DATA_SHEET);
  const corpOrdersSheet = ss.getSheetByName(CORP_ORDERS_SHEET);
  
  if (!sheet || !dataSheet) return;

  // --- 1. TAX & BROKER FEE ---
  let rateMultiplier = 1.046;
  const fee = ss.getRangeByName("FEE_RATE")?.getValue() || 0.01;
  const tax = ss.getRangeByName("TAX_RATE")?.getValue() || 0.036;
  rateMultiplier = (1 + Number(fee) + Number(tax));

  // --- 2. CONTROL PANE ---
  const bParams = sheet.getRange("B5:B11").getValues();
  const filterMinDays = parseFloat(bParams[0][0]) || 0;    
  const filterMinMargin = parseFloat(bParams[4][0]) || 0;  
  const filterGroupName = String(bParams[6][0] || "").toLowerCase().trim(); 

  // --- 3. HEADER FIX (C4:I4) ---
  const headerLabels = [["Item Name", "Manufacturing Projected Cost", "Effective Cost", "Hub Median Buy", "Forensic Margin", "Warehouse Qty", "Total Dump ISK"]];
  sheet.getRange("C4:I4").setValues(headerLabels).setFontWeight("bold").setBackground("#f3f3f3");

  // --- 4. CORP SAFETY (O(1) Lookup) ---
  const corpBuyPrices = new Set(); 
  if (corpOrdersSheet) {
    const corpData = corpOrdersSheet.getDataRange().getValues();
    const h = corpData[1] || []; 
    const pIdx = h.indexOf("price");
    const tIdx = h.indexOf("type_id");
    const bIdx = h.indexOf("is_buy_order");

    for (let i = 2; i < corpData.length; i++) {
      if (corpData[i][bIdx] === true || corpData[i][bIdx] === "TRUE") {
        corpBuyPrices.add(`${corpData[i][tIdx]}_${parseFloat(corpData[i][pIdx]).toFixed(2)}`);
      }
    }
  }

  // --- 5. COLUMN MAPPING ---
  const fullData = dataSheet.getDataRange().getValues();
  let hIdx = -1;
  for (let i = 0; i < 10; i++) {
    if (fullData[i].indexOf("Item Name") > -1) { hIdx = i; break; }
  }
  if (hIdx === -1) return;
  
  const headers = fullData[hIdx];
  const col = {
    id: headers.indexOf("type_id"),
    item: headers.indexOf("Item Name"),
    group: headers.indexOf("Group"),
    effCost: headers.indexOf("Effective Cost"),
    buildNow: headers.indexOf("Manufacturing Projected Unit Cost"),
    medianBuy: headers.indexOf("Hub Median Buy"),
    whQty: headers.indexOf("Warehouse Qty"),
    daysInv: headers.indexOf("Days of Inventory")
  };

  const rawData = fullData.slice(hIdx + 1);
  const dumpResults = [];
  const MIN_VALID_COST = 5.00; 
  const showAll = (!filterGroupName || filterGroupName === "manufacturing");

  // --- 6. SCAN DATA ---
  for (let i = 0; i < rawData.length; i++) {
    const r = rawData[i];
    const name = r[col.item];
    if (!name) continue;

    const group = String(r[col.group] || "").toLowerCase().trim();
    if (!showAll && group !== filterGroupName) continue;
    
    const daysOfInv = r[col.daysInv] || 0;
    if (daysOfInv < filterMinDays) continue;

    const hubBuy = r[col.medianBuy] || 0;
    if (hubBuy < MIN_VALID_COST) continue; 

    if (corpBuyPrices.has(`${r[col.id]}_${hubBuy.toFixed(2)}`)) continue;

    const effCost = r[col.effCost] || 0;
    const buildNow = r[col.buildNow] || 0;
    
    // Reality Floor Logic
    let realityFloor = 0;
    if (buildNow > MIN_VALID_COST) {
      realityFloor = effCost > buildNow ? effCost : buildNow; 
    } else if (effCost > MIN_VALID_COST) {
      realityFloor = effCost;
    } else {
      continue; 
    }

    const netProceeds = hubBuy / rateMultiplier; 
    const margin = (netProceeds - realityFloor) / realityFloor;

    if (margin >= filterMinMargin) {
      const qty = r[col.whQty] || 0;
      if (qty > 0) {
        dumpResults.push([
          name,
          buildNow <= 0 ? "" : buildNow, // READABILITY: Mask zero build costs as blank
          effCost,
          hubBuy,
          margin,
          qty,
          (hubBuy * qty)
        ]);
      }
    }
  }

  // --- 7. SORT & BATCH WRITE ---
  dumpResults.sort((a, b) => b[4] - a[4]); 

  const START_ROW = 5;
  const maxRows = sheet.getMaxRows();
  if (maxRows >= START_ROW) {
    sheet.getRange(START_ROW, 3, maxRows - (START_ROW - 1), 7).clearContent();
  }

  if (dumpResults.length > 0) {
    sheet.getRange(START_ROW, 3, dumpResults.length, 7).setValues(dumpResults);
    sheet.getRange(START_ROW, 7, dumpResults.length, 1).setNumberFormat("0.00%");
  }
  
  ss.toast("Strategic Recovery complete. Meta-modules now show blank Build Costs for readability.", "Forensic Engine");
}

function generateRestockItemsOnHand(ss) {
  const TARGET_SHEET = 'Restock Items On Hand';
  const DATA_SHEET = 'MarketOverviewData';
  const AUDIT_SHEET = 'Audit items';

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET);
  const dataSheet = ss.getSheetByName(DATA_SHEET);
  const auditSheet = ss.getSheetByName(AUDIT_SHEET);

  if (!sheet || !dataSheet || !auditSheet) return;

  const clean = (v) => {
    if (typeof v === 'number') return v;
    return parseFloat(String(v).replace(/[^0-9.-]/g, '')) || 0;
  };

  // --- OPTIMIZATION 1: PRE-LOAD AUDIT DATA (Outside Loop) ---
  const auditValues = auditSheet.getDataRange().getValues();
  const auditMap = new Map(auditValues.slice(1).map(r => [String(r[0]), r[1]]));

  // --- OPTIMIZATION 2: BATCH LOAD MARKET DATA ---
  const rawDataValues = dataSheet.getDataRange().getValues();
  const headers = rawDataValues[2]; // Row 3
  const rawData = rawDataValues.slice(3); // Row 4+

  const getIdx = (name) => {
    let i = headers.indexOf(name);
    return (i === -1 && name === "Posted Sell Quantuty") ? headers.indexOf("Quantity Left") : i;
  };

  const col = {
    item: getIdx("Item Name"),
    targetGoal: getIdx("Target"),
    sellQty: getIdx("Posted Sell Quantuty"),
    whQty: getIdx("Warehouse Qty"),
    effVel: getIdx("Effective Daily Velocity (u/d)"),
    hubSell: getIdx("Hub Sell Price"),
    medROI: getIdx("Median ROI"),
    sellAct: getIdx("Sell Action"),
    hubBuy: getIdx("Hub Buy Price"),
    mktQty: getIdx("Total Market Quantity"),
    effCost: getIdx("Effective Cost"),
    mfgCost: getIdx("Manufacturing Unit Cost")
  };

  // --- OPTIMIZATION 3: PRE-LOAD FILTERS ---
  const bCol = sheet.getRange("A1:B45").getValues();
  const seedDays = clean(bCol[38][0]) || 3;
  const minROI = clean(bCol[7][1]);
  const minOrderValue = 1000000; // Add an optional ISK floor (1M ISK)

  const OUT_HEADERS = [
    "Item Name", "Posting Price", "Hub Sell Price", "Quantity", "Total Value", "Delta Sell", "Delta Buy",
    "Warehouse Level", "Pending Orders", "Total Market Quantity", "Warehouse Qty", "Acquisition (30d)",
    "Effective Daily Velocity (u/d)", "30-day traded volume", "Listed Volume (Feed Sell)",
    "Feed Days of Book", "Hub Median Buy", "Effective Cost", "Sell Action", "Buy Action", "Sell Quantity"
  ];

  let resultRows = [];

  // --- MAIN PROCESSING LOOP ---
  for (let r of rawData) {
    const rawName = String(r[col.item] || "");
    if (!rawName) continue;

    // --- THE AUDIT GATE ---
    if (auditMap.get(rawName) !== true) continue;

    const action = String(r[col.sellAct] || "");
    if (/SKIP|HOLD|IGNORE/i.test(action) || action === "") continue;

    const velocity = clean(r[col.effVel]);
    const warehouseStock = clean(r[col.whQty]);
    const currentMarket = clean(r[col.sellQty]);
    const targetGoal = clean(r[col.targetGoal]);

    // Calculate how much we can actually restock from the warehouse
    let targetNeeded = velocity * seedDays;
    if (targetGoal > 0) targetNeeded = Math.min(targetNeeded, targetGoal);

    let gap = Math.max(0, targetNeeded - currentMarket);
    let finalQuantity = Math.round(Math.min(gap, warehouseStock));

    if (finalQuantity <= 0) continue;

    const hubSell = clean(r[col.hubSell]);
    const baseCost = clean(r[col.effCost]) || clean(r[col.mfgCost]);
    const postPrice = Math.max(hubSell, baseCost * (1 + minROI));
    const totalOrderValue = finalQuantity * postPrice;

    // --- MINIMUM ISK FILTER ---
    if (totalOrderValue < minOrderValue) continue;

    let itemName = (rawName.charAt(0) === "'") ? rawName : rawName;

    resultRows.push([
      itemName, postPrice, hubSell, finalQuantity, totalOrderValue,
      r[getIdx("Delta Sell")], r[getIdx("Delta Buy")], r[getIdx("Warehouse Level")],
      r[getIdx("Pending Orders")], r[col.mktQty], warehouseStock,
      r[getIdx("Acquisition (30d)")], velocity, r[getIdx("30-day traded volume")],
      r[getIdx("Listed Volume (Feed Sell)")], r[getIdx("Feed Days of Book")],
      clean(r[col.hubBuy]), baseCost, action, r[getIdx("Buy Action")], currentMarket
    ]);
  }

  // --- OPTIMIZATION 4: BATCH CLEAR AND WRITE ---
  const maxRows = Math.max(1, sheet.getLastRow());
  sheet.getRange(3, 3, maxRows, 21).clearContent();
  sheet.getRange(3, 3, 1, 21).setValues([OUT_HEADERS]).setFontWeight("bold");

  if (resultRows.length > 0) {
    sheet.getRange(4, 3, resultRows.length, 21).setValues(resultRows);
  }
}


function ON_SDE_START() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert('⚠️ Update SDE Database?', 'This will download fresh data and pause the orchestrator. Proceed?', ui.ButtonSet.YES_NO);
  if (response == ui.Button.NO) return false;
  _manageOrchestrator(false);
  return true;
}

function ON_SDE_COMPLETE() {
  _manageOrchestrator(true);
}

function _manageOrchestrator(turnOn) {
  const FUNCTION_NAME = 'masterOrchestrator';
  const allTriggers = ScriptApp.getProjectTriggers();
  allTriggers.forEach(t => { if (t.getHandlerFunction() === FUNCTION_NAME) ScriptApp.deleteTrigger(t); });
  if (turnOn) ScriptApp.newTrigger(FUNCTION_NAME).timeBased().everyMinutes(10).create();
}

function sqlFromHeaderNames(rangeName, queryString, useColNums) {
  let ss = SpreadsheetApp.getActiveSpreadsheet();
  let range = ss.getRangeByName(rangeName) || ss.getRange(rangeName);
  let headers = range.getValues()[0];
  for (var i = 0; i < headers.length; i++) {
    if (headers[i].length < 1) continue;
    var re = new RegExp("\\b" + headers[i] + "\\b", "gm");
    queryString = queryString.replace(re, useColNums ? "Col" + (i + 1) : range.getCell(1, i + 1).getA1Notation().split(/[0-9]/)[0]);
  }
  return queryString;
}