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
    //refreshData();
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

/**
 * EVE ONLINE MARKET TRADER SCRIPT: LCM EDITION
 * This script plugs ISK leaks by choosing the lowest of:
 * Blended Cost, Manufacturing Cost, or Hub Buy Price.
 */

function generateRestockQuery(ss) {
  const TARGET_SHEET_NAME = 'Need To Buy';
  const DATA_SHEET_NAME = 'MarketOverviewData';
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  const dataSheet = ss.getSheetByName(DATA_SHEET_NAME);

  if (!sheet || !dataSheet) return;

  // --- 1. DYNAMIC COLUMN MAPPING ---
  const lastCol = dataSheet.getLastColumn();
  const headers = dataSheet.getRange(3, 1, 1, lastCol).getValues()[0];
  const getIdx = (name) => headers.indexOf(name);

  const colMap = {
    itemName: getIdx("Item Name"), 
    group: getIdx("Group"), 
    qtyLeft: getIdx("Quantity Left"),
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
    buyAction: getIdx("Buy Action"),
    effCost: getIdx("Effective Cost"), // <--- THE LCM COLUMN
    mfgCost: getIdx("Manufacturing Unit Cost")
  };

  if (colMap.itemName === -1) return;

  // --- 2. READ FILTERS ---
  const filterValues = sheet.getRange('B5:B26').getValues();
  const filterMinDays = parseFloat(filterValues[0][0]) || 0;
  const filterTargetDays = parseFloat(filterValues[2][0]) || 0;
  const filterMargin = parseFloat(filterValues[4][0]) || 0;
  const filterGroups = (filterValues[6][0] || "").toString().toLowerCase().split(',').map(g => g.trim()).filter(g => g);
  const sortDirection = (filterValues[8][0] || "ASC").toString().toUpperCase();
  const sortColumnHeader = (filterValues[9][0] || "Item Name").toString().trim();
  let limitNum = filterValues[14][0] === "No Limit" ? 999999 : (parseFloat(filterValues[14][0]) || 999999);
  const filterIgnoreGroups = (filterValues[21][0] || "").toString().toLowerCase().split(',').map(g => g.trim()).filter(g => g);

  // --- 3. READ TAX RATES ---
  let rateMultiplier = 1.0;
  try {
    const fee = ss.getRangeByName("FEE_RATE")?.getValue() || 0;
    const tax = ss.getRangeByName("TAX_RATE")?.getValue() || 0;
    rateMultiplier = (1 + Number(fee) + Number(tax));
  } catch (e) { }

  const lastRow = dataSheet.getLastRow();
  if (lastRow < 4) return;
  const rawData = dataSheet.getRange(4, 1, lastRow - 3, lastCol).getValues();

  // --- 5. PROCESS DATA ---
  let processedData = [];

  for (let i = 0; i < rawData.length; i++) {
    const row = rawData[i];
    const rawName = String(row[colMap.itemName] || "");
    if (!rawName) continue;

    const itemName = (rawName.charAt(0) === "'") ? "'" + rawName : rawName;
    const warehouseAmount = Number(row[colMap.warehouseQty]) || 0;
    const buyAction = String(row[colMap.buyAction] || "");
    if (buyAction.includes("SKIP") || buyAction.includes("HOLD") || buyAction === "") continue;

    const margin = Number(row[colMap.margin]) || 0;
    const daysInv = Number(row[colMap.daysOfInv]) || 0;
    const volume = Number(row[colMap.volume]) || 0;
    const group = String(row[colMap.group] || "").toLowerCase().trim();

    // Inventory Calc
    const stock = warehouseAmount + (Number(row[colMap.qtyLeft]) || 0) + (Number(row[colMap.activeJobs]) || 0) + (Number(row[colMap.buyOrderQty]) || 0) + (Number(row[colMap.deliveries]) || 0);
    const velocity = Number(row[colMap.effectiveVel]) || 0;
    const targetQty = velocity * filterTargetDays;

    let restockNeed = Math.round(targetQty - stock);
    if (restockNeed <= 0) continue;
    if (margin < filterMargin || daysInv >= filterMinDays) continue;
    if (filterGroups.length > 0 && !filterGroups.includes(group)) continue;
    if (filterIgnoreGroups.includes(group)) continue;

    // --- LCM PRICE SELECTION ---
    const hubBuy = Number(row[colMap.medianBuy]) || 0;
    const mfg = Number(row[colMap.mfgCost]) || 0;
    const eff = Number(row[colMap.effCost]) || 0;

    // Use the cheapest available acquisition cost
    const baseCost = (eff > 0) ? eff : (mfg > 0 ? mfg : hubBuy);
    const orderCost = restockNeed * baseCost * rateMultiplier;

    processedData.push({
      row: [itemName, restockNeed, baseCost, orderCost, Number(row[colMap.totalMarketQty]) || 0, volume, 0, warehouseAmount, margin, buyAction],
      sortObj: { itemName, restockNeed, orderCost, margin, volume }
    });
  }

  // --- 6. SORT ---
  processedData.sort((a, b) => {
    let vA = a.sortObj.itemName, vB = b.sortObj.itemName;
    if (sortColumnHeader === 'Quantity') { vA = a.sortObj.restockNeed; vB = b.sortObj.restockNeed; }
    else if (sortColumnHeader === 'Order Cost') { vA = a.sortObj.orderCost; vB = b.sortObj.orderCost; }
    else if (sortColumnHeader === 'Margin') { vA = a.sortObj.margin; vB = b.sortObj.margin; }
    else if (sortColumnHeader === 'Volume') { vA = a.sortObj.volume; vB = b.sortObj.volume; }
    if (vA < vB) return sortDirection === 'ASC' ? -1 : 1;
    if (vA > vB) return sortDirection === 'ASC' ? 1 : -1;
    return 0;
  });

  if (limitNum && processedData.length > limitNum) processedData = processedData.slice(0, limitNum);

  // --- 7. WRITE ---
  const HEADER_ROW = 4;
  sheet.getRange(HEADER_ROW, 3, Math.max(1, sheet.getLastRow() - HEADER_ROW + 1), 10).clearContent();
  sheet.getRange(HEADER_ROW, 3, 1, 10).setValues([["Item Name", "Quantity", "Effective Cost", "Order Cost", "Total Market Quantity", "Volume", "0", "Warehouse Qty", "Margin", "Buy Action"]]).setFontWeight("bold");

  if (processedData.length > 0) {
    const out = processedData.map(p => p.row);
    sheet.getRange(HEADER_ROW + 1, 3, out.length, out[0].length).setValues(out);
  }
}

function generateRestockItemsOnHand(ss) {
  const TARGET_SHEET = 'Restock Items On Hand';
  const DATA_SHEET = 'MarketOverviewData';
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET);
  const dataSheet = ss.getSheetByName(DATA_SHEET);
  if (!sheet || !dataSheet) return;

  const clean = (v) => {
    if (typeof v === 'number') return v;
    return parseFloat(String(v).replace(/[^0-9.-]/g, '')) || 0;
  };

  const headers = dataSheet.getRange(3, 1, 1, dataSheet.getLastColumn()).getValues()[0];
  const getIdx = (name) => {
    let i = headers.indexOf(name);
    return (i === -1 && name === "Posted Sell Quantuty") ? headers.indexOf("Quantity Left") : i;
  };

  const colMap = {
    item: getIdx("Item Name"), targetGoal: getIdx("Target"), 
    sellQty: getIdx("Posted Sell Quantuty"), whQty: getIdx("Warehouse Qty"),
    effVel: getIdx("Effective Daily Velocity (u/d)"), hubSell: getIdx("Hub Sell Price"),
    medROI: getIdx("Median ROI"), sellAct: getIdx("Sell Action"),
    hubBuy: getIdx("Hub Buy Price"), mktQty: getIdx("Total Market Quantity"),
    effCost: getIdx("Effective Cost"), mfgCost: getIdx("Manufacturing Unit Cost")
  };

  const bCol = sheet.getRange("A1:B45").getValues();
  const seedDays = clean(bCol[38][0]) || 3; 
  const minROI = clean(bCol[7][1]);

  const lastRow = dataSheet.getLastRow();
  if (lastRow < 4) return;
  const rawData = dataSheet.getRange(4, 1, lastRow - 3, dataSheet.getLastColumn()).getValues();

  const OUT_HEADERS = [
    "Item Name", "Posting Price", "Hub Sell Price", "Quantity", "Total Value", "Delta Sell", "Delta Buy",
    "Warehouse Level", "Pending Orders", "Total Market Quantity", "Warehouse Qty", "Acquisition (30d)",
    "Effective Daily Velocity (u/d)", "30-day traded volume", "Listed Volume (Feed Sell)", 
    "Feed Days of Book", "Hub Median Buy", "Effective Cost", "Sell Action", "Buy Action", "Sell Quantity"
  ];

  let resultRows = [];

  for (let r of rawData) {
    if (!r[colMap.item]) continue;

    let itemName = String(r[colMap.item]);
    if (itemName.charAt(0) === "'") itemName = "'" + itemName;

    const velocity = clean(r[colMap.effVel]);
    const warehouseStock = clean(r[colMap.whQty]);
    const currentMarket = clean(r[colMap.sellQty]);
    const targetGoal = clean(r[colMap.targetGoal]);

    let targetNeeded = velocity * seedDays;
    if (targetGoal > 0) targetNeeded = Math.min(targetNeeded, targetGoal);

    let gap = Math.max(0, targetNeeded - currentMarket);
   let finalQuantity = Math.round(Math.min(gap, warehouseStock));

    if (finalQuantity <= 0) continue;

    const action = String(r[colMap.sellAct] || "");
    if (action.includes("SKIP") || action.includes("HOLD") || action.includes("IGNORE")) continue;

    const roiVal = clean(r[colMap.medROI]);
    if (roiVal < minROI) continue;

    const hubSell = clean(r[colMap.hubSell]);
    
    // --- LCM SYNC ---
    // Look at your sheet's Effective Cost column first (it's the smartest value).
    const baseCost = clean(r[colMap.effCost]) || clean(r[colMap.mfgCost]);
    
    // This price is now "Un-Ghosted"
    const postPrice = Math.max(hubSell, baseCost * (1 + minROI));

    resultRows.push([
      itemName, postPrice, hubSell, finalQuantity, (finalQuantity * postPrice),
      r[getIdx("Delta Sell")], r[getIdx("Delta Buy")], r[getIdx("Warehouse Level")],
      r[getIdx("Pending Orders")], r[colMap.mktQty], warehouseStock,
      r[getIdx("Acquisition (30d)")], velocity, r[getIdx("30-day traded volume")],
      r[getIdx("Listed Volume (Feed Sell)")], r[getIdx("Feed Days of Book")],
      clean(r[colMap.hubBuy]), baseCost, action, r[getIdx("Buy Action")], currentMarket
    ]);
  }

  const clearRows = Math.max(1, sheet.getLastRow() - 2);
  sheet.getRange(3, 3, clearRows, 21).clearContent();
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