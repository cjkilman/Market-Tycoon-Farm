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
    .addItem('Generate Projected Build Costs','generateProjectedCostTable')
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
  const CELLS_RESTOCK = ['B6', 'B8', 'B10', 'B12', 'B14', 'B15', 'B17', 'B18', 'B20', 'B21', 'B23', 'B25', 'B27', 'B28', 'B33', 'B36', 'B39', 'B42'];

  if (sheetName === SHEET_BUY && CELLS_BUY.includes(cellA1)) {
    generateRestockQuery(ss);
  } else if (sheetName === SHEET_RESTOCK && CELLS_RESTOCK.includes(cellA1)) {
    generateRestockItemsOnHand(ss);
  }
}

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
    itemName: getIdx("Item Name"), group: getIdx("Group"), qtyLeft: getIdx("Quantity Left"),
    activeJobs: getIdx("Active Jobs"), deliveries: getIdx("Deliveries"),
    volume: getIdx("30-day traded volume"), buyOrderQty: getIdx("Listed Volume (Feed Buy)"),
    effectiveVel: getIdx("Effective Daily Velocity (u/d)"), warehouseQty: getIdx("Warehouse Qty"),
    daysOfInv: getIdx("Days of Inventory"), totalMarketQty: getIdx("Total Market Quantity"),
    medianBuy: getIdx("Hub Median Buy"), margin: getIdx("Margin"), buyAction: getIdx("Buy Action")
  };

  if (colMap.itemName === -1) return;

  // --- 2. READ FILTERS ---
  const filterValues = sheet.getRange('B5:B26').getValues();
  const filterMinDays = parseFloat(filterValues[0][0]) || 0;
  const filterTargetDays = parseFloat(filterValues[2][0]) || 0;
  const filterMargin = parseFloat(filterValues[4][0]) || 0;
  
  // [UPDATED] Split Group Selection by commas & trim for Exact Match
  const filterGroups = (filterValues[6][0] || "").toString().toLowerCase().split(',').map(g => g.trim()).filter(g => g);

  const sortDirection = (filterValues[8][0] || "ASC").toString().toUpperCase();
  const sortColumnHeader = (filterValues[9][0] || "Item Name").toString().trim();
  let limitNum = filterValues[14][0] === "No Limit" ? 999999 : (parseFloat(filterValues[14][0]) || 999999);

  const filterVolumeType = filterValues[16][0];
  const filterVolumeValue = parseFloat(filterValues[17][0]) || 0;
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

  // --- 5. PROCESS DATA (Optimized for Speed & Quotes) ---
  let processedData = [];

  for (let i = 0; i < rawData.length; i++) {
    const row = rawData[i];
    const rawName = String(row[colMap.itemName] || "");
    if (!rawName) continue;

    // --- ESCAPE FIX: Prepend additional quote if name starts with ' ---
    const itemName = (rawName.charAt(0) === "'") ? "'" + rawName : rawName;

    const warehouseAmount = Number(row[colMap.warehouseQty]) || 0;
    // [FIX] Disabled warehouse check to allow restocking empty items
    // if (warehouseAmount <= 0) continue;

    const buyAction = String(row[colMap.buyAction] || "");
    // [FIX] Allowed MANUFACTURE items by only filtering out SKIP/HOLD/Empty
    if (buyAction.includes("SKIP") || buyAction.includes("HOLD") || buyAction === "" ) continue;

    const margin = Number(row[colMap.margin]) || 0;
    const daysInv = Number(row[colMap.daysOfInv]) || 0;
    const volume = Number(row[colMap.volume]) || 0;
    
    // [UPDATED] Trim group name to ensure clean exact matching
    const group = String(row[colMap.group] || "").toLowerCase().trim();

    // Inventory Calc
    const stock = warehouseAmount + (Number(row[colMap.qtyLeft]) || 0) + (Number(row[colMap.activeJobs]) || 0) + (Number(row[colMap.buyOrderQty]) || 0) + (Number(row[colMap.deliveries]) || 0);
    const velocity = Number(row[colMap.effectiveVel]) || 0;
    const targetQty = velocity * filterTargetDays;

    let restockNeed = Math.round(targetQty - stock);
    // [FIX] Removed cap so you buy full amount even if warehouse is 0
    // restockNeed = Math.min(restockNeed, warehouseAmount);

    if (restockNeed <= 0) continue;
    if (margin < filterMargin || daysInv >= filterMinDays) continue;

    // [UPDATED] INCLUSION FILTER: EXACT MATCH (Case Insensitive)
    // Only passes if the exact group name exists in the filter list
    if (filterGroups.length > 0 && !filterGroups.includes(group)) continue;
    
    // EXCLUSION FILTER: Exact match
    if (filterIgnoreGroups.includes(group)) continue;

    const medianBuyPrice = Number(row[colMap.medianBuy]) || 0;
    const orderCost = restockNeed * medianBuyPrice * rateMultiplier;

    processedData.push({
      row: [itemName, restockNeed, medianBuyPrice, orderCost, Number(row[colMap.totalMarketQty]) || 0, volume, 0, warehouseAmount, margin, buyAction],
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
  sheet.getRange(HEADER_ROW, 3, 1, 10).setValues([["Item Name", "Quantity", "Median Buy Price", "Order Cost", "Total Market Quantity", "Volume", "0", "Warehouse Qty", "Margin", "Buy Action"]]).setFontWeight("bold");

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

  const OUT_HEADERS = ["Item Name", "Posting Price", "Hub Sell Price", "Seed Qty (units)", "Seed Posting Cost (ISK)", "Delta Sell", "Delta Buy", "Total Value", "Quantity", "Warehouse Level", "Pending Orders", "Projected Buy", "Projected Value", "Total Market Quantity", "Warehouse Qty", "Acquisition (30d)", "Effective Daily Velocity (u/d)", "30-day traded volume", "Listed Volume (Feed Sell)", "Feed Days of Book", "Hub Median Buy", "Effective Cost", "Sell Action", "Buy Action", "Sell Quantity"];
  const bCol = sheet.getRange("B1:B45").getValues();
  const minROI = parseFloat(bCol[7][0]) || 0;
  const boost = (parseFloat(bCol[9][0]) > 1) ? parseFloat(bCol[9][0]) / 100 : (parseFloat(bCol[9][0]) || 0);
  const sortDir = (bCol[13][0] || "ASC").toString().toUpperCase();
  const sortColName = String(bCol[14][0] || "Item Name");

  const headers = dataSheet.getRange(3, 1, 1, dataSheet.getLastColumn()).getValues()[0];
  const getIdx = (name) => {
    let i = headers.indexOf(name);
    return (i === -1 && name === "Posted Sell Quantuty") ? headers.indexOf("Quantity Left") : i;
  };
  const colMap = { item: getIdx("Item Name"), target: getIdx("Target"), hubSell: getIdx("Hub Sell Price"), effCost: getIdx("Effective Cost"), sellQty: getIdx("Posted Sell Quantuty"), whQty: getIdx("Warehouse Qty"), medROI: getIdx("Median ROI"), sellAct: getIdx("Sell Action"), hubBuy: getIdx("Hub Buy Price"), mktQty: getIdx("Total Market Quantity"), seedQty: getIdx("Seed Qty (units)"), seedCost: getIdx("Seed Posting Cost (ISK)"), deltaSell: getIdx("Delta Sell"), deltaBuy: getIdx("Delta Buy"), whLevel: getIdx("Warehouse Level"), pending: getIdx("Pending Orders"), buyVol: getIdx("Buy Vol"), mfgComp: getIdx("Manufactured Completed"), lootTrans: getIdx("Loot Transfered"), charCont: getIdx("Char Contracts"), effVel: getIdx("Effective Daily Velocity (u/d)"), vol30: getIdx("30-day traded volume"), listVol: getIdx("Listed Volume (Feed Sell)"), feedDays: getIdx("Feed Days of Book"), hubMedBuy: getIdx("Hub Median Buy"), buyAct: getIdx("Buy Action"), mfgCost: getIdx("Manufacturing Unit Cost") };

  const rawData = dataSheet.getRange(4, 1, dataSheet.getLastRow() - 3, headers.length).getValues();
  let resultRows = [];

  for (let r of rawData) {
    const rawName = String(r[colMap.item] || "");
    if (!rawName) continue;

    const whQty = Number(r[colMap.whQty]) || 0;
    if (whQty <= 0) continue;

    // --- ESCAPE FIX: Prepend additional quote for display ---
    const itemName = (rawName.charAt(0) === "'") ? "'" + rawName : rawName;

    const sellAction = String(r[colMap.sellAct] || "");
    if (sellAction.includes("HOLD") || sellAction.includes("IGNORE") || sellAction.includes("SKIP")) continue;

    const hubSell = Number(r[colMap.hubSell]) || 0;
    const effCost = Number(r[colMap.effCost]) || 0;
    const mfgCost = Number(r[colMap.mfgCost]) || 0;
    const baseCost = (mfgCost > 0) ? mfgCost : effCost;
    const floorPrice = baseCost * (1 + minROI);
    const postPrice = (hubSell >= floorPrice) ? hubSell : floorPrice;

    const postedQty = Number(r[colMap.sellQty]) || 0;
    const target = Number(r[colMap.target]) || 0;
    const gap = Math.max(0, target - postedQty);
    let qtyToList = Math.min(gap, whQty);
    if (qtyToList <= 0) continue;

    const roiVal = (typeof r[colMap.medROI] === 'string') ? parseFloat(r[colMap.medROI]) / 100 : (Number(r[colMap.medROI]) || 0);
    if (roiVal < minROI) continue;

    const pending = Number(r[colMap.pending]) || 0;
    const projBuy = Math.max(0, (target * (1 + boost)) - pending);
    const acq30 = (Number(r[colMap.buyVol]) || 0) + (Number(r[colMap.mfgComp]) || 0) + (Number(r[colMap.lootTrans]) || 0) + (Number(r[colMap.charCont]) || 0);

    resultRows.push({
      row: [itemName, postPrice, hubSell, r[colMap.seedQty], r[colMap.seedCost], r[colMap.deltaSell], r[colMap.deltaBuy], (qtyToList * postPrice), qtyToList, r[colMap.whLevel], pending, projBuy, (projBuy * (Number(r[colMap.hubBuy]) || 0)), r[colMap.mktQty], whQty, acq30, r[colMap.effVel], r[colMap.vol30], r[colMap.listVol], r[colMap.feedDays], r[colMap.hubMedBuy], effCost, sellAction, r[colMap.buyAct], postedQty],
      sortVal: (sortColName === 'Margin' || sortColName === 'Median ROI') ? roiVal : itemName
    });
  }

  let sortIdx = OUT_HEADERS.indexOf(sortColName);
  resultRows.sort((a, b) => {
    let vA = (sortIdx > -1) ? a.row[sortIdx] : a.row[0];
    let vB = (sortIdx > -1) ? b.row[sortIdx] : b.row[0];
    if (vA < vB) return sortDir === 'ASC' ? -1 : 1;
    if (vA > vB) return sortDir === 'ASC' ? 1 : -1;
    return 0;
  });

  const clearRows = Math.max(1, sheet.getLastRow() - 2);
  sheet.getRange(3, 3, clearRows, OUT_HEADERS.length).clearContent();
  sheet.getRange(3, 3, 1, OUT_HEADERS.length).setValues([OUT_HEADERS]).setFontWeight("bold");
  if (resultRows.length > 0) {
    const finalData = resultRows.map(o => o.row);
    sheet.getRange(4, 3, finalData.length, finalData[0].length).setValues(finalData);
  }
}

function updateControlSheet() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const GLOBAL_STATE_KEY = 'GLOBAL_SYSTEM_STATE';
  if (typeof isSdeJobRunning !== 'undefined' && isSdeJobRunning()) return;

  try {
    SCRIPT_PROP.setProperty(GLOBAL_STATE_KEY, 'MAINTENANCE');
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

    const itemDataRaw = itemSheet.getDataRange().getValues();
    let itemHeaderRowIdx = -1, nameColIdx = -1, idColIdx = -1;

    for (let r = 0; r < Math.min(10, itemDataRaw.length); r++) {
      const row = itemDataRaw[r].map(c => String(c).trim().toLowerCase());
      CONFIG.ITEM_ID_HEADERS.forEach(h => { if (row.indexOf(h.toLowerCase()) > -1) { idColIdx = row.indexOf(h.toLowerCase()); itemHeaderRowIdx = r; } });
      CONFIG.ITEM_NAME_HEADERS.forEach(h => { if (row.indexOf(h.toLowerCase()) > -1) { nameColIdx = row.indexOf(h.toLowerCase()); } });
      if (idColIdx > -1) break;
    }

    const rawItems = [];
    if (idColIdx > -1) {
      for (let i = itemHeaderRowIdx + 1; i < itemDataRaw.length; i++) {
        const val = Number(itemDataRaw[i][idColIdx]);
        if (val > 0) rawItems.push(val);
      }
    }
    const uniqueItemIds = Array.from(new Set(rawItems));

    const locDataRaw = locationSheet.getDataRange().getValues();
    const locSet = new Set();
    const headerRow = locDataRaw[4].map(c => String(c).trim().toLowerCase());
    const locMap = {};
    CONFIG.LOC_HEADERS.forEach(h => locMap[h] = headerRow.indexOf(h.toLowerCase()));

    for (let i = 5; i < locDataRaw.length; i++) {
      const row = locDataRaw[i];
      CONFIG.LOC_HEADERS.forEach(type => {
        const idx = locMap[type];
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

    const output = [];
    for (const loc of locations) {
      for (const itemId of uniqueItemIds) {
        output.push([itemId, loc.type, loc.id, '']);
      }
    }

    controlSheet.clear();
    controlSheet.getRange(1, 1, 1, 4).setValues([['type_id', 'location_type', 'location_id', 'last_updated']]);
    controlSheet.getRange(2, 1, output.length, 4).setValues(output);

  } catch (e) {
    console.error(e.message);
  } finally {
    SCRIPT_PROP.setProperty(GLOBAL_STATE_KEY, 'RUNNING');
  }
}

function refreshData() {
  const conf = GET_UTILITY_CONFIG();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(conf.sheetName);
  if (sheet) {
    sheet.getRange(conf.range).setValues([[1, 1]]);
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