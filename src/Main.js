// Critical Note
// Posted Buy orders labled Quantity Left
// Market Order Book is Listed Volume



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



function triggerRestockSync() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const props = PropertiesService.getScriptProperties();
  
  ss.toast("🚀 Nitro Sync: Locking RAM...", "Engine Room", 5);
  
  try {
    // Set a lock so Maintenance Jobs don't start and cause a timeout
    props.setProperty('MANUAL_SYNC_ACTIVE', 'TRUE');

    const dataSheet = ss.getSheetByName('MarketOverviewData');
    // Speed Move: Load the entire market state into memory once
    const fullData = dataSheet.getDataRange().getValues();

    // Pass the pre-loaded data to the sub-functions
    generateRestockQuery(ss, fullData);
    generateRestockItemsOnHand(ss, fullData);
    generateDumpToBuyOrder(ss, fullData);

    ss.toast("✅ Sync Complete. Releasing Lock.", "Engine Room", 3);
  } catch (e) {
    ss.toast("❌ Sync failed: " + e.message, "Engine Room Error");
    console.error(e);
  } finally {
    // CRITICAL: Always release the lock, even on failure
    props.setProperty('MANUAL_SYNC_ACTIVE', 'FALSE');
  }
}

function respondToEdit(e) {
  if (!e || !e.range || !e.source) return;

  // SPEED GATE: Only trigger if it's been at least 2 seconds since the last edit
  const now = new Date().getTime();
  const lastRun = parseInt(SCRIPT_PROPS.getProperty('LAST_EDIT_TS') || '0');
  if (now - lastRun < 2000) return; 
  SCRIPT_PROPS.setProperty('LAST_EDIT_TS', now.toString());

  const col = e.range.columnStart;
  if (col > 2) return; // Ignore edits outside columns A and B

  const sheetName = e.range.getSheet().getName();
  if (sheetName === 'Need To Buy') {
    generateRestockQuery(e.source);
  } else if (sheetName === 'Restock Items On Hand') {
    generateRestockItemsOnHand(e.source);
  }
}

function generateRestockQuery(ss, preLoadedData) {
  const TARGET_SHEET_NAME = 'Need To Buy';
  const DATA_SHEET_NAME = 'MarketOverviewData';
  const ITEM_LIST_NAME = 'Item List'; 
  const AUDIT_SHEET_NAME = 'Audit items';

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  const dataSheet = ss.getSheetByName(DATA_SHEET_NAME);
  const auditSheet = ss.getSheetByName(AUDIT_SHEET_NAME);
  const itemListSheet = ss.getSheetByName(ITEM_LIST_NAME);

  if (!sheet || !dataSheet || !auditSheet || !itemListSheet) return;

  // --- HELPER: Clean Number Parser ---
  const parseNum = (v) => {
    if (typeof v === 'number') return v;
    if (!v) return 0;
    const cleanStr = String(v).replace(/[^0-9.-]/g, ''); 
    return parseFloat(cleanStr) || 0;
  };

  // --- BATCH DATA LOAD ---
  const rawDataValues = preLoadedData || dataSheet.getDataRange().getValues();
  const headers = rawDataValues[2]; 
  const marketRows = rawDataValues.slice(3); 

  // --- LOAD ITEM LIST GOALS (The Safety Check - e.g., 3.9M) ---
  const itemListValues = itemListSheet.getDataRange().getValues();
  const goalMap = new Map();
  // Assuming Item Name is Col B (index 1) and Goal is Col C (index 2)
  itemListValues.slice(1).forEach(r => {
    const name = String(r[1]).trim(); 
    const goal = parseNum(r[2]);      
    if (name) goalMap.set(name, goal);
  });

  // --- AUDIT MAP ---
  const auditValues = auditSheet.getDataRange().getValues();
  const auditMap = new Map(auditValues.slice(1).map(r => [String(r[0]).trim(), r[1]]));

  // --- SETTINGS (Mapping to your Need To Buy Sheet) ---
  const filters = sheet.getRange('B5:B26').getValues();
  
  // B5: Min Days (The Trigger - e.g., 30)
  const minDays = parseNum(filters[0][0]) || 0;
  
  // B7: Target Days (The Goal - e.g., 31)
  const targetDays = parseNum(filters[2][0]) || 31; 
  
  // B9: Minimal Margin
  let rawMargin = parseNum(filters[4][0]); 
  const minMargin = (rawMargin > 1) ? rawMargin / 100 : rawMargin;

  const cfg = {
    minDays: minDays,
    targetDays: targetDays,
    minMargin: minMargin || 0,
    limit: filters[14][0] === "No Limit" ? 5000 : parseInt(filters[14][0]) || 5000,
    ignoreGroups: (filters[21][0] || "").toLowerCase().split(',').map(s => s.trim()).filter(s => s),
    groups: (filters[6][0] || "").toLowerCase().split(',').map(s => s.trim()).filter(s => s),
    sortDir: (filters[8][0] || "ASC").toUpperCase(),
    sortCol: (filters[9][0] || "Item Name").trim()
  };

  const col = {
    name: headers.indexOf("Item Name"),        
    group: headers.indexOf("Group"),           
    qLeft: headers.indexOf("Quantity Left"),   
    jobs: headers.indexOf("Active Jobs"),      
    deliv: headers.indexOf("Deliveries"),      
    mfgCost: headers.indexOf("Manufacturing Unit Cost"), 
    vol30: headers.indexOf("30-day traded volume"), 
    vel: headers.indexOf("Effective Daily Velocity (u/d)"), 
    warehouse: headers.indexOf("Warehouse Qty"), 
    totalMkt: headers.indexOf("Total Market Quantity"), 
    hubBuy: headers.indexOf("Hub Median Buy"),   
    effCost: headers.indexOf("Effective Cost"),  
    margin: headers.indexOf("Margin"),           
    action: headers.indexOf("Buy Action")        
  };

  let results = [];
  marketRows.forEach(row => {
    const name = String(row[col.name] || "").trim();
    if (!name) return;

    if (auditMap.get(name) !== true) return; 

    const group = String(row[col.group] || "").toLowerCase().trim();
    if (cfg.ignoreGroups.includes(group)) return;
    if (cfg.groups.length > 0 && !cfg.groups.includes(group)) return;

    // --- STOCK & VELOCITY ---
    const whQty = parseNum(row[col.warehouse]);
    const qLeft = parseNum(row[col.qLeft]); 
    const jobs = parseNum(row[col.jobs]);   
    const deliv = parseNum(row[col.deliv]); 
    
    // Total Inventory = Warehouse + Buy Orders + Manufacturing + Deliveries
    const currentStock = whQty + qLeft + jobs + deliv;
    const velocity = parseNum(row[col.vel]);

    // --- LOGIC: THE RED LINE ---
    // Trigger: Do we have less than 30 Days (B5) of stock?
    const triggerLevel = Math.ceil(velocity * cfg.minDays);

    // If stock is healthy (above 30 days), STOP here.
    if (currentStock > triggerLevel) return;

    // --- CALCULATE BUY GOAL ---
    // 1. Dynamic: Velocity * 31 Days (B7)
    const dynamicTarget = Math.ceil(velocity * cfg.targetDays);
    // 2. Static: Safety Goal from Item List (e.g., 3.9M)
    const safetyGoal = goalMap.get(name) || 0;
    
    // Use the LARGER of the two targets
    const finalGoal = Math.max(dynamicTarget, safetyGoal);

    const restockNeed = Math.round(finalGoal - currentStock);
    if (restockNeed <= 0) return;

    // --- MARGIN CHECK ---
    let itemMargin = parseNum(row[col.margin]);
    if (itemMargin > 1 && itemMargin <= 100 && cfg.minMargin <= 1) itemMargin = itemMargin / 100;
    if (itemMargin < cfg.minMargin) return;

    // --- ACTION CHECK ---
    // If inventory is critical (triggered above), we ignore "HOLD" signals from the market.
    const rawAction = String(row[col.action] || "");
    const finalAction = (/SKIP|HOLD/i.test(rawAction)) ? "FORCE RESTOCK" : rawAction;

    const cost = parseNum(row[col.effCost]) || parseNum(row[col.mfgCost]) || parseNum(row[col.hubBuy]) || 0;

    results.push({
      data: [
        name, 
        restockNeed, 
        cost, 
        restockNeed * cost, 
        parseNum(row[col.totalMkt]), 
        parseNum(row[col.vol30]),    
        0, 
        whQty, 
        itemMargin, 
        finalAction 
      ],
      sortKey: name
    });
  });

  // --- SORT & WRITE ---
  results.sort((a, b) => {
    const modifier = cfg.sortDir === "ASC" ? 1 : -1;
    return a.sortKey.localeCompare(b.sortKey) * modifier;
  });

  const output = results.slice(0, cfg.limit).map(r => r.data);
  const maxRows = sheet.getMaxRows();
  
  if (maxRows >= 5) {
      sheet.getRange(5, 3, maxRows - 4, 10).clearContent();
  }

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

  // --- SETTINGS & FILTERS ---
  const bCol = sheet.getRange("A1:B45").getValues();
  const seedDays = clean(bCol[39][0]) || 4;
  const minROI = clean(bCol[8][1]);
  const minOrderValue = 1000000;

  // Pull Deviation Setting (e.g., 0.0001 for 0.01%) from A6
  const priceDeviationPct = clean(sheet.getRange("A6").getValue());

  const rawDataValues = dataSheet.getDataRange().getValues();
  const headers = rawDataValues[2];
  const rawData = rawDataValues.slice(3);

  // --- INITIALIZE AUDIT MAP ---
  const auditValues = auditSheet.getDataRange().getValues();
  const auditMap = new Map(auditValues.slice(1).map(r => [String(r[0]), r[1]]));

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
    mfgCost: getIdx("Manufacturing Unit Cost"),
    condition: getIdx("Condition")
  };

  const OUT_HEADERS = [
    "Item Name", "Posting Price", "Hub Sell Price", "Quantity", "Total Value", "Delta Sell", "Delta Buy",
    "Warehouse Level", "Pending Orders", "Total Market Quantity", "Warehouse Qty", "Acquisition (30d)",
    "Effective Daily Velocity (u/d)", "30-day traded volume", "Listed Volume (Feed Sell)",
    "Feed Days of Book", "Hub Median Buy", "Effective Cost", "Sell Action", "Buy Action", "Sell Quantity"
  ];

  let resultRows = [];

  for (let r of rawData) {
    const rawName = String(r[col.item] || "");
    if (!rawName) continue;

    const action = String(r[col.sellAct] || "");
    const warehouseStock = clean(r[col.whQty]);
    const currentMarket = clean(r[col.sellQty]);
    const activeJobs = clean(r[col.jobs]); // Add this to track what is being built
    const velocity = clean(r[col.effVel]);
    const targetGoal = clean(r[col.targetGoal]);



    const condition = String(r[col.condition] || "");
    const isEmpty = currentMarket === 0 || /Empty/i.test(condition);

    if (/SKIP|HOLD|IGNORE/i.test(action) || action === "") {
      if (!(isEmpty && warehouseStock > 0)) continue;
    }

    if (auditMap.get(rawName) !== true && !isEmpty) continue;

    // --- QUANTITY LOGIC ---
    let targetNeeded = velocity * seedDays;
    if (targetGoal > 0) {
      targetNeeded = Math.min(targetNeeded, targetGoal);
    }

    let gap = Math.max(0, targetNeeded - currentMarket);
    let finalQuantity = Math.round(Math.min(gap, warehouseStock));

    if (finalQuantity <= 0) continue;

    // --- PRICING LOGIC WITH DEVIATION ---
    const hubSell = clean(r[col.hubSell]);
    const baseCost = clean(r[col.effCost]) || clean(r[col.mfgCost]);

    // 1. Calculate the Undercut Price (Hub Sell - 0.01%)
    let undercutPrice = hubSell * (1 - priceDeviationPct);

    // 2. Determine Floor Price based on minROI
    const floorPrice = baseCost * (1 + minROI);

    // 3. Final Posting Price: Use the undercut, but never go below the floor
    let postPrice = Math.max(undercutPrice, floorPrice);

    // 4. Round to 2 decimal places for EVE
    postPrice = Math.round(postPrice * 100) / 100;

    const totalOrderValue = finalQuantity * postPrice;

    if (totalOrderValue < minOrderValue && !isEmpty) continue;

    resultRows.push([
      rawName, postPrice, hubSell, finalQuantity, totalOrderValue,
      r[getIdx("Delta Sell")], r[getIdx("Delta Buy")], r[getIdx("Warehouse Level")],
      r[getIdx("Pending Orders")], r[col.mktQty], warehouseStock,
      r[getIdx("Acquisition (30d)")], velocity, r[getIdx("30-day traded volume")],
      r[getIdx("Listed Volume (Feed Sell)")], r[getIdx("Feed Days of Book")],
      clean(r[col.hubBuy]), baseCost, action, r[getIdx("Buy Action")], currentMarket
    ]);
  }

  // BATCH WRITE
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