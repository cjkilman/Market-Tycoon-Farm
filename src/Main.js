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


function NUKE_LOADING_ISSUES() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const authToon = getCorpAuthChar(ss);
  
  if (!authToon) {
    console.error("❌ Could not find authorized character.");
    return;
  }
  console.log(`Starting Precision Data Injection for ${authToon}...`);
// --- 1. OVERWRITE CORP ORDERS (Target: B2) ---
  try {
    const orderSheet = ss.getSheetByName("CorpOrdersCalc");
    if (orderSheet) {
      console.log("Igniting true concurrent fetch for Corp Orders...");
      const fullData = _fetchCorpOrdersConcurrently(authToon);
      
      if (fullData && fullData.length > 1) { // >1 because row 0 is headers
        const numRows = fullData.length;
        const numCols = fullData[0].length;
        const lastRow = Math.max(orderSheet.getLastRow(), 2);
        
        // Clear ONLY the data zone, then write
        orderSheet.getRange(2, 2, lastRow, numCols).clearContent();
        orderSheet.getRange(2, 2, numRows, numCols).setValues(fullData);
        console.log(`[SUCCESS] Wrote ${numRows - 1} flawlessly aligned Corp Orders precisely to B2.`);
      } else {
        console.log("[WARN] No active corp orders found to write.");
      }
    }
  } catch(e) { 
    console.error("[ERROR] Corp Orders Injection Failed: " + e.message); 
  }

  // --- 2. OVERWRITE CORP JOBS (Target: C1) ---
  try {
    const jobsSheet = ss.getSheetByName("ESI Corp Jobs");
    if (jobsSheet) {
      const rawJobs = _getCorporateJobsRaw(false); 
      
      if (rawJobs && rawJobs.length > 0) {
        // THE FIX: Hardcode the exact alphabetized headers so columns NEVER shift
        const STANDARD_JOB_HEADERS = [
          "activity_id", "blueprint_id", "blueprint_location_id", "blueprint_type_id",
          "completed_character_id", "completed_date", "cost", "duration", "end_date",
          "facility_id", "installer_id", "job_id", "licensed_runs", "location_id",
          "output_location_id", "pause_date", "probability", "product_type_id",
          "runs", "start_date", "status", "successful_runs"
        ];

        // THE FIX 2: Map the data using null instead of "" to prevent QUERY errors
        const rows = rawJobs.map(obj => STANDARD_JOB_HEADERS.map(key => obj[key] !== undefined ? obj[key] : null));
        const fullData = [STANDARD_JOB_HEADERS, ...rows];
        
        const numRows = fullData.length;
        const numCols = STANDARD_JOB_HEADERS.length;
        const lastRow = Math.max(jobsSheet.getLastRow(), 1);
        
        // Clear ONLY the GESI output zone (C1 downwards)
        jobsSheet.getRange(1, 3, lastRow, numCols).clearContent();
        
        // Write fresh data exactly at C1
        jobsSheet.getRange(1, 3, numRows, numCols).setValues(fullData);
        console.log(`✅ Wrote ${rows.length} Corp Jobs flawlessly to C1.`);
      }
    }
  } catch(e) { 
    console.error("❌ Corp Jobs Injection Failed: " + e.message); 
  }
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

    // 1. RECONCILIATION: Generate Dump list FIRST and capture the Ban List
    const dumpedItems = generateDumpToBuyOrder(ss, fullData) || new Set();

    // 2. Pass the Ban List to the restock functions so they ignore dumped items
    generateRestockQuery(ss, fullData, dumpedItems);
    generateRestockItemsOnHand(ss, fullData, dumpedItems);
    generateConsolidatedRequirements(ss);
    
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
  
  // RECONCILIATION: Always fetch the Dump list before updating Buy lists
  if (sheetName === 'Need To Buy') {
    const dumpedItems = generateDumpToBuyOrder(e.source) || new Set();
    generateRestockQuery(e.source, null, dumpedItems);
  } else if (sheetName === 'Restock Items On Hand') {
    const dumpedItems = generateDumpToBuyOrder(e.source) || new Set();
    generateRestockItemsOnHand(e.source, null, dumpedItems);
  } else if (sheetName === 'Dump to Buy') {
    // If you edit the Dump sheet itself, update it, then instantly sync the Buy sheets
    const dumpedItems = generateDumpToBuyOrder(e.source) || new Set();
    generateRestockQuery(e.source, null, dumpedItems);
    generateRestockItemsOnHand(e.source, null, dumpedItems);
  }
}

/**
 * Generates List for Dumping Profitable Overstocks to Buy Orders
 * RECONCILIATION PATCH: Now returns a Set() of dumped items to ban from Buy lists.
 */
function generateDumpToBuyOrder(ss, fullData) {
  const TARGET_SHEET = 'Dump to Buy';
  const DATA_SHEET = 'MarketOverviewData';
  const CORP_ORDERS_SHEET = 'CorpOrdersCalc';

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET);
  const dataSheet = ss.getSheetByName(DATA_SHEET);
  const corpOrdersSheet = ss.getSheetByName(CORP_ORDERS_SHEET);

  const dumpedItems = new Set(); // <--- THE BAN LIST

  if (!sheet || !dataSheet) return dumpedItems;

  const clean = (v) => (typeof v === 'number') ? v : parseFloat(String(v || 0).replace(/[^0-9.-]/g, '')) || 0;

  let rateMultiplier = 1.046;
  const fee = ss.getRangeByName("FEE_RATE")?.getValue() || 0.01;
  const tax = ss.getRangeByName("TAX_RATE")?.getValue() || 0.036;
  rateMultiplier = (1 + Number(fee) + Number(tax));

  const bParams = sheet.getRange("B5:B11").getValues();
  const filterMinDays = parseFloat(bParams[0][0]) || 0;
  const filterMinMargin = parseFloat(bParams[4][0]) || 0;
  const filterGroupName = String(bParams[6][0] || "").toLowerCase().trim();

  const headerLabels = [["Item Name", "Manufacturing Projected Cost", "Effective Cost", "Hub Median Buy", "Forensic Margin", "Warehouse Qty", "Total Dump ISK"]];
  sheet.getRange("C4:I4").setValues(headerLabels).setFontWeight("bold").setBackground("#f3f3f3");

  const corpBuyPrices = new Set();
  if (corpOrdersSheet) {
    const corpData = corpOrdersSheet.getDataRange().getValues();
    const h = corpData[1] || [];
    const pIdx = h.indexOf("price");
    const tIdx = h.indexOf("type_id");
    const bIdx = h.indexOf("is_buy_order");

    for (let i = 2; i < corpData.length; i++) {
      if (corpData[i][bIdx] === true || corpData[i][bIdx] === "TRUE") {
        corpBuyPrices.add(`${corpData[i][tIdx]}_${clean(corpData[i][pIdx]).toFixed(2)}`);
      }
    }
  }

  const sourceData = fullData || dataSheet.getDataRange().getValues();
  let hIdx = -1;
  for (let i = 0; i < 10; i++) {
    if (sourceData[i].indexOf("Item Name") > -1) { hIdx = i; break; }
  }
  if (hIdx === -1) return dumpedItems;

  const headers = sourceData[hIdx];
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

  const rawData = sourceData.slice(hIdx + 1);
  const dumpResults = [];
  const MIN_VALID_COST = 5.00;
  const showAll = (!filterGroupName || filterGroupName === "manufacturing");

  for (let i = 0; i < rawData.length; i++) {
    const r = rawData[i];
    const name = r[col.item];
    if (!name) continue;

    const group = String(r[col.group] || "").toLowerCase().trim();
    if (!showAll && group !== filterGroupName) continue;

    const daysOfInv = clean(r[col.daysInv]);
    if (daysOfInv < filterMinDays) continue;

    const hubBuy = clean(r[col.medianBuy]);
    if (hubBuy < MIN_VALID_COST) continue;

    if (corpBuyPrices.has(`${r[col.id]}_${hubBuy.toFixed(2)}`)) continue;

    const effCost = clean(r[col.effCost]);
    const buildNow = clean(r[col.buildNow]);

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
      const qty = clean(r[col.whQty]);
      if (qty > 0) {
        dumpResults.push([
          name,
          buildNow <= 0 ? "" : buildNow, 
          effCost,
          hubBuy,
          margin,
          qty,
          (hubBuy * qty)
        ]);
        
        dumpedItems.add(name); // <--- RECORD ITEM AS BANNED FROM BUYING
      }
    }
  }

  dumpResults.sort((a, b) => b[4] - a[4]);

  const START_ROW = 5;
  const maxRows = Math.max(sheet.getMaxRows(), START_ROW);
  if (maxRows >= START_ROW) {
    sheet.getRange(START_ROW, 3, maxRows - (START_ROW - 1), 7).clearContent();
  }

  if (dumpResults.length > 0) {
    sheet.getRange(START_ROW, 3, dumpResults.length, 7).setValues(dumpResults);
    sheet.getRange(START_ROW, 7, dumpResults.length, 1).setNumberFormat("0.00%");
  }

  return dumpedItems; // <--- PASS BAN LIST TO ORCHESTRATOR
}


/**
 * Generates List to Set up Buy orders or Manufacturing Jobs due to low inventory.
 * RECONCILIATION PATCH: Ignores items found in dumpedItems Set.
 */
function generateRestockQuery(ss, fullData, dumpedItems = new Set()) {
  const TARGET_SHEET_NAME = 'Need To Buy';
  const DATA_SHEET_NAME = 'MarketOverviewData';
  const AUDIT_SHEET_NAME = 'Audit items';

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  const dataSheet = ss.getSheetByName(DATA_SHEET_NAME);
  const auditSheet = ss.getSheetByName(AUDIT_SHEET_NAME);

  if (!sheet || !dataSheet || !auditSheet) return;

  const parseNum = (v) => {
    if (typeof v === 'number') return v;
    if (!v) return 0;
    const cleanStr = String(v).replace(/[^0-9.-]/g, '');
    return parseFloat(cleanStr) || 0;
  };

  const rawDataValues = fullData || dataSheet.getDataRange().getValues();
  const headers = rawDataValues[2]; 
  const marketRows = rawDataValues.slice(3);

  const auditValues = auditSheet.getDataRange().getValues();
  const auditMap = new Map(auditValues.slice(1).map(r => [String(r[0]).trim(), r[1]]));

  const filters = sheet.getRange('B5:B26').getValues();
  const minDays = parseNum(filters[0][0]) || 0;    
  const targetDays = parseNum(filters[2][0]) || 31; 
  let rawMargin = parseNum(filters[4][0]);
  const minMargin = (rawMargin > 1) ? rawMargin / 100 : rawMargin;

  const cfg = {
    minDays: minDays,
    targetDays: targetDays,
    minMargin: minMargin || 0,
    limit: filters[14][0] === "No Limit" ? 5000 : parseInt(filters[14][0]) || 5000,
    ignoreGroups: (filters[21][0] || "").toLowerCase().split(',').map(s => s.trim()).filter(s => s),
    groups: (filters[6][0] || "").toLowerCase().split(',').map(s => s.trim()).filter(s => s),
    sortDir: (filters[8][0] || "ASC").toUpperCase()
  };

  const col = {
    name: headers.indexOf("Item Name"),
    group: headers.indexOf("Group"),
    qLeft: headers.indexOf("Quantity Left"),        
    sellQty: headers.indexOf("Posted Sell Quantuty"), 
    jobs: headers.indexOf("Active Jobs"),
    deliv: headers.indexOf("Deliveries"),
    mfgCost: headers.indexOf("Manufacturing Unit Cost"),
    vol30: headers.indexOf("30-day traded volume"),
    vel: headers.indexOf("Effective Daily Velocity (u/d)"),
    warehouse: headers.indexOf("Warehouse Qty"),
    totalMkt: headers.indexOf("Total Market Quantity"),
    effCost: headers.indexOf("Effective Cost"),
    margin: headers.indexOf("Margin"),
    action: headers.indexOf("Buy Action")
  };

  let results = [];
  marketRows.forEach(row => {
    const name = String(row[col.name] || "").trim();
    if (!name || auditMap.get(name) !== true) return;
    
    // --- RECONCILIATION FIX ---
    // If it's being dumped, DO NOT put it on the Need To Buy list
    if (dumpedItems.has(name)) return;

    const group = String(row[col.group] || "").toLowerCase().trim();
    if (cfg.ignoreGroups.includes(group)) return;
    if (cfg.groups.length > 0 && !cfg.groups.includes(group)) return;

    const whQty = parseNum(row[col.warehouse]);
    const qLeft = parseNum(row[col.qLeft]);
    const sQty = parseNum(row[col.sellQty]);
    const jobs = parseNum(row[col.jobs]);
    const deliv = parseNum(row[col.deliv]);

    const currentStock = whQty + qLeft + sQty + jobs + deliv;
    const velocity = parseNum(row[col.vel]);

    const triggerLevel = Math.ceil(velocity * cfg.minDays);
    if (currentStock > triggerLevel) return;

    const finalGoal = Math.ceil(velocity * cfg.targetDays);
    const restockNeed = Math.round(finalGoal - currentStock);
    if (restockNeed <= 0) return;

    let itemMargin = parseNum(row[col.margin]);
    if (itemMargin > 1 && itemMargin <= 100 && cfg.minMargin <= 1) itemMargin /= 100;
    if (itemMargin < cfg.minMargin) return;

    const rawAction = String(row[col.action] || "");
    const finalAction = (/SKIP|HOLD/i.test(rawAction)) ? "FORCE RESTOCK" : rawAction;
    const cost = parseNum(row[col.effCost]) || parseNum(row[col.mfgCost]) || 0;

    results.push({
      data: [
        name, restockNeed, cost, restockNeed * cost, parseNum(row[col.totalMkt]),
        parseNum(row[col.vol30]), 0, whQty, itemMargin, finalAction
      ],
      sortKey: name
    });
  });

  results.sort((a, b) => {
    const modifier = cfg.sortDir === "ASC" ? 1 : -1;
    return a.sortKey.localeCompare(b.sortKey) * modifier;
  });

  const output = results.slice(0, cfg.limit).map(r => r.data);
  const maxRows = Math.max(sheet.getMaxRows(), 5);

  if (maxRows >= 5) {
    sheet.getRange(5, 3, maxRows - 4, 10).clearContent();
  }

  if (output.length > 0) {
    sheet.getRange(5, 3, output.length, 10).setValues(output);
  }
}


function generateRestockItemsOnHand(ss, fullData, dumpedItems = new Set()) {
  const TARGET_SHEET = 'Restock Items On Hand';
  const DATA_SHEET = 'MarketOverviewData';
  const AUDIT_SHEET = 'Audit items';

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('Restock') : console;
  
  const sheet = ss.getSheetByName(TARGET_SHEET);
  const dataSheet = ss.getSheetByName(DATA_SHEET);
  const auditSheet = ss.getSheetByName(AUDIT_SHEET);

  if (!sheet || !dataSheet || !auditSheet) return;

  const clean = (v) => (typeof v === 'number') ? v : parseFloat(String(v || 0).replace(/[^0-9.-]/g, '')) || 0;

  const bCol = sheet.getRange("A1:B45").getValues();
  const seedDays = clean(bCol[39][0]) || 4; 
  const minROI = clean(bCol[8][1]) || 0;         
  const minOrderValue = 1000000; 
  const priceDeviationPct = clean(sheet.getRange("A6").getValue()) || 0;

  const rawDataValues = fullData || dataSheet.getDataRange().getValues();
  const headers = rawDataValues[2]; 
  const rawData = rawDataValues.slice(3);

  const getIdx = (name) => headers.indexOf(name);
  const auditValues = auditSheet.getDataRange().getValues();
  const auditMap = new Map(auditValues.slice(1).map(r => [String(r[0]), String(r[1]).toUpperCase() === 'TRUE']));

  const col = {
    item: getIdx("Item Name"),
    targetGoal: getIdx("Target"),
    sellQty: getIdx("Posted Sell Quantuty"), 
    whQty: getIdx("Warehouse Qty"),
    effVel: getIdx("Effective Daily Velocity (u/d)"),
    hubSell: getIdx("Hub Sell Price"),
    sellAct: getIdx("Sell Action"),
    customPrice: getIdx("Custom Price"), 
    hubBuy: getIdx("Hub Buy Price"),
    mktQty: getIdx("Total Market Quantity"),
    effCost: getIdx("Effective Cost"),
    mfgCost: getIdx("Manufacturing Unit Cost")
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

    // --- RECONCILIATION FIX ---
    // If it's being dumped, DO NOT put it on the Restock list
    if (dumpedItems.has(rawName)) continue;

    const action = String(r[col.sellAct] || "").toUpperCase();
    if (action.includes("SATURATED") || action.includes("SKIP") || action.includes("HOLD") || action.includes("IGNORE")) {
      continue;
    }

    if (auditMap.get(rawName) !== true) continue;

    const warehouseStock = clean(r[col.whQty]);
    const currentMarket = clean(r[col.sellQty]);
    const velocity = clean(r[col.effVel]);
    const targetGoal = clean(r[col.targetGoal]);

    let targetNeeded = velocity * seedDays;
    if (targetGoal > 0) targetNeeded = Math.min(targetNeeded, targetGoal);

    let gap = Math.max(0, targetNeeded - currentMarket);
    let finalQuantity = Math.round(Math.min(gap, warehouseStock));

    if (finalQuantity <= 0) continue;

    let postPrice = 0;
    const manualPrice = clean(r[col.customPrice]);
    const hubSell = clean(r[col.hubSell]); 
    const baseCost = clean(r[col.effCost]) || clean(r[col.mfgCost]); 
    
    if (manualPrice > 0) {
      postPrice = manualPrice;
    } else {
      let undercutPrice = hubSell * (1 - priceDeviationPct);
      const floorPrice = baseCost * (1 + minROI);
      postPrice = Math.max(undercutPrice, floorPrice);
    }
    
    postPrice = Math.round(postPrice * 100) / 100;
    const totalOrderValue = finalQuantity * postPrice; 
    
    if (manualPrice <= 0 && totalOrderValue < minOrderValue && currentMarket !== 0) continue;

    resultRows.push([
      rawName, postPrice, hubSell, finalQuantity, totalOrderValue,
      r[getIdx("Delta Sell")], r[getIdx("Delta Buy")], r[getIdx("Warehouse Level")],
      r[getIdx("Pending Orders")], r[col.mktQty], warehouseStock,
      r[getIdx("Acquisition Velocity (u/d)")], velocity, r[getIdx("30-day traded volume")],
      r[getIdx("Listed Volume (Feed Sell)")], r[getIdx("Feed Days of Book")],
      clean(r[col.hubBuy]), baseCost, action, r[getIdx("Buy Action")], currentMarket
    ]);
  }

  const maxRows = Math.max(1, sheet.getLastRow());
  if (maxRows >= 3) {
    sheet.getRange(3, 3, maxRows, 21).clearContent();
  }
  
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