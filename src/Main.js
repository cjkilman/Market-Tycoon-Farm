// Critical Note
// Posted Buy orders labled Quantity Left
// Market Order Book is Listed Volume



// Global Property Service
const SCRIPT_PROPS = PropertiesService.getScriptProperties();

// --- CONFIGURATION SECTION ---

function GET_SDE_CONFIG() {
  return [
    { name: "SDE_invTypes", file: "invTypes.csv", cols: ["typeID", "groupID", "typeName", "volume", "marketGroupID", "basePrice", "portionSize"] },
    { name: "SDE_invGroups", file: "invGroups.csv", cols: null },
    { name: "SDE_invCategories", file: "invCategories.csv", cols: null },
    { name: "SDE_invMarketGroups", file: "invMarketGroups.csv", cols: null },
    { name: "SDE_staStations", file: "staStations.csv", cols: null },
    { name: "SDE_industryActivity", file: "industryActivity.csv", cols: null },
    { name: "SDE_industryActivityMaterials", file: "industryActivityMaterials.csv", cols: null },
    { name: "SDE_industryActivityProducts", file: "industryActivityProducts.csv", cols: null },
    { name: "SDE_invTypeMaterials", file: "invTypeMaterials.csv", cols: null },
    { name: "SDE_Bonuses", file: "specializedReprocessingBonuses.csv", cols: null },
    { name: "SDE_SkillMap", file: "SDE_oreProcessingGroups.csv", cols: null }
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
    .addItem('🖨️ Sync Corporate BPOs', 'syncCorporateBlueprints') // <--- ADD THIS LINE (Change function name if needed)
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
  } catch (e) {
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
  } catch (e) {
    console.error("❌ Corp Jobs Injection Failed: " + e.message);
  }
        // 3. REPRO ENGINE (The New "Tycoon" Step)
      // Recalculate Melt Values using the fresh market data just swapped in.
      generateReprocessedValueTable(ss);
}

/**
 * Generates List for Dumping Profitable Overstocks to Buy Orders
 * DYNAMIC: Uses Named Range 'g_market_settings' for EPS_PRICE.
 * ROI TARGET: Bottom Buy Price now calculates based on B9 Minimal Margin.
 * FILTER: Never shows losses. Stagnant items bypass B9 only if B20 is Checked.
 */
function generateDumpToBuyOrder(ss, fullData) {
  const TARGET_SHEET = 'Dump to Buy';
  const CORP_ORDERS_SHEET = 'CorpOrdersCalc';

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET);
  const corpOrdersSheet = ss.getSheetByName(CORP_ORDERS_SHEET);
  if (!sheet) return new Set();

  const clean = (v) => (typeof v === 'number') ? v : parseFloat(String(v || 0).replace(/[^0-9.-]/g, '')) || 0;

  // --- 1. FETCH MARKET SETTINGS (Named Range) ---
  const settingsMap = getMarketSettingsMap(ss);
  const epsPrice = settingsMap.get("EPS_PRICE") || 0.01;

  // --- 2. DYNAMIC PARAMETER SCANNER (B-COLUMN) ---
  const paramData = sheet.getRange("B4:B40").getValues();
  let filterMinMargin = 0;
  let filterGroupName = "";
  let hubDaysBuyTarget = 7;
  let liquidateStagnant = false;

  for (let i = 0; i < paramData.length; i++) {
    const label = String(paramData[i][0]).trim().toLowerCase();
    if (label.includes("minimal margin")) {
      let rawVal = paramData[i+1][0];
      filterMinMargin = (typeof rawVal === 'string' && rawVal.includes('%')) ? parseFloat(rawVal)/100 : (parseFloat(rawVal) > 1 ? parseFloat(rawVal)/100 : parseFloat(rawVal) || 0);
    }
    else if (label.includes("group slection") || label.includes("group selection")) {
      filterGroupName = String(paramData[i+1][0] || "").toLowerCase().trim();
    }
    else if (label.includes("full days target")) {
      hubDaysBuyTarget = parseFloat(paramData[i+1][0]) || 7;
    }
    else if (label.includes("liquidate stagnet") || label.includes("liquidate stagnant")) {
      const val = paramData[i+1][0];
      liquidateStagnant = (val === true || String(val).toUpperCase() === "TRUE");
    }
  }

  // Fee + Tax overhead
  const fee = ss.getRangeByName("FEE_RATE")?.getValue() || 0.01;
  const tax = ss.getRangeByName("TAX_RATE")?.getValue() || 0.036;
  const rateMultiplier = (1 + Number(fee) + Number(tax));

  // --- 3. CORP SAFETY CHECK ---
  const activeCorpBuyOrders = new Set();
  if (corpOrdersSheet) {
    const corpData = corpOrdersSheet.getDataRange().getValues();
    const cH = corpData[1] || [];
    const tIdx = cH.indexOf("type_id");
    const bIdx = cH.indexOf("is_buy") > -1 ? cH.indexOf("is_buy") : cH.indexOf("is_buy_order");
    
    if (tIdx > -1 && bIdx > -1) {
      for (let i = 2; i < corpData.length; i++) {
        const isBuy = corpData[i][bIdx];
        if (isBuy === true || String(isBuy).toUpperCase() === "TRUE") {
          activeCorpBuyOrders.add(Number(corpData[i][tIdx]));
        }
      }
    }
  }

  // --- 4. DYNAMIC HEADERS (Matches B9 Target) ---
  const marginPercentLabel = (filterMinMargin * 100).toFixed(0) + "%";
  const headerLabels = [[
    "Item Name", 
    "Bottom Buy (" + marginPercentLabel + ")", 
    "Manufacturing Projected", 
    "Effective Cost", 
    "Hub Median Buy", 
    "Forensic Margin", 
    "Hub Capped Qty", 
    "Total Dump ISK", 
    "Trend"
  ]];
  sheet.getRange("C4:K4").setValues(headerLabels).setFontWeight("bold").setBackground("#f3f3f3");

  // --- 5. SOURCE DATA & PROCESSING ---
  const sourceData = fullData || getOverviewData(ss);
  if (!sourceData || sourceData.length === 0) return new Set();

  const headers = sourceData[0];
  const col = {
    id: headers.indexOf("type_id"),
    item: headers.indexOf("Item Name"),
    group: headers.indexOf("Group"),
    effCost: headers.indexOf("Effective Cost"),
    buildNow: headers.indexOf("Manufacturing Projected Unit Cost"),
    medianBuy: headers.indexOf("Hub Median Buy"),
    whQty: headers.indexOf("Warehouse Qty"),
    signal: headers.indexOf("Signal"),
    hubVelocity: headers.indexOf("Hub Market Velocity (u/d)")
  };

  const rawData = sourceData.slice(1);
  const dumpResults = [];

  for (let i = 0; i < rawData.length; i++) {
    const r = rawData[i];
    const typeId = Number(r[col.id]);
    const name = r[col.item];
    if (!name || activeCorpBuyOrders.has(typeId)) continue;

    const group = String(r[col.group] || "").toLowerCase().trim();
    if (filterGroupName && filterGroupName !== "manufacturing" && group !== filterGroupName) continue;

    const hubBuy = clean(r[col.medianBuy]);
    const effCost = clean(r[col.effCost]);
    const buildNow = clean(r[col.buildNow]);
    const signal = String(r[col.signal] || "").toUpperCase(); 
    const velocity = clean(r[col.hubVelocity]);

    let realityFloor = buildNow > epsPrice ? Math.max(effCost, buildNow) : (effCost > epsPrice ? effCost : 0);
    if (realityFloor === 0 || hubBuy < epsPrice) continue;

    // Bottom Buy now calculates the price needed to hit your Minimal Margin (B9)
    const bottomBuyPrice = realityFloor * (1 + filterMinMargin) * rateMultiplier;
    const margin = ((hubBuy / rateMultiplier) - realityFloor) / realityFloor;

    // --- FILTERING LOGIC ---
    let passesFilter = false;
    let trendOutput = signal || "-";

    if (signal.includes("STAGNANT") && liquidateStagnant) {
      if (margin >= 0) { // Never show negative numbers
        passesFilter = true;
        trendOutput = "STAGNANT (LIQUIDATING)";
      }
    } else {
      if (margin >= filterMinMargin) {
        passesFilter = true;
      }
    }

    if (passesFilter && margin < 5.0) {
      const warehouseQty = clean(r[col.whQty]);
      const marketCap = Math.floor(velocity * hubDaysBuyTarget);
      const dumpQty = (marketCap > 0) ? Math.min(warehouseQty, marketCap) : warehouseQty;

      if (dumpQty > 0) {
        dumpResults.push([name, bottomBuyPrice, buildNow || "", effCost, hubBuy, margin, dumpQty, (hubBuy * dumpQty), trendOutput]);
      }
    }
  }

  // --- 6. OUTPUT ---
  dumpResults.sort((a, b) => b[7] - a[7]); 
  
  const START_ROW = 5;
  const maxRows = Math.max(sheet.getMaxRows(), START_ROW);
  if (maxRows >= START_ROW) {
    sheet.getRange(START_ROW, 3, maxRows - (START_ROW - 1), 9).clearContent();
  }
  
  if (dumpResults.length > 0) {
    sheet.getRange(START_ROW, 3, dumpResults.length, 9).setValues(dumpResults);
    sheet.getRange(START_ROW, 8, dumpResults.length, 1).setNumberFormat("0.00%");
  }

  const dumpedSet = new Set();
  dumpResults.forEach(row => dumpedSet.add(row[0]));
  return dumpedSet;
}


function _getColIndexMap(headers, names) {
  const map = {};
  names.forEach(name => {
    const idx = headers.indexOf(name);
    if (idx === -1) throw new Error("Column not found: " + name);
    map[name] = idx;
  });
  return map;
}

/**
 * Get the OverviewData Table Range
 * @param {*} ss 
 * @returns 
 */
const getOverviewData = (ss) => {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const dataSheet = ss.getSheetByName('MarketOverviewData');
  if (!dataSheet) return []; 

  const lastRow = dataSheet.getLastRow();
  const startRow = 3; // Shifted to 3 to capture the Header Row
  const startCol = 2; // Column B
  
  const numRows = lastRow - startRow + 1;
  if (numRows <= 0) return [];

  const numCols = dataSheet.getLastColumn() - startCol + 1;
  return dataSheet.getRange(startRow, startCol, numRows, numCols).getValues();
};

function triggerRestockSync() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  // Using CacheService for the sync flag is 10x faster than PropertiesService
  const cache = CacheService.getScriptCache();

  ss.toast("🚀 Nitro Sync: Ingesting RAM...", "Engine Room", 5);

  try {
    cache.put('MANUAL_SYNC_ACTIVE', 'TRUE', 300); // 5 min timeout

    // NITRO MOVE: Load memory once
    const fullData = getOverviewData(ss);

    // 1. DUMP ENGINE
    const dumpedItems = generateDumpToBuyOrder(ss, fullData) || new Set();

    // 2. BUY/RESTOCK ENGINES
    generateNeedToBuyQuery(ss, fullData, dumpedItems);
    generateRestockItemsOnHand(ss, fullData);
    generatePVPTrap(ss, fullData);
    
    // 3. CONSOLIDATION
    generateConsolidatedRequirements(ss);

    ss.toast("✅ Sync Complete.", "Engine Room", 3);
  } catch (e) {
   console.error("CRITICAL FAILURE: " + e.toString() + "\nStack: " + e.stack);
    ss.toast("❌ Sync failed: " + e.message, "Engine Room Error");
  } finally {
    cache.remove('MANUAL_SYNC_ACTIVE');
  }
}

/**
 * FIXED: Optimized respondToEdit 
 * - Prevents DEADLINE_EXCEEDED by using CacheService.
 * - Fixes the "Top-Down" bug by only pulling data when needed.
 * - Added a check to ensure fullData isn't empty before processing.
 */
function respondToEdit(e) {
  if (!e || !e.range || !e.source) return;

  // 1. HIGH-SPEED DEBOUNCE (Fixes Deadline Errors)
  // CacheService is 10x faster than PropertiesService
  const cache = CacheService.getScriptCache();
  if (cache.get('editing_lock')) return;
  cache.put('editing_lock', 'true', 2); // 2-second lock

  // 2. QUICK GUARDS
  const col = e.range.columnStart;
  if (col > 2) return; // Exit immediately if edit is not in Col A or B

  const sheet = e.range.getSheet();
  const sheetName = sheet.getName();
  const ss = e.source;

  // 3. LAZY LOADING (The Bug Fix)
  // We only pull the heavy overview data if we are on a relevant sheet.
  const targetSheets = ['Need To Buy', 'Restock Items On Hand', 'Dump to Buy'];
  if (!targetSheets.includes(sheetName)) return;

  const fullData = getOverviewData(ss);
  if (!fullData || fullData.length === 0) {
    console.warn("respondToEdit: Overview Data is empty, skipping logic.");
    return;
  }

  // 4. EXECUTE LOGIC
  if (sheetName === 'Need To Buy') {
    const dumpedItems = generateDumpToBuyOrder(ss, fullData);
    generateNeedToBuyQuery(ss, fullData, dumpedItems);
  } 
  else if (sheetName === 'Restock Items On Hand') {
    generateRestockItemsOnHand(ss, fullData);
  } 
  else if (sheetName === 'Dump to Buy') {
    const dumpedItems = generateDumpToBuyOrder(ss, fullData);
    generateNeedToBuyQuery(ss, fullData, dumpedItems);
    generateRestockItemsOnHand(ss, fullData);
  }
}







function generatePVPTrap(ss, fullData) {
  const TARGET_SHEET = 'PVP Trap';
  const AUDIT_SHEET = 'Audit items';

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET);
  const auditSheet = ss.getSheetByName(AUDIT_SHEET);
  const settings = getMarketSettingsMap(ss);
  const minOrderValue = settings.get("Min Order Value");

  if (!sheet || !auditSheet) return;

  const clean = (v) => (typeof v === 'number') ? v : parseFloat(String(v || 0).replace(/[^0-9.-]/g, '')) || 0;

  // Settings Pull
  const bCol = sheet.getRange("A1:B45").getValues();
  const seedDays = clean(bCol[39][0]) || 4;
  const minROI = clean(bCol[7][1]) || 0; 
  const priceDeviationPct = clean(sheet.getRange("A6").getValue()) || 0; 

  // --- NITRO LOAD ---
  // Use memory array or fall back to getter. 
  // If getOverviewData starts at Row 3, then headers are at index 0.
  const rawDataValues = fullData || getOverviewData(ss);
  if (!rawDataValues || rawDataValues.length === 0) return;

  const headers = rawDataValues[0]; 
  const rawData = rawDataValues.slice(1); 

  const getIdx = (name) => headers.indexOf(name);
  const auditValues = auditSheet.getDataRange().getValues();
  const auditMap = new Map(auditValues.slice(1).map(r => [String(r[0]), String(r[1]).toUpperCase() === 'TRUE']));

  const col = {
    item: getIdx("Item Name"),
    targetGoal: getIdx("Target"),
    sellQty: getIdx("Posted Sell Quantity"),
    whQty: getIdx("Warehouse Qty"),
    effVel: getIdx("Effective Daily Velocity (u/d)"),
    hubSell: getIdx("Hub Sell Price"),
    sellAct: getIdx("Sell Action"),
    customPrice: getIdx("Custom Price"),
    hubBuy: getIdx("Hub Buy Price"),
    mktQty: getIdx("Total Market Quantity"),
    effCost: getIdx("Effective Cost"),
    mfgCost: getIdx("Manufacturing Unit Cost"),
    signal: getIdx("Signal") 
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

    const sellAction = String(r[col.sellAct] || "").toUpperCase();
    if (sellAction.includes("SATURATED") || sellAction.includes("SKIP") || sellAction.includes("HOLD") || sellAction.includes("IGNORE")) {
      continue;
    }

    if (auditMap.get(rawName) !== true) continue;

    const warehouseStock = clean(r[col.whQty]);
    const currentMarket = clean(r[col.sellQty]);
    const velocity = clean(r[col.effVel]);
    const targetGoal = clean(r[col.targetGoal]);
    const signal = String(r[col.signal] || "").toUpperCase(); 

    let targetNeeded = velocity * seedDays;
    
    // --- MACD TRAP DEFENSE ---
    let finalSellAction = "TRAP: FILL BUY";
    if (signal.includes("TRAP")) {
      targetNeeded = velocity * 3; // Cap exposure on manipulated spikes
      finalSellAction = "TRAP: FILL BUY (TRAP)";
    } else if (signal.includes("STAGNANT")) {
      finalSellAction = "TRAP: FILL BUY (STAGNANT)";
    }

    if (targetGoal > 0) targetNeeded = Math.min(targetNeeded, targetGoal);

    let gap = Math.max(0, targetNeeded - currentMarket);
    let finalQuantity = Math.round(Math.min(gap, warehouseStock));

    if (finalQuantity <= 0) continue;

    const hubSell = clean(r[col.hubSell]);
    const hubBuy = clean(r[col.hubBuy]);
    const baseCost = clean(r[col.effCost]) || clean(r[col.mfgCost]);

    const floorPrice = baseCost * (1 + minROI);

    if (hubBuy >= floorPrice) {
      let postPrice = 0;
      const manualPrice = clean(r[col.customPrice]);

      if (manualPrice > 0) {
        postPrice = manualPrice;
      } else {
        let undercutPrice = hubSell * (1 - priceDeviationPct);
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
        hubBuy, baseCost, 
        finalSellAction, 
        r[getIdx("Buy Action")], currentMarket
      ]);
    }
  }

  // Final Write to Sheet
  const maxRows = Math.max(1, sheet.getLastRow());
  if (maxRows >= 3) {
    sheet.getRange(3, 3, maxRows, 21).clearContent();
  }

  sheet.getRange(3, 3, 1, 21).setValues([OUT_HEADERS]).setFontWeight("bold");

  if (resultRows.length > 0) {
    sheet.getRange(4, 3, resultRows.length, 21).setValues(resultRows);
  }
}


/**
 * CONSOLIDATED NEED TO BUY QUERY
 * Fixes: Stock Logic, Duplicate function error, and Header Alignment.
 */
function generateNeedToBuyQuery(ss, fullData, dumpedItems = new Set()) {
  const TARGET_SHEET_NAME = 'Need To Buy';
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  if (!sheet) return;

  const parseNum = (v) => (typeof v === 'number') ? v : parseFloat(String(v || 0).replace(/[^0-9.-]/g, '')) || 0;

  // 1. Headers (Row 4, Col C to P)
  const headerLabels = [["Item Name", "Snag Qty", "Entry Cost", "Order Cost", "Unit Profit", "Total Profit", "My Market Qty", "Volume (30d)", "Signal", "Warehouse Qty", "Margin (Net)", "Buy Action", "Slots", "Runs"]];
  sheet.getRange(4, 3, 1, 14).setValues(headerLabels).setFontWeight("bold").setBackground("#d1e7dd").setHorizontalAlignment("center");

  // 2. Setup
  const fee = ss.getRangeByName("FEE_RATE")?.getValue() || 0.01;
  const tax = ss.getRangeByName("TAX_RATE")?.getValue() || 0.036;
  const rateMultiplier = (1 + Number(fee) + Number(tax));
  const epsPrice = 1;

  const rawDataValues = fullData || getOverviewData(ss);
  if (!rawDataValues || rawDataValues.length === 0) return;

  const headers = rawDataValues[0];
  const marketRows = rawDataValues.slice(1);
  const filters = sheet.getRange('B5:B26').getValues();
  
  const cfg = {
    minDays: parseNum(filters[0][0]),
    targetDays: parseNum(filters[2][0]) || 7,
    minMargin: parseNum(filters[4][0]) > 1 ? parseNum(filters[4][0]) / 100 : parseNum(filters[4][0]) || 0,
    limit: filters[14][0] === "No Limit" ? 5000 : parseInt(filters[14][0]) || 5000,
    ignoreGroups: (filters[21][0] || "").toLowerCase().split(',').map(s => s.trim()).filter(s => s)
  };

  const getIdx = (n) => headers.indexOf(n);
  const col = {
    name: getIdx("Item Name"),
    group: getIdx("Group"),
    buyQty: getIdx("Quantity Left"),
    sellQty: getIdx("Posted Sell Quantity"),
    pending: getIdx("Pending Orders"),
    vol30: getIdx("30-day traded volume"),
    vel: getIdx("Effective Daily Velocity (u/d)"),
    warehouse: getIdx("Warehouse Qty"),
    buildNow: getIdx("Manufacturing Projected Unit Cost"),
    effCost: getIdx("Effective Cost"),
    buyAction: getIdx("Buy Action"),
    signal: getIdx("Signal"),
    hubBuy: getIdx("Hub Median Buy"),
    sellPrice: getIdx("Hub Sell Price")
  };

  let results = [];

  marketRows.forEach(row => {
    const name = String(row[col.name] || "").trim();
    if (!name || dumpedItems.has(name)) return;
    if (cfg.ignoreGroups.includes(String(row[col.group]).toLowerCase().trim())) return;

    const velocity = parseNum(row[col.vel]);
    const currentStock = parseNum(row[col.warehouse]) + parseNum(row[col.buyQty]) + parseNum(row[col.sellQty]) + parseNum(row[col.pending]);

    if (currentStock > Math.ceil(velocity * cfg.minDays)) return;

    const restockNeed = Math.round(Math.ceil(velocity * cfg.targetDays) - currentStock);
    if (restockNeed <= 0) return;

    const entryCost = Math.min(Math.max(epsPrice, parseNum(row[col.hubBuy])), parseNum(row[col.buildNow]) || Infinity, parseNum(row[col.effCost]) || Infinity);
    const unitProfit = (parseNum(row[col.sellPrice]) / rateMultiplier) - entryCost;
    const netMargin = entryCost > 0 ? (unitProfit / entryCost) : 0;

    if (netMargin < cfg.minMargin) return;

    let signal = String(row[col.signal] || "").toUpperCase();
    let buyAction = String(row[col.buyAction] || "BUY").toUpperCase();
    if (signal.includes("TRAP")) buyAction = "SKIP (TRAP)";

    results.push({
      data: [name, restockNeed, entryCost, restockNeed * entryCost, unitProfit, unitProfit * restockNeed, (parseNum(row[col.buyQty]) + parseNum(row[col.sellQty]) + parseNum(row[col.pending])), parseNum(row[col.vol30]), signal || "-", parseNum(row[col.warehouse]), netMargin, buyAction, "", ""],
      profitKey: unitProfit * restockNeed
    });
  });

  results.sort((a, b) => b.profitKey - a.profitKey);
  const output = results.slice(0, cfg.limit).map(r => r.data);
  const maxRows = Math.max(sheet.getLastRow(), 5);
  sheet.getRange(5, 3, maxRows, 14).clearContent();
  if (output.length > 0) sheet.getRange(5, 3, output.length, 14).setValues(output);
}

// Set up Orders to Posting Sell Orders on the Market
function generateRestockItemsOnHand(ss, fullData) {
  const TARGET_SHEET = 'Restock Items On Hand';
  const AUDIT_SHEET = 'Audit items';

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();

  const sheet = ss.getSheetByName(TARGET_SHEET);
  const auditSheet = ss.getSheetByName(AUDIT_SHEET);
  const settings = getMarketSettingsMap(ss);
  
  // RUTHLESS GATE #1: Enforce strict minimum order value
  const minOrderValue = settings.get("Min Order Value") || 500000; 

  if (!sheet || !auditSheet) return;

  const clean = (v) => (typeof v === 'number') ? v : parseFloat(String(v || 0).replace(/[^0-9.-]/g, '')) || 0;

  // READ METADATA 
  const bCol = sheet.getRange("A1:B45").getValues();
  const seedDays = clean(bCol[38][0]) || 28; 
  const minROI = clean(bCol[8][1]) || 0;    
  const priceDeviationPct = clean(sheet.getRange("A6").getValue()) || 0;

  // Top-Up Restrictor from B6
  let rawTopUp = clean(sheet.getRange("B6").getValue());
  const topUpThreshold = (rawTopUp > 1) ? rawTopUp / 100 : (rawTopUp || 0.75);

  // --- NITRO LOAD ---
  // Headers are at index 0 because getOverviewData starts at Row 3
  const rawDataValues = fullData || getOverviewData(ss);
  if (!rawDataValues || rawDataValues.length === 0) return;

  const headers = rawDataValues[0]; 
  const rawData = rawDataValues.slice(1); 

  const getIdx = (name) => headers.indexOf(name);
  const auditValues = auditSheet.getDataRange().getValues();
  const auditMap = new Map(auditValues.slice(1).map(r => [String(r[0]), String(r[1]).toUpperCase() === 'TRUE']));

  const col = {
    item: getIdx("Item Name"),
    targetGoal: getIdx("Target"),
    sellQty: getIdx("Posted Sell Quantity"),
    whQty: getIdx("Warehouse Qty"),
    effVel: getIdx("Effective Daily Velocity (u/d)"),
    hubSell: getIdx("Hub Sell Price"),
    sellAct: getIdx("Sell Action"),
    customPrice: getIdx("Custom Price"),
    hubBuy: getIdx("Hub Buy Price"),
    mktQty: getIdx("Total Market Quantity"),
    effCost: getIdx("Effective Cost"),
    mfgCost: getIdx("Manufacturing Unit Cost"),
    signal: getIdx("Signal") 
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

    const sellAction = String(r[col.sellAct] || "");
    if (sellAction.includes("SATURATED") || sellAction.includes("SKIP") || sellAction.includes("HOLD") || sellAction.includes("IGNORE")) {
      continue;
    } 

    if (auditMap.get(rawName) !== true) continue;

    const warehouseStock = clean(r[col.whQty]);
    const currentMarket = clean(r[col.sellQty]);
    const velocity = clean(r[col.effVel]);
    const targetGoal = clean(r[col.targetGoal]);
    const signal = String(r[col.signal] || "").toUpperCase(); 
    const manualPrice = clean(r[col.customPrice]);

    // RUTHLESS GATE #2: Velocity Check
    if (velocity < 1 && manualPrice <= 0) continue;

    let targetNeeded = velocity * seedDays;
    let finalSellAction = sellAction;

    // MACD TRAP DEFENSE
    if (signal.includes("TRAP")) {
      targetNeeded = velocity * 3; 
      finalSellAction = sellAction ? `${sellAction} (TRAP)` : "WARNING (TRAP)";
    }

    if (targetGoal > 0) targetNeeded = Math.min(targetNeeded, targetGoal);

    // RUTHLESS GATE #3: Top-Up Restrictor
    if (currentMarket > (targetNeeded * topUpThreshold)) continue;

    let gap = targetNeeded - currentMarket;
    let finalQuantity = Math.floor(Math.min(gap, warehouseStock));

    if (finalQuantity <= 0) continue;

    let postPrice = 0;
    const hubSell = clean(r[col.hubSell]);
    const baseCost = clean(r[col.effCost]) || clean(r[col.mfgCost]);

    if (manualPrice > 0) {
      postPrice = manualPrice;
    } else {
      let undercutPrice = hubSell * (1 - priceDeviationPct);
      const floorPrice = baseCost * (1 + minROI);

      // MACD LIQUIDATION PROTOCOL 
      if (signal.includes("STAGNANT")) {
        postPrice = undercutPrice; 
        finalSellAction = "LIQUIDATE (STAGNANT)";
      } else {
        postPrice = Math.max(undercutPrice, floorPrice);
      }
    }

    postPrice = Math.round(postPrice * 100) / 100;
    const totalOrderValue = finalQuantity * postPrice;

    // RUTHLESS GATE #4: Min Order Value
    if (manualPrice <= 0 && totalOrderValue < minOrderValue) continue;

    resultRows.push([
      rawName, postPrice, hubSell, finalQuantity, totalOrderValue,
      r[getIdx("Delta Sell")], r[getIdx("Delta Buy")], r[getIdx("Warehouse Level")],
      r[getIdx("Pending Orders")], r[col.mktQty], warehouseStock,
      r[getIdx("Acquisition Velocity (u/d)")], velocity, r[getIdx("30-day traded volume")],
      r[getIdx("Listed Volume (Feed Sell)")], r[getIdx("Feed Days of Book")],
      clean(r[col.hubBuy]), baseCost, 
      finalSellAction, 
      r[getIdx("Buy Action")], currentMarket
    ]);
  }

  // Write Out
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