// Critical Note
// Posted Buy orders labled Quantity Left
// Market Order Book is Listed Volume



// Global Property Service
const SCRIPT_PROPS = PropertiesService.getScriptProperties();

// --- CONFIGURATION SECTION ---

function GET_SDE_CONFIG() {
  return [
    { name: "SDE_invTypes", file: "invTypes.csv", cols: ["typeID", "groupID", "typeName", "volume", "marketGroupID", "basePrice", "portionSize"] }, // Added portionSize
    { name: "SDE_invGroups", file: "invGroups.csv", cols: null },
    { name: "SDE_staStations", file: "staStations.csv", cols: null },
    { name: "SDE_industryActivityMaterials", file: "industryActivityMaterials.csv", cols: null },
    { name: "SDE_industryActivityProducts", file: "industryActivityProducts.csv", cols: null },
    { name: "SDE_invTypeMaterials", file: "invTypeMaterials.csv", cols: null },
    { name: "SDE_Bonuses", file: "specializedReprocessingBonuses.csv", cols: null }
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
}

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
  const props = PropertiesService.getScriptProperties();

  ss.toast("🚀 Nitro Sync: Ingesting RAM...", "Engine Room", 5);

  try {
    props.setProperty('MANUAL_SYNC_ACTIVE', 'TRUE');

    // NITRO MOVE: Load memory once
    const fullData = getOverviewData(ss);

    // 1. DUMP ENGINE (Returns the Ban List)
    const dumpedItems = generateDumpToBuyOrder(ss, fullData) || new Set();

    // 2. BUY/RESTOCK ENGINES (Receives fullData + Ban List)
    generateNeedToBuyQuery(ss, fullData, dumpedItems);
    generateRestockItemsOnHand(ss, fullData);
    generatePVPTrap(ss, fullData);
    
    // 3. CONSOLIDATION (Ensure this function is updated to accept fullData if needed)
    generateConsolidatedRequirements(ss);

    ss.toast("✅ Sync Complete.", "Engine Room", 3);
  } catch (e) {
    ss.toast("❌ Sync failed: " + e.message, "Engine Room Error");
  } finally {
    props.setProperty('MANUAL_SYNC_ACTIVE', 'FALSE');
  }
}

function respondToEdit(e) {
  if (!e || !e.range || !e.source) return;

  const now = new Date().getTime();
  const lastRun = parseInt(SCRIPT_PROPS.getProperty('LAST_EDIT_TS') || '0');
  if (now - lastRun < 2000) return;
  SCRIPT_PROPS.setProperty('LAST_EDIT_TS', now.toString());

  const col = e.range.columnStart;
  if (col > 2) return; 

  const sheetName = e.range.getSheet().getName();
  const fullData = getOverviewData(e.source); // Pull once for the edit event

  if (sheetName === 'Need To Buy') {
    const dumpedItems = generateDumpToBuyOrder(e.source, fullData);
    generateNeedToBuyQuery(e.source, fullData, dumpedItems);
  } else if (sheetName === 'Restock Items On Hand') {
    generateRestockItemsOnHand(e.source, fullData);
  } else if (sheetName === 'Dump to Buy') {
    const dumpedItems = generateDumpToBuyOrder(e.source, fullData);
    generateNeedToBuyQuery(e.source, fullData, dumpedItems);
    generateRestockItemsOnHand(e.source, fullData);
  }
}

/**
 * Generates List for Dumping Profitable Overstocks to Buy Orders
 * COLUMN D PATCH: Inserted "Bottom Buy Price" at 20% Margin target.
 */
function generateDumpToBuyOrder(ss, fullData) {
  const TARGET_SHEET = 'Dump to Buy';
  const CORP_ORDERS_SHEET = 'CorpOrdersCalc';

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET);
  const corpOrdersSheet = ss.getSheetByName(CORP_ORDERS_SHEET);

  const dumpedItems = new Set(); 

  if (!sheet) return dumpedItems;

  const clean = (v) => (typeof v === 'number') ? v : parseFloat(String(v || 0).replace(/[^0-9.-]/g, '')) || 0;

  // Calculate Fee + Tax overhead
  let rateMultiplier = 1.046;
  const fee = ss.getRangeByName("FEE_RATE")?.getValue() || 0.01;
  const tax = ss.getRangeByName("TAX_RATE")?.getValue() || 0.036;
  rateMultiplier = (1 + Number(fee) + Number(tax));

  const bParams = sheet.getRange("B5:B15").getValues();
  const filterMinDays = parseFloat(bParams[0][0]) || 0;     
  const filterMinMargin = parseFloat(bParams[4][0]) || 0;   
  const filterGroupName = String(bParams[6][0] || "").toLowerCase().trim(); 

  // --- HEADERS (Now 9 Columns total) ---
  // Inserted "Bottom Buy (20%)" at Index 1 (Column D)
  const headerLabels = [[
    "Item Name", 
    "Bottom Buy (20%)", 
    "Manufacturing Projected Cost", 
    "Effective Cost", 
    "Hub Median Buy", 
    "Forensic Margin", 
    "Warehouse Qty", 
    "Total Dump ISK", 
    "Trend"
  ]];
  sheet.getRange("C4:K4").setValues(headerLabels).setFontWeight("bold").setBackground("#f3f3f3");

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

  const sourceData = fullData || getOverviewData(ss);
  if (!sourceData || sourceData.length === 0) return dumpedItems;

  let hIdx = -1;
  for (let i = 0; i < Math.min(sourceData.length, 5); i++) {
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
    daysInv: headers.indexOf("Days of Inventory"),
    signal: headers.indexOf("Signal") 
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

    const hubBuy = clean(r[col.medianBuy]);
    if (hubBuy < MIN_VALID_COST) continue;

    const effCost = clean(r[col.effCost]);
    const buildNow = clean(r[col.buildNow]);
    const signal = String(r[col.signal] || "").toUpperCase(); 

    // Determine the Reality Floor
    let realityFloor = 0;
    if (buildNow > MIN_VALID_COST) {
      realityFloor = effCost > buildNow ? effCost : buildNow;
    } else if (effCost > MIN_VALID_COST) {
      realityFloor = effCost;
    } else {
      continue;
    }

    // --- BOTTOM BUY CALCULATION ---
    // This is the price you need to see on the market to hit 20% ROI
    const bottomBuyPrice = realityFloor * 1.20 * rateMultiplier;

    const netProceeds = hubBuy / rateMultiplier;
    const margin = (netProceeds - realityFloor) / realityFloor;

    let currentMinMargin = filterMinMargin;
    let trendOutput = signal || "-";

    if (signal.includes("STAGNANT")) {
      currentMinMargin = 0; 
      trendOutput = "STAGNANT (LIQUIDATING)";
    }

    if (margin >= currentMinMargin && margin < 5.0) {
      const qty = clean(r[col.whQty]);
      if (qty > 0) {
        dumpResults.push([
          name,
          bottomBuyPrice, // Column D
          buildNow <= 0 ? "" : buildNow, // Column E
          effCost, // Column F
          hubBuy, // Column G
          margin, // Column H
          qty, // Column I
          (hubBuy * qty), // Column J
          trendOutput // Column K
        ]);
        dumpedItems.add(name); 
      }
    }
  }

  dumpResults.sort((a, b) => b[7] - a[7]); 

  const START_ROW = 5;
  const maxRows = Math.max(sheet.getMaxRows(), START_ROW);
  // Clear 9 columns now
  if (maxRows >= START_ROW) {
    sheet.getRange(START_ROW, 3, maxRows - (START_ROW - 1), 9).clearContent();
  }

  if (dumpResults.length > 0) {
    sheet.getRange(START_ROW, 3, dumpResults.length, 9).setValues(dumpResults);
    // Forensic Margin is now in Column H (Index 8 in Sheet, Index 5 in array)
    sheet.getRange(START_ROW, 8, dumpResults.length, 1).setNumberFormat("0.00%");
  }

  return dumpedItems; 
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


function generateNeedToBuyQuery(ss, fullData, dumpedItems = new Set()) {
  const TARGET_SHEET_NAME = 'Need To Buy';
  const AUDIT_SHEET_NAME = 'Audit items';
  const CONFIG_SHEET_NAME = 'Config_BPC_Runs';

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  const auditSheet = ss.getSheetByName(AUDIT_SHEET_NAME);
  const configSheet = ss.getSheetByName(CONFIG_SHEET_NAME);

  if (!sheet || !auditSheet) return;

  const parseNum = (v) => {
    if (typeof v === 'number') return v;
    if (!v) return 0;
    const cleanStr = String(v).replace(/[^0-9.-]/g, '');
    return parseFloat(cleanStr) || 0;
  };

  // --- CONFIG LOADER ---
  const configMap = new Map();
  if (configSheet) {
    const configData = configSheet.getDataRange().getValues();
    const cHeaders = configData[0] || [];
    const bpoIdx = cHeaders.indexOf("available_bpos");
    const capIdx = cHeaders.indexOf("hard_run_cap");
    const quotaIdx = cHeaders.indexOf("daily_quota");

    for (let i = 1; i < configData.length; i++) {
      const row = configData[i];
      const nameStr = String(row[2] || "").trim();
      if (nameStr) {
        configMap.set(nameStr, {
          available_bpos: bpoIdx > -1 ? parseNum(row[bpoIdx]) : 0,
          hard_run_cap: capIdx > -1 ? parseNum(row[capIdx]) : 0,
          daily_quota: quotaIdx > -1 ? parseNum(row[quotaIdx]) : 0
        });
      }
    }
  }

  // --- NITRO LOAD ---
  const rawDataValues = fullData || getOverviewData(ss);
  if (!rawDataValues || rawDataValues.length === 0) return;

  // Assuming getOverviewData starts at Row 3, headers are index 0
  const headers = rawDataValues[0];
  const marketRows = rawDataValues.slice(1);

  const auditValues = auditSheet.getDataRange().getValues();
  const auditMap = new Map(auditValues.slice(1).map(r => [String(r[0]).trim(), r[1]]));

  const filters = sheet.getRange('B5:B26').getValues();
  const minDays = parseNum(filters[0][0]) || 0;
  const targetDays = parseNum(filters[2][0]) || 31;
  let rawMargin = parseNum(filters[4][0]);
  const minMargin = (rawMargin > 1) ? rawMargin / 100 : rawMargin;
  const globalBpoLimit = parseNum(filters[19][0]) || 11;

  const cfg = {
    minDays: minDays,
    targetDays: targetDays,
    minMargin: minMargin || 0,
    limit: filters[14][0] === "No Limit" ? 5000 : parseInt(filters[14][0]) || 5000,
    ignoreGroups: (filters[21][0] || "").toLowerCase().split(',').map(s => s.trim()).filter(s => s),
    groups: (filters[6][0] || "").toLowerCase().split(',').map(s => s.trim()).filter(s => s),
    sortDir: (filters[8][0] || "ASC").toUpperCase()
  };

  const getIdx = (n) => headers.indexOf(n);
  const col = {
    name: getIdx("Item Name"),
    group: getIdx("Group"),
    qLeft: getIdx("Quantity Left"),
    sellQty: getIdx("Posted Sell Quantity"),
    jobs: getIdx("Active Jobs"),
    deliv: getIdx("Deliveries"),
    mfgCost: getIdx("Manufacturing Unit Cost"),
    vol30: getIdx("30-day traded volume"),
    vel: getIdx("Effective Daily Velocity (u/d)"),
    warehouse: getIdx("Warehouse Qty"),
    totalMkt: getIdx("Total Market Quantity"),
    effCost: getIdx("Effective Cost"),
    margin: getIdx("Margin"),
    buyAction: getIdx("Buy Action"),
    signal: getIdx("Signal")
  };

  let results = [];

  marketRows.forEach(row => {
    const name = String(row[col.name] || "").trim();
    if (!name || auditMap.get(name) !== true) return;
    if (dumpedItems.has(name)) return; // Reconciliation Gate

    const group = String(row[col.group] || "").toLowerCase().trim();
    if (cfg.ignoreGroups.includes(group)) return;
    if (cfg.groups.length > 0 && !cfg.groups.includes(group)) return;

    const currentStock = parseNum(row[col.warehouse]) + parseNum(row[col.qLeft]) + 
                         parseNum(row[col.sellQty]) + parseNum(row[col.jobs]) + parseNum(row[col.deliv]);
    const velocity = parseNum(row[col.vel]);

    if (currentStock > Math.ceil(velocity * cfg.minDays)) return;

    const restockNeed = Math.round(Math.ceil(velocity * cfg.targetDays) - currentStock);
    if (restockNeed <= 0) return;

    let itemMargin = parseNum(row[col.margin]);
    if (itemMargin > 1 && itemMargin <= 100 && cfg.minMargin <= 1) itemMargin /= 100;
    if (itemMargin < cfg.minMargin) return;

    const rawBuyAction = String(row[col.buyAction] || "").toUpperCase();
    let finalBuyAction = (/SKIP|HOLD/i.test(rawBuyAction)) ? "FORCE RESTOCK" : rawBuyAction;
    const cost = parseNum(row[col.effCost]) || parseNum(row[col.mfgCost]) || 0;
    const signal = String(row[col.signal] || "").toUpperCase();

    if (signal.includes("TRAP")) finalBuyAction = "SKIP (TRAP)";
    else if (signal.includes("STAGNANT")) finalBuyAction = "SKIP (STAGNANT)";

    let slotOutput = "";
    let runOutput = "";

    if (finalBuyAction === "MANUFACTURE") {
      const configData = configMap.get(name) || {};
      const isAmmo = /Missile|Torpedo|Charge|Crystal/i.test(name);
      const unitsPerRun = isAmmo ? 100 : 1;
      const hardCap = configData.hard_run_cap || (isAmmo ? 1500 : 300);
      const bpoLimit = configData.available_bpos || globalBpoLimit;
      const dailyInstallQuota = configData.daily_quota || bpoLimit;

      const totalRunsNeeded = Math.ceil(restockNeed / unitsPerRun);
      let slotsToUse = Math.min(bpoLimit, totalRunsNeeded);
      let isRolling = false;

      if (slotsToUse > dailyInstallQuota) {
        slotsToUse = dailyInstallQuota;
        isRolling = true;
      }

      let runsPerSlot = Math.min(hardCap, Math.ceil(totalRunsNeeded / Math.max(1, slotsToUse)));

      if (isRolling) finalBuyAction = `ROLLING THUNDER: INSTALL ${slotsToUse} SLOTS`;
      else {
        const pilots = Math.ceil(slotsToUse / 11);
        finalBuyAction = (pilots > 1) ? `SIEGE: WAKE ${pilots} PILOTS` : `WAKE 1 PILOT`;
      }
      slotOutput = slotsToUse;
      runOutput = runsPerSlot;
    }

    results.push({
      data: [name, restockNeed, cost, restockNeed * cost, parseNum(row[col.totalMkt]), 
             parseNum(row[col.vol30]), signal || "-", parseNum(row[col.warehouse]), 
             itemMargin, finalBuyAction, slotOutput, runOutput],
      sortKey: name
    });
  });

  results.sort((a, b) => (cfg.sortDir === "ASC" ? 1 : -1) * a.sortKey.localeCompare(b.sortKey));

  const output = results.slice(0, cfg.limit).map(r => r.data);
  const maxRows = Math.max(sheet.getLastRow(), 5);
  sheet.getRange(5, 3, maxRows, 12).clearContent();

  if (output.length > 0) {
    sheet.getRange(5, 3, output.length, 12).setValues(output);
  }
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