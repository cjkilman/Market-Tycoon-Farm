// Critical Note
// Posted Buy orders labled Quantity Left
// Market Order Book is Listed Volume

/**
 * BREAKS CIRCULAR LOOPS: Snapshots Market Overview data into the ProductionList.
 * Run this to update your build targets without triggering a dependency error.
 */
function snapshotMarketRatios() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const overviewSheet = ss.getSheetByName("Market Overview");
  const prodSheet = ss.getSheetByName("ProductionList ");
  
  // 1. Grab the live ratios from the Overview (Static Pull)
  const overviewData = overviewSheet.getDataRange().getValues();
  const ratioMap = new Map(overviewData.map(r => [r[0], r[34]])); // type_id and Saturation/Ratio
  
  // 2. Map them to the Production List
  const prodData = prodSheet.getRange("B6:D" + prodSheet.getLastRow()).getValues();
  const updatedRatios = prodData.map(row => {
    const typeID = row[0]; // Product ID
    return [ratioMap.get(typeID) || 0];
  });
  
  // 3. Write them as hard values (Breaks the loop)
  prodSheet.getRange(6, 4, updatedRatios.length, 1).setValues(updatedRatios);
  
  ss.toast("Market Ratios Snapshotted. Circular Dependency Cleared.", "Engine Room");
}

/**
 * HARDENED BOM ENGINE: Dynamic column mapping to prevent crashes.
 */
function generateFullBOMData(ss) {
  if(!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('BOM_Engine') : console;
  const clean = (v) => (typeof v === 'number') ? v : parseFloat(String(v).replace(/[^0-9.-]/g, '')) || 0;

  // --- 1. Load Data ---
  const prodSheet = ss.getSheetByName("ProductionList ");
  const sdeMatSheet = ss.getSheetByName("SDE_industryActivityMaterials");
  const sdeProdSheet = ss.getSheetByName("SDE_industryActivityProducts");
  
  if (!prodSheet || !sdeMatSheet || !sdeProdSheet) return;

  const prodRaw = prodSheet.getDataRange().getValues();
  const pHeaders = prodRaw[4]; // Assumes headers on Row 5
  const prodData = prodRaw.slice(5);

  // --- 2. DYNAMIC MAPPING ---
  const pCol = {
    prodID: pHeaders.indexOf("Product ID"),
    bpID:   pHeaders.indexOf("Blueprint Type ID"),
    me:     pHeaders.indexOf("Material Efficiency (ME)"),
    runs:   pHeaders.indexOf("Total Runs")
  };

  const productToBpMap = new Map();
  sdeProdSheet.getDataRange().getValues().forEach(r => { 
    if (Number(r[1]) === 1) productToBpMap.set(Number(r[2]), Number(r[0])); 
  });

  const jobMap = new Map();
  prodData.forEach(row => {
    const pID = Number(row[pCol.prodID]);
    const manualVal = clean(row[pCol.bpID]);
    let bpID = (manualVal > 0 && manualVal < 1000000) ? manualVal : productToBpMap.get(pID);
    
    if (bpID) {
      const runs = clean(row[pCol.runs]);
      const me = row[pCol.me] === "" ? 10 : clean(row[pCol.me]);
      jobMap.set(bpID, { me, runs: (jobMap.get(bpID)?.runs || 0) + runs });
    }
  });

  // --- 3. Process Materials ---
  const sdeMatData = sdeMatSheet.getDataRange().getValues();
  const outputRows = [];
  for (let i = 1; i < sdeMatData.length; i++) {
    const sdeBpID = Number(sdeMatData[i][0]);
    if (sdeMatData[i][1] === 1 && jobMap.has(sdeBpID)) {
      const job = jobMap.get(sdeBpID);
      const adjQty = Number(sdeMatData[i][3]) * ((100 - job.me) / 100);
      outputRows.push([sdeBpID, 1, Number(sdeMatData[i][2]), Number(sdeMatData[i][3]), job.me, job.runs, adjQty, Math.ceil(adjQty * job.runs)]);
    }
  }

  // --- 4. Output ---
  const outSheet = ss.getSheetByName("Full_BOM_Data");
  outSheet.clearContents();
  outSheet.getRange(1, 1, 1, 8).setValues([["BP ID", "Act ID", "Mat ID", "Base Qty", "ME", "Runs", "Adj Qty", "Total Req"]]);
  if (outputRows.length > 0) outSheet.getRange(2, 1, outputRows.length, 8).setValues(outputRows);
  
  LOG.info(`BOM Fixed: Processed ${outputRows.length} lines.`);
}

/**
 * NITRO CONSOLIDATOR: Generates a 100% static requirement and shopping list.
 * Logic: Aggregates BOM, calculates Shopping List/Cost, and outputs static values.
 */
function generateConsolidatedRequirements(ss) {
  if(!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('BOM_Consolidator') : console;
  
  // Helper to handle ISK strings and ensure clean numbers
  const clean = (v) => {
    if (typeof v === 'number') return v;
    if (!v) return 0;
    return parseFloat(String(v).replace(/[^0-9.-]/g, '')) || 0;
  };

  // --- 1. Load All Data into RAM ---
  const getValues = (name) => {
    const sh = ss.getSheetByName(name);
    return sh ? sh.getDataRange().getValues() : [];
  };

  const bomDataRaw = getValues("Full_BOM_Data");
  const sdeData = getValues("SDE_invTypes");
  const blendedCostData = getValues("Blended_Cost");
  const marketRawData = getValues("Market_Data_Raw");
  const hangarData = getValues("MaterialHangar");

  if (bomDataRaw.length < 2) return;

  // --- 2. Build Lookup Maps (O(1) Access) ---
  const nameMap = new Map(sdeData.map(r => [Number(r[0]), r[2]]));
  
  // FIXED: Total Quantity is Column E (index 4) in your MaterialHangar
  const hangarMap = new Map();
  hangarData.forEach(r => {
    const id = Number(r[1]);
    if (id) hangarMap.set(id, (hangarMap.get(id) || 0) + clean(r[4]));
  });
  
  // Tiered Cost Map: Blended first, then Market Raw (buy_max * 1.11)
  const costMap = new Map();
  marketRawData.forEach(r => { 
    const price = clean(r[5]);
    if (price > 0) costMap.set(Number(r[1]), price * 1.11); 
  });
  blendedCostData.forEach(r => { 
    const price = clean(r[2]);
    if (price > 0) costMap.set(Number(r[0]), price); 
  });

  // --- 3. Aggregate Requirements ---
  const aggregation = {}; 

  for (let i = 1; i < bomDataRaw.length; i++) {
    const id = Number(bomDataRaw[i][2]); // Column C
    if (!id) continue;
    
    const qty = clean(bomDataRaw[i][7]); // Column H
    
    if (!aggregation[id]) {
      const unitCost = costMap.get(id) || 0;
      const onHand = hangarMap.get(id) || 0;
      aggregation[id] = {
        id: id,
        name: nameMap.get(id) || "Unknown",
        totalReq: 0,
        onHand: onHand,
        unitCost: unitCost
      };
    }
    aggregation[id].totalReq += qty;
  }

  // --- 4. Logic Processing (Replaces your ARRAYFORMULAs) ---
  const outputRows = Object.values(aggregation)
    .map(item => {
      // Logic: IF((Req - Stock) > 0, Req - Stock, 0)
      const shoppingList = Math.max(0, item.totalReq - item.onHand);
      const shoppingCost = shoppingList * item.unitCost;

      return [
        item.id, 
        item.name, 
        item.totalReq, 
        item.onHand, 
        item.unitCost, 
        shoppingList, 
        shoppingCost
      ];
    })
    .sort((a, b) => b[2] - a[2]); // Sort by Total Required DESC

  // --- 5. Output & Station Service ---
  const outSheet = ss.getSheetByName("Consolidated_Requirements");
  if (!outSheet) return;

  outSheet.clearContents();
  const headers = [["Type ID", "Material Name", "Total Required", "On Hand", "Unit Cost", "Shopping List", "Shopping Cost"]];
  outSheet.getRange(1, 1, 1, 7).setValues(headers);

  if (outputRows.length > 0) {
    outSheet.getRange(2, 1, outputRows.length, 7).setValues(outputRows);
    
    // Formatting: Make it readable
    outSheet.getRange(2, 5, outputRows.length, 1).setNumberFormat("#,##0.00\" ISK\""); // Unit Cost
    outSheet.getRange(2, 7, outputRows.length, 1).setNumberFormat("#,##0.00\" ISK\""); // Total Cost
  }

  // Trim the hull
  const maxRows = outSheet.getMaxRows();
  const lastDataRow = outputRows.length + 1;
  if (maxRows > lastDataRow + 5) {
    outSheet.deleteRows(lastDataRow + 1, maxRows - lastDataRow);
  }

  LOG.info(`BOM Consolidated: ${outputRows.length} materials. Static Shopping List generated.`);
}

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