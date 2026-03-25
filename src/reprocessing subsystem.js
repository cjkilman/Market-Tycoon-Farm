
/**
 * TRIGGER-READY WRAPPER
 * Point your timed trigger at this function.
 */
function trigger_generateProjectedCostTable() {
  // Use getActiveSpreadsheet() for bound scripts
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // Call your main logic
  generateProjectedCostTable(ss);
}

/**
 * OPTIMIZED: generateProjectedCostTable(ss)
 * Logic: 3-tier fallback with "Manufacturing" Group Filter.
 * Optimizations: O(1) Indexing, Early Exit, Memory-Resident Processing.
 */
function generateProjectedCostTable(ss) {
  if (!ss || typeof ss.getSheetByName !== 'function') {
    ss = SpreadsheetApp.getActiveSpreadsheet();
  }
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ProjectedCost') : console;
  
  // --- 1. Load Constants & Pricing Maps ---
  const ACTIVITY_MANUFACTURING = 1;
  const ME_LEVEL = 10; 
  const EST_INSTALL_RATE = 0.05; 
  const ACQUISITION_MULTIPLIER = 1.11;

  // A. Internal Stock Map (O(1) Map)
  const costSheet = ss.getSheetByName("Manufaturing Inputs Effective Cost");
  const costData = costSheet ? costSheet.getDataRange().getValues() : [];
  const myCostMap = new Map(costData.slice(1).map(r => [Number(r[0]), parseFloat(String(r[1]).replace(/[^0-9.]/g, ''))]).filter(r => r[1] > 0));

  // B. Market Cache Map (Market_Data_Raw)
  const rawMarketSheet = ss.getSheetByName("Market_Data_Raw");
  const rawMarketData = rawMarketSheet ? rawMarketSheet.getDataRange().getValues() : [];
  const marketFeedMap = new Map();
  if (rawMarketData.length > 1) {
    for (let i = 1; i < rawMarketData.length; i++) {
      const id = Number(rawMarketData[i][1]);
      const buyMax = Number(rawMarketData[i][5]);
      if (buyMax > 0) marketFeedMap.set(id, buyMax * ACQUISITION_MULTIPLIER);
    }
  }

  // --- 2. SDE Mapping & API Prep ---
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  const productToBpMap = new Map();
  for (const [bpID, prodObj] of sdeProdMap.entries()) {
      productToBpMap.set(prodObj.productTypeID, { bpID: bpID, yield: prodObj.quantity });
  }

  // Get MarketOverviewData into memory
  const overviewSheet = ss.getSheetByName("MarketOverviewData");
  const overviewData = overviewSheet.getDataRange().getValues();
  const headers = overviewData[2]; // Headers are usually on row 3 (index 2)
  
  const col = {
    id: headers.indexOf("type_id"),
    name: headers.indexOf("Item Name"),
    group: headers.indexOf("Group")
  };

  const missingForAPI = new Set();
  const validTargets = [];

  // --- 3. FILTER & API PREP ---
  for (let i = 3; i < overviewData.length; i++) {
    const row = overviewData[i];
    const group = String(row[col.group] || "");
    
    // EARLY EXIT: Skip anything not in a Manufacturing group (kills Meta Module scrap math)
    if (group.indexOf("Manufacturing") === -1) continue;

    const typeID = Number(row[col.id]);
    const prodData = productToBpMap.get(typeID);
    if (!prodData) continue;

    validTargets.push({ typeID, name: row[col.name], prodData });

    const materials = sdeMatMap.get(prodData.bpID);
    if (materials) {
      materials.forEach(m => {
        if (m.activityID === ACTIVITY_MANUFACTURING) {
          if (!myCostMap.has(m.materialTypeID) && !marketFeedMap.has(m.materialTypeID)) {
            missingForAPI.add(m.materialTypeID);
          }
        }
      });
    }
  }

  // C. API Fallback (Fuzzwork)
  const apiMap = new Map();
  if (missingForAPI.size > 0 && typeof fuzAPI !== 'undefined') {
    try {
      const res = fuzAPI.requestItems(10000002, 'region', Array.from(missingForAPI));
      res.forEach(item => {
        const p = _extractMetric_(item, 'buy', 'max');
        if (p > 0) apiMap.set(item.type_id, p * ACQUISITION_MULTIPLIER);
      });
    } catch (e) { LOG.error("API Fail: " + e.message); }
  }

  // --- 4. CALCULATION LOOP ---
  const outputRows = validTargets.map(target => {
    const materials = sdeMatMap.get(target.prodData.bpID);
    let totalBatchCost = 0;
    const sources = new Set();

    materials.forEach(m => {
      if (m.activityID !== ACTIVITY_MANUFACTURING) return;
      const qty = Math.max(1, Math.ceil(m.quantity * ((100 - ME_LEVEL) / 100)));
      
      let price = 0;
      if (myCostMap.has(m.materialTypeID)) {
        price = myCostMap.get(m.materialTypeID);
        sources.add("Stock");
      } else if (marketFeedMap.has(m.materialTypeID)) {
        price = marketFeedMap.get(m.materialTypeID);
        sources.add("Cache");
      } else if (apiMap.has(m.materialTypeID)) {
        price = apiMap.get(m.materialTypeID);
        sources.add("API");
      } else {
        sources.add("ZERO");
      }
      totalBatchCost += (qty * price);
    });

    const unitCost = (totalBatchCost * (1 + EST_INSTALL_RATE)) / target.prodData.yield;
    return [target.typeID, target.name, unitCost, Array.from(sources).join("/"), new Date()];
  });

 // --- 5. OUTPUT & MAINTENANCE (THE "STATION SERVICE") ---
  const outSheet = ss.getSheetByName("Projected_Build_Costs");
  if (!outSheet) {
    LOG.error("Sheet 'Projected_Build_Costs' not found.");
    return;
  }

  // 1. Clear contents and formatting to keep the sheet light
  outSheet.clearContents(); 
  
  // 2. Define specific output headers (Don't reuse headers from the Overview sheet)
  const outputHeaders = [["Type ID", "Item Name", "Cost", "Source Tier", "Updated"]];
  outSheet.getRange(1, 1, 1, 5).setValues(outputHeaders);
  
  // 3. Write Data in one single batch (Fastest method)
  if (outputRows.length > 0) {
    outSheet.getRange(2, 1, outputRows.length, 5).setValues(outputRows);
  }

  // 4. TRIM BLANK ROWS: This physically removes the lag-inducing empty space
  const maxRows = outSheet.getMaxRows();
  const lastRowWithData = outputRows.length + 1; // Data rows + Header

  // If we have more than 5 extra rows, purge them to optimize the calculation engine
  if (maxRows > lastRowWithData + 5) {
    try {
      // Deleting rows forces the spreadsheet to "compact" its internal memory
      outSheet.deleteRows(lastRowWithData + 1, maxRows - lastRowWithData);
      LOG.info(`Trimmed ${maxRows - lastRowWithData} excess rows. Sheet is now compact.`);
    } catch (e) {
      LOG.warn("Row trim skipped (might be already compact): " + e.message);
    }
  }
  outSheet.getRange(2, 5, outputRows.length, 1).setNumberFormat("yyyy-mm-dd hh:mm");
  LOG.info(`Optimization complete. Processed ${outputRows.length} Manufacturing items.`);
}

/**
 * SDE MATERIAL ENGINE (The Pantry)
 * Key: Parent TypeID | Value: Array of { matID: number, qty: number }
 */
function getSdeMaterialMap(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("SDE_invTypeMaterials");
  if (!sheet) return new Map();

  const data = sheet.getDataRange().getValues();
  const materialMap = new Map();
  // Skip headers if the first cell is a string
  const startRow = (isNaN(data[0][0])) ? 1 : 0;

  for (let i = startRow; i < data.length; i++) {
    const parentId = Number(data[i][0]);
    if (!parentId) continue;

    if (!materialMap.has(parentId)) materialMap.set(parentId, []);
    materialMap.get(parentId).push({ 
      matID: Number(data[i][1]), 
      qty: Number(data[i][2]) 
    });
  }
  return materialMap;
}

/**
 * UNIVERSAL TYPE ENGINE (The Pantry)
 */
function getSdeTypeEngine(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("SDE_invTypes");
  const byName = new Map();
  const byId = new Map();
  if (!sheet) return { byName, byId };

  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  const idIdx = headers.indexOf('typeID');
  const nameIdx = headers.indexOf('typeName');
  const portionIdx = headers.indexOf('portionSize');

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const id = Number(row[idIdx]);
    const name = String(row[nameIdx]).trim().toLowerCase();
    const portion = Number(row[portionIdx]) || 1;

    if (id > 0) {
      const typeObj = { id, name, portion };
      byId.set(id, typeObj);
      if (name !== "") byName.set(name, typeObj);
    }
  }
  return { byName, byId };
}

/**
 * CORE REPROCESSING MATH ENGINE (The Chef)
 */
function calculateMeltValue(materials, efficiency, batchCount, priceMap) {
  let totalValue = 0;
  const yieldDetails = [];

  materials.forEach(mat => {
    const yieldQty = Math.floor(mat.qty * efficiency * batchCount);
    const unitPrice = priceMap.get(Number(mat.matID)) || 0;
    const matValue = yieldQty * unitPrice;

    totalValue += matValue;
    if (yieldQty > 0) {
      yieldDetails.push({ id: mat.matID, qty: yieldQty, value: matValue, unitPrice: unitPrice });
    }
  });

  return { totalValue: totalValue, yields: yieldDetails };
}

/**
 * REPROCESSED VALUE ENGINE (Sprinting & Compacting Version)
 * Optimizations: Single-pass I/O, zero-latency caching, auto-trimming.
 */
function generateReprocessedValueTable(ss) {
  const start = new Date().getTime();
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ReproValue') : console;
  
  LOG.info("--- Sprinting: Repro Table Generation ---");

  // 1. BULK DATA LOAD
  const overviewSheet = ss.getSheetByName("MarketOverviewData");
  if (!overviewSheet) {
    LOG.error("Source 'MarketOverviewData' not found.");
    return;
  }
  const overviewData = overviewSheet.getDataRange().getValues();
  
  const marketMap = getMarketPriceMapFor(ss, 'buy_max'); 
  const materialMap = getSdeMaterialMap(ss);            
  const { byId: typeMap } = getSdeTypeEngine(ss);      
  const totalEfficiency = 0.50 * 1.69; 

  const headers = overviewData[2];
  const colId = headers.indexOf("type_id");
  const colName = headers.indexOf("Item Name");

  // 2. RAM-RESIDENT REDUCTION
  const outputRows = overviewData.slice(3).reduce((acc, row) => {
    const typeID = Number(row[colId]);
    if (!typeID) return acc;

    const materials = materialMap.get(typeID);
    const typeInfo = typeMap.get(typeID);
    
    if (materials && typeInfo) {
      let totalValue = 0;
      const batchCount = 1 / typeInfo.portion;
      
      for (const mat of materials) {
        const yieldQty = Math.floor(mat.qty * totalEfficiency * batchCount);
        totalValue += (yieldQty * (marketMap.get(Number(mat.matID)) || 0));
      }

      if (totalValue > 0) {
        acc.push([typeID, row[colName], totalValue, new Date()]);
      }
    }
    return acc;
  }, []);

  // 3. STATION SERVICE (One-shot Write)
  const SHEET_NAME = "Reprocessed_Material_Values";
  let outSheet = ss.getSheetByName(SHEET_NAME) || ss.insertSheet(SHEET_NAME);

  // Safety: If outputRows is 0, we don't want to clear headers and exit
  if (outputRows.length === 0) {
    LOG.warn("Zero reprocessable items found. Aborting write to protect sheet.");
    return;
  }

  outSheet.clearContents();
  const finalPayload = [["Type ID", "Item Name", "Melt Value (Unit)", "Updated"], ...outputRows];
  outSheet.getRange(1, 1, finalPayload.length, 4).setValues(finalPayload);

 // 4. THE COMPACTOR (Purge Ghost Rows/Columns)
  const lastRow = outSheet.getLastRow();
  const lastCol = outSheet.getLastColumn();
  
  // Aggressive Row Trim
  const currentMaxRows = outSheet.getMaxRows();
  if (currentMaxRows > lastRow + 1) {
    outSheet.deleteRows(lastRow + 2, currentMaxRows - (lastRow + 1));
  }

  // Aggressive Column Trim (Fixed: deleteColumns)
  const currentMaxCols = outSheet.getMaxColumns();
  if (currentMaxCols > lastCol) {
    outSheet.deleteColumns(lastCol + 1, currentMaxCols - lastCol);
  }

  // 5. NAMED RANGE SYNC
  const RANGE_NAME = "NR_REPRO_VALUE_TABLE";
  const finalRange = outSheet.getRange(1, 1, lastRow, lastCol);
  
  // Update logic: Remove old reference and set new one
  try {
    const existingRange = ss.getRangeByName(RANGE_NAME);
    if (existingRange) {
      ss.removeNamedRange(RANGE_NAME);
    }
    ss.setNamedRange(RANGE_NAME, finalRange);
  } catch (e) {
    LOG.warn("Named Range Sync Issue: " + e.message);
  }

  const end = new Date().getTime();
  LOG.info(`Sprint Finished: ${outputRows.length} items in ${(end - start) / 1000}s. Named Range Synced.`);
}