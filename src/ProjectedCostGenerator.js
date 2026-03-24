
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