/**
 * TRIGGER-READY WRAPPER
 * Point your timed trigger at this function.
 */
function trigger_generateProjectedCostTable() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  generateProjectedCostTable(ss);
}

/**
 * OPTIMIZED: generateProjectedCostTable(ss)
 * Logic: 3-tier fallback (Stock -> Cache -> hubFallBack) with Group Filter.
 */
function generateProjectedCostTable(ss) {
  if (!ss || typeof ss.getSheetByName !== 'function') ss = SpreadsheetApp.getActiveSpreadsheet();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ProjectedCost') : console;
  
  // --- 1. Load Constants & Pricing Maps ---
  const ACTIVITY_MANUFACTURING = 1;
  const ME_LEVEL = 10; 
  const EST_INSTALL_RATE = 0.05; 
  const ACQUISITION_MULTIPLIER = 1.11;

  // A. Internal Stock Map - Fixed Typo: "Manufacturing"
  const costSheet = ss.getSheetByName("Manufacturing Inputs Effective Cost");
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

  // --- 2. SDE Mapping & Overview Prep ---
  // Ensure _getSdeMaps(ss) is defined in your project!
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  const productToBpMap = new Map();
  for (const [bpID, prodObj] of sdeProdMap.entries()) {
      productToBpMap.set(prodObj.productTypeID, { bpID: bpID, yield: prodObj.quantity });
  }

  const overviewSheet = ss.getSheetByName("MarketOverviewData");
  const overviewData = overviewSheet.getDataRange().getValues();
  const headers = overviewData[2]; 
  const col = { id: headers.indexOf("type_id"), name: headers.indexOf("Item Name"), group: headers.indexOf("Group") };

  const missingForAPI = new Set();
  const validTargets = [];

  // --- 3. FILTER & "SHOPPING LIST" PREP ---
  for (let i = 3; i < overviewData.length; i++) {
    const row = overviewData[i];
    if (String(row[col.group] || "").indexOf("Manufacturing") === -1) continue;

    const typeID = Number(row[col.id]);
    const prodData = productToBpMap.get(typeID);
    if (!prodData) continue;

    validTargets.push({ typeID, name: row[col.name], prodData });

    const materials = sdeMatMap.get(prodData.bpID);
    if (materials) {
      materials.forEach(m => {
        if (m.activityID === ACTIVITY_MANUFACTURING) {
          const matID = Number(m.materialTypeID);
          if (!myCostMap.has(matID) && !marketFeedMap.has(matID)) {
            missingForAPI.add(matID);
          }
        }
      });
    }
  }

  // --- C. THE HUB FALLBACK (Plan B for Missing Prices) ---
  const apiMap = new Map();
  if (missingForAPI.size > 0) {
    try {
      const missingIds = Array.from(missingForAPI);
      const fallbackPrices = hubFallBack(missingIds.map(id => [id]), "buy", "max", ss);

      missingIds.forEach((id, index) => {
        const price = fallbackPrices[index][0];
        if (price > 0 && price !== "") {
          apiMap.set(Number(id), price * ACQUISITION_MULTIPLIER);
        }
      });
      LOG.info(`Fallback: Fetched ${apiMap.size} items via Hub.`);
    } catch (e) { LOG.error("Fallback Fail: " + e.message); }
  }

  // --- 4. CALCULATION LOOP ---
  const outputRows = validTargets.map(target => {
    const materials = sdeMatMap.get(target.prodData.bpID);
    let totalBatchCost = 0;
    const sources = new Set();

    materials.forEach(m => {
      if (m.activityID !== ACTIVITY_MANUFACTURING) return;
      const matID = Number(m.materialTypeID);
      const qty = Math.max(1, Math.ceil(m.quantity * ((100 - ME_LEVEL) / 100)));
      
      let price = 0;
      if (myCostMap.has(matID)) {
        price = myCostMap.get(matID);
        sources.add("Stock");
      } else if (marketFeedMap.has(matID)) {
        price = marketFeedMap.get(matID);
        sources.add("Cache");
      } else if (apiMap.has(matID)) {
        price = apiMap.get(matID);
        sources.add("Hub");
      } else {
        sources.add("ZERO");
      }
      totalBatchCost += (qty * price);
    });

    const unitCost = (totalBatchCost * (1 + EST_INSTALL_RATE)) / target.prodData.yield;
    return [target.typeID, target.name, unitCost, Array.from(sources).join("/"), new Date()];
  });

  // --- 5. OUTPUT TO SHEET ---
  const outSheet = ss.getSheetByName("Projected_Build_Costs");
  if (!outSheet) return;

  outSheet.clearContents(); 
  outSheet.getRange(1, 1, 1, 5).setValues([["Type ID", "Item Name", "Cost", "Source Tier", "Updated"]]);
  
  if (outputRows.length > 0) {
    outSheet.getRange(2, 1, outputRows.length, 5).setValues(outputRows);
    outSheet.getRange(2, 5, outputRows.length, 1).setNumberFormat("yyyy-mm-dd hh:mm");
  }

  const maxRows = outSheet.getMaxRows();
  const lastRow = outputRows.length + 1;
  if (maxRows > lastRow + 5) outSheet.deleteRows(lastRow + 1, maxRows - lastRow);
  
  LOG.info(`Complete. Processed ${outputRows.length} items.`);
}