/**
 * Calculates Blueprint costs with a 3-tier fallback:
 * 1. Stock (Effective Cost) -> 2. Cache (Market_Data_Raw) -> 3. API (Fuzzwork)
 */
function generateProjectedCostTable(ss) {
  if(!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ProjectedCost') : console;
  
  // --- 1. Load Constants & Maps ---
  const ACTIVITY_MANUFACTURING = 1;
  const ME_LEVEL = 10; 
  const EST_INSTALL_RATE = 0.05; 
  const ACQUISITION_MULTIPLIER = 1.11; // 11% tax/fee buffer for market buys

  // A. Internal Stock Map
  const costSheet = ss.getSheetByName("Manufaturing Inputs Effective Cost");
  const costData = costSheet ? costSheet.getDataRange().getValues() : [];
  let myCostMap = new Map();
  for (let i = 1; i < costData.length; i++) {
    let id = Number(costData[i][0]);
    let val = parseFloat(String(costData[i][1]).replace(/[^0-9.]/g, ''));
    if (val > 0) myCostMap.set(id, val);
  }

  // B. Market Cache Map (Market_Data_Raw)
  const rawMarketSheet = ss.getSheetByName("Market_Data_Raw");
  const rawMarketData = rawMarketSheet ? rawMarketSheet.getDataRange().getValues() : [];
  let marketFeedMap = new Map();
  if (rawMarketData.length > 1) {
    for (let i = 1; i < rawMarketData.length; i++) {
      let id = Number(rawMarketData[i][1]); // type_id
      let buyMax = Number(rawMarketData[i][5]); // buy_max
      if (buyMax > 0) marketFeedMap.set(id, buyMax * ACQUISITION_MULTIPLIER);
    }
  }

  // --- 2. Identify Missing Materials for API Fetch ---
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  let productToBpMap = new Map();
  for (const [bpID, prodObj] of sdeProdMap.entries()) {
      productToBpMap.set(prodObj.productTypeID, { bpID: bpID, yield: prodObj.quantity });
  }

  let missingForAPI = new Set();
  const marketOverview = ss.getSheetByName("MarketOverviewData").getDataRange().getValues();
  
  for (let i = 3; i < marketOverview.length; i++) {
    let typeID = Number(marketOverview[i][1]);
    let prodData = productToBpMap.get(typeID);
    if (!prodData) continue;

    let materials = sdeMatMap.get(prodData.bpID);
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
  let apiMap = new Map();
  if (missingForAPI.size > 0 && typeof fuzAPI !== 'undefined') {
    try {
      const res = fuzAPI.requestItems(10000002, 'region', Array.from(missingForAPI));
      res.forEach(item => {
        let p = _extractMetric_(item, 'buy', 'max');
        if (p > 0) apiMap.set(item.type_id, p * ACQUISITION_MULTIPLIER);
      });
    } catch (e) { LOG.error("API Fail: " + e.message); }
  }

  // --- 3. Final Calculation Loop ---
  let outputRows = [];
  for (let i = 3; i < marketOverview.length; i++) {
    let typeID = Number(marketOverview[i][1]);
    let typeName = marketOverview[i][2];
    let prodData = productToBpMap.get(typeID);
    if (!prodData) continue;

    let materials = sdeMatMap.get(prodData.bpID);
    let totalBatchCost = 0;
    let sources = new Set();

    materials.forEach(m => {
      if (m.activityID !== ACTIVITY_MANUFACTURING) return;
      let qty = Math.max(1, Math.ceil(m.quantity * ((100 - ME_LEVEL) / 100)));
      
      let price = 0;
      if (myCostMap.has(m.materialTypeID)) {
        price = myCostMap.get(m.materialTypeID);
        sources.add("Stock");
      } else if (marketFeedMap.has(m.materialTypeID)) {
        price = marketFeedMap.get(m.materialTypeID);
        sources.add("Cache");
      } else {
        price = apiMap.get(m.materialTypeID) || 0;
        sources.add(price > 0 ? "API" : "ZERO");
      }
      totalBatchCost += (qty * price);
    });

    let unitCost = (totalBatchCost * (1 + EST_INSTALL_RATE)) / prodData.yield;
    outputRows.push([typeID, typeName, unitCost, Array.from(sources).join("/"), new Date()]);
  }

  // --- 4. Output ---
  let outSheet = ss.getSheetByName("Projected_Build_Costs");
  outSheet.clear().appendRow(["Type ID", "Item Name", "Cost", "Source Tier", "Updated"]);
  if (outputRows.length > 0) outSheet.getRange(2,1,outputRows.length,5).setValues(outputRows);
}