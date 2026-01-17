/**
 * Generates a static table of Projected Manufacturing Costs.
 * * DEPENDENCIES: 
 * - IndustryLedger.gs.js (for SDE maps and Helpers)
 * - fuzAPI_combined.js (for Jita pricing)
 */

function generateProjectedCostTable() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  // Reuse Logger from Ledger if available, else console
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ProjectedCost') : console;
  
  LOG.info("Starting Projected Cost Generation (Linked Mode)...");

  // --- 1. Load Settings (Using Ledger Helper) ---
  // Uses _getNamedOr_ from IndustryLedger.gs.js
  const BROKER_FEE_RATE = Number(_getNamedOr_('FEE_RATE', 0.03)); 
  const TRANSACTION_TAX = Number(_getNamedOr_('TAX_RATE', 0.08)); 
  const ACQUISITION_MULTIPLIER = 1 + BROKER_FEE_RATE + TRANSACTION_TAX; 
  const EST_INSTALL_RATE = 0.05; 

  // --- 2. Setup Output Sheet ---
  const OUTPUT_SHEET = "Projected_Build_Costs";
  const HEADERS_OUT = ["Type ID", "Item Name", "Projected Build Cost", "Breakdown (Mat+Install)", "Source Notes", "Last Updated"];
  
  let targetSheet = ss.getSheetByName(OUTPUT_SHEET);
  if (!targetSheet) targetSheet = ss.insertSheet(OUTPUT_SHEET);
  targetSheet.clear(); 
  targetSheet.appendRow(HEADERS_OUT); 

  // --- 3. Load Inputs ---
  
  // A. Market Overview
  const marketSheet = ss.getSheetByName("MarketOverviewData");
  if (!marketSheet || marketSheet.getLastRow() < 4) return;
  const mktHeaders = marketSheet.getRange(3, 1, 1, marketSheet.getLastColumn()).getValues()[0];
  // Uses _getColIndexMap from IndustryLedger.gs.js
  const mktCol = _getColIndexMap(mktHeaders, ['type_id', 'Item Name']);
  const marketData = marketSheet.getRange(4, 1, marketSheet.getLastRow() - 3, marketSheet.getLastColumn()).getValues();

  // B. Your Stock Costs
  const costSheet = ss.getSheetByName("Manufaturing Inputs Effective Cost");
  const costData = costSheet ? costSheet.getDataRange().getValues() : [];
  
  // C. Market Cache
  const rawMarketSheet = ss.getSheetByName("Market_Data_Raw");
  const rawMarketData = rawMarketSheet ? rawMarketSheet.getDataRange().getValues() : [];

  // --- 4. Build Maps ---

  // Use the MASTER SDE MAP from IndustryLedger
  // Note: These maps contain BOTH Mfg and Invention data, so we must filter later.
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);

  // Map 1: Internal Stock
  let myCostMap = new Map();
  for (let i = 1; i < costData.length; i++) {
    let row = costData[i];
    let matID = Number(row[0]);
    let costStr = String(row[1]).replace(/ISK/gi, '').replace(/,/g, '').trim();
    let val = parseFloat(costStr);
    if (val > 0) myCostMap.set(matID, val);
  }

  // Map 2: Market Feed (With Fees)
  let marketFeedMap = new Map();
  if (rawMarketData.length > 1) {
    const rawHeaders = rawMarketData[0];
    const rawCol = _getColIndexMap(rawHeaders, ['type_id', 'buy_max']);
    for (let i = 1; i < rawMarketData.length; i++) {
      let row = rawMarketData[i];
      let id = Number(row[rawCol.type_id]);
      let val = Number(row[rawCol.buy_max]);
      if (val > 0) marketFeedMap.set(id, val * ACQUISITION_MULTIPLIER);
    }
  }

  // --- 5. Fetch Missing Tier 3 (Fuzzwork) ---
  let neededMaterials = new Set();
  const ME_LEVEL = 10; 
  // Ensure we use the global constant from Ledger
  const ACTIVITY_MANUFACTURING = (typeof INDUSTRY_ACTIVITY_MANUFACTURING !== 'undefined') ? INDUSTRY_ACTIVITY_MANUFACTURING : 1;

  for (let row of marketData) {
    let typeID = Number(row[mktCol.type_id]);
    if (!typeID) continue;
    
    // Product Map looks up by Product ID -> returns { productTypeID, quantity }
    // But wait, Ledger's _getSdeMaps returns map[BlueprintID] -> Array[Mats]
    // We need to find the Blueprint for this Product.
    // Ledger's sdeProdMap is Key: BlueprintID -> Value: Product Object.
    // We actually need a reverse lookup (Product -> Blueprint) which Ledger doesn't fully export in a convenient way.
    // Let's build a quick reverse map using the data we just fetched.
  }
  
  // FIX: Ledger's sdeProdMap is keyed by Blueprint ID. We need to key by Product ID.
  let productToBpMap = new Map();
  for (const [bpID, prodObj] of sdeProdMap.entries()) {
     productToBpMap.set(prodObj.productTypeID, { bpID: bpID, yield: prodObj.quantity });
  }

  // Scan for missing items
  for (let row of marketData) {
    let typeID = Number(row[mktCol.type_id]);
    let prodData = productToBpMap.get(typeID);
    
    if (prodData) {
      let materials = sdeMatMap.get(prodData.bpID);
      if (materials) {
        for (let m of materials) {
          // STRICT FILTER: Only Manufacturing Materials (Ignore Invention datacores)
          if (m.activityID !== ACTIVITY_MANUFACTURING) continue;

          if (!myCostMap.has(m.materialTypeID) && !marketFeedMap.has(m.materialTypeID)) {
            neededMaterials.add(m.materialTypeID);
          }
        }
      }
    }
  }

  let tier3Map = new Map();
  if (neededMaterials.size > 0 && typeof fuzAPI !== 'undefined') {
    LOG.info(`Fetching Tier 3 prices for ${neededMaterials.size} items...`);
    try {
      const res = fuzAPI.requestItems(10000002, 'region', Array.from(neededMaterials));
      res.forEach(item => {
        // Use standard Ledger helper _extractMetric_
        const price = _extractMetric_(item, 'buy', 'max'); 
        if (price > 0) tier3Map.set(item.type_id, price * ACQUISITION_MULTIPLIER);
      });
    } catch (e) {
      LOG.error("fuzAPI fetch failed: " + e.message);
    }
  }

  // --- 6. Calculate Costs ---
  let outputRows = [];
  let now = new Date();

  for (let row of marketData) {
    let typeID = Number(row[mktCol.type_id]);
    let typeName = row[mktCol['Item Name']];
    if (!typeID) continue;

    let prodData = productToBpMap.get(typeID);

    if (prodData) {
      let materials = sdeMatMap.get(prodData.bpID);

      if (materials) {
        let totalMatCost = 0;
        let sourceFlags = new Set();

        for (let m of materials) {
          // STRICT FILTER: Only Manufacturing Materials
          if (m.activityID !== ACTIVITY_MANUFACTURING) continue;

          let meMultiplier = (100 - ME_LEVEL) / 100;
          let requiredQty = Math.max(1, Math.ceil(m.quantity * meMultiplier));
          
          let matCost = 0;
          let matID = m.materialTypeID;

          if (myCostMap.has(matID)) {
            matCost = myCostMap.get(matID);
            sourceFlags.add("Stock");
          } else if (marketFeedMap.has(matID)) {
            matCost = marketFeedMap.get(matID);
            sourceFlags.add("Cache+Fees");
          } else if (tier3Map.has(matID)) {
            matCost = tier3Map.get(matID);
            sourceFlags.add("API+Fees");
          } else {
            sourceFlags.add("MISSING");
          }

          totalMatCost += (requiredQty * matCost);
        }

        let installFee = totalMatCost * EST_INSTALL_RATE;
        let totalBatchCost = totalMatCost + installFee;
        let unitCost = totalBatchCost / prodData.yield;
        
        let breakdown = `M: ${(totalMatCost/prodData.yield).toFixed(0)} / I: ${(installFee/prodData.yield).toFixed(0)}`;
        let note = Array.from(sourceFlags).join("/");
        
        outputRows.push([typeID, typeName, unitCost, breakdown, note, now]);

      } else {
        outputRows.push([typeID, typeName, 0, "", "No Materials (Filtered)", now]);
      }
    } else {
      outputRows.push([typeID, typeName, "N/A", "", "Not Manufacturable", now]);
    }
  }

  // --- 7. Write Result ---
  if (outputRows.length > 0) {
    targetSheet.getRange(2, 1, outputRows.length, HEADERS_OUT.length).setValues(outputRows);
    targetSheet.getRange(2, 3, outputRows.length, 1).setNumberFormat("#,##0.00");
    targetSheet.autoResizeColumns(1, HEADERS_OUT.length);
  }
  
  LOG.info(`Completed. Calculated ${outputRows.length} items.`);
}