/**
 * Generates a static table of Projected Manufacturing Costs for items 
 * currently being tracked in MarketOverviewData.
 * Output Sheet: "Projected_Build_Costs"
 */
function generateProjectedCostTable() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // 1. Setup Output Sheet
  const SHEET_NAME = "Projected_Build_Costs";
  let targetSheet = ss.getSheetByName(SHEET_NAME);
  if (!targetSheet) {
    targetSheet = ss.insertSheet(SHEET_NAME);
  }
  targetSheet.clear(); // Wipe clean
  targetSheet.appendRow(["Type ID", "Item Name", "Projected Build Cost", "Last Updated"]); // Headers

  // 2. Load Inputs
  // Source of Truth for what to track: MarketOverviewData (Column A = ID, Column B = Name)
  const marketSheet = ss.getSheetByName("MarketOverviewData");
  const marketData = marketSheet.getRange(3, 1, marketSheet.getLastRow() - 2, 2).getValues(); // Skip header rows
  
  // Cost Data
  const costSheet = ss.getSheetByName("Manufaturing Inputs Effective Cost");
  const costData = costSheet.getDataRange().getValues();
  
  // SDE Data
  const sdeProducts = ss.getSheetByName("SDE_industryActivityProducts").getDataRange().getValues();
  const sdeMaterials = ss.getSheetByName("SDE_industryActivityMaterials").getDataRange().getValues();

  // 3. Build Lookup Maps (Optimize for speed)
  
  // Map: MaterialID -> Cost (Strip " ISK" and parse)
  let materialCostMap = {};
  for (let i = 1; i < costData.length; i++) {
    let matID = costData[i][0];
    let costStr = String(costData[i][1]).replace(/ISK/gi, '').replace(/,/g, '').trim();
    materialCostMap[matID] = parseFloat(costStr) || 0;
  }

  // Map: ProductID -> {bpID, yield}
  let productToBpMap = {};
  for (let i = 1; i < sdeProducts.length; i++) {
    if (sdeProducts[i][1] == 1) { // Manufacture Activity
      productToBpMap[sdeProducts[i][2]] = { bpID: sdeProducts[i][0], yield: sdeProducts[i][3] };
    }
  }

  // Map: BlueprintID -> Array of Materials
  let bpToMatsMap = {};
  for (let i = 1; i < sdeMaterials.length; i++) {
    if (sdeMaterials[i][1] == 1) {
      let bpID = sdeMaterials[i][0];
      if (!bpToMatsMap[bpID]) bpToMatsMap[bpID] = [];
      bpToMatsMap[bpID].push({ matID: sdeMaterials[i][2], qty: sdeMaterials[i][3] });
    }
  }

  // 4. Calculate Costs
  const ME_LEVEL = 10; // Assuming ME 10 for projections
  let outputRows = [];
  let now = new Date();

  // Iterate only through items we are tracking
  for (let i = 0; i < marketData.length; i++) {
    let typeID = marketData[i][0];
    let typeName = marketData[i][1];
    
    if (!typeID) continue;

    // Check if buildable
    if (productToBpMap[typeID]) {
      let bpData = productToBpMap[typeID];
      let materials = bpToMatsMap[bpData.bpID];
      
      if (materials) {
        let totalBatchCost = 0;
        
        materials.forEach(m => {
          // Standard ME Formula: ceil(base * ((100-ME)/100))
          let meMultiplier = (100 - ME_LEVEL) / 100;
          let requiredQty = Math.max(1, Math.ceil(m.qty * meMultiplier));
          
          let matCost = materialCostMap[m.matID] || 0; // Default to 0 if we don't have stock/price
          totalBatchCost += (requiredQty * matCost);
        });

        let unitCost = totalBatchCost / bpData.yield;
        outputRows.push([typeID, typeName, unitCost, now]);
      } else {
        // Buildable but no materials found (strange, but handle it)
        outputRows.push([typeID, typeName, 0, now]); 
      }
    } else {
      // Not a manufactured item (e.g. Faction loot, Minerals)
      outputRows.push([typeID, typeName, "N/A", now]);
    }
  }

  // 5. Write to Table
  if (outputRows.length > 0) {
    targetSheet.getRange(2, 1, outputRows.length, 4).setValues(outputRows);
    
    // Formatting
    targetSheet.getRange(2, 3, outputRows.length, 1).setNumberFormat("#,##0.00");
    targetSheet.autoResizeColumns(1, 4);
  }
  
  Logger.log(`Generated costs for ${outputRows.length} items.`);
}
