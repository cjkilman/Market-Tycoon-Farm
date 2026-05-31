/**
 * TRIGGER-READY WRAPPER
 * Point your timed trigger at this function.
 */
function trigger_generateProjectedCostTable() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  generateProjectedCostTable(ss);
}

/**
 * 🏭 PROJECTED MANUFACTURING COSTS ENGINE
 * Computes exact item assembly unit costs using dynamic SDE blueprint structures
 * paired with live tiered inventory valuations (Blended Costs -> Market -> API).
 */
function generateProjectedCostTable(ss) {
  ss = (ss && typeof ss.getSheetByName === 'function') ? ss : SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) {
    console.error("Could not find active spreadsheet anchor.");
    return;
  }
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ProjectedCost') : console;

  // 1. Load SDE Maps and Master Dashboard Matrix
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  const overviewData = getOverviewData(ss); 
  if (!overviewData || overviewData.length === 0) return;

  const headers = overviewData[0]; 
  const col = { 
    id: headers.indexOf("type_id"), 
    name: headers.indexOf("Item Name"), 
    group: headers.indexOf("Group") 
  };

  if (col.id === -1 || col.group === -1) {
    console.error("Critical failure: Mandatory header index matching aborted.");
    return;
  }

  const validTargets = [];
  const allRequiredMatIds = new Set();

 // =================================================================
  // 2. Pre-Scan Phase: Isolate items where Group contains "Manufacturing"
  // =================================================================
  for (let i = 1; i < overviewData.length; i++) {
    const row = overviewData[i];
    const groupStr = String(row[col.group] || "").toLowerCase().trim();
    
    // Generalized keyword check: catches "Manufacturing", "Manufacturing Fuel Block", etc.
    if (groupStr.indexOf("manufacturing") === -1) continue;

    const typeID = Number(row[col.id]);
    if (!typeID || isNaN(typeID)) continue;

    const bpInfo = _getBpFromProduct(typeID, sdeProdMap); 
    if (!bpInfo) continue;

    validTargets.push({ 
      typeID: typeID, 
      name: row[col.name], 
      bpID: bpInfo.bpID, 
      yield: Number(bpInfo.yield) || 1 
    });

    const materials = sdeMatMap.get(bpInfo.bpID);
    if (materials) {
      materials.forEach(m => {
        if (m.activityID === 1) allRequiredMatIds.add(Number(m.materialTypeID));
      });
    }
  }

  // 3. Initialize Cost Map utilizing your Tiered Hangar/Market Ledger
  const costMap = _getBlendedCostMap(ss, Array.from(allRequiredMatIds));

  // Global industrial system parameters
  const ME_LEVEL = 10; 
  const EST_INSTALL_RATE = 0.05; 

  // 4. Core Mathematical Assembly Evaluation Loop
  const outputRows = validTargets.map(target => {
    const materials = sdeMatMap.get(target.bpID);
    let totalBatchCost = 0;

    if (!materials) return [target.typeID, target.name, 0, "No SDE Materials", new Date()];

    materials.forEach(m => {
      if (m.activityID !== 1) return; // Process only raw manufacturing inputs
      const matID = Number(m.materialTypeID);
      
      // Compute standard industry blueprint material conservation adjustments
      const qty = Math.max(1, Math.ceil(m.quantity * ((100 - ME_LEVEL) / 100)));
      const unitCost = costMap.get(matID) || 0;
      
      totalBatchCost += (qty * unitCost);
    });

    // Divide final batch footprint evenly by your portion yields (Essential for 40-count Fuel Blocks)
    const unitCost = (totalBatchCost * (1 + EST_INSTALL_RATE)) / target.yield;
    return [target.typeID, target.name, unitCost, "Calculated", new Date()];
  });

  // 5. Memory-Safe Pre-Allocated Output Management
  const SHEET_NAME = "Projected_Build_Costs";
  let outSheet = ss.getSheetByName(SHEET_NAME) || ss.insertSheet(SHEET_NAME);
  if (outputRows.length === 0) return;

  const finalPayload = [
    ["Type ID", "Item Name", "Cost", "Source Tier", "Updated"], 
    ...outputRows
  ];
  
  const requiredRows = finalPayload.length;
  const currentMaxRows = outSheet.getMaxRows();

  // Reset text parameters without corrupting active sheet dimensions
  outSheet.clearContents();

  // Enforce structural scaling safety gates BEFORE popping matrix lines down to cells
  if (requiredRows > currentMaxRows) {
    outSheet.insertRowsAfter(currentMaxRows, requiredRows - currentMaxRows);
  }

  outSheet.getRange(1, 1, requiredRows, finalPayload[0].length).setValues(finalPayload);

  // Clean trailing deadspace safely without risking null indexing anomalies
  const postWriteMaxRows = outSheet.getMaxRows();
  if (postWriteMaxRows > requiredRows) {
    outSheet.deleteRows(requiredRows + 1, postWriteMaxRows - requiredRows);
  }

  // 6. Synchronize Named Range For Sub-Calculators
  const RANGE_NAME = "NR_PROJECTED_BUILD_COSTS";
  const finalRange = outSheet.getRange(1, 1, requiredRows, finalPayload[0].length);
  const existingRange = ss.getNamedRanges().find(r => r.getName() === RANGE_NAME);
  
  if (existingRange) {
    existingRange.setRange(finalRange);
  } else {
    ss.setNamedRange(RANGE_NAME, finalRange);
  }

  LOG.info(`Done: ${outputRows.length} items. Projected manufacturing costs updated seamlessly via Array.`);
}