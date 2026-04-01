/**
 * TRIGGER-READY WRAPPER
 * Point your timed trigger at this function.
 */
function trigger_generateProjectedCostTable() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  generateProjectedCostTable(ss);
}

/**
 * REPROCESSED VALUE ENGINE - ARRAY OPTIMIZED
 * Logic: Uses _getBlendedCostMap for Tiered Pricing (Hangar -> Market -> API)
 * Output: Reprocessed_Material_Values with Named Range Sync
 */
function generateProjectedCostTable(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ProjectedCost') : console;

  // 1. Setup Data (Replaced Sheet Object with Overview Array)
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  
  // Use the array-returning function directly
  const overviewData = getOverviewData(ss); 
  if (!overviewData || overviewData.length === 0) return;

  // Since getOverviewData starts at the header row, we adjust indices
  // Original overviewData[2] (Row 3) becomes overviewData[0] (Index 0)
  const headers = overviewData[0]; 
  const col = { 
    id: headers.indexOf("type_id"), 
    name: headers.indexOf("Item Name"), 
    group: headers.indexOf("Group") 
  };

  const validTargets = [];
  const allRequiredMatIds = new Set();

  // 2. Pre-Scan: Filter Manufacturing items
  // Starting at index 1 because index 0 is now the headers
  for (let i = 1; i < overviewData.length; i++) {
    const row = overviewData[i];
    if (String(row[col.group] || "").indexOf("Manufacturing") === -1) continue;

    const typeID = Number(row[col.id]);
    const bpInfo = _getBpFromProduct(typeID, sdeProdMap); 
    if (!bpInfo) continue;

    validTargets.push({ typeID, name: row[col.name], bpID: bpInfo.bpID, yield: bpInfo.yield });

    const materials = sdeMatMap.get(bpInfo.bpID);
    if (materials) {
      materials.forEach(m => {
        if (m.activityID === 1) allRequiredMatIds.add(Number(m.materialTypeID));
      });
    }
  }

  // 3. Initialize Cost Engine (Tiered Fallback)
  const costMap = _getBlendedCostMap(ss, Array.from(allRequiredMatIds));

  // 4. Calculation Loop (ME 10 / 5% Install Fee)
  const ME_LEVEL = 10; 
  const EST_INSTALL_RATE = 0.05; 

  const outputRows = validTargets.map(target => {
    const materials = sdeMatMap.get(target.bpID);
    let totalBatchCost = 0;

    materials.forEach(m => {
      if (m.activityID !== 1) return;
      const matID = Number(m.materialTypeID);
      const qty = Math.max(1, Math.ceil(m.quantity * ((100 - ME_LEVEL) / 100)));
      
      const unitCost = costMap.get(matID) || 0;
      totalBatchCost += (qty * unitCost);
    });

    const unitCost = (totalBatchCost * (1 + EST_INSTALL_RATE)) / target.yield;
    return [target.typeID, target.name, unitCost, 0, 0, 0, new Date()];
  });

  // 5. WRITE & COMPACT
  const SHEET_NAME = "Reprocessed_Material_Values";
  let outSheet = ss.getSheetByName(SHEET_NAME) || ss.insertSheet(SHEET_NAME);
  if (outputRows.length === 0) return;

  outSheet.clearContents();
  const finalPayload = [
    ["Type ID", "Item Name", "Market Cost", "Melt Value", "Profit", "Margin %", "Updated"], 
    ...outputRows
  ];
  
  outSheet.getRange(1, 1, finalPayload.length, 7).setValues(finalPayload);

  const lastRow = outSheet.getLastRow();
  const maxRows = outSheet.getMaxRows();
  if (maxRows > lastRow) outSheet.deleteRows(lastRow + 1, maxRows - lastRow);

  const RANGE_NAME = "NR_REPRO_VALUE_TABLE";
  const finalRange = outSheet.getRange(1, 1, lastRow, 7);
  const existing = ss.getNamedRanges().find(r => r.getName() === RANGE_NAME);
  if (existing) {
    existing.setRange(finalRange);
  } else {
    ss.setNamedRange(RANGE_NAME, finalRange);
  }

  LOG.info(`Done: ${outputRows.length} items. Projected Costs updated via Array.`);
}