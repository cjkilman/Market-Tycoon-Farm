
/**
 * TRIGGER-READY WRAPPER
 * Point your timed trigger at this function.
 */
function trigger_generateReprocessedNalueTable() {
  // Use getActiveSpreadsheet() for bound scripts
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // Call your main logic
  generateReprocessedValueTable(ss);
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
 * CALCULATE MELT VALUE (Asset Floor Logic)
 * Logic: Calculates the total market ISK value of an item's reprocessed materials.
 * Handles EVE's "round down" yield mechanics and normalizes output to a single unit.
 *
 * @param {Array} materials - SDE material array [{matID, qty}, ...]
 * @param {number} efficiency - Net reprocessing yield (e.g., 0.5 * 1.69)
 * @param {number} batchCount - Processing multiplier (1 / portionSize)
 * @param {Map} priceMap - Reference prices for minerals (e.g., Amarr Buy)
 * @returns {Object} - {totalValue: number, yields: Array}
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
 * DERIVE EFFECTIVE MATERIAL COSTS
 * Logic: Takes what you PAID for an item and distributes that cost 
 * across the resulting minerals based on their relative market value.
 * * @param {Array} materials - SDE material array [{matID, qty}, ...]
 * @param {number} efficiency - Your repro efficiency (e.g., 0.5 * 1.69)
 * @param {number} batchCount - 1 / portionSize
 * @param {Map} priceMap - Current Amarr Buy prices for minerals
 * @param {number} acquisitionCost - What you actually paid for the item (e.g., 1 ISK)
 */
function deriveEffectiveMaterialCosts(materials, efficiency, batchCount, priceMap, acquisitionCost) {
  // 1. Calculate the actual Yields and the current Market "Melt Value"
  // We use your existing math engine for this part
  const melt = calculateMeltValue(materials, efficiency, batchCount, priceMap);

  if (melt.totalValue === 0) return [];

  // 2. Calculate the Arbitrage Ratio (Cost vs. Value)
  // Example: Paid 1 ISK / Worth 119 ISK = 0.0084 ratio
  const costRatio = acquisitionCost / melt.totalValue;

  // 3. Distribute the cost across each mineral yield
  return melt.yields.map(y => {
    return {
      materialID: y.id,
      yieldQty: y.qty,
      marketUnitPrice: y.unitPrice,
      // THE END GAME: This is your actual ISK cost for this specific mineral
      effectiveUnitPrice: y.unitPrice * costRatio
    };
  });
}

/**
 * REPROCESSED VALUE ENGINE
 * Logic: Uses _getBlendedCostMap for Tiered Pricing (Hangar -> Market -> API)
 * Output: Reprocessed_Material_Values with Named Range Sync
 */
function generateProjectedCostTable(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ProjectedCost') : console;

  // 1. Setup Data & SDE Maps
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  const overviewSheet = ss.getSheetByName("MarketOverviewData");
  const overviewData = overviewSheet.getDataRange().getValues();
  const headers = overviewData[2]; 
  const col = { 
    id: headers.indexOf("type_id"), 
    name: headers.indexOf("Item Name"), 
    group: headers.indexOf("Group") 
  };

  const validTargets = [];
  const allRequiredMatIds = new Set();

  // 2. Pre-Scan: Filter Manufacturing items and collect Mat IDs
  for (let i = 3; i < overviewData.length; i++) {
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
    
    // Formatting for the 7-column header you defined
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

  // THE COMPACTOR: Kill extra rows
  const lastRow = outSheet.getLastRow();
  const maxRows = outSheet.getMaxRows();
  if (maxRows > lastRow) outSheet.deleteRows(lastRow + 1, maxRows - lastRow);

  // NAMED RANGE SYNC: Keep the target range tight
  const RANGE_NAME = "NR_REPRO_VALUE_TABLE";
  const finalRange = outSheet.getRange(1, 1, lastRow, 7);
  const existing = ss.getNamedRanges().find(r => r.getName() === RANGE_NAME);
  if (existing) {
    existing.setRange(finalRange);
  } else {
    ss.setNamedRange(RANGE_NAME, finalRange);
  }

  LOG.info(`Done: ${outputRows.length} items. Cost mapped from Blended Cost.`);
}

