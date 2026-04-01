
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
 * REPROCESSED VALUE ENGINE - COMPLETE SDE COVERAGE
 */
function generateReprocessedValueTable(ss) {
  const start = new Date().getTime();
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ReproValue') : console;

  // 1. MAPS & SDE DATA
  const materialMap = getSdeMaterialMap(ss); // Map of all typeIDs to their material components
  const { byId: typeMap } = getSdeTypeEngine(ss); // Map of typeIDs to basic info (Name, Portion)

  // Get all IDs that have a material recipe in the SDE
  const allProcessableIds = Array.from(materialMap.keys());

  // 2. PRICING DATA
  // Identify all unique minerals/materials needed for ALL recipes
  const requiredMatIds = new Set();
  allProcessableIds.forEach(id => {
    const recipe = materialMap.get(id);
    if (recipe) recipe.forEach(mat => requiredMatIds.add(Number(mat.matID)));
  });

  // Get prices for minerals (to calculate Melt Value) and items (to calculate ROI)
  const mineralPriceMap = _getBlendedCostMap(ss, Array.from(requiredMatIds)); 
  const costMap = _getBlendedCostMap(ss, allProcessableIds); 
  
  // Set efficiency (pull from sheet if possible, otherwise hardcoded)
  const efficiency = 0.50 * 1.69; 

  // 3. CORE CALCULATION - Process every meltable item in the game
  const outputRows = allProcessableIds.reduce((acc, tid) => {
    const materials = materialMap.get(tid);
    const typeInfo = typeMap.get(tid);
    
    if (materials && typeInfo) {
      let meltValue = 0.0;
      const portionSize = typeInfo.portionSize || typeInfo.portion || 1;
      const batchCount = 1.0 / portionSize;

      for (const mat of materials) {
        // Floor the qty per portion as per EVE mechanics
        const qty = Math.floor(mat.qty * efficiency * batchCount);
        const price = parseFloat(mineralPriceMap.get(Number(mat.matID))) || 0.0;
        meltValue += (qty * price);
      }

      const marketCost = parseFloat(costMap.get(tid)) || 0.0;
      const profit = meltValue - marketCost;
      const margin = marketCost > 0 ? (profit / marketCost) : 0.0;

      acc.push([
        tid,
        typeInfo.typeName || "Unknown Item",
        marketCost,
        meltValue,
        profit,
        margin,
        new Date()
      ]);
    }
    return acc;
  }, []);

  // 4. WRITE & RANGE BINDING
  const SHEET_NAME = "Reprocessed_Material_Values";
  let outSheet = ss.getSheetByName(SHEET_NAME) || ss.insertSheet(SHEET_NAME);
  if (outputRows.length === 0) return;

  outSheet.clearContents();
  const finalPayload = [["Type ID", "Item Name", "Market Cost", "Melt Value", "Profit", "Margin %", "Updated"], ...outputRows];
  
  outSheet.getRange(1, 1, finalPayload.length, 7).setValues(finalPayload);

  // Clean up sheet length
  const lastRow = outSheet.getLastRow();
  const maxRows = outSheet.getMaxRows();
  if (maxRows > lastRow) outSheet.deleteRows(lastRow + 1, maxRows - lastRow);

  // Update Named Range
  const RANGE_NAME = "NR_REPRO_VALUE_TABLE";
  const finalRange = outSheet.getRange(1, 1, lastRow, 7);
  const existing = ss.getNamedRanges().find(r => r.getName() === RANGE_NAME);
  if (existing) existing.setRange(finalRange); else ss.setNamedRange(RANGE_NAME, finalRange);

  LOG.info(`Done: ${outputRows.length} items processed from SDE.`);
}

