
/**
 * TRIGGER-READY WRAPPER
 * Point your timed trigger at this function.
 */
function trigger_generateReprocessedValueTable() {
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
 * UNIVERSAL TYPE ENGINE (The Pantry) - ALIGNED
 */
function getSdeTypeEngine(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("SDE_invTypes");
  const byName = new Map();
  const byId = new Map();
  if (!sheet) return { byName, byId };

  const data = sheet.getDataRange().getValues();
  if (data.length < 2) return { byName, byId }; // Empty sheet check

  const headers = data[0];
  const idIdx = headers.indexOf('typeID');
  const nameIdx = headers.indexOf('typeName');
  const portionIdx = headers.indexOf('portionSize');

  // Safety check: if headers are completely missing, don't crash, just return empty maps
  if (idIdx === -1) {
    console.error("SDE_invTypes is missing the 'typeID' column header.");
    return { byName, byId };
  }

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const id = Number(row[idIdx]);

    // Some older SDE formats might not have portionSize, safely default to 1
    const rawPortion = portionIdx !== -1 ? Number(row[portionIdx]) : 1;
    const portionSize = isNaN(rawPortion) || rawPortion === 0 ? 1 : rawPortion;

    const typeName = nameIdx !== -1 ? String(row[nameIdx]).trim() : `Unknown Item (${id})`;

    if (id > 0) {
      // Changed keys to match the main engine expectations
      const typeObj = { typeID: id, typeName: typeName, portionSize: portionSize };

      byId.set(id, typeObj);
      if (typeName !== "") byName.set(typeName.toLowerCase(), typeObj);
    }
  }
  return { byName, byId };
}

/**
 * REPROCESS CORE ENGINE (Raw Feed Execution - No Pre-stacking)
 */
function processReprocessingCore(ss, rawData, headerRow, options = {}) {
  const { 
    onlyProcessValidBatches = true, 
    onItemProcessed = null 
  } = options;

  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ReproCore') : console;
  const headers = rawData[headerRow].map(h => String(h).trim().toLowerCase());

  const colIdx = {
    typeId: headers.indexOf("type id") > -1 ? headers.indexOf("type id") : headers.indexOf("type_id"),
    qty: headers.indexOf("quantity") > -1 ? headers.indexOf("quantity") : headers.indexOf("qty")
  };

  if (colIdx.typeId === -1 || colIdx.qty === -1) {
    LOG.error("Reprocessing Core: Could not find required 'Type ID' and 'Quantity' columns.");
    return { masterAggregation: new Map(), detailedResults: [] };
  }

  const materialMap = getSdeMaterialMap(ss);
  const typeMap = getSdeTypeEngine(ss).byId;

  const materialIdsToPrice = new Set();
  materialMap.forEach(matList => {
    matList.forEach(m => materialIdsToPrice.add(Number(m.matID)));
  });

  const mineralPriceMap = _getBlendedCostMap(ss, Array.from(materialIdsToPrice), true);
  const costMap = _getBlendedCostMap(ss, Array.from(materialMap.keys()), false);

  const efficiency = 0.50 * (1 + (0.02 * 4)) * (1 - 0.00);

  const masterAggregation = new Map();
  const detailedResults = [];
  let skippedIncomplete = 0;

  for (let i = headerRow + 1; i < rawData.length; i++) {
    const row = rawData[i];
    const typeId = parseInt(row[colIdx.typeId], 10);
    const totalQty = parseInt(row[colIdx.qty], 10) || 0;

    if (!typeId || isNaN(typeId)) continue;

    const typeInfo = typeMap.get(typeId) || {};
    const portionSize = typeInfo.portionSize || 1;

    // Evaluate each row's quantity directly against its portion size
    let batches = 0;
    if (portionSize === 1) {
      batches = totalQty; // Unstackable modules/crystals processed per unit
    } else {
      batches = Math.floor(totalQty / portionSize);
    }

    if (batches <= 0) {
      skippedIncomplete++;
      continue;
    }

    const materials = materialMap.get(typeId);
    if (!materials) continue;

    const costObj = costMap.get(typeId);
    const inputUnitCost = (costObj && typeof costObj.landed === 'number') ? costObj.landed : 0;
    const totalAcqCost = inputUnitCost > 0 ? inputUnitCost * (batches * portionSize) : -1;

    const derivedYields = deriveEffectiveMaterialCosts(materials, efficiency, batches, mineralPriceMap, totalAcqCost / batches);

    const itemResult = {
      typeId: typeId,
      typeName: typeInfo.typeName || `Unknown Item (${typeId})`,
      totalQty: totalQty,
      batches: batches,
      yields: derivedYields,
      totalMeltValue: derivedYields.reduce((sum, y) => sum + (y.yieldQty * (y.marketUnitPrice || 0)), 0),
      marketCost: inputUnitCost * totalQty
    };

    if (onItemProcessed) onItemProcessed(itemResult);
    detailedResults.push(itemResult);

    derivedYields.forEach(y => {
      if (!masterAggregation.has(y.materialID)) masterAggregation.set(y.materialID, { qty: 0, val: 0 });
      const agg = masterAggregation.get(y.materialID);
      agg.qty += y.yieldQty;
      agg.val += (y.yieldQty * (y.effectiveUnitPrice || 0));
    });
  }

  LOG.info(`Processed raw rows. Skipped ${skippedIncomplete} items due to quantity being below portion size.`);

  return { masterAggregation, detailedResults };
}

/**
 * REPROCESS TO LEDGER ENGINE (Aggregated) - Using Core D.R.Y.
 */
function reprocessItemsToLedger(ss) {
  ss = (ss && typeof ss.getSheetByName === 'function') ? ss : SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) return console.error("Could not find active spreadsheet.");
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ReproLedger') : console;

  try {
    if (SpreadsheetApp.getUi().alert('Confirm', 'Process valid items to Ledger?', SpreadsheetApp.getUi().ButtonSet.YES_NO) !== SpreadsheetApp.getUi().Button.YES) return;
  } catch (e) { return; }

  const reproSheet = ss.getSheetByName("Reprocess Items");
  if (!reproSheet) return LOG.error("Missing sheet 'Reprocess Items'.");

  const lastRow = reproSheet.getLastRow();
  const rawData = reproSheet.getRange(1, 1, lastRow, 15).getValues();
  const headerRow = 3;

  const typeMap = getSdeTypeEngine(ss).byId;
  const coreResult = processReprocessingCore(ss, rawData, headerRow);
  const masterAggregation = coreResult.masterAggregation;

  if (masterAggregation.size > 0) {
    const ledgerPayload = [];
    const reproEventId = Utilities.getUuid();
    const timestamp = Utilities.formatDate(new Date(), ss.getSpreadsheetTimeZone(), "yyyy-MM-dd");

    masterAggregation.forEach((data, matID) => {
      ledgerPayload.push({
        date: timestamp,
        type_id: matID,
        item_name: typeMap.has(matID) ? typeMap.get(matID).typeName : `Unknown (${matID})`,
        qty: data.qty,
        unit_value: '',
        source: "REPROCESS",
        contract_id: reproEventId,
        char: "SYSTEM",
        unit_value_filled: data.qty > 0 ? Number(data.val / data.qty) : 0
      });
    });

    const result = ML.forSheet("Material_Ledger").upsert(['date', 'source', 'char', 'contract_id', 'type_id'], ledgerPayload);
    LOG.info(`Aggregated reprocessing complete. Upserted ${result.rows} rows.`);
  } else {
    LOG.info("No valid items to reprocess.");
  }
}

/**
 * Generates the Pending Reprocessing Yield summary table 
 * matching your target schema: type_id, item_name, quantity, price, total value
 */
function generatePendingReprocessingYield(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('PendingYield') : console;

  const reproSheet = ss.getSheetByName("Reprocessing Bin");
  if (!reproSheet) {
    LOG.error("Missing sheet 'Reprocessing Bin'.");
    return;
  }

  const rawData = reproSheet.getDataRange().getValues();
  if (rawData.length <= 1) {
    LOG.info("Reprocessing Bin is empty.");
    return;
  }

  // Run through our flexible core engine with header at row 0 (or adjust if you have header rows)
  const headerRow = 0;
  const coreResult = processReprocessingCore(ss, rawData, headerRow, { onlyProcessValidBatches: false });
  const masterAggregation = coreResult.masterAggregation;

  const typeMap = getSdeTypeEngine(ss).byId;
  const outputRows = [];

  masterAggregation.forEach((data, matID) => {
    const typeInfo = typeMap.get(matID) || {};
    const itemName = typeInfo.typeName || `Unknown Item (${matID})`;
    const qty = data.qty;
    const unitPrice = qty > 0 ? (data.val / qty) : 0;
    const totalValue = qty * unitPrice;

    if (qty > 0) {
      outputRows.push([
        Number(matID),
        itemName,
        qty,
        Number(unitPrice.toFixed(2)),
        Number(totalValue.toFixed(2))
      ]);
    }
  });

  // Sort by total value descending so top high-value minerals appear first
  outputRows.sort((a, b) => b[4] - a[4]);

  const SHEET_NAME = "Pending Reprocessing Yield";
  let outSheet = ss.getSheetByName(SHEET_NAME) || ss.insertSheet(SHEET_NAME);
  outSheet.clearContents();

  const finalPayload = [
    ["type_id", "item_name", "quantity", "price", "total value"],
    ...outputRows
  ];

  if (finalPayload.length > 1) {
    outSheet.getRange(1, 1, finalPayload.length, 5).setValues(finalPayload);
    outSheet.getRange(2, 3, outputRows.length, 1).setNumberFormat("#,##0");
    outSheet.getRange(2, 4, outputRows.length, 2).setNumberFormat("#,##0.00");
    LOG.info(`Pending Reprocessing Yield updated with ${outputRows.length} materials.`);
  } else {
    LOG.info("No materials generated from reprocessing bin.");
  }
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
    // THE FIX: EVE floors the yield PER BATCH, discarding fractional dust, 
    // BEFORE multiplying by the number of batches being melted.
    const yieldQty = Math.floor(mat.qty * efficiency) * batchCount;

    let unitPrice = priceMap.get(Number(mat.matID));
    if (!unitPrice || unitPrice <= 0) {
      unitPrice = 1;
    }

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
  // ADAPTER: Ensure priceMap returns a number, not an object
  // If priceMap stores {raw, landed}, grab .raw or .landed explicitly
  const numericPriceMap = new Map();
  priceMap.forEach((val, key) => {
    // If it's an object, grab the cost basis; if it's already a number, keep it
    numericPriceMap.set(key, (typeof val === 'object' && val.landed) ? val.landed : val);
  });

  const melt = calculateMeltValue(materials, efficiency, batchCount, numericPriceMap);

  if (!melt.totalValue || melt.totalValue <= 0) return [];

  const costRatio = (acquisitionCost <= 0) ? 1.0 : (acquisitionCost / melt.totalValue);

  return melt.yields.map(y => {
    // Ensure y.unitPrice is treated as a number
    const price = (typeof y.unitPrice === 'object' && y.unitPrice.landed) ? y.unitPrice.landed : y.unitPrice;

    return {
      materialID: y.id,
      yieldQty: y.qty,
      marketUnitPrice: price,
      effectiveUnitPrice: price * costRatio
    };
  });
}

/**
 * REPROCESSED VALUE ENGINE - COMPLETE SDE COVERAGE (FIXED)
 */
function generateReprocessedValueTable(ss) {
  const start = new Date().getTime();
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ReproValue') : console;

  // 1. MAPS & SDE DATA
  const materialMap = getSdeMaterialMap(ss);

  // FIX: Safe fallback for type engine if it fails or is missing data
  let typeMap = new Map();
  try {
    const typeEngine = getSdeTypeEngine(ss);
    if (typeEngine && typeEngine.byId) typeMap = typeEngine.byId;
  } catch (e) {
    LOG.warn("getSdeTypeEngine missing or failed. Defaulting to empty names.");
  }

  const allProcessableIds = Array.from(materialMap.keys());

  // 2. PRICING DATA
  const requiredMatIds = new Set();
  allProcessableIds.forEach(id => {
    const recipe = materialMap.get(id);
    if (recipe) recipe.forEach(mat => requiredMatIds.add(Number(mat.matID)));
  });

  // FIX: Safe fallbacks for cost maps so the script doesn't crash if prices are missing
  let mineralPriceMap = new Map();
  let costMap = new Map();
  try {
    mineralPriceMap = _getBlendedCostMap(ss, Array.from(requiredMatIds), true);
    costMap = _getBlendedCostMap(ss, allProcessableIds, false);
  } catch (e) {
    LOG.warn("_getBlendedCostMap missing or failed. Defaulting prices to 0.");
  }

  // To a dynamic check (Example for NPC Station with Scrapmetal IV and 0% Tax):
  const isNpcStation = true; // You can pull this from your Location List
  const scrapmetalLevel = 4;
  const stationTax = 0.00;

  const efficiency = isNpcStation
    ? (0.50 * (1 + (0.02 * scrapmetalLevel)) * (1 - stationTax))
    : (0.50 * 1.69);

  // 3. CORE CALCULATION
  const outputRows = allProcessableIds.reduce((acc, tid) => {
    const materials = materialMap.get(tid);
    // FIX: Fallback to an empty object if typeInfo is missing for this ID
    const typeInfo = typeMap.get(tid) || {};

    // FIX: Removed strict dependency on typeInfo. If it has materials, calculate it!
    if (materials) {
      let meltValue = 0.0;
      const portionSize = typeInfo.portionSize || typeInfo.portion || 1;

      for (const mat of materials) {
        const qtyPerUnit = (mat.qty * efficiency) / portionSize;

        // FIX: Access the .raw or .landed property explicitly
        const priceObj = mineralPriceMap.get(Number(mat.matID));
        const price = (priceObj && typeof priceObj.raw === 'number') ? priceObj.raw : 0.0;

        meltValue += (qtyPerUnit * price);
      }

      // Add this safety check before your acc.push
      const marketCost = (typeof costMap.get(tid) === 'object') ? costMap.get(tid).raw : (costMap.get(tid) || 0.0);
      const profit = meltValue - marketCost;
      const margin = marketCost > 0 ? (profit / marketCost) : 0.0;

      acc.push([
        parseInt(tid),
        typeInfo.typeName || `Unknown Item (${tid})`, // Show ID if name is unknown
        parseFloat(marketCost),
        parseFloat(meltValue),
        parseFloat(profit),
        parseFloat(margin),
        new Date()
      ]);
    }
    return acc;
  }, []);

  // 4. WRITE & RANGE BINDING
  const SHEET_NAME = "Reprocessed_Material_Values";
  let outSheet = ss.getSheetByName(SHEET_NAME) || ss.insertSheet(SHEET_NAME);
  if (outputRows.length === 0) {
    LOG.error("No items processed. Check if SDE_invTypeMaterials has data.");
    return;
  }

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

  // BONUS: Format columns to make it look clean and sort by best Margin
  if (lastRow > 1) {
    outSheet.getRange(2, 3, lastRow - 1, 3).setNumberFormat("#,##0.00");
    outSheet.getRange(2, 6, lastRow - 1, 1).setNumberFormat("0.00%");
    finalRange.sort({ column: 6, ascending: false });
  }

  LOG.info(`Done: ${outputRows.length} items processed from SDE.`);
}

