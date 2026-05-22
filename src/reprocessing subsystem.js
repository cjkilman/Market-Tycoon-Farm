
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
 * REPROCESS TO LEDGER ENGINE
 * Reads the "Reprocess Items" sheet and commits the material yields to the Material_Ledger via ML.
 */
function reprocessItemsToLedger(ss) {
// Use logical OR (||) and verify the object has the required method
  ss = (ss && typeof ss.getSheetByName === 'function') ? ss : SpreadsheetApp.getActiveSpreadsheet();
  
  if (!ss) {
    console.error("Could not find active spreadsheet.");
    return;
  }
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ReproLedger') : console;


  /** 
   * Reprocess Sheet Layout
   * B5:f Paste Range fromClient Hangers
   * G4 starts Folumas type_id Total Reprocess Value ROI Action
  */

  // --- 1. UI CONFIRMATION POPUP ---
  try {
    const ui = SpreadsheetApp.getUi();
    const response = ui.alert('Confirm Reprocessing', 'Reprocess these items?', ui.ButtonSet.YES_NO);
    if (response !== ui.Button.YES) return;
  } catch (e) {
    return; // Silent Catch
  }

  const reproSheet = ss.getSheetByName("Reprocess Items");
  if (!reproSheet) {
    LOG.error("Missing 'Reprocess Items' sheet.");
    return;
  }

  // 2. LOAD SDE MAPS
  const materialMap = getSdeMaterialMap(ss);
  let typeMap = new Map();
  try {
    const typeEngine = getSdeTypeEngine(ss);
    if (typeEngine && typeEngine.byId) typeMap = typeEngine.byId;
  } catch (e) {
    LOG.warn("getSdeTypeEngine missing or failed.");
  }

  const allProcessableIds = Array.from(materialMap.keys());
  // To a dynamic check (Example for NPC Station with Scrapmetal IV and 0% Tax):
  const isNpcStation = true; // You can pull this from your Location List
  const scrapmetalLevel = 4;
  const stationTax = 0.00;

  const efficiency = isNpcStation
    ? (0.50 * (1 + (0.02 * scrapmetalLevel)) * (1 - stationTax))
    : (0.50 * 1.69);

  // 3. LOAD COST MAPS (Required for unit_value_filled)
  let mineralPriceMap = new Map();
  let costMap = new Map();
  try {
    const requiredMatIds = new Set();
    allProcessableIds.forEach(id => {
      const recipe = materialMap.get(id);
      if (recipe) recipe.forEach(mat => requiredMatIds.add(Number(mat.matID)));
    });

    mineralPriceMap = _getBlendedCostMap(ss, Array.from(requiredMatIds), true);
    costMap = _getBlendedCostMap(ss, allProcessableIds, false); // See Point 2 below!
  } catch (e) {
    LOG.warn("Pricing maps failed to load. Costs will default to 0.");
  }


  // 4. MAP THE REPROCESS SHEET
  const data = reproSheet.getDataRange().getValues();

  // Change this from 2 to 3 (This points to the 4th row in the sheet)
  const headerRow = 3;
  const headers = data[headerRow];

  // Ensure these match your sheet exactly
  const colIdx = {
    action: headers.indexOf("Action"),   // Found at index 9
    typeId: headers.indexOf("type_id"),  // Found at index 6
    qty: headers.indexOf("Qty")         // Found at index 1
  };

  if (colIdx.action === -1 || colIdx.typeId === -1 || colIdx.qty === -1) {
    LOG.error("Could not find required columns on Reprocess Items sheet.");
    return;
  }

  const ledgerPayload = [];
  const rowsToUpdate = [];
  const timestamp = Utilities.formatDate(new Date(), ss.getSpreadsheetTimeZone(), "yyyy-MM-dd");

  // 5. PROCESS EACH ROW
  for (let i = headerRow + 1; i < data.length; i++) {
    const row = data[i];


    const typeId = parseInt(row[colIdx.typeId], 10);
    const totalQty = parseInt(row[colIdx.qty], 10);

    const typeInfo = typeMap.get(typeId) || {};
    const portionSize = typeInfo.portionSize || 1;

    const batches = Math.floor(totalQty / portionSize);

    if (batches > 0) {
      const reproEventId = Utilities.getUuid();
      const materials = materialMap.get(typeId);

      if (materials) {
        // Check the price. If it's 0 or missing, pass a -1 flag.
        const inputUnitCost = costMap.get(typeId) || 0;
        const totalAcquisitionCost = (inputUnitCost > 0)
          ? inputUnitCost * (batches * portionSize)
          : -1;

        // Run your derivation engine to distribute the cost
        const derivedYields = deriveEffectiveMaterialCosts(materials, efficiency, batches, mineralPriceMap, totalAcquisitionCost);

        // Pass the Data onto Material Ledger System for Proper Handling.
        if (derivedYields && derivedYields.length > 0) {
          derivedYields.forEach(yieldData => {
            const matName = typeMap.has(yieldData.materialID) ? typeMap.get(yieldData.materialID).typeName : `Unknown Mat (${yieldData.materialID})`;

            ledgerPayload.push({
              date: timestamp,
              type_id: yieldData.materialID,
              item_name: matName,
              qty: yieldData.yieldQty,
              unit_value: '', // leave blank, reserved for Manual overides
              source: "REPROCESS",
              contract_id: reproEventId,
              char: "SYSTEM",
              unit_value_filled: yieldData.effectiveUnitPrice // Look Upstream for Possible Issues.. there should Always Be a Value here
            });
          });
        }

        rowsToUpdate.push(i + 1);
      } else {
        LOG.warn(`No materials found in SDE for Type ID: ${typeId}`);
      }
    } else {
      LOG.warn(`Skipped ${typeInfo.typeName || typeId} - Not enough quantity to meet the portion size of ${portionSize}.`);
    }

  }

  // 6. ML ENGINE WRITE & UPDATE STATUS
  if (ledgerPayload.length > 0) {
    try {
      const LEDGER_SHEET_NAME = "Material_Ledger";
      const MaterialLedger = ML.forSheet(LEDGER_SHEET_NAME);

      const keys = ['source', 'char', 'contract_id', 'type_id'];
      const result = MaterialLedger.upsert(keys, ledgerPayload);
      /** -Removed. due to Indexing alignment issues and Questional Purpse
       * 
            rowsToUpdate.forEach(rowNum => {
              reproSheet.getRange(rowNum, colIdx.action + 1).setValue("DONE");
            });*/

      LOG.info(`Success: Upserted ${result.rows} rows to ${LEDGER_SHEET_NAME}. Updated ${rowsToUpdate.length} orders.`);

    } catch (e) {
      LOG.error('REPROCESS WRITE FAILED: ' + e.message);
      throw e;
    }
  } else {
    LOG.info("No items marked for REPROCESS were found, or batch quantities were too low.");
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
  // 1. Calculate the actual Yields and the current Market "Melt Value"
  // We use your existing math engine for this part
  const melt = calculateMeltValue(materials, efficiency, batchCount, priceMap);

  if (melt.totalValue === 0) return [];

  // 2. Calculate the Arbitrage Ratio (Cost vs. Value)
  // Example: Paid 1 ISK / Worth 119 ISK = 0.0084 ratio
  // If we don't know what we paid (-1), assume we paid exactly what the scrap is worth (Ratio = 1.0)
  // This prevents 0 ISK minerals from poisoning your manufacturing ledger.
  const costRatio = (acquisitionCost === -1) ? 1.0 : (acquisitionCost / melt.totalValue);

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
    mineralPriceMap = _getBlendedCostMap(ss, Array.from(requiredMatIds),true);
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
        const price = parseFloat(mineralPriceMap.get(Number(mat.matID))) || 0.0;
        meltValue += (qtyPerUnit * price);
      }

      const marketCost = parseFloat(costMap.get(tid)) || 0.0;
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

