
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

  // A. Initialization (Do this once at the start of your function)
  const { characterSkills, skillMap } = getGlobalContext(ss);
  const currentStation = getActiveStationConfig(ss);



  // 3. LOAD COST MAPS
  let mineralPriceMap = new Map();
  let costMap = new Map();
  try {
    const requiredMatIds = new Set();
    allProcessableIds.forEach(id => {
      const recipe = materialMap.get(id);
      if (recipe) recipe.forEach(mat => requiredMatIds.add(Number(mat.matID)));
    });

    mineralPriceMap = _getBlendedCostMap(ss, Array.from(requiredMatIds), true);
    costMap = _getBlendedCostMap(ss, allProcessableIds, false);
  } catch (e) {
    LOG.warn("Pricing maps failed to load. Costs will default to 0.");
  }

  // 4. MAP THE REPROCESS SHEET
  const data = reproSheet.getDataRange().getValues();
  const headerRow = 3;
  const headers = data[headerRow];

  const colIdx = {
    action: headers.indexOf("Action"),
    typeId: headers.indexOf("type_id"),
    qty: headers.indexOf("Qty")
  };

  if (colIdx.action === -1 || colIdx.typeId === -1 || colIdx.qty === -1) {
    LOG.error("Could not find required columns on Reprocess Items sheet.");
    return;
  }

  const ledgerPayload = [];
  const rowsToUpdate = [];
  const timestamp = Utilities.formatDate(new Date(), ss.getSpreadsheetTimeZone(), "yyyy-MM-dd-HHmmss");

  // NEW: The O(1) Accumulator and Single Deterministic Batch ID
  const batchAccumulator = new Map();
  const batchEventId = "REPRO-" + timestamp;

 // 5. PROCESS EACH ROW
  for (let i = headerRow + 1; i < data.length; i++) {
    const row = data[i];

    // --- MOVE LOGIC INTO THE LOOP ---
    // Now 'row' is defined, so these calls will work perfectly
    const typeId = parseInt(row[colIdx.typeId], 10);
    const totalQty = parseInt(row[colIdx.qty], 10);
    
    // Station config and efficiency need 'typeId' from the current row
    const efficiency = getReprocessEfficiency(currentStation, characterSkills, typeId, skillMap);
    // --------------------------------

    // SAFETY SHIELD: Drop blank rows or cross-wired string pastes immediately
    if (isNaN(typeId) || isNaN(totalQty) || totalQty <= 0) continue;

    const typeInfo = typeMap.get(typeId) || {};
    const portionSize = typeInfo.portionSize || 1;
    const batches = Math.floor(totalQty / portionSize);

    if (batches > 0) {
      const materials = materialMap.get(typeId);

      if (materials) {
        const inputUnitCost = costMap.get(typeId) || 0;
        const totalAcquisitionCost = (inputUnitCost > 0)
          ? inputUnitCost * (batches * portionSize)
          : -1;

        const derivedYields = deriveEffectiveMaterialCosts(materials, efficiency, batches, mineralPriceMap, totalAcquisitionCost);

        if (derivedYields && derivedYields.length > 0) {
          // NEW: Accumulate yields in memory instead of pushing directly to ledger
          derivedYields.forEach(yieldData => {
            const matId = yieldData.materialID;
            const matName = typeMap.has(matId) ? typeMap.get(matId).typeName : `Unknown Mat (${matId})`;

            if (!batchAccumulator.has(matId)) {
              batchAccumulator.set(matId, { qty: 0, totalCost: 0, name: matName });
            }

            const current = batchAccumulator.get(matId);
            current.qty += yieldData.yieldQty;
            current.totalCost += (yieldData.effectiveUnitPrice * yieldData.yieldQty);
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

  // 5.5 CONVERT ACCUMULATOR TO PAYLOAD
  batchAccumulator.forEach((data, matId) => {
    // Aggressive trim: Kill any ghost rows before they hit the ledger
    if (data.qty <= 0) return;

    // Calculate the weighted average cost for the entire melt session
    const batchWeightedCost = data.totalCost / data.qty;

    ledgerPayload.push({
      date: timestamp,
      type_id: matId,
      item_name: data.name,
      qty: data.qty,
      unit_value: '', // leave blank, reserved for Manual overrides
      source: "REPROCESS",
      contract_id: batchEventId,
      char: "SYSTEM",
      unit_value_filled: batchWeightedCost
    });
  });

  // 6. ML ENGINE WRITE & UPDATE STATUS
  if (ledgerPayload.length > 0) {
    try {
      const LEDGER_SHEET_NAME = "Material_Ledger";
      const MaterialLedger = ML.forSheet(LEDGER_SHEET_NAME);

      const keys = ['source', 'char', 'contract_id', 'type_id'];

      // One single, perfectly clean upsert call for the entire session
      const result = MaterialLedger.upsert(keys, ledgerPayload);

      LOG.info(`Success: Upserted ${result.rows} stacked rows to ${LEDGER_SHEET_NAME}. Processed ${rowsToUpdate.length} input stacks.`);

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
 * Handles EVE's yield mechanics by calculating total mass before flooring.
 *
 * @param {Array} materials - SDE material array [{matID, qty}, ...]
 * @param {number} efficiency - Net reprocessing yield (e.g., 0.5 * 1.69)
 * @param {number} batchCount - Total processable batches
 * @param {Map} priceMap - Reference prices for minerals
 * @returns {Object} - {totalValue: number, yields: Array}
 */
function calculateMeltValue(materials, efficiency, batchCount, priceMap) {
  let totalValue = 0;
  const yieldDetails = [];

  materials.forEach(mat => {
    // EVE multiplies total quantity by base yield and efficiency BEFORE flooring.
    const yieldQty = Math.floor(mat.qty * batchCount * efficiency);

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

function getGlobalContext(ss) {
  const skillSheet = ss.getSheetByName("Character_Profile");
  const skillData = skillSheet.getDataRange().getValues();
  const characterSkills = {};
  
  for (let i = 1; i < skillData.length; i++) {
    // Stores: { "Reprocessing": { level: 5, multiplier: 1.15 } }
    characterSkills[skillData[i][0]] = {
      level: Number(skillData[i][1]),
      mult: Number(skillData[i][2])
    };
  }

  const mapSheet = ss.getSheetByName("SDE_SkillMap");
  const mapData = mapSheet.getDataRange().getValues();
  const skillMap = new Map();
  for (let i = 1; i < mapData.length; i++) {
    skillMap.set(Number(mapData[i][0]), mapData[i][1]);
  }

  return { characterSkills, skillMap };
}

function getReprocessEfficiency(station, skills, typeID, skillMap) {
  // 1. Identify if this item is Scrapmetal
  // If the skill required is specifically "Scrapmetal Processing", we are in "Scrap Mode"
  const requiredSkillName = skillMap.get(typeID) || "Scrapmetal Processing";
  const isScrap = (requiredSkillName === "Scrapmetal Processing");
  
  // 2. Fetch skill and base variables
  const skill = skills[requiredSkillName] || { level: 0, mult: 1.0 };
  const repro = skills['Reprocessing'] || { level: 0, mult: 1.0 };
  const eff = skills['Reprocessing Efficiency'] || { level: 0, mult: 1.0 };

  // 3. APPLY EVE REPROCESSING MODES
  if (isScrap) {
    // SCRAP MODE: Structure base yield is IGNORED. 
    // Hard cap is 54% (0.54) with Scrapmetal V (1.08x)
    // Formula: Base 0.50 * Scrapmetal Skill Multiplier
    return (0.50 * skill.mult);
  } else {
    // ORE/ICE/MOON MODE: Uses the structure's base yield
    const baseYield = (station.type === 'NPC') ? 0.50 : station.baseYield;
    return (baseYield * repro.mult * eff.mult * skill.mult);
  }
}


/**
 * DYNAMIC STATION LOADER (Named Range Version)
 * @param {string} rangeName - The Named Range pointing to the selected station name
 */
function getActiveStationConfig(ss) {
  const range = ss.getRangeByName("NR_RepoStation");
  if (!range) {
    console.error("Named Range NR_RepoStation not found!");
    return { type: 'NPC', baseYield: 0.50 };
  }

  const stationName = range.getValue();
  const sheet = ss.getSheetByName("Structure_Settings");
  const data = sheet.getDataRange().getValues();

  // Find the row that matches the stationName in the first column
  const row = data.find(r => r[0] === stationName);

  if (!row) return { type: 'NPC', baseYield: 0.50 };

  // Corrected return logic
  return {
    // Check column 1 (Structure Type) for the string
    type: row[1] === 'NPC Station' ? 'NPC' : 'STRUCTURE',
    // Check column 2 (Base Yield) for the number
    baseYield: Number(row[2]) || 0.50
  };
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
 * ♻️ REPROCESSED VALUE ENGINE - STREAMLINED CUSTOM SCHEMA
 * Computes asset floor melt values based on character efficiency parameters.
 */
function generateReprocessedValueTable(ss) {
  const start = new Date().getTime();
  ss = (ss && typeof ss.getSheetByName === 'function') ? ss : SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) return;
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ReproValue') : console;

  // 1. EXTRACT DATA & CONTEXT (Executed Once)
  const { characterSkills, skillMap } = getGlobalContext(ss);
  const materialMap = getSdeMaterialMap(ss);

  let typeMap = new Map();
  try {
    const typeEngine = getSdeTypeEngine(ss);
    if (typeEngine && typeEngine.byId) typeMap = typeEngine.byId;
  } catch (e) {
    LOG.warn("getSdeTypeEngine uninitialized. Defaulting to fallback labels.");
  }

  const allProcessableIds = Array.from(materialMap.keys());

  // 2. COMPILE PRICING
  let mineralPriceMap = new Map();
  let costMap = new Map();
  try {
    const requiredMatIds = new Set();
    allProcessableIds.forEach(id => {
      const recipe = materialMap.get(id);
      if (recipe) recipe.forEach(mat => requiredMatIds.add(Number(mat.matID)));
    });

    mineralPriceMap = _getBlendedCostMap(ss, Array.from(requiredMatIds), true);
    costMap = _getBlendedCostMap(ss, allProcessableIds, false);
  } catch (e) {
    LOG.warn("_getBlendedCostMap lookup failed. Initializing baseline prices to 0.");
  }

  // 3. CORE CALCULATION LOOP
  const currentStation = getActiveStationConfig(ss);

  const outputRows = allProcessableIds.reduce((acc, tid) => {
    const materials = materialMap.get(tid);
    const typeInfo = typeMap.get(tid) || {};

    if (materials) {
      // Get the perfect dynamic efficiency for THIS specific item ID
     const efficiency = getReprocessEfficiency(currentStation, characterSkills, tid, skillMap);

      let singleUnitMeltValue = 0.0;
      for (const mat of materials) {
        const qtyPerUnit = Number(mat.qty || 0) * efficiency;
        const price = parseFloat(mineralPriceMap.get(Number(mat.matID))) || 0.0;
        singleUnitMeltValue += (qtyPerUnit * price);
      }

      const marketCost = parseFloat(costMap.get(tid)) || 0.0;
      const profit = singleUnitMeltValue - marketCost;
      const margin = marketCost > 0 ? (profit / marketCost) : 0.0;

      acc.push([
        parseInt(tid, 10),
        typeInfo.typeName || `Unknown Item (${tid})`,
        marketCost,
        singleUnitMeltValue,
        profit,
        margin,
        new Date()
      ]);
    }
    return acc;
  }, []);

  // 4. WRITE BACK TO SHEET (Standardized block)
  const SHEET_NAME = "Reprocessed_Material_Values";
  let outSheet = ss.getSheetByName(SHEET_NAME) || ss.insertSheet(SHEET_NAME);

  if (outputRows.length === 0) {
    LOG.error("Write aborted: Zero processed output lines generated.");
    return;
  }

  const finalPayload = [
    ["Type ID", "Item Name", "Market Cost", "Melt Value", "Profit", "Margin %", "Updated"],
    ...outputRows
  ];

  outSheet.clearContents();
  outSheet.getRange(1, 1, finalPayload.length, 7).setValues(finalPayload);

  // Format & Sort
  outSheet.getRange(2, 1, finalPayload.length - 1, 1).setNumberFormat("0");
  outSheet.getRange(2, 3, finalPayload.length - 1, 3).setNumberFormat("#,##0.00");
  outSheet.getRange(2, 6, finalPayload.length - 1, 1).setNumberFormat("0.00%");
  outSheet.getRange(2, 1, finalPayload.length - 1, 7).sort({ column: 6, ascending: false });

  LOG.info(`Done: ${outputRows.length} items processed.`);
}
