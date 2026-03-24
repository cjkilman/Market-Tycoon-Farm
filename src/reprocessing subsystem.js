
/**
 * Standalone Trigger for Hangar Auditing.
 * Set this to run on its own timer (e.g., every 30 or 60 minutes)
 * to keep the Industry Ledger fast.
 */
function scheduledReprocessingAudit() {
  const log = LoggerEx.withTag('HANGAR_AUDIT');
  log.info('--- Starting Dedicated Reprocessing Audit ---');

  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    runReprocessingAudit(ss);
    log.info('Reprocessing Audit Complete.');
  } catch (e) {
    log.error('Reprocessing Audit FAILED: ' + e.message);
  }
}


/**
 * UNIVERSAL BLENDED DATA ENGINE
 * Builds a high-speed, in-memory Map of item values (Costs or Sales) from a Named Range.
 * Automatically sanitizes string inputs into raw numbers for accurate margin math.
 *
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss - The active Google Spreadsheet object.
 * @param {string} rangeName - The target Named Range (e.g., "NR_BLENDED_COST" or "NR_BLENDED_SALES").
 * @returns {Map<number, number>} A Map where Key = TypeID (Number) and Value = Blended Price (Number).
 */
function getBlendedMapFor(ss, rangeName) {
  const map = new Map();

  // Fail-safe: if no range name is passed, bail out
  if (!rangeName) {
    console.warn("getBlendedMapFor: No rangeName provided.");
    return map;
  }

  try {
    const range = ss.getRangeByName(rangeName);

    if (!range) {
      console.warn("getBlendedMapFor: Could not find Named Range [" + rangeName + "].");
      return map;
    }

    const data = range.getValues();
    if (data.length < 1) {
      console.warn("getBlendedMapFor: Data source [" + rangeName + "] is empty.");
      return map;
    }

    for (let i = 0; i < data.length; i++) {
      // Assuming Column A (index 0) is TypeID and Column C (index 2) is the Value
      const typeId = Number(data[i][0]);
      const val = parseFloat(String(data[i][2]).replace(/[^0-9.-]/g, "")) || 0;

      if (typeId > 0) {
        map.set(typeId, val);
      }
    }
  } catch (e) {
    console.error("CRITICAL ERROR in getBlendedMapFor (" + rangeName + "): " + e.message + " at " + e.stack);
  }

  return map;
}

/**
 * ARRAY-COMPATIBLE REPROCESSING ENGINE
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss - The active spreadsheet.
 * @param {number|number[][]} typeIDs - Single ID or Range of IDs.
 * @param {number} stationYield - Base yield (e.g., 0.50 or 0.54).
 * @param {number} playerSkill - Character multiplier (e.g., 1.15).
 * @returns {number[]} Array of ISK values per portion.
 */
function getReprocessValue(ss, typeIDs, stationYield, playerSkill) {
  // 1. Handle input flexibility
  if (!Array.isArray(typeIDs)) {
    typeIDs = [[typeIDs]];
  }

  // 2. USE THE FERRARI ENGINE (DRY Utility)
  // This replaces 15 lines of manual sheet grabbing and looping
  const priceMap = getMarketPriceMapFor(ss, 'buy_max');

  // 3. Optimized Material Lookups
  const matData = ss.getSheetByName("SDE_invTypeMaterials").getDataRange().getValues();
  const typeData = ss.getSheetByName("SDE_invTypes").getDataRange().getValues();

  const portionMap = new Map(typeData.map(r => [Number(r[0]), Number(r[6]) || 1]));

  const materialMap = new Map();
  matData.forEach(r => {
    const pId = Number(r[0]);
    if (!materialMap.has(pId)) materialMap.set(pId, []);
    materialMap.get(pId).push({ matID: Number(r[1]), qty: Number(r[2]) });
  });

  // 4. Process the range with zero-latency lookups
  return typeIDs.map(row => {
    const typeID = Number(row[0]);
    if (!typeID) return 0;

    const materials = materialMap.get(typeID) || [];
    const portionSize = portionMap.get(typeID) || 1;
    let totalBatchIsk = 0;

    materials.forEach(mat => {
      const unitPrice = priceMap.get(Number(mat.matID)) || 0;
      totalBatchIsk += (mat.qty * stationYield * playerSkill * unitPrice);
    });

    return totalBatchIsk / portionSize;
  });
}


// Get the base yield for a specific location
function getStructureBaseYield(locationNickname,ss) {
  if(!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const settings = ss.getSheetByName("Structure_Settings").getDataRange().getValues();

  const match = settings.find(row => row[0] === locationNickname);
  return match ? parseFloat(match[2]) : 0.50; // Default to 50% NPC station
}

/**
 * REPROCESSING FORENSIC AUDIT
 * Calculates mineral yields and flags losses in the MiningHanger.
 */
function runReprocessingAudit(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const hangerSheet = ss.getSheetByName("MiningHanger");
  const sdeMatSheet = ss.getSheetByName("SDE_invTypeMaterials");
  const sdeTypeSheet = ss.getSheetByName("SDE_invTypes");

  if (!hangerSheet || !sdeMatSheet || !sdeTypeSheet) return;

  const hangerData = hangerSheet.getDataRange().getValues().slice(1);
  const sdeMatData = sdeMatSheet.getDataRange().getValues();
  const sdeTypeData = sdeTypeSheet.getDataRange().getValues();
  const priceMap = getMarketPriceMapFor(ss, 'buy_max');
  const portionMap = new Map(sdeTypeData.map(r => [Number(r[0]), Number(r[6]) || 1]));

  // Get Jason Kilman's Max Efficiency (L5 + RX-810)
  // Assuming 1.69 total multiplier based on previous settings
  const totalEfficiency = 0.50 * 1.69;

  const matYieldMap = new Map();
  sdeMatData.forEach(r => {
    const parentID = Number(r[0]);
    if (!matYieldMap.has(parentID)) matYieldMap.set(parentID, []);
    matYieldMap.get(parentID).push({ id: Number(r[1]), qty: Number(r[2]) });
  });

  const projectedMinerals = {};
  const hangerActions = [];

  hangerData.forEach(row => {
    const typeID = Number(row[1]);
    const qty = Number(row[4]);
    if (!typeID || qty <= 0) return;

    const materials = matYieldMap.get(typeID) || [];
    const portionSize = portionMap.get(typeID) || 1;
    const batchCount = qty / portionSize;

    // Use the engine
    const melt = calculateMeltValue(materials, totalEfficiency, batchCount, priceMap);

    // Update projections for the Consolidator
    melt.yields.forEach(item => {
      projectedMinerals[item.id] = (projectedMinerals[item.id] || 0) + item.qty;
    });

    const marketValue = (priceMap.get(typeID) || 0) * qty;
    const action = (melt.totalValue < (marketValue * 0.8)) ? "!! LOSS WARNING !!" : "REPROCESS";
    hangerActions.push([action]);
  });

  if (hangerActions.length > 0) hangerSheet.getRange(2, 6, hangerActions.length, 1).setValues(hangerActions);

  // Cache the results so the Consolidator can see them
  PropertiesService.getScriptProperties().setProperty('PROJECTED_SCRAP_MINERALS', JSON.stringify(projectedMinerals));
}

// Get the total skill/implant multiplier for Jason Kilman (Loot/Modules Only)
function getCharacterMeltMultiplier() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const profile = ss.getSheetByName("Character_Profile").getDataRange().getValues();

  let totalMult = 1.0;

  // Whitelist: ONLY apply skills/implants relevant to melting scrapmetal
  const activeScrapSkills = [
    "Reprocessing",
    "Reprocessing Efficiency",
    "Scrapmetal Processing",
    "Zainou 'Beancounter' RX-810"
  ];

  for (let i = 1; i < profile.length; i++) {
    const skillName = String(profile[i][0]).trim();
    const val = parseFloat(profile[i][2]);

    // Safety check: Only multiply if it's on the whitelist
    if (activeScrapSkills.includes(skillName) && !isNaN(val) && val > 0) {
      totalMult *= val;
    }
  }
  return totalMult;
}

/**
 * CORE REPROCESSING MATH ENGINE (DRY)
 * Calculates the final ISK value for a single item's mineral yield.
 * * @param {Array} materials - Array of {matID, qty} from SDE.
 * @param {number} efficiency - Total multiplier (Base * Skills * Implants).
 * @param {number} batchCount - (Total Qty / Portion Size).
 * @param {Map} priceMap - Map of Material ID -> Price.
 * @returns {Object} {totalValue: number, yields: Array<{id, qty, value}>}
 */
function calculateMeltValue(materials, efficiency, batchCount, priceMap) {
  let totalValue = 0;
  const yieldDetails = [];

  materials.forEach(mat => {
    // EVE Math: Math.floor is the standard for final output quantities
    const yieldQty = Math.floor(mat.qty * efficiency * batchCount);
    const unitPrice = priceMap.get(Number(mat.matID)) || 0;
    const matValue = yieldQty * unitPrice;

    totalValue += matValue;

    if (yieldQty > 0) {
      yieldDetails.push({
        id: mat.matID,
        qty: yieldQty,
        value: matValue,
        unitPrice: unitPrice
      });
    }
  });

  return {
    totalValue: totalValue,
    yields: yieldDetails
  };
}

/**
 * GROUP CATEGORY MAPPING
 * Maps GroupID -> CategoryID so the math engine knows what it's melting.
 */
function getGroupCategoryMap(ss) {
  const sheet = ss.getSheetByName("SDE_invGroups");
  const map = new Map();
  if (!sheet) return map;

  const data = sheet.getDataRange().getValues();
  // Assume Column A (0) is groupID, Column B (1) is categoryID
  for (let i = 1; i < data.length; i++) {
    map.set(Number(data[i][0]), Number(data[i][1]));
  }
  return map;
}

/**
 * MASTER SDE SEARCH ENGINE
 * Returns a Map keyed by lowercase Name for instant text-based lookups.
 * Includes ID and Portion Size for math operations.
 */
function getSdeSearchMap(ss) {
  const sheet = ss.getSheetByName("SDE_invTypes");
  const map = new Map();
  if (!sheet) return map;

  const data = sheet.getDataRange().getValues();
  const headers = data[0];
  
  const idIdx = headers.indexOf('typeID');
  const nameIdx = headers.indexOf('typeName');
  const portionIdx = headers.indexOf('portionSize');
  const groupIdx = headers.indexOf('groupID');

  for (let i = 1; i < data.length; i++) {
    const name = String(data[i][nameIdx]).trim().toLowerCase();
    const id = Number(data[i][idIdx]);
    const portion = Number(data[i][portionIdx]) || 1;
    const groupId = Number(data[i][groupIdx]);

    if (id > 0 && name !== "") {
      map.set(name, { id, portion, groupId });
    }
  }
  return map;
}

/**
 * ORE SKILL ENGINE
 * Maps GroupIDs to specific Processing Skills from the Character Profile.
 * Category 25 (Asteroids) and 42 (Moon Ore).
 */
function getOreSkillMultiplier(groupId,ss) {
  if(!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const profile = ss.getSheetByName("Character_Profile").getDataRange().getValues();
  
  // Mapping Groups to Skill Names
  const skillMap = {
    462: "Veldspar Processing",
    460: "Scordite Processing",
    459: "Pyroxeres Processing",
    461: "Plagioclase Processing",
    467: "Kernite Processing",
    468: "Omber Processing",
    469: "Jaspet Processing",
    470: "Hemorphite Processing",
    465: "Ice Processing", // Technically Category 465
    // Add Moon Ore groups as needed
  };

  const targetSkill = skillMap[groupId] || "Reprocessing Efficiency";
  
  // Base Character Mults
  let reprocessing = 1.0;
  let efficiency = 1.0;
  let specificSkill = 1.0;
  let implant = 1.0;

  for (let i = 1; i < profile.length; i++) {
    const name = String(profile[i][0]).trim();
    const val = parseFloat(profile[i][2]);
    if (isNaN(val)) continue;

    if (name === "Reprocessing") reprocessing = val;
    if (name === "Reprocessing Efficiency") efficiency = val;
    if (name === targetSkill) specificSkill = val;
    if (name === "Zainou 'Beancounter' RX-810") implant = val;
  }

  // Ore Formula: (Skill1 * Skill2 * Skill3 * Implant)
  return reprocessing * efficiency * specificSkill * implant;
}

/**
 * REPROCESSING INTERFACE EVALUATOR & COMMITTER
 * Reads a pasted inventory list, evaluates profitability against Blended Costs,
 * and commits to the Material Ledger.
 */
function evaluateReprocessingInterface() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const ui = SpreadsheetApp.getUi();
  const log = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('REPRO_EVAL') : console;

  log.info("=== Starting Reprocessing Evaluation ===");

  const INTERFACE_SHEET = "Preproceeing Interface";
  const SDE_MAT_SHEET = "SDE_invTypeMaterials";

  const sheet = ss.getSheetByName(INTERFACE_SHEET);
  if (!sheet) {
    log.error("CRITICAL: Could not find sheet: " + INTERFACE_SHEET);
    return;
  }

  // --- UI TWEAK: CAPTURE STRUCTURE FROM C3 ---
  const structureName = sheet.getRange("C3").getValue();
  log.info(`Target Structure from C3: ${structureName}`);
  ss.toast(`Analyzing inventory for ${structureName}...`, "Engine Room", 3);

  // 1. Data Lookups & Engine Initialization
  const rawData = sheet.getDataRange().getValues();
  const typeMap = getSdeSearchMap(ss);
  const blendedMap = getBlendedMapFor(ss, "NR_BLENDED_COST");
  const marketMap = getMarketPriceMapFor(ss, 'buy_max');
  const groupToCatMap = getGroupCategoryMap(ss);

  const matData = ss.getSheetByName(SDE_MAT_SHEET).getDataRange().getValues();
  const materialMap = new Map();
  matData.forEach(r => {
    const pId = Number(r[0]);
    if (!materialMap.has(pId)) materialMap.set(pId, []);
    materialMap.get(pId).push({ matID: Number(r[1]), qty: Number(r[2]) });
  });

  // 2. Efficiency Calculation (SCRAPMETAL TRACK)
  let scrapEfficiency = 0.50; 
  try {
    const baseYield = getStructureBaseYield(structureName,ss);
    const charMult = getCharacterMeltMultiplier();
    scrapEfficiency = baseYield * charMult;
   log.info(`Max Scrapmetal Efficiency: ${(scrapEfficiency * 100).toFixed(2)}% (Hard-capped at 50% base)`);
  } catch (e) {
    log.warn(`Lookup failed for [${structureName}]. Using 50% fallback.`);
  }

  // 3. Process the Pasted Rows
  const OUT_HEADERS = ["Blended Unit Cost", "Total Input Cost", "Scrap Value (Amarr Buy)", "Net Profit", "Margin", "Action"];
  const outData = [];
  const headerRowIndex = 3; 
  const startRow = 4; 

  const ledgerEntries = [];
  const batchId = Utilities.getUuid().substring(0, 8);
  const todayStr = new Date().toISOString().split('T')[0];

  const headers = rawData.length > headerRowIndex ? rawData[headerRowIndex] : [];
  const itemIdx = headers.indexOf("Item") !== -1 ? headers.indexOf("Item") : 1;
  const qtyIdx = headers.indexOf("Qty") !== -1 ? headers.indexOf("Qty") : 2;
  const typeIdx = headers.indexOf("Type") !== -1 ? headers.indexOf("Type") : 3;

  for (let i = startRow; i < rawData.length; i++) {
    const row = rawData[i];
    const qty = parseFloat(String(row[qtyIdx] || "").replace(/[^0-9.-]/g, '')) || 0;

    if (qty <= 0) {
      outData.push(["", "", "", "", "", ""]);
      continue;
    }

    let lookupName = (String(row[itemIdx]).trim() || String(row[typeIdx]).trim()).toLowerCase();
    let typeInfo = typeMap.get(lookupName);

    if (!typeInfo) {
      log.warn(`Row ${i + 1}: Unrecognized Item -> "${lookupName}"`);
      outData.push(["Unrecognized Item", "", "", "", "", ""]);
      continue;
    }

    const typeId = typeInfo.id;
    const batchCount = qty / typeInfo.portion;

    const blendedUnitCost = blendedMap.get(typeId) || 0;
    const totalInputCost = blendedUnitCost * qty;

    // --- FIX: DEFINE MATERIALS BEFORE RUNNING THE ENGINE ---
    const materials = materialMap.get(typeId) || []; 
    const catId = groupToCatMap.get(typeInfo.groupId);
    let activeEfficiency = 0.50;

    if (catId === 25 || catId === 42) {
      // ORE TRACK: Uses the Structure Rig Bonus (C3) + Ore Skills
      const baseYield = getStructureBaseYield(structureName, ss); 
      activeEfficiency = baseYield * getOreSkillMultiplier(typeInfo.groupId, ss); 
    } else {
      // SCRAPMETAL TRACK: Hard-coded 50% Base (82.2% max)
      activeEfficiency = 0.50 * getCharacterMeltMultiplier(); 
    }

    // RUN THE ENGINE
    const melt = calculateMeltValue(materials, activeEfficiency, batchCount, marketMap);
    const totalScrapValue = melt.totalValue;
    const currentItemLedgerPayload = [];

    melt.yields.forEach(item => {
      currentItemLedgerPayload.push({
        date: todayStr,
        type_id: item.id,
        qty: item.qty,
        unit_value: item.unitPrice,
        source: "REPROCESSING",
        contract_id: `REPRO-${batchId}-${typeId}`,
        char: "Jason Kilman"
      });
    });

    const profit = totalScrapValue - totalInputCost;
    let margin = totalInputCost > 0 ? profit / totalInputCost : 0;

    let action = "SELL RAW (LOSS)";
    if (totalInputCost === 0 && totalScrapValue > 0) {
      action = "REPROCESS (FREE LOOT)";
      ledgerEntries.push(...currentItemLedgerPayload);
    } else if (profit > 0) {
      action = "REPROCESS (PROFITABLE)";
      ledgerEntries.push(...currentItemLedgerPayload);
    }

    outData.push([blendedUnitCost, totalInputCost, totalScrapValue, profit, margin, action]);
  }

  // 4. Output to Sheet
  const targetCol = 9;
  const headerRow = 4;
  const dataRow = 5;

  const numRowsToClear = sheet.getMaxRows() - headerRow + 1;
  if (numRowsToClear > 0) {
    sheet.getRange(headerRow, targetCol, numRowsToClear, OUT_HEADERS.length).clearContent();
  }

  sheet.getRange(headerRow, targetCol, 1, OUT_HEADERS.length)
       .setValues([OUT_HEADERS]).setFontWeight("bold").setBackground("#f3f3f3");

  if (outData.length > 0) {
    const dataRange = sheet.getRange(dataRow, targetCol, outData.length, OUT_HEADERS.length);
    dataRange.setValues(outData);
    sheet.getRange(dataRow, targetCol, outData.length, 4).setNumberFormat("#,##0.00 [$ISK]");
    sheet.getRange(dataRow, targetCol + 4, outData.length, 1).setNumberFormat("0.00%");
  }

  // 5. THE LEDGER COMMIT
  if (ledgerEntries.length > 0) {
    const response = ui.alert(
      'Commit to Ledger?',
      `Evaluation complete for ${structureName}.\n\nFound ${ledgerEntries.length} profitable yields.\nCommit to Material Ledger?`,
      ui.ButtonSet.YES_NO
    );

    if (response == ui.Button.YES) {
      if (typeof ML !== 'undefined') {
        ML.forSheet("Material_Ledger").upsertBy(['contract_id', 'type_id'], ledgerEntries);
        ss.toast(`Committed ${ledgerEntries.length} yields to Ledger.`, "Success", 5);
      }
    }
  }

  log.info("=== Reprocessing Evaluation Finished ===");
}