
/**
 * ARRAY-COMPATIBLE REPROCESSING ENGINE
 */
function getReprocessValue(typeIDs, stationYield, playerSkill) {
  // If typeIDs is a single value, wrap it in an array to treat everything as a range
  if (!Array.isArray(typeIDs)) {
    typeIDs = [[typeIDs]];
  }

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const matData = ss.getSheetByName("SDE_invTypeMaterials").getDataRange().getValues();
  const typeData = ss.getSheetByName("SDE_invTypes").getDataRange().getValues();
  const priceData = ss.getSheetByName("Market_Data_Raw").getDataRange().getValues();

  // Create Maps for O(1) lookup speed
  const priceMap = new Map(priceData.map(r => [r[1], r[5]])); // type_id -> buy_max
  const portionMap = new Map(typeData.map(r => [r[0], r[6] || 1])); // type_id -> portionSize

  // Pre-filter materials into a Map of arrays for speed
  const materialMap = new Map();
  matData.forEach(r => {
    if (!materialMap.has(r[0])) materialMap.set(r[0], []);
    materialMap.get(r[0]).push({ matID: r[1], qty: r[2] });
  });

  // Process the range
  return typeIDs.map(row => {
    const typeID = row[0];
    if (!typeID) return "";

    const materials = materialMap.get(typeID) || [];
    const portionSize = portionMap.get(typeID) || 1;
    let totalBatchIsk = 0;

    materials.forEach(mat => {
      const unitPrice = priceMap.get(mat.matID) || 0;
      totalBatchIsk += (mat.qty * stationYield * playerSkill * unitPrice);
    });

    return totalBatchIsk / portionSize;
  });
}


// Get the total skill/implant multiplier for Jason Kilman
function getCharacterMeltMultiplier() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const profile = ss.getSheetByName("Character_Profile").getDataRange().getValues();
  
  // Basic math: (Repro * Repro Eff * Specific Skill * Implant)
  // These indices assume your CSV structure: Name (0), Level (1), Multiplier (2)
  let totalMult = 1.0;
  for (let i = 1; i < profile.length; i++) {
    totalMult *= parseFloat(profile[i][2]);
  }
  return totalMult;
}

// Get the base yield for a specific location
function getStructureBaseYield(locationNickname) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
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
  const priceSheet = ss.getSheetByName("Market_Data_Raw");

  if (!hangerSheet || !sdeMatSheet || !sdeTypeSheet) return;

  const hangerData = hangerSheet.getDataRange().getValues().slice(1);
  const sdeMatData = sdeMatSheet.getDataRange().getValues();
  const sdeTypeData = sdeTypeSheet.getDataRange().getValues();
  const priceMap = new Map(priceSheet.getDataRange().getValues().map(r => [Number(r[1]), Number(r[5])]));
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

    let totalMeltValue = 0;
    materials.forEach(mat => {
      const yieldQty = Math.floor(mat.qty * totalEfficiency * batchCount);
      const unitPrice = priceMap.get(mat.id) || 0;
      totalMeltValue += (yieldQty * unitPrice);
      
      // Store for requirements link
      projectedMinerals[mat.id] = (projectedMinerals[mat.id] || 0) + yieldQty;
    });

    const marketValue = (priceMap.get(typeID) || 0) * qty;
    const action = (totalMeltValue < (marketValue * 0.8)) ? "!! LOSS WARNING !!" : "REPROCESS";
    hangerActions.push([action]);
  });

  if (hangerActions.length > 0) hangerSheet.getRange(2, 6, hangerActions.length, 1).setValues(hangerActions);
  
  // Cache the results so the Consolidator can see them
  PropertiesService.getScriptProperties().setProperty('PROJECTED_SCRAP_MINERALS', JSON.stringify(projectedMinerals));
}