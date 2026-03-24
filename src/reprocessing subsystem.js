
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

/**
 * REPROCESSING INTERFACE EVALUATOR & COMMITTER
 * Reads a pasted inventory list, evaluates profitability against Blended Costs,
 * and commits the resulting minerals to the Material Ledger.
 */
function evaluateReprocessingInterface() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const ui = SpreadsheetApp.getUi();
  const log = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('REPRO_EVAL') : console;
  
  // Sheet Names
  const INTERFACE_SHEET = "Preproceeing Interface"; 
  const BLENDED_COST_SHEET = "Blended_Cost";
  const SDE_MAT_SHEET = "SDE_invTypeMaterials";
  const SDE_TYPE_SHEET = "SDE_invTypes";
  const MARKET_SHEET = "Market_Data_Raw";
  
  const sheet = ss.getSheetByName(INTERFACE_SHEET);
  if (!sheet) {
    log.error("Could not find sheet: " + INTERFACE_SHEET);
    return;
  }
  
  ss.toast("Analyzing pasted inventory...", "Engine Room", 3);

  // 1. Data Lookups
  const rawData = sheet.getDataRange().getValues();
  const headers = rawData[1]; // Row 2 contains "Item, Qty, Type..."
  const itemIdx = headers.indexOf("Item");
  const qtyIdx = headers.indexOf("Qty");

  // Type Map
  const typeData = ss.getSheetByName(SDE_TYPE_SHEET).getDataRange().getValues();
  const typeMap = new Map();
  typeData.forEach(r => {
    typeMap.set(String(r[2]).trim().toLowerCase(), { id: Number(r[0]), portion: Number(r[6]) || 1 });
  });

  // Blended Cost Map (Input Value)
  const blendedData = ss.getSheetByName(BLENDED_COST_SHEET).getDataRange().getValues();
  const blendedMap = new Map();
  blendedData.forEach(r => {
    const cost = parseFloat(String(r[2]).replace(/[^0-9.-]/g, '')) || 0;
    blendedMap.set(Number(r[0]), cost);
  });

  // Market Map (Output Value - Amarr Median Buy)
  const marketData = ss.getSheetByName(MARKET_SHEET).getDataRange().getValues();
  const marketMap = new Map();
  marketData.forEach(r => {
    marketMap.set(Number(r[1]), Number(r[5])); // buy_max
  });

  // Reprocess Materials Map
  const matData = ss.getSheetByName(SDE_MAT_SHEET).getDataRange().getValues();
  const materialMap = new Map();
  matData.forEach(r => {
    const pId = Number(r[0]);
    if (!materialMap.has(pId)) materialMap.set(pId, []);
    materialMap.get(pId).push({ matID: Number(r[1]), qty: Number(r[2]) });
  });

  // 2. Jason Kilman's Efficiency Calculation
  let totalEfficiency = 0.50; // Fallback
  try {
    const baseYield = getStructureBaseYield("Domain Industry (Athanor)"); 
    const charMult = getCharacterMeltMultiplier(); 
    totalEfficiency = baseYield * charMult;
  } catch(e) {
    log.warn("Using fallback efficiency. Could not load profile.");
  }

  // 3. Process the Pasted Rows
  const OUT_HEADERS = ["Blended Unit Cost", "Total Input Cost", "Scrap Value (Amarr Buy)", "Net Profit", "Margin", "Action"];
  const outData = [];
  const startRow = 2; // Data starts on Row 3 (index 2)
  
  // LEDGER PREPARATION
  const ledgerEntries = [];
  const batchId = Utilities.getUuid().substring(0, 8);
  const todayStr = new Date().toISOString().split('T')[0];

  for(let i = startRow; i < rawData.length; i++) {
    const row = rawData[i];
    const itemName = String(row[itemIdx] || "").trim();
    const qty = parseFloat(String(row[qtyIdx] || "").replace(/[^0-9.-]/g, '')) || 0;

    if (!itemName || qty <= 0) {
        outData.push(["", "", "", "", "", ""]);
        continue;
    }

    const typeInfo = typeMap.get(itemName.toLowerCase());
    if(!typeInfo) {
         outData.push(["Unrecognized Item", "", "", "", "", ""]);
         continue;
    }

    const typeId = typeInfo.id;
    const portionSize = typeInfo.portion;
    const batchCount = qty / portionSize;

    // Determine Input Costs
    const blendedUnitCost = blendedMap.get(typeId) || 0;
    const totalInputCost = blendedUnitCost * qty;

    // Determine Output Value & Prepare Ledger Data
    const materials = materialMap.get(typeId) || [];
    let totalScrapValue = 0;
    const currentItemLedgerPayload = [];

    materials.forEach(mat => {
       const yieldQty = Math.floor(mat.qty * totalEfficiency * batchCount);
       const unitPrice = marketMap.get(mat.matID) || 0;
       totalScrapValue += (yieldQty * unitPrice);
       
       if (yieldQty > 0) {
         currentItemLedgerPayload.push({
           date: todayStr,
           type_id: mat.matID,
           qty: yieldQty,
           unit_value: unitPrice, // Opportunity Cost scaling
           source: "REPROCESSING",
           contract_id: `REPRO-${batchId}-${typeId}`, // Unique ID binds the output to the source item
           char: "Jason Kilman"
         });
       }
    });

    const profit = totalScrapValue - totalInputCost;
    let margin = 0;
    if (totalInputCost > 0) margin = profit / totalInputCost;

    // Logic Gate
    let action = "SELL RAW (LOSS)";
    if (totalInputCost === 0 && totalScrapValue > 0) {
       action = "REPROCESS (FREE LOOT)";
       ledgerEntries.push(...currentItemLedgerPayload); // Queue for Ledger
    } else if (profit > 0) {
       action = "REPROCESS (PROFITABLE)";
       ledgerEntries.push(...currentItemLedgerPayload); // Queue for Ledger
    }

    outData.push([blendedUnitCost, totalInputCost, totalScrapValue, profit, margin, action]);
  }

  // 4. Output to Sheet (Writes to Columns G through L)
  const targetCol = headers.length + 1; // Dynamically find the first empty column after paste
  
  const maxRows = Math.max(sheet.getMaxRows(), 3);
  if (maxRows >= 3) {
     sheet.getRange(2, targetCol, maxRows, OUT_HEADERS.length).clearContent();
  }

  sheet.getRange(2, targetCol, 1, OUT_HEADERS.length).setValues([OUT_HEADERS]).setFontWeight("bold").setBackground("#f3f3f3");
  
  if (outData.length > 0) {
    const dataRange = sheet.getRange(3, targetCol, outData.length, OUT_HEADERS.length);
    dataRange.setValues(outData);
    sheet.getRange(3, targetCol, outData.length, 4).setNumberFormat("#,##0.00 [$ISK]");
    sheet.getRange(3, targetCol + 4, outData.length, 1).setNumberFormat("0.00%");
  }
  
  // 5. THE LEDGER COMMIT
  if (ledgerEntries.length > 0) {
    const response = ui.alert(
      'Commit to Ledger?', 
      `Evaluation complete.\n\nFound ${ledgerEntries.length} profitable mineral yields.\nDo you want to permanently commit these to the Material Ledger?`, 
      ui.ButtonSet.YES_NO
    );
    
    if (response == ui.Button.YES) {
      ss.toast("Committing to Material Ledger...", "Engine Room", 5);
      
      // Ensure ML is defined (from MaterialLedger.js)
      if (typeof ML !== 'undefined') {
        ML.forSheet("Material_Ledger").upsertBy(['contract_id', 'type_id'], ledgerEntries);
        ss.toast(`✅ Successfully committed ${ledgerEntries.length} yields to Blended Cost.`, "Engine Room", 5);
      } else {
        log.error("Material Ledger (ML) class not found. Ensure MaterialLedger.js is loaded.");
        ui.alert('Error: Material Ledger script not found.');
      }
    } else {
      ss.toast("Evaluation only. Ledger was NOT updated.", "Engine Room", 3);
    }
  } else {
    ss.toast("Evaluation Complete! No profitable items to commit.", "Engine Room", 3);
  }
}