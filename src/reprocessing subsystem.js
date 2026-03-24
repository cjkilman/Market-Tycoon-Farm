
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
 * REPROCESSING INTERFACE EVALUATOR & COMMITTER
 * Reads a pasted inventory list, evaluates profitability against Blended Costs,
 * and commits to the Material Ledger. Includes detailed execution logging.
 */
function evaluateReprocessingInterface() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const ui = SpreadsheetApp.getUi();
  const log = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('REPRO_EVAL') : console;
  
  log.info("=== Starting Reprocessing Evaluation ===");
  
  // Sheet Names
  const INTERFACE_SHEET = "Preproceeing Interface"; 
  const BLENDED_COST_SHEET = "Blended_Cost";
  const SDE_MAT_SHEET = "SDE_invTypeMaterials";
  const SDE_TYPE_SHEET = "SDE_invTypes";
  const MARKET_SHEET = "Market_Data_Raw";
  
  const sheet = ss.getSheetByName(INTERFACE_SHEET);
  if (!sheet) {
    log.error("CRITICAL: Could not find sheet: " + INTERFACE_SHEET);
    return;
  }
  
  ss.toast("Analyzing pasted inventory...", "Engine Room", 3);

  // 1. Data Lookups
  const rawData = sheet.getDataRange().getValues();
  
  const headerRowIndex = 2;
  const headers = rawData.length > headerRowIndex ? rawData[headerRowIndex] : [];
  
  const itemIdx = headers.indexOf("Item") !== -1 ? headers.indexOf("Item") : 1; 
  const qtyIdx = headers.indexOf("Qty") !== -1 ? headers.indexOf("Qty") : 2;
  const typeIdx = headers.indexOf("Type") !== -1 ? headers.indexOf("Type") : 3;

  log.info(`Column Mapping -> Item: ${itemIdx}, Qty: ${qtyIdx}, Type: ${typeIdx}`);

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

  log.info(`Data Loaded -> Types: ${typeMap.size}, Blended Costs: ${blendedMap.size}, Market Prices: ${marketMap.size}, Materials: ${materialMap.size}`);

  // 2. Jason Kilman's Efficiency Calculation
  let totalEfficiency = 0.50; // Fallback
  try {
    const baseYield = getStructureBaseYield("Domain Industry (Athanor)"); 
    const charMult = getCharacterMeltMultiplier(); 
    totalEfficiency = baseYield * charMult;
    log.info(`Calculated Total Melt Efficiency: ${(totalEfficiency * 100).toFixed(2)}%`);
  } catch(e) {
    log.warn("Using fallback efficiency (50%). Could not load profile.");
  }

  // 3. Process the Pasted Rows
  const OUT_HEADERS = ["Blended Unit Cost", "Total Input Cost", "Scrap Value (Amarr Buy)", "Net Profit", "Margin", "Action"];
  const outData = [];
  const startRow = 3; 
  
  const ledgerEntries = [];
  const batchId = Utilities.getUuid().substring(0, 8);
  const todayStr = new Date().toISOString().split('T')[0];
  
  let processedCount = 0;

  for(let i = startRow; i < rawData.length; i++) {
    const row = rawData[i];
    const qty = parseFloat(String(row[qtyIdx] || "").replace(/[^0-9.-]/g, '')) || 0;

    if (qty <= 0) {
        outData.push(["", "", "", "", "", ""]);
        continue;
    }

    let typeInfo = null;
    let lookupName = "";
    
    // SMART LOOKUP
    if (typeIdx !== -1 && String(row[typeIdx]).trim() !== "") {
        lookupName = String(row[typeIdx]).trim();
        typeInfo = typeMap.get(lookupName.toLowerCase());
    }
    if (!typeInfo) {
        lookupName = String(row[itemIdx] || "").trim();
        typeInfo = typeMap.get(lookupName.toLowerCase());
    }

    if(!typeInfo) {
         log.warn(`Row ${i + 1}: Unrecognized Item -> "${lookupName}"`);
         outData.push(["Unrecognized Item", "", "", "", "", ""]);
         continue;
    }

    processedCount++;
    const typeId = typeInfo.id;
    const portionSize = typeInfo.portion;
    const batchCount = qty / portionSize;

    const blendedUnitCost = blendedMap.get(typeId) || 0;
    const totalInputCost = blendedUnitCost * qty;

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
           unit_value: unitPrice, 
           source: "REPROCESSING",
           contract_id: `REPRO-${batchId}-${typeId}`, 
           char: "Jason Kilman"
         });
       }
    });

    const profit = totalScrapValue - totalInputCost;
    let margin = 0;
    if (totalInputCost > 0) margin = profit / totalInputCost;

    let action = "SELL RAW (LOSS)";
    if (totalInputCost === 0 && totalScrapValue > 0) {
       action = "REPROCESS (FREE LOOT)";
       ledgerEntries.push(...currentItemLedgerPayload);
    } else if (profit > 0) {
       action = "REPROCESS (PROFITABLE)";
       ledgerEntries.push(...currentItemLedgerPayload);
    }

    log.info(`Row ${i + 1}: [${lookupName}] Qty: ${qty} | InCost: ${totalInputCost.toFixed(2)} | ScrapVal: ${totalScrapValue.toFixed(2)} | Profit: ${profit.toFixed(2)} | Action: ${action}`);

    outData.push([blendedUnitCost, totalInputCost, totalScrapValue, profit, margin, action]);
  }

  log.info(`Evaluation Loop Complete. Processed ${processedCount} valid items. Ledger entries queued: ${ledgerEntries.length}`);

  // 4. Output to Sheet
  const targetCol = 9; 
  const headerRow = 3; 
  const dataRow = 4;   
  
  const numRowsToClear = sheet.getMaxRows() - headerRow + 1;
  if (numRowsToClear > 0) {
     sheet.getRange(headerRow, targetCol, numRowsToClear, OUT_HEADERS.length).clearContent();
  }

  sheet.getRange(headerRow, targetCol, 1, OUT_HEADERS.length).setValues([OUT_HEADERS]).setFontWeight("bold").setBackground("#f3f3f3");
  
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
      `Evaluation complete.\n\nFound ${ledgerEntries.length} profitable mineral yields.\nDo you want to permanently commit these to the Material Ledger?`, 
      ui.ButtonSet.YES_NO
    );
    
    if (response == ui.Button.YES) {
      log.info("User selected YES to commit to ledger.");
      ss.toast("Committing to Material Ledger...", "Engine Room", 5);
      
      if (typeof ML !== 'undefined') {
        ML.forSheet("Material_Ledger").upsertBy(['contract_id', 'type_id'], ledgerEntries);
        log.info("Successfully upserted data to Material Ledger via ML Engine.");
        ss.toast(`Successfully committed ${ledgerEntries.length} yields to Blended Cost.`, "Engine Room", 5);
      } else {
        log.error("CRITICAL: Material Ledger (ML) class not found during commit.");
        ui.alert('Error: Material Ledger script not found.');
      }
    } else {
      log.info("User selected NO. Ledger commit cancelled.");
      ss.toast("Evaluation only. Ledger was NOT updated.", "Engine Room", 3);
    }
  } else {
    log.info("No profitable items found. Bypassing ledger commit prompt.");
    ss.toast("Evaluation Complete! No profitable items to commit.", "Engine Room", 3);
  }
  
  log.info("=== Reprocessing Evaluation Finished ===");
}