/**
 * TRIGGER-READY WRAPPER
 * Point your timed trigger at this function.
 */
function trigger_generateProjectedCostTable() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  generateProjectedCostTable(ss);
}

function repairBpcWacCache() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  // Query the Material Ledger for all Invention and Copying entries
  const ledgerData = ML.forSheet("Material_Ledger", ss).query({
    source: ["INVENTION", "COPYING"]
  });

  const wacMap = {};

  // Aggregate weighted average costs per blueprint type ID
  ledgerData.forEach(r => {
    const typeId = Number(r.type_id);
    const qty = Number(r.qty) || 1;
    const totalVal = Number(r.total_value) || (Number(r.unit_value_filled) * qty) || 0;

    if (typeId > 0 && totalVal > 0) {
      if (!wacMap[typeId]) {
        wacMap[typeId] = { totalCost: 0, totalQty: 0 };
      }
      wacMap[typeId].totalCost += totalVal;
      wacMap[typeId].totalQty += qty;
    }
  });

  const finalWacData = {};
  for (const [typeId, data] of Object.entries(wacMap)) {
    finalWacData[typeId] = data.totalCost / data.totalQty;
  }

  // Save directly to the exact script property key the projection engine looks for
  SCRIPT_PROP.setProperty(BPC_WAC_KEY, JSON.stringify(finalWacData));
  console.log(`[REPAIR SUCCESS] Loaded WAC data for ${Object.keys(finalWacData).length} blueprints into ${BPC_WAC_KEY}.`);
}

function SNIPER_TRACE_EM_RIG() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const log = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('SNIPER') : Logger;
  
  const TARGET_BP_ID = 2206; // Hobgoblin II
  const TARGET_PRODUCT_ID = 2205; 

  const { sdeMatMap } = _getSdeMaps(ss);
  const costMap = _getBlendedCostMap(ss);
  const nameMap = _getSdeNameMap(ss);
  const bpcWacData = JSON.parse(PropertiesService.getScriptProperties().getProperty(BPC_WAC_KEY) || '{}');
  
  const configMap = _getMasterBlueprintConfig(ss);
  const waterfallStatsMap = _buildWaterfallBlueprintStatsMap([TARGET_BP_ID], configMap);
  const waterfallStats = waterfallStatsMap.get(TARGET_BP_ID);
  
  const materials = sdeMatMap.get(TARGET_BP_ID) || [];
  
  log.log(`\n\n========== SNIPER TRACE: SMALL EM SHIELD REINFORCER II ==========`);
  log.log(`1. AMORTIZATION (BPC COST)`);
  log.log(`   - WAC Cache Memory  : ${bpcWacData[TARGET_BP_ID] || 0} ISK`);
  log.log(`   - Waterfall History : ${waterfallStats ? waterfallStats.avgCost : 0} ISK`);
  
  log.log(`\n2. MATERIAL BREAKDOWN (ME: ${waterfallStats ? waterfallStats.me : 0})`);
  let totalMatCost = 0;
  
  materials.forEach(m => {
    if (m.activityID !== 1) return;
    const matName = nameMap.get(m.materialTypeID) || m.materialTypeID;
    const baseQty = Number(m.quantity);
    const unitPrice = (costMap.get(m.materialTypeID) || { landed: 0 }).landed;
    const lineCost = baseQty * unitPrice;
    totalMatCost += lineCost;
    
    log.log(`   - [${matName}] Qty: ${baseQty} | Unit Price: ${unitPrice.toLocaleString(undefined, {minimumFractionDigits: 2})} ISK | Line Cost: ${lineCost.toLocaleString(undefined, {minimumFractionDigits: 2})} ISK`);
  });
  
  log.log(`\n3. TOTALS`);
  log.log(`   - Raw Material Sum : ${totalMatCost.toLocaleString(undefined, {minimumFractionDigits: 2})} ISK`);
  log.log(`=================================================================\n\n`);
}

/**
 * PROJECTED MANUFACTURING COSTS ENGINE
 * Computes assembly unit costs by feeding data directly from getOverviewData()
 * UPGRADE: Now uses True EVE Online Projected Taxes via System Indexes.
 * UPGRADE: API calls safely hoisted outside the execution loop to prevent timeouts.
 */
function generateProjectedCostTable(ss) {
  ss = (ss && typeof ss.getSheetByName === 'function') ? ss : SpreadsheetApp.getActiveSpreadsheet();
  if (!ss) return;

  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ProjectedCost') : console;
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  // =================================================================
  // 1. Load SDE Maps & Master Config
  // =================================================================
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  const overviewData = getOverviewData(ss);
  if (!overviewData || overviewData.length < 2) return;

  const configMap = _getMasterBlueprintConfig(ss);

  const headers = overviewData[0];
  const col = {
    id: headers.map(h => String(h).toLowerCase().trim()).indexOf("type_id"),
    name: headers.map(h => String(h).toLowerCase().trim()).indexOf("item name")
  };

  if (col.id === -1) {
    console.error("Critical failure: Mandatory 'type_id' header missing from overview data.");
    return;
  }

  const validTargets = [];
  const allRequiredMatIds = new Set();

  // INVERT SDE PRODUCT MAP FOR FAST O(1) LOOKUPS
  const invertedProductMap = new Map();
  for (const [compositeKey, prodObj] of sdeProdMap.entries()) {
    const parts = String(compositeKey).split(':');
    const actID = parts.length > 1 ? Number(parts[0]) : 1;
    const bpID = parts.length > 1 ? Number(parts[1]) : Number(parts[0]);

    if (actID === 1) {
      invertedProductMap.set(Number(prodObj.productTypeID), {
        bpID: bpID,
        yield: Number(prodObj.quantity) || 1
      });
    }
  }

  // =================================================================
  // 2. Pre-Scan Phase
  // =================================================================
  for (let i = 1; i < overviewData.length; i++) {
    const row = overviewData[i];
    const typeID = Number(row[col.id]);
    if (!typeID || isNaN(typeID)) continue;

    const bpInfo = invertedProductMap.get(typeID);
    if (!bpInfo) continue;

    validTargets.push({
      typeID: typeID,
      name: col.name !== -1 ? row[col.name] : `Item ${typeID}`,
      bpID: bpInfo.bpID,
      yield: bpInfo.yield
    });

    const materials = sdeMatMap.get(bpInfo.bpID);
    if (materials) {
      materials.forEach(m => {
        if (m.activityID === 1) allRequiredMatIds.add(Number(m.materialTypeID));
      });
    }
  }

  // =================================================================
  // 3. Initialize Cost, Amortization, & True Tax Maps (API SAFE ZONE)
  // =================================================================
  const costMap = _getBlendedCostMap(ss, Array.from(allRequiredMatIds));
  const amortMap = _getBpoAmortizationMap(ss);
  const internalBpcMap = _buildInternalBpcMap_(ss);
  const bpcWacData = JSON.parse(SCRIPT_PROP.getProperty(BPC_WAC_KEY) || '{}');

  // --- THE TRUE TAX ENGINE MAPS ---
  const sdeBasePriceMap = _getSdeBasePriceMap(ss);
  const systemIndexMap = _getSystemCostIndexMap(ss);
  const targetSystemId = _getNamedOr_(ss, 'setting_production_system', 30002187); // Default: Amarr
  
  const bpoAttributesMap = _getBpoAttributesMapFromEsi();

  const allTargetBpIds = Array.from(new Set(validTargets.map(t => t.bpID)));
  const waterfallStatsMap = _buildWaterfallBlueprintStatsMap(allTargetBpIds, configMap);

  // Pull active location and its config row once globally
  const activeLocation = _getNamedOr_(ss, 'setting_production_structure', 'Amarr VIII (NPC)');
  const structureMap = _getStructureSettingsMap(ss);
  const activeSetting = structureMap.get(activeLocation) || { taxRate: 0.0, rigBonus: 1.0, structureType: 'NPC Station' };

  // =================================================================
  // 4. Core Mathematical Assembly Evaluation Loop
  // =================================================================
  const outputRows = validTargets.map(target => {
    const materials = sdeMatMap.get(target.bpID);
    if (!materials) return [target.typeID, target.name, 0, "No SDE Materials", new Date()];

    const waterfallStats = waterfallStatsMap.get(target.bpID);
    const bpoItemAttributes = bpoAttributesMap.get(target.bpID);
    const baseConfig = _resolveSmartConfig(target.bpID, target.name, ss);

    let ME_LEVEL = 0;
    let meSourceLog = "";

    // PRIORITY 1: Waterfall Ledger
    if (waterfallStats && waterfallStats.me > 0) {
      ME_LEVEL = waterfallStats.me;
      meSourceLog = `Ledger (ME: ${ME_LEVEL})`;
    }
    // PRIORITY 2: ESI Hangar Cache
    else if (bpoItemAttributes && bpoItemAttributes.material_efficiency > 0) {
      ME_LEVEL = Number(bpoItemAttributes.material_efficiency);
      meSourceLog = `ESI Hangar (ME: ${ME_LEVEL})`;
    }
    // PRIORITY 3: Config Engine
    else {
      ME_LEVEL = baseConfig.maxMe;
      meSourceLog = `${baseConfig.source || 'Config CSV'} (ME: ${ME_LEVEL})`;
    }

    // Pass everything dynamically into the math call
    const financials = _calculateJobFinancials({
      bpId: target.bpID,
      activityId: 1,
      runs: baseConfig.presetRuns,
      meLevel: ME_LEVEL,
      materials: materials,
      costMap: costMap,
      amortMap: amortMap,
      internalBpcMap: internalBpcMap,
      bpcWacData: bpcWacData,
      presetRuns: baseConfig.presetRuns,
      baseYield: target.yield,

      waterfallAvgCost: waterfallStats ? waterfallStats.avgCost : 0,

      actualInstallCost: undefined,
      systemId: targetSystemId,
      sdeBasePriceMap: sdeBasePriceMap,
      systemIndexMap: systemIndexMap,

      // FULLY DYNAMIC FROM STRUCTURE_SETTINGS:
      facilityTaxRate: activeSetting.taxRate,
      structureRigBonus: activeSetting.rigBonus
    });

    // The unitCost returned by financials now perfectly includes the live Amarr installation tax
    return [target.typeID, target.name, financials.unitCost, meSourceLog, new Date()];
  });

  // =================================================================
  // 5. Output Management
  // =================================================================
  const SHEET_NAME = "Projected_Build_Costs";
  let outSheet = ss.getSheetByName(SHEET_NAME) || ss.insertSheet(SHEET_NAME);
  if (outputRows.length === 0) return;

  const finalPayload = [
    ["Type ID", "Item Name", "Cost", "Source Tier", "Updated"],
    ...outputRows
  ];

  const requiredRows = finalPayload.length;
  outSheet.clearContents();

  if (requiredRows > outSheet.getMaxRows()) {
    outSheet.insertRowsAfter(outSheet.getMaxRows(), requiredRows - outSheet.getMaxRows());
  }
  outSheet.getRange(1, 1, requiredRows, finalPayload[0].length).setValues(finalPayload);

  const postWriteMaxRows = outSheet.getMaxRows();
  if (postWriteMaxRows > requiredRows) {
    outSheet.deleteRows(requiredRows + 1, postWriteMaxRows - requiredRows);
  }

  const RANGE_NAME = "NR_PROJECTED_BUILD_COSTS";
  const finalRange = outSheet.getRange(1, 1, requiredRows, finalPayload[0].length);

  const namedRanges = ss.getNamedRanges();
  const existingRange = namedRanges.find(r => r.getName() === RANGE_NAME);

  if (existingRange) {
    existingRange.setRange(finalRange);
  } else {
    ss.setNamedRange(RANGE_NAME, finalRange);
  }

  LOG.info(`Done: ${outputRows.length} items updated seamlessly via Array.`);
}