/**
 * TRIGGER-READY WRAPPER
 * Point your timed trigger at this function.
 */
function trigger_generateProjectedCostTable() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  generateProjectedCostTable(ss);
}

/**
 * PROJECTED MANUFACTURING COSTS ENGINE
 * Computes assembly unit costs by feeding data directly from getOverviewData()
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
  
  // THE FIX: Switch entirely to the centralized Master Config Engine
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
  // 3. Initialize Cost & Amortization Maps (WITH CACHE ARMOR)
  // =================================================================
  const costMap = _getBlendedCostMap(ss, Array.from(allRequiredMatIds));
  const amortMap = _getBpoAmortizationMap(ss);
  const internalBpcMap = _buildInternalBpcMap_(ss);
  const bpcWacData = JSON.parse(SCRIPT_PROP.getProperty("BPC_WAC_KEY") || '{}');

  const allTargetBpIds = Array.from(new Set(validTargets.map(t => t.bpID)));
  
  // Pass the new configMap to the Waterfall Engine
  const waterfallStatsMap = _buildWaterfallBlueprintStatsMap(allTargetBpIds, configMap);
  let bpoAttributesMap = _getBpoAttributesMapFromEsi();

  const EST_INSTALL_RATE = 0.05;

  // =================================================================
  // 4. Core Mathematical Assembly Evaluation Loop
  // =================================================================
  const outputRows = validTargets.map(target => {
    const materials = sdeMatMap.get(target.bpID);
    if (!materials) return [target.typeID, target.name, 0, "No SDE Materials", new Date()];

    const waterfallStats = waterfallStatsMap.get(target.bpID);
    const bpoItemAttributes = bpoAttributesMap.get(target.bpID);

    // THE DRY CALL: Ask the Smart Config Engine for the baseline rules
    const baseConfig = _resolveSmartConfig(target.bpID, target.name, ss);

    let ME_LEVEL = 0; 
    let meSourceLog = ""; 
    
    // PRIORITY 1: Waterfall Ledger (Actual historical build data)
    if (waterfallStats && waterfallStats.me > 0) {
      ME_LEVEL = waterfallStats.me; 
      meSourceLog = `Ledger (ME: ${ME_LEVEL})`;
    } 
    // PRIORITY 2: ESI Hangar Cache (Physical blueprint on hand)
    else if (bpoItemAttributes && bpoItemAttributes.material_efficiency > 0) {
      ME_LEVEL = Number(bpoItemAttributes.material_efficiency); 
      meSourceLog = `ESI Hangar (ME: ${ME_LEVEL})`;
    } 
    // PRIORITY 3: Config Engine (CSV Explicit Override OR Smart Default)
    else {
      ME_LEVEL = baseConfig.maxMe;
      meSourceLog = `${baseConfig.source || 'Config CSV'} (ME: ${ME_LEVEL})`;
    }

    // THE DRY CALL: Let the centralized engine do all the math
    const financials = _calculateJobFinancials({
      bpId: target.bpID,
      activityId: 1, // Projected is always Manufacturing
      runs: baseConfig.presetRuns,
      meLevel: ME_LEVEL,
      materials: materials,
      costMap: costMap,
      amortMap: amortMap,
      internalBpcMap: internalBpcMap,
      bpcWacData: bpcWacData,
      presetRuns: baseConfig.presetRuns,
      baseYield: target.yield,
      actualInstallCost: undefined, // Triggers the EST_INSTALL_RATE fallback
      estInstallRate: EST_INSTALL_RATE
    });

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

