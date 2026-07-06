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

  // 1. Load SDE Maps
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  const overviewData = getOverviewData(ss);
  if (!overviewData || overviewData.length < 2) return;
  const presetMap = _getConfigPresetRuns(ss) || new Map();
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
    // Break apart the new "ActivityID:BlueprintID" string format
    const parts = String(compositeKey).split(':');
    
    // Fallback logic just in case an old numerical cache is active
    const actID = parts.length > 1 ? Number(parts[0]) : 1;
    const bpID = parts.length > 1 ? Number(parts[1]) : Number(parts[0]);

    // The Manufacturing Cost Engine ONLY cares about Activity 1
    if (actID === 1) {
      invertedProductMap.set(Number(prodObj.productTypeID), {
        bpID: bpID, // Pass the clean, raw Blueprint ID number to the engine
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
  const presetRunsMap = _getConfigPresetRuns(ss);
  const bpcWacData = JSON.parse(SCRIPT_PROP.getProperty("BPC_WAC_KEY") || '{}');

  // NEW: Initialize the Waterfall Map
  const allTargetBpIds = Array.from(new Set(validTargets.map(t => t.bpID)));
  const waterfallStatsMap = _buildWaterfallBlueprintStatsMap(allTargetBpIds);

  // CACHE WRAPPER: Stop pinging the API every single run
  let bpoAttributesMap = _getBpoAttributesMapFromEsi();


  const EST_INSTALL_RATE = 0.05;

  // =================================================================
  // 4. Core Mathematical Assembly Evaluation Loop
  // =================================================================
  const outputRows = validTargets.map(target => {
    const materials = sdeMatMap.get(target.bpID);
    if (!materials) return [target.typeID, target.name, 0, "No SDE Materials", new Date()];

    // Get the waterfall stats for Material Efficiency (ME)
    const waterfallStats = waterfallStatsMap.get(target.bpID);
    const bpoItemAttributes = bpoAttributesMap.get(target.bpID);

    let ME_LEVEL = 0;
    if (waterfallStats) {
      ME_LEVEL = waterfallStats.me; // Tier 1 & 2 (Contracts & Industry Ledger)
    } else if (bpoItemAttributes) {
      ME_LEVEL = Number(bpoItemAttributes.material_efficiency); // Tier 3 (ESI Hangar BPOs)
    }

    // Establish total job parameters from configuration presets
    const presetRuns = presetRunsMap.get(target.bpID) || 1;
    const meMultiplier = (100 - ME_LEVEL) / 100;

    let totalJobMaterialCost = 0;

    // Evaluate material needs across the entire batch sequence
    materials.forEach(m => {
      if (m.activityID !== 1) return;
      const matID = Number(m.materialTypeID);
      const baseQty = Number(m.quantity);

      // EVE INDUSTRY BATCH FORMULA: Apply ME and runs first, then ceil, clamp to at least 1 per run
      const totalJobQty = Math.max(presetRuns, Math.ceil(baseQty * meMultiplier * presetRuns));

      const costData = costMap.get(matID) || { raw: 0, landed: 0 };
      const unitCost = costData.landed;

      if (unitCost <= 0) {
        LOG.warn(`ProjectedCost: No price found for TypeID ${matID}`);
      }

      totalJobMaterialCost += (totalJobQty * unitCost);
    });

    // Calculate blueprint amortization
    let totalJobAmortization = 0;
    if (amortMap.has(target.bpID)) {
      // Amortization map supplies cost per run
      totalJobAmortization = amortMap.get(target.bpID) * presetRuns;
    } else {
      const contractBpcValue = internalBpcMap.get(target.bpID);
      if (contractBpcValue && contractBpcValue > 0) {
        totalJobAmortization = contractBpcValue;
      } else {
        totalJobAmortization = (Number(bpcWacData[target.bpID]) || 0) * presetRuns;
      }
    }

    // Calculate the complete job yield
    const totalJobYield = presetRuns * target.yield;

    // Final Unit Cost: Total Material (with fee) + Total Amortization divided by Total Yield
    const unitCost = ((totalJobMaterialCost * (1 + EST_INSTALL_RATE)) + totalJobAmortization) / totalJobYield;

    return [target.typeID, target.name, unitCost, "Calculated", new Date()];
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

  // REPLACE your named range logic (near the end of Step 5) with this:
  const RANGE_NAME = "NR_PROJECTED_BUILD_COSTS";
  const finalRange = outSheet.getRange(1, 1, requiredRows, finalPayload[0].length);

  // CORRECTED LOGIC:
  const namedRanges = ss.getNamedRanges();
  const existingRange = namedRanges.find(r => r.getName() === RANGE_NAME);

  if (existingRange) {
    existingRange.setRange(finalRange);
  } else {
    ss.setNamedRange(RANGE_NAME, finalRange);
  }


  LOG.info(`Done: ${outputRows.length} items updated seamlessly via Array.`);
}

