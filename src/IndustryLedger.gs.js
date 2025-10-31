/**
 * IndustryLedger.gs.js
 *
 * This module is split into two stages:
 * 1. Process BPC creation (Copy/Invention) jobs to calculate the cost-per-run (WAC).
 * 2. Process Manufacturing jobs, applying BPO/BPC costs and ME material savings.
 */

/* global SpreadsheetApp, PropertiesService, LoggerEx, ScriptApp, getOrCreateSheet, deleteTriggersByName */

// --- Constants ---
const INDUSTRY_JOB_KEY = 'processedIndustryJobIds';
const BPC_JOB_KEY = 'processedBpcJobIds';
const BPC_WAC_KEY = 'BpcWeightedAverageCost';

const INDUSTRY_ACTIVITY_MANUFACTURING = 1;
const INDUSTRY_ACTIVITY_COPYING = 5;
const INDUSTRY_ACTIVITY_INVENTION = 8;

const LOG_INDUSTRY = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('IndustryLedger') : console);

// ----------------------------------------------------------------------
// --- MAIN FUNCTION STAGE 1: BPC Cost Calculation ---
// ----------------------------------------------------------------------

/**
 * Processes completed Copying and Invention jobs to calculate the cost-per-run
 * and stores the Weighted Average Cost (WAC) in PropertiesService.
 * This runs BEFORE runIndustryLedgerUpdate.
 */
function runBpcCreationLedger() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  LOG_INDUSTRY.info("Running BPC Creation Ledger update (Stage 1)...");

  // 1. Get Cost Data and Config
  const costMap = _getBlendedCostMap(ss);
  const presetRunsMap = _getConfigPresetRuns(ss);
  if (presetRunsMap.size === 0) {
    LOG_INDUSTRY.warn("Config sheet 'Config_BPC_Runs' is missing or empty. Cannot calculate BPC costs. Skipping Stage 1.");
    return;
  }

  // 2. Get SDE/System Data (Needed for full job costing/validation)
  const { sdeMatMap } = _getSdeMaps(ss);
  // const costIndexMap = _getCopyingCostIndexMap(ss); // Optional: Cost index for validation
  // const stationSystemMap = _getStationSystemMap(ss); // Optional: Location mapping

  // 3. Get previously processed BPC job IDs
  const processedJobIds = new Set(
    JSON.parse(SCRIPT_PROP.getProperty(BPC_JOB_KEY) || '[]')
  );

  // 4. Find new completed BPC jobs (Copying and Invention)
  const newBpcJobs = _getNewCompletedJobs(ss, processedJobIds, [INDUSTRY_ACTIVITY_COPYING, INDUSTRY_ACTIVITY_INVENTION]);
  if (newBpcJobs.length === 0) {
    LOG_INDUSTRY.info("No new BPC creation jobs to process.");
    return;
  }

  const bpcCostMap = new Map(); 
  const newlyProcessedIds = [];

  for (const job of newBpcJobs) {
    let totalMaterialCost = 0;
    let missingCost = false;

    // A. Get Materials Cost (Only Invention has material cost)
    if (job.activity_id === INDUSTRY_ACTIVITY_INVENTION) {
      const materials = sdeMatMap.get(job.blueprint_type_id);
      if (materials) {
        for (const mat of materials) {
          const matCost = costMap.get(mat.materialTypeID);
          if (matCost === undefined || matCost === null) {
            LOG_INDUSTRY.warn(`Missing blended cost for invention material ${mat.materialTypeID}. Cannot price BPC job ${job.job_id}.`);
            missingCost = true;
            break;
          }
          // The invention materials are consumed ONCE per BPC
          totalMaterialCost += (matCost * mat.quantity) * job.runs; // Total cost for all BPCs made
        }
      }
    }

    if (missingCost) continue;

    // B. Get Preset Runs per BPC (The standardizing variable)
    const presetRuns = presetRunsMap.get(job.blueprint_type_id);
    if (!presetRuns) {
      LOG_INDUSTRY.warn(`Missing 'Preset_Runs_per_BPC' in Config for blueprint ${job.blueprint_type_id}. Skipping job ${job.job_id}.`);
      continue;
    }

    // C. Get Total Costs
    const totalJobInstallationCost = job.cost;
    const totalActualCost = totalMaterialCost + totalJobInstallationCost;
    
    // D. Get Total Runs
    const totalBpcsMade = job.runs;
    const totalRunsProduced = totalBpcsMade * presetRuns; 

    if (totalRunsProduced === 0) continue;

    // E. Aggregate data for the Weighted Average Cost (WAC) calculation
    const bpID = job.blueprint_type_id;
    if (!bpcCostMap.has(bpID)) {
      bpcCostMap.set(bpID, { totalCost: 0, totalRuns: 0 });
    }
    
    const currentData = bpcCostMap.get(bpID);
    currentData.totalCost += totalActualCost;
    currentData.totalRuns += totalRunsProduced;
    
    newlyProcessedIds.push(job.job_id);
  }

  // 5. Calculate WAC and Save State
  const finalWAC = {}; 
  
  for (const [bpID, data] of bpcCostMap.entries()) {
    const costPerRun = data.totalCost / data.totalRuns;
    finalWAC[bpID] = costPerRun;
    LOG_INDUSTRY.info(`WAC calculated for BP ${bpID}: ${costPerRun.toFixed(2)} ISK/run.`);
  }

  // Save the new WAC map to properties service
  SCRIPT_PROP.setProperty(BPC_WAC_KEY, JSON.stringify(finalWAC));
  
  // 6. Save new job IDs
  newlyProcessedIds.forEach(id => processedJobIds.add(id));
  const trimmedJobIds = Array.from(processedJobIds).slice(-1000);
  SCRIPT_PROP.setProperty(BPC_JOB_KEY, JSON.stringify(trimmedJobIds));
}


// ----------------------------------------------------------------------
// --- MAIN FUNCTION STAGE 2: MANUFACTURING LEDGER UPDATE ---
// ----------------------------------------------------------------------

/**
 * Processes completed Manufacturing jobs and writes to Material_Ledger.
 */
function runIndustryLedgerUpdate() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  // 1. Get Cost Data
  const costMap = _getBlendedCostMap(ss);
  if (costMap.size === 0) {
    LOG_INDUSTRY.warn("Blended_Cost sheet is empty. Skipping.");
    return;
  }
  
  // 2. Get Amortization, WAC, and ESI Attributes
  const amortMap = _getBpoAmortizationMap(ss); 
  const bpcWacData = JSON.parse(SCRIPT_PROP.getProperty(BPC_WAC_KEY) || '{}');
  const bpoAttributesMap = _getBpoAttributesMapFromEsi(ss);

  const getBpcCostPerRun = (bpID) => {
    const cost = bpcWacData[bpID];
    return cost ? Number(cost) : 0;
  };

  // 3. Get SDE Material Requirements
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  if (sdeMatMap.size === 0) {
    LOG_INDUSTRY.warn("SDE sheets are empty. Skipping.");
    return;
  }
  
  // 4. Get SDE Item Names
  const nameMap = _getSdeNameMap(ss);

  // 5. Get last processed job ID
  const processedJobIds = new Set(
    JSON.parse(SCRIPT_PROP.getProperty(INDUSTRY_JOB_KEY) || '[]')
  );

  // 6. Find new completed manufacturing jobs
  const newJobs = _getNewCompletedJobs(ss, processedJobIds, [INDUSTRY_ACTIVITY_MANUFACTURING]);
  if (newJobs.length === 0) {
    LOG_INDUSTRY.info("No new manufacturing jobs to process.");
    return;
  }

  // 7. Calculate cost and create ledger rows
  const ledgerRows = [];
  const newlyProcessedIds = [];

  for (const job of newJobs) {
    const materials = sdeMatMap.get(job.blueprint_type_id);
    const product = sdeProdMap.get(job.blueprint_type_id);

    if (!materials || !product) {
      LOG_INDUSTRY.warn(`Missing SDE data for job ${job.job_id} (BP ${job.blueprint_type_id}). Skipping.`);
      continue;
    }

    // A. Apply Material Efficiency (ME) Discount
    const bpoItemAttributes = bpoAttributesMap.get(job.blueprint_type_id); 
    const meLevel = bpoItemAttributes ? bpoItemAttributes.material_efficiency : 0; 
    const materialDiscountFactor = 1 - (meLevel / 100);

    let totalMaterialCostPerRun = 0;
    let missingCost = false;

    for (const mat of materials) {
      const matCost = costMap.get(mat.materialTypeID);
      if (matCost === undefined || matCost === null) {
        LOG_INDUSTRY.warn(`Missing blended cost for material ${mat.materialTypeID}. Cannot price job ${job.job_id}.`);
        missingCost = true;
        break;
      }
      
      // Apply ME discount to required quantity
      const finalQuantity = mat.quantity * materialDiscountFactor;
      totalMaterialCostPerRun += matCost * finalQuantity;
    }

    if (missingCost) continue;

    // B. Calculate Total Costs (Amortization + ISK Fee + Materials)
    const totalMaterialCostForAllRuns = totalMaterialCostPerRun * job.runs;
    const totalJobInstallationCost = job.cost; // Already includes TE discount
    
    // C. Apply BPC Amortization or BPO Amortization Surcharge
    let amortizationSurcharge = 0;
    
    // Check if the BPO type ID is in the Amortization config sheet (signifying a BPO we amortize)
    if (amortMap.has(job.blueprint_type_id)) {
        // This is a BPO. Use the BPO's calculated amortization surcharge.
        amortizationSurcharge = amortMap.get(job.blueprint_type_id) * job.runs;
        LOG_INDUSTRY.info(`Job ${job.job_id}: Added BPO research amortization cost of ${amortizationSurcharge.toFixed(2)} ISK.`);
    } 
    // Otherwise, assume it's a BPC and use the WAC calculated in Stage 1.
    else {
        const bpcCostPerRun = getBpcCostPerRun(job.blueprint_type_id);
        amortizationSurcharge = bpcCostPerRun * job.runs;
        if (bpcCostPerRun > 0) {
            LOG_INDUSTRY.info(`Job ${job.job_id}: Added BPC creation cost of ${amortizationSurcharge.toFixed(2)} ISK.`);
        }
    }

    const totalActualCost = totalMaterialCostForAllRuns + totalJobInstallationCost + amortizationSurcharge;
    
    // D. Calculate Unit Cost
    const productQtyPerRun = product.quantity;
    const totalUnitsProduced = productQtyPerRun * job.runs;
    
    if (totalUnitsProduced === 0) continue;

    const unitManufacturingCost = totalActualCost / totalUnitsProduced;

    // 8. Create the row for Material_Ledger
    ledgerRows.push([
      job.end_date,
      job.product_type_id,
      nameMap.get(job.product_type_id) || `Product ${job.product_type_id}`,
      totalUnitsProduced,
      unitManufacturingCost,
      "INDUSTRY", // Source
      job.job_id,
      job.installer_id, // Character
      unitManufacturingCost // unit_value_filled
    ]);
    
    newlyProcessedIds.push(job.job_id);
  }

  // 9. Append new rows and save state
  if (ledgerRows.length > 0) {
    const ledgerSheet = ss.getSheetByName("Material_Ledger");
    if (!ledgerSheet) {
      LOG_INDUSTRY.error("Cannot find 'Material_Ledger' sheet!");
      return;
    }
    const startRow = ledgerSheet.getLastRow() + 1;
    ledgerSheet.getRange(startRow, 1, ledgerRows.length, ledgerRows[0].length).setValues(ledgerRows);
    LOG_INDUSTRY.info(`Successfully added ${ledgerRows.length} manufacturing jobs to the ledger.`);
  }

  newlyProcessedIds.forEach(id => processedJobIds.add(id));
  const trimmedJobIds = Array.from(processedJobIds).slice(-1000);
  SCRIPT_PROP.setProperty(INDUSTRY_JOB_KEY, JSON.stringify(trimmedJobIds));
}


// ----------------------------------------------------------------------
// --- HELPER FUNCTIONS ---
// ----------------------------------------------------------------------

// *** NOTE: YOUR EXISTING HELPERS (_getBlendedCostMap, _getSdeMaps, _getSdeNameMap, _getNewCompletedJobs) GO HERE ***
// Ensure _getSdeMaps reads activity 8 (Invention) materials, and _getNewCompletedJobs accepts activityIds array.


/**
 * Helper to get the Market Median price for a given item type ID as a fallback.
 */
function _getMarketMedianMap(ss) {
    const sheet = ss.getSheetByName("Market_Data_Raw");
    const medianMap = new Map();

    if (!sheet || sheet.getLastRow() < 2) {
        LOG_INDUSTRY.warn("Sheet 'Market_Data_Raw' is empty or missing. Cannot provide price fallback.");
        return medianMap;
    }

    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const col = {
        type_id: headers.indexOf('type_id'),
        sell_median: headers.indexOf('sell_median') // ASSUMES THIS COLUMN EXISTS
    };

    if (col.type_id === -1 || col.sell_median === -1) {
        LOG_INDUSTRY.error("Missing required columns (type_id or sell_median) in 'Market_Data_Raw'.");
        return medianMap;
    }

    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();

    for (const row of data) {
        const type_id = Number(row[col.type_id]);
        const median_price = Number(row[col.sell_median]);
        
        if (!isNaN(type_id) && median_price > 0) {
            medianMap.set(type_id, median_price); 
        }
    }
    return medianMap;
}

/**
 * Helper to get BPC preset runs from a 'Config' sheet (Needed for BPC WAC).
 */
function _getConfigPresetRuns(ss) {
    const sheet = ss.getSheetByName("Config_BPC_Runs");
    const presetMap = new Map();
    
    if (!sheet) {
        LOG_INDUSTRY.warn("Sheet 'Config_BPC_Runs' not found. BPC WAC calculation will fail.");
        return presetMap;
    }

    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, 2).getValues();
    for (const row of data) {
      const bp_type_id = Number(row[0]);
      const preset_runs = Number(row[1]);
      if (!isNaN(bp_type_id) && !isNaN(preset_runs) && preset_runs > 0) {
        presetMap.set(bp_type_id, preset_runs);
      }
    }
    return presetMap;
}


/**
 * Helper to get the manual amortization surcharge for BPOs (Market Median Fallback).
 */
function _getBpoAmortizationMap(ss) {
    const AMORT_SHEET_NAME = "BPO_Amortization";
    const sheet = ss.getSheetByName(AMORT_SHEET_NAME);
    const amortMap = new Map();

    if (!sheet) {
        LOG_INDUSTRY.error(`Sheet '${AMORT_SHEET_NAME}' not found. BPO amortization will be 0.`);
        return amortMap;
    }

    const blendedCostMap = _getBlendedCostMap(ss); 
    const marketMedianMap = _getMarketMedianMap(ss);

    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, 2).getValues(); 

    for (const row of data) {
        const bp_type_id = Number(row[0]);
        const totalRuns = Number(row[1]); 
        let bpoValue = 0;

        if (totalRuns <= 0) continue; 

        // 1. TRY PRIMARY: Blended Average (Blended_Cost sheet)
        bpoValue = blendedCostMap.get(bp_type_id) || 0;

        if (bpoValue === 0) {
            // 2. FALLBACK: Market Median (Market_Data_Raw sheet)
            bpoValue = marketMedianMap.get(bp_type_id) || 0;
            if (bpoValue > 0) {
                LOG_INDUSTRY.warn(`BPO ${bp_type_id}: Using Market Median for amortization.`);
            }
        }
        
        if (bpoValue > 0) {
            const surchargePerRun = bpoValue / totalRuns;
            amortMap.set(bp_type_id, surchargePerRun);
        } else {
            LOG_INDUSTRY.warn(`BPO ${bp_type_id}: No market value found. Amortization skipped.`);
        }
    }
    return amortMap;
}

/**
 * REVISED Helper: Pulls ME/TE attributes from the ESI-populated 'ESI Corp Blueprints' sheet.
 */
function _getBpoAttributesMapFromEsi(ss) {
    const sheet = ss.getSheetByName("ESI Corp Blueprints");
    const attributesMap = new Map();

    if (!sheet) {
        LOG_INDUSTRY.error("Sheet 'ESI Corp Blueprints' not found. Material efficiency savings cannot be calculated.");
        return attributesMap;
    }

    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const col = {
        type_id: headers.indexOf('type_id'), 
        me: headers.indexOf('material_efficiency'),
        te: headers.indexOf('time_efficiency')
    };
    
    if (col.type_id === -1 || col.me === -1 || col.te === -1) {
        LOG_INDUSTRY.error("Missing required columns in 'ESI Corp Blueprints'.");
        return attributesMap;
    }

    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();

    for (const row of data) {
        const bp_type_id = Number(row[col.type_id]);
        const me = Number(row[col.me]);
        // TE is optional for cost, but we'll include it.
        // The ME value is the only one critical for the COGS calculation.
        const te = Number(row[col.te]); 

        if (!isNaN(bp_type_id) && bp_type_id > 0) {
            attributesMap.set(bp_type_id, {
                material_efficiency: me,
                time_efficiency: te
            });
        }
    }
    return attributesMap;
}

/**
 * Helper to map Station/Structure IDs to their containing Solar System IDs.
 * (Included for completeness, but not strictly needed for the final COGS calculation)
 */
function _getStationSystemMap(ss) {
    const sheet = ss.getSheetByName("SDE_staStations");
    const systemMap = new Map();

    if (!sheet || sheet.getLastRow() < 2) {
        LOG_INDUSTRY.warn("Sheet 'SDE_staStations' is empty or missing. Cannot map job location to system.");
        return systemMap;
    }

    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const col = {
        stationID: headers.indexOf('stationID'),
        solarSystemID: headers.indexOf('solarSystemID')
    };
    
    if (col.stationID === -1 || col.solarSystemID === -1) {
        LOG_INDUSTRY.error("Missing required columns (stationID or solarSystemID) in 'SDE_staStations'.");
        return systemMap;
    }

    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();

    for (const row of data) {
        const stationID = Number(row[col.stationID]);
        const systemID = Number(row[col.solarSystemID]);
        
        if (!isNaN(stationID) && !isNaN(systemID)) {
            systemMap.set(stationID, systemID); 
        }
    }
    return systemMap;
}

/**
 * Utility function to dynamically find column indices by header name.
 * @param {string[]} headers - The array of header names from the spreadsheet.
 * @param {string[]} requiredHeaders - The list of column names needed by the function.
 * @returns {Object<string, number>} An object mapping the required header name to its column index.
 * @throws {Error} if any required header is missing.
 */
function _getColIndexMap(headers, requiredHeaders) {
    const col = {};
    const lowerCaseHeaders = headers.map(h => h.toLowerCase().trim());
    
    for (const req of requiredHeaders) {
        const index = lowerCaseHeaders.indexOf(req.toLowerCase().trim());
        if (index === -1) {
            // Throw a specific error to halt execution if data is unusable
            throw new Error(`CRITICAL HEADER ERROR: Sheet is missing required column "${req}".`);
        }
        // Store the index of the required header
        col[req] = index;
    }
    return col;
}

/**
 * Helper to get the current blended cost for all items (from Blended_Cost).
 */
function _getBlendedCostMap(ss) {
    const sheet = ss.getSheetByName("Blended_Cost");
    if (!sheet || sheet.getLastRow() < 2) { LOG_INDUSTRY.warn("Blended_Cost sheet is empty."); return new Map(); }

    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const col = _getColIndexMap(headers, ['type_id', 'unit_weighted_average']);

    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
    const costMap = new Map();
    
    for (const row of data) {
        const type_id = Number(row[col.type_id]);
        const cost = Number(row[col.unit_weighted_average]); 
        
        if (!isNaN(type_id) && !isNaN(cost) && cost > 0) {
            costMap.set(type_id, cost);
        }
    }
    return costMap;
}

/**
 * Helper to get the Market Median price for BPO amortization fallback.
 */
function _getMarketMedianMap(ss) {
    const sheet = ss.getSheetByName("Market_Data_Raw");
    if (!sheet || sheet.getLastRow() < 2) { LOG_INDUSTRY.warn("Market_Data_Raw sheet is empty."); return new Map(); }

    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const col = _getColIndexMap(headers, ['type_id', 'sell_median']); // Assumes 'sell_median' exists

    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
    const medianMap = new Map();

    for (const row of data) {
        const type_id = Number(row[col.type_id]);
        const median_price = Number(row[col.sell_median]);
        
        if (!isNaN(type_id) && median_price > 0) {
            medianMap.set(type_id, median_price); 
        }
    }
    return medianMap;
}

/**
 * Helper to get SDE material and product definitions (Recipes).
 */
function _getSdeMaps(ss) {
    const matSheet = ss.getSheetByName("SDE_industryActivityMaterials");
    const prodSheet = ss.getSheetByName("SDE_industryActivityProducts");

    if (!matSheet || !prodSheet || matSheet.getLastRow() < 2) { LOG_INDUSTRY.error("SDE sheets are missing."); return { sdeMatMap: new Map(), sdeProdMap: new Map() }; }

    const matHeaders = matSheet.getRange(1, 1, 1, matSheet.getLastColumn()).getValues()[0];
    const prodHeaders = prodSheet.getRange(1, 1, 1, prodSheet.getLastColumn()).getValues()[0];
    
    const matCol = _getColIndexMap(matHeaders, ['typeID', 'activityID', 'materialTypeID', 'quantity']);
    const prodCol = _getColIndexMap(prodHeaders, ['typeID', 'activityID', 'productTypeID', 'quantity']);

    const matData = matSheet.getRange(2, 1, matSheet.getLastRow() - 1, matSheet.getLastColumn()).getValues();
    const prodData = prodSheet.getRange(2, 1, prodSheet.getLastRow() - 1, prodSheet.getLastColumn()).getValues();

    const sdeMatMap = new Map();
    const sdeProdMap = new Map();

    // Process Materials
    for (const row of matData) {
        const activityID = Number(row[col.activityID]);
        if (activityID !== INDUSTRY_ACTIVITY_MANUFACTURING && activityID !== INDUSTRY_ACTIVITY_INVENTION) continue; 
        
        const bp_type_id = Number(row[matCol.typeID]);
        const mat_type_id = Number(row[matCol.materialTypeID]);
        const qty = Number(row[matCol.quantity]);

        if (!sdeMatMap.has(bp_type_id)) { sdeMatMap.set(bp_type_id, []); }
        sdeMatMap.get(bp_type_id).push({ materialTypeID: mat_type_id, quantity: qty });
    }

    // Process Products
    for (const row of prodData) {
        const activityID = Number(row[prodCol.activityID]);
        if (activityID !== INDUSTRY_ACTIVITY_MANUFACTURING) continue;
        
        const bp_type_id = Number(row[prodCol.typeID]);
        const prod_type_id = Number(row[prodCol.productTypeID]);
        const qty = Number(row[prodCol.quantity]);

        sdeProdMap.set(bp_type_id, { productTypeID: prod_type_id, quantity: qty });
    }

    return { sdeMatMap, sdeProdMap };
}

/**
 * Helper to get item names from SDE_invTypes.
 */
function _getSdeNameMap(ss) {
    const sheet = ss.getSheetByName("SDE_invTypes");
    if (!sheet || sheet.getLastRow() < 2) { LOG_INDUSTRY.error("SDE_invTypes sheet is missing."); return new Map(); }
        
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const col = _getColIndexMap(headers, ['typeID', 'typeName']);

    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
    const nameMap = new Map();
    
    for (const row of data) {
        const type_id = Number(row[col.typeID]);
        const type_name = row[col.typeName];
        if (!isNaN(type_id) && type_name) {
            nameMap.set(type_id, type_name);
        }
    }
    return nameMap;
}

/**
 * Helper to get BPO/BPC efficiency attributes from ESI Corp Blueprints.
 */
function _getBpoAttributesMapFromEsi(ss) {
    const sheet = ss.getSheetByName("ESI Corp Blueprints");
    if (!sheet) { LOG_INDUSTRY.error("ESI Corp Blueprints sheet not found."); return new Map(); }

    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const col = _getColIndexMap(headers, ['type_id', 'material_efficiency', 'time_efficiency']);
    
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
    const attributesMap = new Map();

    for (const row of data) {
        const bp_type_id = Number(row[col.type_id]);
        const me = Number(row[col.material_efficiency]);
        const te = Number(row[col.time_efficiency]); 

        if (!isNaN(bp_type_id) && bp_type_id > 0) {
            attributesMap.set(bp_type_id, {
                material_efficiency: me,
                time_efficiency: te
            });
        }
    }
    return attributesMap;
}

/**
 * Helper to map Station/Structure IDs to their containing Solar System IDs.
 */
function _getStationSystemMap(ss) {
    const sheet = ss.getSheetByName("SDE_staStations");
    if (!sheet || sheet.getLastRow() < 2) { LOG_INDUSTRY.warn("SDE_staStations sheet is missing."); return new Map(); }

    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const col = _getColIndexMap(headers, ['stationID', 'solarSystemID']);

    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
    const systemMap = new Map();

    for (const row of data) {
        const stationID = Number(row[col.stationID]);
        const systemID = Number(row[col.solarSystemID]);
        
        if (!isNaN(stationID) && !isNaN(systemID)) {
            systemMap.set(stationID, systemID); 
        }
    }
    return systemMap;
}

/**
 * Helper to get the Copying Cost Index from ESI Cost Indexes data.
 */
function _getCopyingCostIndexMap(ss) {
    const sheet = ss.getSheetByName("Cost Indexes");
    if (!sheet || sheet.getLastRow() < 2) { LOG_INDUSTRY.warn("Cost Indexes sheet is missing."); return new Map(); }

    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const col = _getColIndexMap(headers, ['solar_system_id', 'Copying']);
    
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
    const indexMap = new Map();

    for (const row of data) {
        const system_id = Number(row[col.solar_system_id]);
        const index_value = Number(row[col.Copying]);
        
        if (!isNaN(system_id) && index_value > 0) {
            indexMap.set(system_id, index_value); 
        }
    }
    return indexMap;
}