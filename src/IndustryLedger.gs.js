/**
 * IndustryLedger.gs.js
 *
 * This module is the Industry Ledger Add-on. It executes in two stages:
 * 1. Process BPC creation (Copy/Invention) jobs to calculate the cost-per-run (WAC).
 * 2. Process Manufacturing jobs, applying BPO/BPC costs and ME material savings,
 * and writes final COGS data to the Material_Ledger via the ML API.
 *
 * NOTE: This file assumes 'getOrCreateSheet', 'ML.forSheet', and 'LoggerEx' are in scope.
 */

// --- GLOBAL CONSTANTS ---
const INDUSTRY_JOB_KEY = 'processedIndustryJobIds';
const BPC_JOB_KEY = 'processedBpcJobIds';
const BPC_WAC_KEY = 'BpcWeightedAverageCost';

const INDUSTRY_ACTIVITY_MANUFACTURING = 1;
const INDUSTRY_ACTIVITY_COPYING = 5;
const INDUSTRY_ACTIVITY_INVENTION = 8;

const LOG_INDUSTRY = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('IndustryLedger') : console);


// ----------------------------------------------------------------------
// --- CORE UTILITY: DYNAMIC HEADER MAPPING (MOVED TO TOP FOR SCOPE FIX) ---
// ----------------------------------------------------------------------

/**
 * Utility function to dynamically find column indices by header name.
 * @throws {Error} if any required header is missing.
 */
function _getColIndexMap(headers, requiredHeaders) {
    const col = {};
    const lowerCaseHeaders = headers.map(h => h.toLowerCase().trim());
    
    for (const req of requiredHeaders) {
        const index = lowerCaseHeaders.indexOf(req.toLowerCase().trim());
        if (index === -1) {
            throw new Error(`CRITICAL HEADER ERROR: Sheet is missing required column "${req}".`);
        }
        col[req] = index;
    }
    return col;
}


// ----------------------------------------------------------------------
// --- MASTER ADD-ON INTEGRATION ---
// ----------------------------------------------------------------------

/**
 * Executes the full two-stage Industry Ledger process under the main system lock.
 */
function runIndustryLedgerPhase(ss) {
    const log = LoggerEx.withTag('MASTER_SYNC');
    
    log.info('--- Starting Industry Ledger Phase (BPC Costing & Manufacturing COGS) ---');

    // --- STAGE 1: Calculate WAC (Cost of BPC per run) ---
    try {
        log.info('Running BPC Creation Ledger (Stage 1: Calculate WAC)...');
        runBpcCreationLedger(ss);
    } catch (e) {
        log.error('BPC Creation Ledger (Stage 1) FAILED. Subsequent costing may use stale BPC data.', e);
    }

    // --- STAGE 2: Process Manufacturing Jobs (Calculate final COGS) ---
    try {
        log.info('Running Manufacturing Ledger Update (Stage 2: COGS)...');
        runIndustryLedgerUpdate(ss);
    } catch (e) {
        log.error('Manufacturing Ledger Update (Stage 2) FAILED', e);
    }
    
    log.info('--- Industry Ledger Phase Complete ---');
}


// ----------------------------------------------------------------------
// --- MAIN FUNCTION STAGE 1: BPC Cost Calculation (WAC) ---
// ----------------------------------------------------------------------

function runBpcCreationLedger() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  LOG_INDUSTRY.info("Running BPC Creation Ledger update (Stage 1)...");

  // 1. Get Cost Data and Config
  const costMap = _getBlendedCostMap(ss);
  const presetRunsMap = _getConfigPresetRuns(ss);

  // 2. Get SDE Material Requirements (Invention uses materials)
  const { sdeMatMap } = _getSdeMaps(ss);

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
          totalMaterialCost += (matCost * mat.quantity) * job.runs;
        }
      }
    }

    if (missingCost) continue;

    // B. Get Preset Runs per BPC (Standardizing variable)
    const presetRuns = presetRunsMap.get(job.blueprint_type_id) || 1;
    if (presetRuns === 1 && job.activity_id === INDUSTRY_ACTIVITY_COPYING) {
         LOG_INDUSTRY.warn(`Using 1 as Preset_Runs for BP ${job.blueprint_type_id}. Copying BPCs may have inaccurate cost-per-run.`);
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
  const finalWAC = JSON.parse(SCRIPT_PROP.getProperty(BPC_WAC_KEY) || '{}');
  
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

function runIndustryLedgerUpdate() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  // 1. Get Cost Data and Amortization/WAC
  const costMap = _getBlendedCostMap(ss);
  if (costMap.size === 0) { LOG_INDUSTRY.warn("Blended_Cost sheet is empty. Skipping."); return; }
  
  const amortMap = _getBpoAmortizationMap(ss); 
  const bpcWacData = JSON.parse(SCRIPT_PROP.getProperty(BPC_WAC_KEY) || '{}');
  const bpoAttributesMap = _getBpoAttributesMapFromEsi(); // Direct GESI call
  
  const getBpcCostPerRun = (bpID) => {
    const cost = bpcWacData[bpID];
    return cost ? Number(cost) : 0;
  };

  // 2. Get SDE Data
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  if (sdeMatMap.size === 0) { LOG_INDUSTRY.warn("SDE sheets are empty. Skipping."); return; }
  
  const nameMap = _getSdeNameMap(ss);

  // 3. Find new completed manufacturing jobs
  const processedJobIds = new Set(JSON.parse(SCRIPT_PROP.getProperty(INDUSTRY_JOB_KEY) || '[]'));
  const newJobs = _getNewCompletedJobs(ss, processedJobIds, [INDUSTRY_ACTIVITY_MANUFACTURING]);
  if (newJobs.length === 0) { LOG_INDUSTRY.info("No new manufacturing jobs to process."); return; }

  // 4. Calculate cost and generate ledger row objects
  const ledgerObjects = [];
  const newlyProcessedIds = [];
  const ledgerAPI = ML.forSheet('Material_Ledger'); // Initialize Ledger API

  for (const job of newJobs) {
    const materials = sdeMatMap.get(job.blueprint_type_id);
    const product = sdeProdMap.get(job.blueprint_type_id);

    if (!materials || !product) { LOG_INDUSTRY.warn(`Missing SDE data for job ${job.job_id} (BP ${job.blueprint_type_id}). Skipping.`); continue; }

    // A. Apply Material Efficiency (ME) Discount
    const bpoItemAttributes = bpoAttributesMap.get(job.blueprint_type_id); 
    const meLevel = bpoItemAttributes ? bpoItemAttributes.material_efficiency : 0; 
    const materialDiscountFactor = 1 - (meLevel / 100);

    let totalMaterialCostPerRun = 0;
    let missingCost = false;

    for (const mat of materials) {
      const matCost = costMap.get(mat.materialTypeID);
      if (matCost === undefined || matCost === null) { LOG_INDUSTRY.warn(`Missing blended cost for material ${mat.materialTypeID}. Cannot price job ${job.job_id}.`); missingCost = true; break; }
      
      const finalQuantity = mat.quantity * materialDiscountFactor;
      totalMaterialCostPerRun += matCost * finalQuantity;
    }

    if (missingCost) continue;

    // B. Calculate Total Costs (Amortization + ISK Fee + Materials)
    const totalMaterialCostForAllRuns = totalMaterialCostPerRun * job.runs;
    const totalJobInstallationCost = job.cost; // Includes TE discount
    
    let amortizationSurcharge = 0;
    
    // BPO AMORTIZATION (Capital Cost)
    if (amortMap.has(job.blueprint_type_id)) {
        amortizationSurcharge = amortMap.get(job.blueprint_type_id) * job.runs;
        LOG_INDUSTRY.info(`Job ${job.job_id}: Added BPO research amortization cost of ${amortizationSurcharge.toFixed(2)} ISK.`);
    } 
    // BPC AMORTIZATION (Disposable Asset Cost)
    else {
        const bpcCostPerRun = getBpcCostPerRun(job.blueprint_type_id);
        amortizationSurcharge = bpcCostPerRun * job.runs;
        if (bpcCostPerRun > 0) {
            LOG_INDUSTRY.info(`Job ${job.job_id}: Added BPC creation cost of ${amortizationSurcharge.toFixed(2)} ISK.`);
        }
    }

    const totalActualCost = totalMaterialCostForAllRuns + totalJobInstallationCost + amortizationSurcharge;
    
    // C. Calculate Unit Cost
    const productQtyPerRun = product.quantity;
    const totalUnitsProduced = productQtyPerRun * job.runs;
    
    if (totalUnitsProduced === 0) continue;

    const unitManufacturingCost = totalActualCost / totalUnitsProduced;

    // D. Create Normalized Object for ML API
    ledgerObjects.push({
      date: job.end_date,
      type_id: job.product_type_id,
      item_name: nameMap.get(job.product_type_id) || `Product ${job.product_type_id}`,
      qty: totalUnitsProduced,
      unit_value: '', 
      source: "INDUSTRY",
      contract_id: job.job_id,
      char: job.installer_id,
      unit_value_filled: unitManufacturingCost
    });
    
    newlyProcessedIds.push(job.job_id);
  }

  // 5. Upsert new rows using the ML API
  if (ledgerObjects.length > 0) {
    const writtenCount = ledgerAPI.upsert(['source', 'contract_id'], ledgerObjects);
    LOG_INDUSTRY.info(`Successfully processed and wrote ${writtenCount} new manufacturing jobs to the Material_Ledger.`);
  } else {
    LOG_INDUSTRY.info("Finished processing. No new rows to write to Material_Ledger.");
  }

  // 6. Save new state (processed job IDs)
  newlyProcessedIds.forEach(id => processedJobIds.add(id));
  const trimmedJobIds = Array.from(processedJobIds).slice(-1000);
  SCRIPT_PROP.setProperty(INDUSTRY_JOB_KEY, JSON.stringify(trimmedJobIds));
}


// ----------------------------------------------------------------------
// --- DATA HELPER FUNCTIONS (Consolidated) ---
// ----------------------------------------------------------------------

/**
 * Helper to get the current blended cost for all items (from Blended_Cost).
 */
function _getBlendedCostMap(ss) {
    const sheet = ss.getSheetByName("Blended_Cost");
    if (!sheet || sheet.getLastRow() < 2) { LOG_INDUSTRY.warn("Blended_Cost sheet is empty."); return new Map(); }

    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    try {
      const col = _getColIndexMap(headers, ['type_id', 'unit_weighted_average']);
      const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getMaxColumns()).getValues();
      const costMap = new Map();
      
      for (const row of data) {
          const type_id = Number(row[col.type_id]);
          const cost = Number(row[col.unit_weighted_average]); 
          if (!isNaN(type_id) && !isNaN(cost) && cost > 0) { costMap.set(type_id, cost); }
      }
      return costMap;
    } catch(e) { LOG_INDUSTRY.error(`Error reading Blended_Cost: ${e.message}`); return new Map(); }
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
    
    try {
        const matCol = _getColIndexMap(matHeaders, ['typeID', 'activityID', 'materialTypeID', 'quantity']);
        const prodCol = _getColIndexMap(prodHeaders, ['typeID', 'activityID', 'productTypeID', 'quantity']);

        const matData = matSheet.getRange(2, 1, matSheet.getLastRow() - 1, matSheet.getMaxColumns()).getValues();
        const prodData = prodSheet.getRange(2, 1, prodSheet.getLastRow() - 1, prodSheet.getMaxColumns()).getValues();

        const sdeMatMap = new Map();
        const sdeProdMap = new Map();

        // Process Materials (Activity 1 & 8)
        for (const row of matData) {
            const activityID = Number(row[matCol.activityID]);
            if (activityID !== INDUSTRY_ACTIVITY_MANUFACTURING && activityID !== INDUSTRY_ACTIVITY_INVENTION) continue; 
            
            const bp_type_id = Number(row[matCol.typeID]);
            const mat_type_id = Number(row[matCol.materialTypeID]);
            const qty = Number(row[matCol.quantity]);

            if (!sdeMatMap.has(bp_type_id)) { sdeMatMap.set(bp_type_id, []); }
            sdeMatMap.get(bp_type_id).push({ materialTypeID: mat_type_id, quantity: qty });
        }

        // Process Products (Activity 1 only)
        for (const row of prodData) {
            const activityID = Number(row[prodCol.activityID]);
            if (activityID !== INDUSTRY_ACTIVITY_MANUFACTURING) continue;
            
            const bp_type_id = Number(row[prodCol.typeID]);
            const prod_type_id = Number(row[prodCol.productTypeID]);
            const qty = Number(row[prodCol.quantity]);

            sdeProdMap.set(bp_type_id, { productTypeID: prod_type_id, quantity: qty });
        }

        return { sdeMatMap, sdeProdMap };
    } catch(e) { LOG_INDUSTRY.error(`Error reading SDE sheets: ${e.message}`); return { sdeMatMap: new Map(), sdeProdMap: new Map() }; }
}

/**
 * Helper to get item names from SDE_invTypes.
 */
function _getSdeNameMap(ss) {
    const sheet = ss.getSheetByName("SDE_invTypes");
    if (!sheet || sheet.getLastRow() < 2) { LOG_INDUSTRY.error("SDE_invTypes sheet is missing."); return new Map(); }
        
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    try {
        const col = _getColIndexMap(headers, ['typeID', 'typeName']);

        const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getMaxColumns()).getValues();
        const nameMap = new Map();
        
        for (const row of data) {
            const type_id = Number(row[col.typeID]);
            const type_name = row[col.typeName];
            if (!isNaN(type_id) && type_name) {
                nameMap.set(type_id, type_name);
            }
        }
        return nameMap;
    } catch(e) { LOG_INDUSTRY.error(`Error reading SDE_invTypes: ${e.message}`); return new Map(); }
}

/**
 * Helper to find new, completed jobs by activity.
 */
function _getNewCompletedJobs(ss, processedJobIds, activityIds) { 
  const sheet = ss.getSheetByName("ESI Corp Jobs");
  if (!sheet) { LOG_INDUSTRY.error("Cannot find 'ESI Corp Jobs' sheet!"); return []; }
  
  try {
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const requiredHeaders = ['job_id', 'activity_id', 'status', 'blueprint_type_id', 'product_type_id', 'runs', 'end_date', 'installer_id', 'cost', 'location_id'];
    const col = _getColIndexMap(headers, requiredHeaders);
    
    // Safe reading of data block
    let data = [];
    const lastRow = sheet.getLastRow();
    const numRows = lastRow - 1;
    if (numRows > 0) {
        data = sheet.getRange(2, 1, numRows, sheet.getMaxColumns()).getValues();
    }
    
    const newJobs = [];
    const activitySet = new Set(activityIds); 

    for (const row of data) {
      const job_id = Number(row[col.job_id]);
      const activity_id = Number(row[col.activity_id]);
      const status = row[col.status];

      if (status === 'delivered' && activitySet.has(activity_id) && !processedJobIds.has(job_id)) {
        newJobs.push({
          job_id: job_id, activity_id: activity_id,
          blueprint_type_id: Number(row[col.blueprint_type_id]),
          product_type_id: Number(row[col.product_type_id]),
          runs: Number(row[col.runs]),
          end_date: new Date(row[col.end_date]),
          installer_id: row[col.installer_id],
          cost: Number(row[col.cost]),
          location_id: Number(row[col.location_id])
        });
      }
    }
    return newJobs;
  } catch(e) { LOG_INDUSTRY.error(`Error reading ESI Corp Jobs: ${e.message}`); return []; }
}

/**
 * Helper to get BPC preset runs from a 'Config_BPC_Runs' sheet (with defaults).
 */
function _getConfigPresetRuns(ss) {
    const CONFIG_NAME = "Config_BPC_Runs";
    const CONFIG_HEADERS = ['bp_type_id', 'preset_runs'];
    const presetMap = new Map();
    
    const DEFAULT_PRESETS = [
        { id: 237, runs: 100 }, 
        { id: 3529, runs: 10 },
    ];

    const sheet = getOrCreateSheet(ss, CONFIG_NAME, CONFIG_HEADERS); 
    const lastRow = sheet ? sheet.getLastRow() : 0;
    
    if (lastRow >= 2) {
        const headers = sheet.getRange(1, 1, 1, sheet.getMaxColumns()).getValues()[0];
        try {
            const col = _getColIndexMap(headers, CONFIG_HEADERS);
            let data = [];
            const numRows = lastRow - 1;

            if (numRows > 0) { data = sheet.getRange(2, 1, numRows, sheet.getMaxColumns()).getValues(); }

            for (const row of data) {
                const bp_type_id = Number(row[col.bp_type_id]);
                const preset_runs = Number(row[col.preset_runs]);
                if (!isNaN(bp_type_id) && !isNaN(preset_runs) && preset_runs > 0) { presetMap.set(bp_type_id, preset_runs); }
            }
        } catch (e) { LOG_INDUSTRY.error(`Configuration Error in ${CONFIG_NAME}: ${e.message}`); }
    }
    
    // Apply Defaults
    if (presetMap.size === 0) {
        LOG_INDUSTRY.warn(`Config sheet '${CONFIG_NAME}' is empty. Applying ${DEFAULT_PRESETS.length} hardcoded defaults.`);
        DEFAULT_PRESETS.forEach(d => { presetMap.set(d.id, d.runs); });
    }

    return presetMap;
}

/**
 * Helper to get the Market Median price for BPO amortization fallback.
 */
function _getMarketMedianMap(ss) {
    const sheet = ss.getSheetByName("Market_Data_Raw");
    if (!sheet || sheet.getLastRow() < 2) { LOG_INDUSTRY.warn("Market_Data_Raw sheet is empty."); return new Map(); }

    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    try {
        const col = _getColIndexMap(headers, ['type_id', 'sell_median']);
        
        let data = [];
        const numRows = sheet.getLastRow() - 1;
        if (numRows > 0) { data = sheet.getRange(2, 1, numRows, sheet.getMaxColumns()).getValues(); }

        const medianMap = new Map();

        for (const row of data) {
            const type_id = Number(row[col.type_id]);
            const median_price = Number(row[col.sell_median]);
            if (!isNaN(type_id) && median_price > 0) { medianMap.set(type_id, median_price); }
        }
        return medianMap;
    } catch(e) { LOG_INDUSTRY.error(`Error reading Market_Data_Raw: ${e.message}`); return new Map(); }
}

/**
 * Helper to get the manual amortization surcharge for BPOs (Market Median Fallback).
 */
function _getBpoAmortizationMap(ss) {
    const AMORT_SHEET_NAME = "BPO_Amortization";
    const AMORT_HEADERS = ['bp_type_id', 'Amortization_Runs'];
    const amortMap = new Map();

    const sheet = getOrCreateSheet(ss, AMORT_SHEET_NAME, AMORT_HEADERS);
    const lastRow = sheet ? sheet.getLastRow() : 0;
    if (lastRow < 2) { LOG_INDUSTRY.error(`Sheet '${AMORT_SHEET_NAME}' has no data rows. Amortization is 0.`); return amortMap; }

    const blendedCostMap = _getBlendedCostMap(ss); 
    const marketMedianMap = _getMarketMedianMap(ss);
    
    const headers = sheet.getRange(1, 1, 1, sheet.getMaxColumns()).getValues()[0];
    try {
        const col = _getColIndexMap(headers, AMORT_HEADERS);
        const numRows = lastRow - 1;
        
        let data = [];
        if (numRows > 0) { data = sheet.getRange(2, 1, numRows, sheet.getMaxColumns()).getValues(); } 

        for (const row of data) {
            const bp_type_id = Number(row[col.bp_type_id]);
            const totalRuns = Number(row[col.Amortization_Runs]); 
            let bpoValue = 0;

            if (totalRuns <= 0) continue; 

            // 1. TRY PRIMARY: Blended Average
            bpoValue = blendedCostMap.get(bp_type_id) || 0;

            if (bpoValue === 0) {
                // 2. FALLBACK: Market Median
                bpoValue = marketMedianMap.get(bp_type_id) || 0;
                if (bpoValue > 0) { LOG_INDUSTRY.warn(`BPO ${bp_type_id}: Using Market Median for amortization.`); }
            }
            
            if (bpoValue > 0) {
                const surchargePerRun = bpoValue / totalRuns;
                amortMap.set(bp_type_id, surchargePerRun);
            } else { LOG_INDUSTRY.warn(`BPO ${bp_type_id}: No market value found. Amortization skipped.`); }
        }
        return amortMap;
    } catch(e) { LOG_INDUSTRY.error(`Configuration Error in ${AMORT_SHEET_NAME}: ${e.message}`); return new Map(); }
}

/**
 * Helper to get BPO/BPC efficiency attributes by calling GESI.corporation_blueprints() directly.
 */
function _getBpoAttributesMapFromEsi() {
    // NOTE: Assumes getCorpAuthChar() is defined and available.
    const authToon = getCorpAuthChar(); 
    const ENDPOINT = 'corporations_corporation_blueprints'; 

    if (!authToon) {
        LOG_INDUSTRY.error("Cannot resolve authorized corporation character for GESI call.");
        return new Map();
    }
    
    try {
        // Correctly processes the JSON-parsed array of objects returned by invokeRaw.
        const rawObjects = GESI.invokeRaw(
            ENDPOINT,
            {
                name: authToon, 
                show_column_headings: false, // Don't rely on headers in this low-level format
                version: null 
            }
        );
        
        if (!Array.isArray(rawObjects) || rawObjects.length === 0) { 
            LOG_INDUSTRY.error(`GESI.invokeRaw(${ENDPOINT}) returned no usable data or format was unexpected.`); 
            return new Map(); 
        }

        const attributesMap = new Map();

        // Process the array of objects directly (Property names are the ESI JSON keys)
        for (const bpObj of rawObjects) {
            const bp_type_id = Number(bpObj.type_id);
            const me = Number(bpObj.material_efficiency);
            const te = Number(bpObj.time_efficiency); 

            if (!isNaN(bp_type_id) && bp_type_id > 0) {
                attributesMap.set(bp_type_id, {
                    material_efficiency: me,
                    time_efficiency: te
                });
            }
        }
        
        LOG_INDUSTRY.info(`Loaded attributes for ${attributesMap.size} unique blueprints via Raw Invoke.`);
        return attributesMap;
    } catch(e) { 
        LOG_INDUSTRY.error(`Failed to invoke GESI endpoint ${ENDPOINT}: ${e.message}.`); 
        throw e;
    }
}