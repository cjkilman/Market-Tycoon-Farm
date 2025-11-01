/**
 * IndustryLedger.gs.js
 *
 * This module is the Industry Ledger Add-on. It executes in two stages:
 * 1. Process BPC creation (Copy/Invention) jobs to calculate the cost-per-run (WAC).
 * 2. Process Manufacturing jobs, applying BPO/BPC costs and ME material savings,
 * and writes final COGS data to the Material_Ledger via the ML API.
 *
 * NOTE: This file assumes 'getOrCreateSheet', 'ML', 'LoggerEx', and GESI are in scope.
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
// --- CORE UTILITY: DYNAMIC HEADER MAPPING (MUST BE AT TOP) ---
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
 * Utility function to dynamically find column indices by header name.
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

/**
 * REVISED HELPER (Raw Invoke Corrected): Pulls ME/TE attributes by directly invoking the ESI endpoint.
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

// ... (Rest of the helper functions: _getBlendedCostMap, _getSdeMaps, _getSdeNameMap, _getNewCompletedJobs, _getConfigPresetRuns, _getMarketMedianMap, _getBpoAmortizationMap) ...