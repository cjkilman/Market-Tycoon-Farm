/**
 * IndustryLedger.gs.js
 *
 * This module is the Industry Ledger Add-on, built for robust COGS accounting.
 * It includes sharding utilities to bypass the Google Apps Script Cache limit.
 *
 * NOTE: This file assumes 'getOrCreateSheet', 'ML.forSheet', 'getCorpAuthChar', and 'LoggerEx' are in scope.
 */

// --- GLOBAL CONSTANTS ---
const INDUSTRY_JOB_KEY = 'processedIndustryJobIds';
const BPC_JOB_KEY = 'processedBpcJobIds';
const BPC_WAC_KEY = 'BpcWeightedAverageCost';

const INDUSTRY_ACTIVITY_MANUFACTURING = 1;
const INDUSTRY_ACTIVITY_COPYING = 5;
const INDUSTRY_ACTIVITY_INVENTION = 8;

// --- CACHE SHARDING CONSTANTS ---
const BPO_RAW_CACHE_KEY = 'BPO_RAW_INVENTORY_V1';
const BPO_RAW_CACHE_TTL = 3600; // 1 hour TTL
const MAX_CACHE_CHUNK_SIZE = 90000; // Max chars per chunk (staying below 100KB limit)
const CHUNK_INDEX_SUFFIX = ':IDX';

const LOG_INDUSTRY = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('IndustryLedger') : console);


// ----------------------------------------------------------------------
// --- CORE UTILITY: CACHE SHARDING FUNCTIONS (FIXES ARGUMENT TOO LARGE) ---
// ----------------------------------------------------------------------

/**
 * Stores a string of arbitrary length into the User Cache using multiple keys (sharding).
 */
function _chunkAndPut(key, largeString, ttl) {
    const cache = CacheService.getUserCache();
    const chunks = [];
    const numChunks = Math.ceil(largeString.length / MAX_CACHE_CHUNK_SIZE);
    
    // 1. Split the string into chunks
    for (let i = 0; i < numChunks; i++) {
        const start = i * MAX_CACHE_CHUNK_SIZE;
        const end = start + MAX_CACHE_CHUNK_SIZE;
        chunks.push(largeString.substring(start, end));
    }
    
    // 2. Build the map of keys to write (baseKey:0, baseKey:1, etc.)
    const keysToWrite = {};
    for (let i = 0; i < chunks.length; i++) {
        keysToWrite[key + ':' + i] = chunks[i];
    }
    // 3. Write a master index key containing the number of chunks
    keysToWrite[key + CHUNK_INDEX_SUFFIX] = String(numChunks);

    // 4. Put all chunks and the index key into the cache
    cache.putAll(keysToWrite, ttl);
}

/**
 * Retrieves a large sharded string from the User Cache and reconstructs it.
 */
function _getAndDechunk(key) {
    const cache = CacheService.getUserCache();
    
    // 1. Get the index key to find the number of chunks
    const numChunksRaw = cache.get(key + CHUNK_INDEX_SUFFIX);
    if (!numChunksRaw) {
        return null;
    }
    const numChunks = parseInt(numChunksRaw, 10);

    // 2. Build the list of keys to retrieve
    const keysToGet = [];
    for (let i = 0; i < numChunks; i++) {
        keysToGet.push(key + ':' + i);
    }

    // 3. Get all chunks
    const chunks = cache.getAll(keysToGet);

    // 4. Reconstruct the string
    const result = [];
    for (let i = 0; i < numChunks; i++) {
        const chunk = chunks[key + ':' + i];
        if (chunk == null) {
            // If any chunk is missing, the data is corrupt/expired
            return null; 
        }
        result.push(chunk);
    }

    return result.join('');
}


// ----------------------------------------------------------------------
// --- CORE UTILITY: DYNAMIC HEADER MAPPING (FIXES SCOPE/REFERENCE) ---
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
    const product = prodMap.get(job.blueprint_type_id);

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
 * FIX: Reads from 'market price Tracker' using 'Median Sell'.
 */
function _getMarketMedianMap(ss) {
    const TRACKER_SHEET_NAME = "market price Tracker";
    const MEDIAN_HEADER = 'Median Sell';
    const ID_HEADER = 'type_id_filtered';
    
    const sheet = ss.getSheetByName(TRACKER_SHEET_NAME);
    if (!sheet || sheet.getLastRow() < 2) { 
        LOG_INDUSTRY.warn(`Sheet '${TRACKER_SHEET_NAME}' is empty or missing. Cannot provide median price fallback.`);
        return new Map(); 
    }

    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    try {
        const col = _getColIndexMap(headers, [ID_HEADER, MEDIAN_HEADER]);
        
        let data = [];
        const numRows = sheet.getLastRow() - 1;
        if (numRows > 0) { data = sheet.getRange(2, 1, numRows, sheet.getMaxColumns()).getValues(); }

        const medianMap = new Map();

        for (const row of data) {
            const type_id = Number(row[col[ID_HEADER]]);
            // Price cleanup: Remove ISK and commas
            const priceStr = String(row[col[MEDIAN_HEADER]]).replace(/ISK/gi, '').replace(/,/g, '').trim(); 
            const median_price = Number(priceStr);

            if (!isNaN(type_id) && median_price > 0) { medianMap.set(type_id, median_price); }
        }
        return medianMap;
    } catch(e) { LOG_INDUSTRY.error(`Error reading ${TRACKER_SHEET_NAME}: ${e.message}`); return new Map(); }
}

/**
 * Helper to get the manual amortization surcharge for BPOs.
 * Implements a three-tiered pricing fallback: Blended > Tracker Median > Fuzzwork API.
 */
function _getBpoAmortizationMap(ss) {
    const AMORT_SHEET_NAME = "BPO_Amortization";
    const AMORT_HEADERS = ['bp_type_id', 'Amortization_Runs'];
    const amortMap = new Map();
    const log = LoggerEx.withTag('BPO_AMORT');

    // 1. Retrieve essential data maps (Assuming SDE functions are available)
    const { sdeProdMap } = _getSdeMaps(ss);
    const blendedCostMap = _getBlendedCostMap(ss); 
    const marketMedianMap = _getMarketMedianMap(ss);
    
    const sheet = getOrCreateSheet(ss, AMORT_SHEET_NAME, AMORT_HEADERS);
    const lastRow = sheet ? sheet.getLastRow() : 0;
    if (lastRow < 2) { log.error(`Sheet '${AMORT_SHEET_NAME}' has no data rows. Amortization is 0.`); return amortMap; }

    // --- NAMED RANGE SETTINGS FOR FUZZWORK FALLBACK (Tier 3) ---
    // NOTE: Assumes _getNamedOr_ is available globally
    const locationId = _getNamedOr_('setting_sell_loc', 60003760); // Default to Jita 4-4
    const marketType = _getNamedOr_('setting_market_list', 'region'); 
    // We want the highest price a buyer is offering (Max Buy Order)
    const orderType = 'buy'; 
    const orderLevel = 'max'; 
    // -----------------------------------------------------------

    const headers = sheet.getRange(1, 1, 1, sheet.getMaxColumns()).getValues()[0];
    try {
        const col = _getColIndexMap(headers, AMORT_HEADERS);
        const numRows = lastRow - 1;
        
        let data = [];
        if (numRows > 0) { data = sheet.getRange(2, 1, numRows, sheet.getMaxColumns()).getValues(); } 

        const typeIdsToFetch = [];
        const amortizationData = [];

        // --- PHASE 1: IDENTIFY MISSING PRICES & BUILD DATA STRUCTURE ---
        for (const row of data) {
            const bp_type_id = Number(row[col.bp_type_id]);
            const totalRuns = Number(row[col.Amortization_Runs]); 
            if (totalRuns <= 0) continue; 
            
            // Map BPO Asset ID to final Product Item ID
            const productObj = sdeProdMap.get(bp_type_id);
            if (!productObj) {
                log.warn(`BPO ${bp_type_id}: Cannot find manufactured product in SDE. Skipping.`);
                continue;
            }
            const product_id = productObj.productTypeID;
            
            const localValue = blendedCostMap.get(product_id) || marketMedianMap.get(product_id) || 0;
            
            // Collect data needed for final calculation pass
            amortizationData.push({ bpId: bp_type_id, productId: product_id, runs: totalRuns, localValue: localValue });
            
            // If local value is zero (Tier 1 & 2 failed), add to the Fuzzwork fetch list
            if (localValue === 0) {
                typeIdsToFetch.push(product_id);
            }
        }
        
        // --- PHASE 2: EXECUTE FUZZWORK API FALLBACK (Tier 3) ---
        let fuzzworkPrices = new Map();
        if (typeIdsToFetch.length > 0) {
             // NOTE: This assumes 'fuzAPI.requestItems' is defined in FuzzApiPrice.js
             // We use a simplified form that only requests the items and processes the price directly.
             const rawFuzResults = fuzAPI.requestItems(locationId, marketType, typeIdsToFetch);
             
             // Process the raw results to get the requested metric (Max Buy)
             rawFuzResults.forEach(item => {
                 // Assumes _extractMetric_ is available in the GESI Extentions/FuzzApiPrice file
                 const price = _extractMetric_(item, orderType, orderLevel);
                 if (price > 0) {
                    fuzzworkPrices.set(item.type_id, price);
                 }
             });
             log.info(`Fetched ${fuzzworkPrices.size} fallback prices from Fuzzwork API (Tier 3).`);
        }

        // --- PHASE 3: FINAL CALCULATION PASS ---
        for (const item of amortizationData) {
            let bpoValue = item.localValue;
            
            // Check Tier 3: External Fuzzwork API (Only runs if localValue was 0)
            if (bpoValue === 0) {
                bpoValue = fuzzworkPrices.get(item.productId) || 0;
                if (bpoValue > 0) {
                    log.warn(`BPO ${item.bpId}: Using external Fuzzwork API fallback price.`);
                }
            }
            
            // Final Amortization Assignment
            if (bpoValue > 0) {
                const surchargePerRun = bpoValue / item.runs;
                amortMap.set(item.bpId, surchargePerRun);
            } else { 
                log.warn(`BPO ${item.bpId}: No market value found (All sources failed). Amortization skipped.`); 
            }
        }
        return amortMap;
    } catch(e) { 
        log.error(`Configuration Error in ${AMORT_SHEET_NAME}: ${e.message}`); 
        throw e;
    }
}

/**
 * Helper to get BPO/BPC efficiency attributes by calling GESI.corporation_blueprints() directly.
 * * Uses the shared, cached data source.
 */
function _getBpoAttributesMapFromEsi() {
    const rawObjects = _getCorporateBlueprintsRaw(); 
    
    if (!rawObjects) { 
        LOG_INDUSTRY.error("Blueprint Raw Data Fetch failed. Cannot calculate attributes."); 
        return new Map(); 
    }

    const attributesMap = new Map();

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
    
    LOG_INDUSTRY.info(`Loaded attributes for ${attributesMap.size} unique blueprints.`);
    return attributesMap;
}

/**
 * SHARED FUNCTION: Fetches and Caches the raw array of corporate blueprints (BPOs/BPCs) 
 * using sharding to bypass the "Argument too large" limitation.
 */
function _getCorporateBlueprintsRaw(forceRefresh) {
    const log = LoggerEx.withTag('BPO_DATA');
    const authToon = getCorpAuthChar(); 
    const ENDPOINT = 'corporations_corporation_blueprints';
    const userCache = CacheService.getUserCache();

    if (!authToon) {
        log.error("Cannot resolve authorized corporation character.");
        return null;
    }

    const cacheKey = BPO_RAW_CACHE_KEY + ':' + authToon;
    
    // 1. Attempt to read from cache (using de-chunking)
    if (!forceRefresh) {
        const cachedJson = _getAndDechunk(cacheKey);
        if (cachedJson) {
            log.info("Blueprints fetched from User Cache (De-chunked).");
            return JSON.parse(cachedJson);
        }
    }

    // 2. Live API Call (If not in cache or forced)
    try {
        log.info("Fetching corporate blueprints via GESI.invokeRaw (Live API call).");
        
        const rawObjects = GESI.invokeRaw(
            ENDPOINT,
            {
                name: authToon,
                show_column_headings: false,
                version: null
            }
        );

        if (!Array.isArray(rawObjects) || rawObjects.length === 0) {
            log.error(`GESI.invokeRaw(${ENDPOINT}) returned no usable data.`);
            return null;
        }

        // 3. Store in cache (using chunking)
        const rawJsonString = JSON.stringify(rawObjects);
        _chunkAndPut(cacheKey, rawJsonString, BPO_RAW_CACHE_TTL);
        
        return rawObjects;

    } catch (e) {
        log.error(`Failed to invoke GESI endpoint ${ENDPOINT} (Final Attempt): ${e.message}.`);
        throw e;
    }
}

/**
 * Automatically populates Column A (bp_type_id) of the BPO_Amortization sheet 
 * with all Blueprint Originals (BPOs) owned by the corporation.
 */
function autofillBpoAmortizationInventory() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const log = LoggerEx.withTag('BPO_AMORT');
    
    const AMORT_SHEET_NAME = "BPO_Amortization";
    const AMORT_HEADERS = ['bp_type_id', 'Amortization_Runs'];

    try {
        // 1. Get ALL Blueprints using the shared, cached function (FORCING refresh)
        const allBlueprints = _getCorporateBlueprintsRaw(true); 

        if (!allBlueprints || allBlueprints.length === 0) {
            log.error("Failed to fetch blueprint data for inventory sync. Aborting.");
            return 0;
        }

        // 2. Filter for Blueprint Originals (BPOs)
        const uniqueBpoTypeIds = new Set();
        for (const bp of allBlueprints) {
            if (Number(bp.runs) === -1) { 
                const typeId = Number(bp.type_id);
                if (typeId > 0) {
                    uniqueBpoTypeIds.add(typeId);
                }
            }
        }
        
        const bpoTypeIds = Array.from(uniqueBpoTypeIds).sort((a, b) => a - b);
        
        if (bpoTypeIds.length === 0) {
            log.warn("Found no Blueprint Originals (BPOs) in the corporation inventory.");
            return 0;
        }

        // 3. Prepare data for sheet rewrite (Preserve existing runs)
        const sheet = getOrCreateSheet(ss, AMORT_SHEET_NAME, AMORT_HEADERS);
        const lastRow = sheet.getLastRow();
        const existingValues = sheet.getRange(2, 1, Math.max(1, lastRow - 1), sheet.getMaxColumns()).getValues();
        
        const existingRunsMap = new Map();
        if (lastRow > 1) {
            existingValues.forEach(row => {
                const existingId = Number(row[0]);
                if (existingId > 0 && row[1] != null && row[1] !== "") { 
                    existingRunsMap.set(existingId, row[1]);
                }
            });
        }
        
        // 4. Build Final Data (ID + Preserve existing Runs or set to 0)
        const finalData = bpoTypeIds.map(id => [
            id,
            existingRunsMap.get(id) || 0 
        ]);

        // 5. Clear and Rewrite
        if (lastRow > 1) {
            sheet.getRange(2, 1, lastRow - 1, sheet.getMaxColumns()).clearContent();
        }
        
        if (finalData.length > 0) {
            sheet.getRange(2, 1, finalData.length, 2).setValues(finalData);
            log.info(`Successfully synchronized ${finalData.length} BPO Type IDs from inventory to ${AMORT_SHEET_NAME}.`);
            return finalData.length;
        } else {
            return 0;
        }

    } catch (e) {
        log.error(`autofillBpoAmortizationInventory FAILED: ${e.message}`);
        throw e;
    }
}

/**
 * FINALIZED MASTER FUNCTION: Executes the complete BPO Amortization setup.
 * 1. Synchronizes BPO list (Inventory).
 * 2. Calculates economic lifespan (Market Demand) and overwrites Column B.
 */
function runBpoAmortizationSetupAndCalculate() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const log = LoggerEx.withTag('BPO_AMORT');
    
    const AMORT_SHEET_NAME = "BPO_Amortization";
    const AMORT_HEADERS = ['bp_type_id', 'Amortization_Runs'];
    const MARKET_SHARE_TARGET = 0.10; 
    const PRODUCTION_WINDOW_MONTHS = 12; 

    log.info('--- Starting BPO Amortization Setup (One-Step Sync) ---');

    // --- PHASE 1: INVENTORY SYNCHRONIZATION (Initializes Column A) ---
    const allBlueprints = _getCorporateBlueprintsRaw(true); 
    if (!allBlueprints || allBlueprints.length === 0) {
        log.error("Failed to fetch blueprint data. Aborting.");
        return 0;
    }

    const uniqueBpoTypeIds = new Set();
    for (const bp of allBlueprints) {
        if (Number(bp.runs) === -1) { 
            uniqueBpoTypeIds.add(Number(bp.type_id));
        }
    }
    const bpoTypeIds = Array.from(uniqueBpoTypeIds).filter(id => id > 0).sort((a, b) => a - b);
    const sheet = getOrCreateSheet(ss, AMORT_SHEET_NAME, AMORT_HEADERS);
    const lastRow = sheet.getLastRow();
    
    // Preserve existing data for column B values
    const existingRunsMap = new Map();
    if (lastRow > 1) {
        const existingValues = sheet.getRange(2, 1, lastRow - 1, sheet.getMaxColumns()).getValues();
        existingValues.forEach(row => {
            const existingId = Number(row[0]);
            if (existingId > 0 && row[1] != null && row[1] !== "") { 
                existingRunsMap.set(existingId, row[1]);
            }
        });
    }
    
    // Build Initial Data (IDs + Preserved/Zero Runs)
    const syncedData = bpoTypeIds.map(id => [
        id,
        existingRunsMap.get(id) || 0 // Initializing to 0 if no value found
    ]);

    // Write back the clean, synchronized list (Column A updated)
    if (lastRow > 1) { sheet.getRange(2, 1, lastRow - 1, sheet.getMaxColumns()).clearContent(); }
    if (syncedData.length === 0) {
         log.warn("No BPOs found to synchronize. Setup complete (but empty).");
         return 0;
    }
    sheet.getRange(2, 1, syncedData.length, 2).setValues(syncedData);
    log.info(`Synchronized ${syncedData.length} BPO Type IDs. Now calculating lifespan...`);


    // --- PHASE 2: ECONOMIC LIFESPAN CALCULATION (OVERWRITES COLUMN B) ---
    const demandMap = _getMarketDemandMap(ss);
    if (demandMap.size === 0) {
        log.error("Could not retrieve market demand volume. Cannot calculate Amortization Runs.");
        return 0;
    }

    const finalData = [];
    for (const row of syncedData) { // Use the freshly synchronized data
        const bp_type_id = Number(row[0]);
        const demandVolume = demandMap.get(bp_type_id) || 0;
        let newRuns = 0;
        
        if (demandVolume > 0) {
            // Calculation: (30-Day Volume / 30 days) * 365 days * 10% market share
            const calculatedRuns = Math.round(
                (demandVolume / 30) * 365 * MARKET_SHARE_TARGET
            );
            
            // Set a minimum floor of 100 runs if calculated value is tiny
            newRuns = Math.max(100, calculatedRuns); 
        } else {
            // If market data is missing, we use the existing value (which may be 0) or set the floor to 100
            newRuns = row[1] > 0 ? row[1] : 100; 
        }

        finalData.push([bp_type_id, newRuns]); 
    }

    // --- PHASE 3: FINAL WRITE (Atomic Update) ---
    // Overwrite the synchronized data with the final calculated runs
    sheet.getRange(2, 1, finalData.length, 2).setValues(finalData);
    log.info(`Amortization setup complete. Calculated and wrote economic lifespan for ${finalData.length} BPOs.`);
    
    return finalData.length;
}


/**
 * Helper to get the manual amortization surcharge for BPOs.
 * Implements a three-tiered pricing fallback: Blended > Tracker Median > Fuzzwork API.
 */
function _getBpoAmortizationMap(ss) {
    const AMORT_SHEET_NAME = "BPO_Amortization";
    const AMORT_HEADERS = ['bp_type_id', 'Amortization_Runs'];
    const amortMap = new Map();
    const log = LoggerEx.withTag('BPO_AMORT');

    // 1. Retrieve essential data maps (Assuming SDE functions are available)
    const { sdeProdMap } = _getSdeMaps(ss);
    const blendedCostMap = _getBlendedCostMap(ss); 
    const marketMedianMap = _getMarketMedianMap(ss);
    
    const sheet = getOrCreateSheet(ss, AMORT_SHEET_NAME, AMORT_HEADERS);
    const lastRow = sheet ? sheet.getLastRow() : 0;
    if (lastRow < 2) { log.error(`Sheet '${AMORT_SHEET_NAME}' has no data rows. Amortization is 0.`); return amortMap; }

    // --- NAMED RANGE SETTINGS FOR FUZZWORK FALLBACK (Tier 3) ---
    // NOTE: Assumes _getNamedOr_ is available globally
    const locationId = _getNamedOr_('setting_sell_loc', 60003760); // Default to Jita 4-4
    const marketType = _getNamedOr_('setting_market_list', 'region'); 
    // We want the highest price a buyer is offering (Max Buy Order)
    const orderType = 'buy'; 
    const orderLevel = 'max'; 
    // -----------------------------------------------------------

    const headers = sheet.getRange(1, 1, 1, sheet.getMaxColumns()).getValues()[0];
    try {
        const col = _getColIndexMap(headers, AMORT_HEADERS);
        const numRows = lastRow - 1;
        
        let data = [];
        if (numRows > 0) { data = sheet.getRange(2, 1, numRows, sheet.getMaxColumns()).getValues(); } 

        const typeIdsToFetch = [];
        const amortizationData = [];

        // --- PHASE 1: IDENTIFY MISSING PRICES & BUILD DATA STRUCTURE ---
        for (const row of data) {
            const bp_type_id = Number(row[col.bp_type_id]);
            const totalRuns = Number(row[col.Amortization_Runs]); 
            if (totalRuns <= 0) continue; 
            
            // Map BPO Asset ID to final Product Item ID
            const productObj = sdeProdMap.get(bp_type_id);
            if (!productObj) {
                log.warn(`BPO ${bp_type_id}: Cannot find manufactured product in SDE. Skipping.`);
                continue;
            }
            const product_id = productObj.productTypeID;
            
            const localValue = blendedCostMap.get(product_id) || marketMedianMap.get(product_id) || 0;
            
            // Collect data needed for final calculation pass
            amortizationData.push({ bpId: bp_type_id, productId: product_id, runs: totalRuns, localValue: localValue });
            
            // If local value is zero (Tier 1 & 2 failed), add to the Fuzzwork fetch list
            if (localValue === 0) {
                typeIdsToFetch.push(product_id);
            }
        }
        
        // --- PHASE 2: EXECUTE FUZZWORK API FALLBACK (Tier 3) ---
        let fuzzworkPrices = new Map();
        if (typeIdsToFetch.length > 0) {
             // NOTE: This assumes 'fuzAPI.requestItems' is defined in FuzzApiPrice.js
             // We use a simplified form that only requests the items and processes the price directly.
             const rawFuzResults = fuzAPI.requestItems(locationId, marketType, typeIdsToFetch);
             
             // Process the raw results to get the requested metric (Max Buy)
             rawFuzResults.forEach(item => {
                 // Assumes _extractMetric_ is available in the GESI Extentions/FuzzApiPrice file
                 const price = _extractMetric_(item, orderType, orderLevel);
                 if (price > 0) {
                    fuzzworkPrices.set(item.type_id, price);
                 }
             });
             log.info(`Fetched ${fuzzworkPrices.size} fallback prices from Fuzzwork API (Tier 3).`);
        }

        // --- PHASE 3: FINAL CALCULATION PASS ---
        for (const item of amortizationData) {
            let bpoValue = item.localValue;
            
            // Check Tier 3: External Fuzzwork API (Only runs if localValue was 0)
            if (bpoValue === 0) {
                bpoValue = fuzzworkPrices.get(item.productId) || 0;
                if (bpoValue > 0) {
                    log.warn(`BPO ${item.bpId}: Using external Fuzzwork API fallback price.`);
                }
            }
            
            // Final Amortization Assignment
            if (bpoValue > 0) {
                const surchargePerRun = bpoValue / item.runs;
                amortMap.set(item.bpId, surchargePerRun);
            } else { 
                log.warn(`BPO ${item.bpId}: No market value found (All sources failed). Amortization skipped.`); 
            }
        }
        return amortMap;
    } catch(e) { 
        log.error(`Configuration Error in ${AMORT_SHEET_NAME}: ${e.message}`); 
        throw e;
    }
}