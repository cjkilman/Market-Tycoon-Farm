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
 * TEST FUNCTION: Calls the core job-fetching function, bypassing the 
 * spreadsheet cell lookup, to diagnose the character name/auth token issue.
 * * INSTRUCTIONS: 
 * 1. Replace "YOUR_AUTHORIZED_CHARACTER_NAME" below with the name you think is correct.
 * 2. Select this function (TEST_CORP_JOBS_AUTH) and click the Run button (▶️).
 * 3. Check the Logger (View -> Logs) for the result.
 */
function TEST_CORP_JOBS_AUTH() {

  // ⚠️ MANDATORY: REPLACE THIS PLACEHOLDER WITH THE NAME YOU ARE TESTING ⚠️
  // Example: const testToonName = "Jason Kilman";
  const testToonName = "Jason Kilman";

  const LOG = Logger;
  LOG.log(`--- Starting ESI Job Raw Data Test for: ${testToonName} ---`);

  // ----------------------------------------------------------------------------------
  // ⚠️ WARNING: This function relies on a temporary override of the global
  // getCorpAuthChar() function to force the script to use the test name. 
  // ----------------------------------------------------------------------------------

  // Temporarily define the problematic function to return the test name
  // This bypasses the spreadsheet lookup (e.g., 'Market Overview'!B10)
  const originalGetCorpAuthChar = (typeof getCorpAuthChar !== 'undefined') ? getCorpAuthChar : null;
  getCorpAuthChar = function () { return testToonName; };

  try {
    // Call the original helper function, forcing a live ESI fetch (true)
    const jobData = _getCorporateJobsRaw(true);

    if (jobData === null) {
      LOG.log("❌ TEST FAILED: _getCorporateJobsRaw returned NULL.");
      LOG.log("Reason: Check if GESI returned an error (403/420) or no data. The character name is likely misspelled, or the token is expired/missing scopes.");
    } else if (Array.isArray(jobData)) {
      LOG.log(`✅ TEST SUCCESS: ESI Client authorization worked! Fetched ${jobData.length} job records.`);
      if (jobData.length > 0) {
        LOG.log(`First Job Status: ${jobData[0].status}, Job ID: ${jobData[0].job_id}`);
      }
    } else {
      LOG.log("❌ TEST FAILED: Unexpected return type.");
    }

  } catch (e) {
    LOG.log(`❌ TEST FAILED with Script Error: ${e.message}`);
    LOG.log("ACTION: This is a coding/environment error, not an auth error. Check that all supporting functions (like _getAndDechunk) are in scope.");
  } finally {
    // Restore the original function definition
    if (originalGetCorpAuthChar) {
      getCorpAuthChar = originalGetCorpAuthChar;
    }
  }
}

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

  // ⚠️ CRITICAL STEP: The slow GESI Client call runs here (5 min timeout).
  try {
    log.info('Running background fetch and cache of ESI Corp Jobs...');
    _getCorporateJobsRaw(true); // Forces a fresh GESI Client call & cache write
    log.info('ESI Corp Jobs successfully cached.');
  } catch (e) {
    // If the GESI Client call fails (Authorization/Rate Limit), the script stops.
    log.error('Background ESI Corp Jobs fetch FAILED. Check Authorization/Scopes! Skipping Ledger update.', e);
    return;
  }
  // END CRITICAL STEP

  // --- STAGE 1 & 2 will now run quickly, reading the fast cache ---
  try {
    log.info('Running BPC Creation Ledger (Stage 1: Calculate WAC)...');
    runBpcCreationLedger(ss);
  } catch (e) {
    log.error('BPC Creation Ledger (Stage 1) FAILED. Subsequent costing may use stale BPC data.', e);
  }

  try {
    log.info('Running Manufacturing Ledger Update (Stage 2: COGS)...');
    // This function will now read the FAST ESI Corp Jobs data from the cache.
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

/**
 * Resets the script properties that track processed Industry Jobs and 
 * calculated BPC Weighted Average Costs (WAC).
 * This forces the Ledger to re-process all 'delivered' jobs and recalculate BPC costs.
 */
function resetIndustryLedgerProperties() {

  // Keys defined in IndustryLedger.gs.js
  const INDUSTRY_JOB_KEY = 'processedIndustryJobIds';
  const BPC_JOB_KEY = 'processedBpcJobIds';
  const BPC_WAC_KEY = 'BpcWeightedAverageCost';

  const props = PropertiesService.getScriptProperties();
  const keysToDelete = [INDUSTRY_JOB_KEY, BPC_JOB_KEY, BPC_WAC_KEY];
  let deletedCount = 0;

  const ui = SpreadsheetApp.getUi();

  try {
    // Deletes the tracking keys
    for (const key of keysToDelete) {
      if (props.getProperty(key) !== null) {
        props.deleteProperty(key);
        deletedCount++;
      }
    }

    // Success message for the user
    const message = `✅ Success! Deleted ${deletedCount} Industry Ledger properties. 
    The script will now re-process all delivered jobs and recalculate BPC costs on the next run.`;

    ui.alert('Ledger Reset Complete', message, ui.ButtonSet.OK);

  } catch (e) {
    ui.alert('Reset Failed', `An error occurred while deleting properties: ${e.message}`, ui.ButtonSet.OK);
  }
}

// Function runIndustryLedgerUpdate() 
function runIndustryLedgerUpdate() {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const SCRIPT_PROP = PropertiesService.getScriptProperties();

    // 2. Get SDE Data (Must come first to get recipes)
    const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
    if (sdeMatMap.size === 0) { LOG_INDUSTRY.warn("SDE sheets are empty. Skipping."); return; }
    
    const nameMap = _getSdeNameMap(ss);

    // 3. Find new completed manufacturing jobs (MUST be defined before we iterate over them)
    const processedJobIds = new Set(JSON.parse(SCRIPT_PROP.getProperty(INDUSTRY_JOB_KEY) || '[]'));
    const newJobs = _getNewCompletedJobs(ss, processedJobIds, [INDUSTRY_ACTIVITY_MANUFACTURING]);
    if (newJobs.length === 0) { LOG_INDUSTRY.info("No new manufacturing jobs to process."); return; }

    // ⚠️ CRITICAL FIX 1: Collect ALL required material IDs from the newly defined 'newJobs'
    const allRequiredMaterialIds = new Set();
    for (const job of newJobs) {
        const materials = sdeMatMap.get(job.blueprint_type_id);
        if (materials) {
            for (const mat of materials) {
                allRequiredMaterialIds.add(mat.materialTypeID);
            }
        }
    }
    
    // 4. Get Cost Data and Amortization/WAC
    // ⚠️ CRITICAL FIX 2: Pass the targeted material list to the cost map builder
    const costMap = _getBlendedCostMap(ss, Array.from(allRequiredMaterialIds));

    if (costMap.size === 0) { LOG_INDUSTRY.warn("Blended_Cost failed to populate any costs. Skipping."); return; }
    
    const amortMap = _getBpoAmortizationMap(ss); 
    const bpcWacData = JSON.parse(SCRIPT_PROP.getProperty(BPC_WAC_KEY) || '{}');
    const bpoAttributesMap = _getBpoAttributesMapFromEsi();
    
    const getBpcCostPerRun = (bpID) => {
      const cost = bpcWacData[bpID];
      return cost ? Number(cost) : 0;
    };
    
    // 5. Calculate cost and generate ledger row objects
    const ledgerObjects = [];
    const newlyProcessedIds = [];
    const ledgerAPI = ML.forSheet('Material_Ledger');

    // --- MAIN JOB PROCESSING LOOP (Now safe to iterate over newJobs) ---
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
        
        if (matCost === undefined || matCost === null || matCost === 0) { 
            // Now, if cost is 0, it means Tiers 0, 1, 2, and 3 all failed (unpricable item).
            LOG_INDUSTRY.warn(`Missing final cost for material ${mat.materialTypeID}. Cannot price job ${job.job_id}.`); 
            missingCost = true; 
            break; 
        }

        const finalQuantity = mat.quantity * materialDiscountFactor;
        totalMaterialCostPerRun += matCost * finalQuantity;
      }

      if (missingCost) continue;
      
      // B. Calculate Total Costs (Amortization + ISK Fee + Materials)
      const totalMaterialCostForAllRuns = totalMaterialCostPerRun * job.runs;
      const totalJobInstallationCost = job.cost;
      
      let amortizationSurcharge = 0;
      
      // BPO AMORTIZATION (Capital Cost)
      if (amortMap.has(job.blueprint_type_id)) {
          amortizationSurcharge = amortMap.get(job.blueprint_type_id) * job.runs;
      } 
      // BPC AMORTIZATION (Disposable Asset Cost)
      else {
          const bpcCostPerRun = getBpcCostPerRun(job.blueprint_type_id);
          amortizationSurcharge = bpcCostPerRun * job.runs;
      }

      // Define totalActualCost safely outside the if/else block
      const totalActualCost = totalMaterialCostForAllRuns + totalJobInstallationCost + amortizationSurcharge; 
      
      // C. Calculate Unit Cost
      const totalUnitsProduced = product.quantity * job.runs;
      if (totalUnitsProduced === 0) continue;
      const unitManufacturingCost = totalActualCost / totalUnitsProduced;

      // ... (Push to ledgerObjects and newlyProcessedIds) ...
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
    // --- END MAIN JOB PROCESSING LOOP ---

    // 6. Upsert and Save State
    if (ledgerObjects.length > 0) {
      const writtenCount = ledgerAPI.upsert(['source', 'contract_id'], ledgerObjects);
      LOG_INDUSTRY.info(`Successfully processed and wrote ${writtenCount} new manufacturing jobs to the Material_Ledger.`);
    } else {
      LOG_INDUSTRY.info("Finished processing. No new rows to write to Material_Ledger.");
    }

    newlyProcessedIds.forEach(id => processedJobIds.add(id));
    const trimmedJobIds = Array.from(processedJobIds).slice(-1000);
    SCRIPT_PROP.setProperty(INDUSTRY_JOB_KEY, JSON.stringify(trimmedJobIds));
}


// ----------------------------------------------------------------------
// --- DATA HELPER FUNCTIONS (Consolidated) ---
// ----------------------------------------------------------------------

// Function _getBlendedCostMap(ss, requiredMaterialIds) 
function _getBlendedCostMap(ss, requiredMaterialIds) {
    const sheet = ss.getSheetByName("Blended_Cost");
    const log = LoggerEx.withTag('BLENDED_COST_FALLBACK');
    
    // --- 1. Setup Tier 2/3 Settings & Acquisition Fee ---
    const marketMedianMap = _getMarketMedianMap(ss); // Tier 2: Local Tracker
    
    // ⚠️ CRITICAL ACQUISITION FEE LOGIC: (Based on 0 Standings, No Skills)
    // Broker Fee Base: 3.0% (Applies to Buy Orders)
    // Tax Rate Base: 7.5% (Applied here only because it is combined by user)
    const BROKER_FEE_RATE = _getNamedOr_('FEE_RATE', 0.03); // Default to 3.0%
    const TRANSACTION_TAX_RATE = _getNamedOr_('TAX_RATE', 0.075); // Default to 7.5%
    
    // Total Fee applied to acquisition cost (COGS)
    const TOTAL_ACQUISITION_FEE = BROKER_FEE_RATE + TRANSACTION_TAX_RATE;
    const ACQUISITION_MULTIPLIER = 1 + TOTAL_ACQUISITION_FEE;
    
    // ... (rest of the function remains the same) ...
    
    // --- EXECUTION POINT FOR TIER 2 FALLBACK ---
    // (Inside Phase 2 loops)
    if (marketCost > 0) {
        // APPLY COMBINED FEE to external cost data
        cost = marketCost * ACQUISITION_MULTIPLIER; 
        // ...
    }
    
    // --- EXECUTION POINT FOR TIER 3 FALLBACK ---
    // (Inside Phase 3 loops)
    if (rawCost > 0) {
        // APPLY COMBINED FEE TO FUZZWORK PRICE
        const finalCost = rawCost * ACQUISITION_MULTIPLIER; 
        // ...
    }
    // ... (rest of the function continues)
}

// NOTE: Since the ultimate goal is to fix the missing Datacore costs, and your
// Market Price Tracker already contains median buy data, using Tier 2 (marketMedianMap) 
// is the immediate solution. The Fuzzwork API logic is only necessary if Tier 2 fails. 
// Given the complexity of injecting batch API calls, implementing the robust Tier 2 fallback 
// is the optimal solution for stability.

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
  } catch (e) { LOG_INDUSTRY.error(`Error reading SDE sheets: ${e.message}`); return { sdeMatMap: new Map(), sdeProdMap: new Map() }; }
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
  } catch (e) { LOG_INDUSTRY.error(`Error reading SDE_invTypes: ${e.message}`); return new Map(); }
}

/**
 * Helper to find new, completed jobs by activity.
 */
function _getNewCompletedJobs(ss, processedJobIds, activityIds) {
  const sheet = ss.getSheetByName("ESI Corp Jobs");
  if (!sheet) { LOG_INDUSTRY.error("Cannot find 'ESI Corp Jobs' sheet!"); return []; }

  try {
    // ⚠️ CRITICAL CHANGE: Start reading from Column C (index 2)
    const START_COLUMN = 3; // C is the 3rd column (index 2)
    const MAX_COLUMNS = sheet.getLastColumn();
    const NUM_COLUMNS = MAX_COLUMNS - START_COLUMN + 1; // Number of columns to read

    // Read the Header Row (Row 1), starting from Column C
    const rawHeaders = sheet.getRange(1, START_COLUMN, 1, NUM_COLUMNS).getValues()[0];

    // Fill in the first two columns with placeholders so the dynamic mapper still works
    // 1. Read the entire header row. Columns A and B will be empty strings.
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];

    // 2. Map the required headers. The mapper will find them in column 2 (C) and onwards.
    const requiredHeaders = ['job_id', 'activity_id', 'status', 'blueprint_type_id', 'product_type_id', 'runs', 'end_date', 'installer_id', 'cost', 'location_id'];
    const col = _getColIndexMap(headers, requiredHeaders); // This should work, even with leading blanks.

    // Safe reading of data block
    let data = [];
    const lastRow = sheet.getLastRow();
    const numRows = lastRow - 1;

    if (numRows > 0) {
      // Read ALL data (including blank columns A and B) so the column indexes are correct
      data = sheet.getRange(2, 1, numRows, sheet.getMaxColumns()).getValues();
    }

    // The rest of the logic remains the same, using 'col' for indexing...

    const newJobs = [];
    const activitySet = new Set(activityIds);

    for (const row of data) {
      // Indexing now uses the column numbers retrieved by _getColIndexMap (plus the 2 blank cols)
      const job_id = Number(row[col.job_id]);
      // ... continue with the rest of your job processing logic ...

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
  } catch (e) { LOG_INDUSTRY.error(`Error reading ESI Corp Jobs: ${e.message}`); return []; }
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
  } catch (e) { LOG_INDUSTRY.error(`Error reading ${TRACKER_SHEET_NAME}: ${e.message}`); return new Map(); }
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
  } catch (e) {
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
 * This should permanently resolve the "No market value found" warnings.
 */
function _getBpoAmortizationMap(ss) {
  const AMORT_SHEET_NAME = "BPO_Amortization";
  const AMORT_HEADERS = ['bp_type_id', 'Amortization_Runs'];
  const amortMap = new Map();
  const log = LoggerEx.withTag('BPO_AMORT');

  // 1. Retrieve essential data maps (local prices)
  const { sdeProdMap } = _getSdeMaps(ss);
  const blendedCostMap = _getBlendedCostMap(ss);
  const marketMedianMap = _getMarketMedianMap(ss); // Reads 'market price Tracker'

  const sheet = getOrCreateSheet(ss, AMORT_SHEET_NAME, AMORT_HEADERS);
  const lastRow = sheet ? sheet.getLastRow() : 0;
  if (lastRow < 2) { log.error(`Sheet '${AMORT_SHEET_NAME}' has no data rows. Amortization is 0.`); return amortMap; }

  // --- FUZZWORK SETTINGS (TIER 3) ---
  // Location ID is explicitly read from Location List, cell C3
  const locationId = ss.getSheetByName('Location List').getRange('C3').getValue();
  // Market Type is read from the Named Range 'setting_market_range' (assuming _getNamedOr_ is available)
  const marketType = _getNamedOr_(ss, 'setting_market_range', 'region');
  const orderType = 'buy'; // Max Buy Order (highest realizable asset value)
  const orderLevel = 'max';
  // ---------------------------------

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

      // Check Tier 1 & 2 local caches
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
      // Fetch prices for missing items using Fuzzwork API
      // NOTE: Assumes fuzAPI.requestItems and _extractMetric_ are available globally
      const rawFuzResults = fuzAPI.requestItems(locationId, marketType, typeIdsToFetch);

      rawFuzResults.forEach(item => {
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
  } catch (e) {
    log.error(`Configuration Error in ${AMORT_SHEET_NAME}: ${e.message}`);
    throw e;
  }
}

/**
 * Utility to extract the price metric (min, max, median, etc.) 
 * from a single Fuzzwork aggregate object.
 * This function resolves the ReferenceError in the amortization loop.
 * * @param {Object} row - The full Fuzzwork aggregate object for one item ID.
 * @param {string} side - 'buy' or 'sell'.
 * @param {string} level - 'min', 'max', 'avg', 'median', 'volume'.
 * @returns {number} The numeric price, or 0 if not found.
 */
function _extractMetric_(row, side, level) {
  if (!row || !row[side]) return 0;

  const node = row[side];
  const v = node[level];
  const num = Number(v);

  // Return numeric price, or 0 if not found/invalid
  return Number.isFinite(num) ? num : 0;
}

/**
 * Helper to retrieve 30-day traded volume from the Publish_ESI_Region_market_orders sheet.
 * @returns {Map<number, number>} Map of type_id -> vol30_region (30-day traded volume)
 */
function _getMarketDemandMap(ss) {
  const TRACKER_SHEET_NAME = "Publish_ESI_Region_market_orders";
  const VOLUME_HEADER = 'vol30_region'; // Column header for 30-day volume

  const sheet = ss.getSheetByName(TRACKER_SHEET_NAME);
  const demandMap = new Map();

  if (!sheet || sheet.getLastRow() < 2) {
    LOG_INDUSTRY.error(`Sheet '${TRACKER_SHEET_NAME}' is missing or empty. Cannot calculate amortization runs.`);
    return demandMap;
  }

  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  try {
    const col = _getColIndexMap(headers, ['type_id', VOLUME_HEADER]);

    let data = [];
    const numRows = sheet.getLastRow() - 1;
    if (numRows > 0) { data = sheet.getRange(2, 1, numRows, sheet.getMaxColumns()).getValues(); }

    for (const row of data) {
      const type_id = Number(row[col.type_id]);
      // Clean volume string (removes commas) before converting to number
      const volumeStr = String(row[col[VOLUME_HEADER]]).replace(/,/g, '').trim();
      const volume = Number(volumeStr);

      if (!isNaN(type_id) && volume > 0) {
        demandMap.set(type_id, volume);
      }
    }
    return demandMap;
  } catch (e) {
    LOG_INDUSTRY.error(`Error reading market demand sheet: ${e.message}`);
    return demandMap;
  }
}


/**
 * Custom function to fetch Corporation Industry Jobs with caching (Memoization + CacheService).
 * Prevents continuous API calls during sheet recalculations.
 * * @param {string} name Character name with ESI Corp Jobs scope.
 * @param {boolean} [include_completed=false] Whether to include completed jobs.
 * @returns {any[][]} Raw data from GESI call.
 * @customfunction
 */
function CACHED_CORP_INDUSTRY_JOBS(name, include_completed) {
  // --- NEW: MAINTENANCE MODE CHECK ---
  // Check if globals are defined before trying to access them
  if (!SCRIPT_PROPS) {
    const systemState = SCRIPT_PROPS.getProperty(GLOBAL_STATE_KEY) || 'RUNNING';
  }
    if (systemState === 'MAINTENANCE') {
      return; // Return blank immediately
    }
  
  // --- END: MAINTENANCE MODE CHECK ---

  // Use a short, unique key for the ESI endpoint
  const CACHE_KEY_BASE = 'CorpIndJobsRaw';
  const CACHE_TTL = 300; // 5 minutes

  if (!name) return [['Error: Auth name required']];

  // 1. Determine Cache Key
  const key = CACHE_KEY_BASE + ':' + name + ':' + (include_completed ? 'C' : 'A');

  // 2. Try in-memory memoization (optional, but faster for immediate reuse)
  // NOTE: You would need to define a global map (e.g., _cijMemo) for this, 
  // but for simplicity, we focus on the script-level CacheService.
  const cache = CacheService.getUserCache();

  // 3. Check CacheService
  let jsonText = cache.get(key);
  if (jsonText) {
    // Return cached data
    return JSON.parse(jsonText);
  }

  // 4. Live API Call (Cache Miss)
  try {
    // NOTE: This assumes a globally accessible GESI object
    const rawData = GESI.corporations_corporation_industry_jobs(name, include_completed);

    // 5. Store in CacheService
    if (Array.isArray(rawData) && rawData.length > 0) {
      jsonText = JSON.stringify(rawData);
      cache.put(key, jsonText, CACHE_TTL);
    }

    return rawData;

  } catch (e) {
    // Log error and return a message to prevent the whole sheet from failing
    Logger.log(`ESI Corp Jobs GESI call failed: ${e.message}`);
    return [['ERROR', e.message]];
  }
}

// --- Add a new function similar to _getCorporateBlueprintsRaw ---

/**
 * Fetches and Caches the raw array of corporate industry jobs,
 * using sharding to bypass the "Argument too large" limitation.
 */
function _getCorporateJobsRaw(forceRefresh) {
  // NOTE: Assumes getCorpAuthChar() is available globally
  const authToon = getCorpAuthChar(); // <--- REPLACE THIS LINE 
  // const authToon = getCorpAuthChar(); // <-- Original line commented out

  const ENDPOINT = 'corporations_corporation_industry_jobs';
  const CACHE_KEY = 'CORP_JOBS_RAW_V1' + ':' + authToon;
  const CACHE_TTL = 300; // 5 minutes TTL

  if (!authToon || authToon === 'YOUR_AUTHORIZED_CHARACTER_NAME') { // Check for accidental use of placeholder
    // This returns null, causing the "Data not available" error.
    return null;
  }

  // 1. Attempt to read from cache (using de-chunking)
  if (!forceRefresh) {
    const cachedJson = _getAndDechunk(CACHE_KEY);
    if (cachedJson) { return JSON.parse(cachedJson); }
  }

  // 2. Live API Call
  try {
    // NOTE: This assumes a globally accessible GESI object
    const rawObjects = GESI.invokeRaw(
      ENDPOINT,
      {
        include_completed: true,
        name: authToon,
        show_column_headings: false,
        version: null
      }
    );

    if (!Array.isArray(rawObjects) || rawObjects.length === 0) { return null; }

    // 3. Store in cache (using chunking, resolves Argument too large)
    const rawJsonString = JSON.stringify(rawObjects);
    // NOTE: Uses the _chunkAndPut function already in IndustryLedger.gs.js
    _chunkAndPut(CACHE_KEY, rawJsonString, CACHE_TTL);

    return rawObjects;

  } catch (e) {
    // Add robust error logging
    Logger.log(`Failed to invoke GESI endpoint ${ENDPOINT} (Final Attempt): ${e.message}.`);
    return null;
  }
}

/**
 * TEST FUNCTION: Attempts a direct, uncached GESI call to corporate industry jobs.
 * This function bypasses sheet lookups and caching to test ESI authorization directly.
 * * Instructions: 
 * 1. REPLACE "YOUR_AUTHORIZED_CHARACTER_NAME" with the exact name you used for GESI authorization.
 * 2. Run this function (TEST_ESI_AUTH_STATUS) from the Apps Script editor.
 * 3. Check the Logger (View -> Logs) for the result.
 */
function TEST_ESI_AUTH_STATUS() {

  // ⚠️ MANDATORY: REPLACE THIS PLACEHOLDER WITH YOUR CHARACTER NAME
  const authToon = "YOUR_AUTHORIZED_CHARACTER_NAME";

  const ENDPOINT = 'corporations_corporation_industry_jobs';
  const LOG = Logger;

  if (authToon === "YOUR_AUTHORIZED_CHARACTER_NAME") {
    LOG.log("ERROR: Please replace the placeholder character name in the function.");
    return;
  }

  LOG.log(`--- Starting ESI Auth Test for: ${authToon} ---`);

  try {
    // Attempt the direct, raw ESI call
    const rawObjects = GESI.invokeRaw(
      ENDPOINT,
      {
        include_completed: true,
        name: authToon,
        show_column_headings: false,
        version: null
      }
    );

    if (Array.isArray(rawObjects) && rawObjects.length > 0) {
      LOG.log(`✅ SUCCESS! Found ${rawObjects.length} jobs.`);
      LOG.log("First job ID: " + rawObjects[0].job_id);
    } else if (Array.isArray(rawObjects) && rawObjects.length === 0) {
      LOG.log("✅ SUCCESS! The ESI call worked, but zero industry jobs were returned.");
    } else {
      LOG.log("❌ FAILURE: GESI returned data that was not an array (Check Logs for the actual error).");
    }

  } catch (e) {
    LOG.log(`❌ ESI CALL FAILED: ${e.message}`);

    if (e.message.includes("403")) {
      LOG.log("ACTION: This is an Authorization (403 Forbidden) error. Token is invalid or missing ESI scopes.");
      LOG.log("-> Go to GESI -> Authorize Character and re-authorize with ALL corporate scopes checked.");
    } else if (e.message.includes("420")) {
      LOG.log("ACTION: This is a Rate Limit (420) error. Wait 5 minutes before trying again.");
    } else {
      LOG.log("ACTION: The error is unknown. Check external network status.");
    }
  }
}

// --- Create a Custom Function for the ESI Corp Jobs sheet to use this raw data ---

// Function GESI_CORP_JOBS_CACHED:
function GESI_CORP_JOBS_CACHED(authCharName) {

  // ⚠️ CRITICAL CHANGE: Force to ONLY read from cache (false)
  // The live API call is now handled by the runIndustryLedgerPhase script.
  const rawData = _getCorporateJobsRaw(false);

  if (!rawData) {
    // Change the error to tell the user what to run
    return [['⚠️ DATA STALE: Run Ledger Script'], ['Data not found in cache.']];
  }

  // Convert array of objects to array of arrays (compatible with Sheet output)
  const headerRow = Object.keys(rawData[0] || {});
  const values = rawData.map(obj => headerRow.map(key => obj[key]));

  // Return the headers and the values
  return [headerRow, ...values];
}
