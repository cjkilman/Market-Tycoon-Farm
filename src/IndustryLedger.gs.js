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

const INDUSTRY_JOB_PHASE = 'IndustryJobPhase';
const SOFT_TIME_LIMIT_MS = 280000; // 4 minutes 40 seconds soft limit

// --- CACHE SHARDING CONSTANTS ---
const BPO_RAW_CACHE_KEY = 'BPO_RAW_INVENTORY_V1';
const BPO_RAW_CACHE_TTL = 3600; // 1 hour TTL


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
  // 1. Input Validation for 'headers'
  if (!headers || !Array.isArray(headers) || headers.length === 0) {
    throw new Error("Invalid argument: 'headers' must be a non-empty array.");
  }

  // 2. Input Validation for 'requiredHeaders'
  if (!requiredHeaders || !Array.isArray(requiredHeaders) || requiredHeaders.length === 0) {
    throw new Error("Invalid argument: 'requiredHeaders' must be a non-empty array.");
  }

  const col = {};

  // 3. CRITICAL ROBUSTNESS FIX (The "Empty String Gate"): 
  // Ensures 'h' is treated as a safe string ("") if it's null, undefined, or empty, 
  // preventing the 'h.toLowerCase is not a function' crash.
  const lowerCaseHeaders = headers.map(h => String(h || '').toLowerCase().trim());

  for (const req of requiredHeaders) {
    // Normalize and clean the required header name.
    const cleanReq = String(req || '').toLowerCase().trim();

    if (cleanReq === '') continue; // Skip if the requirement itself is blank.

    const index = lowerCaseHeaders.indexOf(cleanReq);
    if (index === -1) {
      throw new Error(`CRITICAL HEADER ERROR: Sheet is missing required column "${cleanReq}".`);
    }
    col[req] = index;
  }
  return col;
}


// ----------------------------------------------------------------------
// --- MASTER ADD-ON INTEGRATION ---
// ----------------------------------------------------------------------

// src/IndustryLedger.gs.js

/**
 * Executes the full two-stage Industry Ledger process under the main system lock.
 * Now acts as a resumable state machine to prevent exceeding maximum execution time.
 */
function runIndustryLedgerPhase(ss) {
  const log = LoggerEx.withTag('MASTER_SYNC');
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  // --- CRITICAL SAFETY GUARD: WAIT FOR CONTRACT PRICING ---
  // If the Contract Unit Cost worker is still calculating, we must wait.
  // Otherwise, we risk reading "Zero Cost" materials and ruining the WAC.
  const cogsState = SCRIPT_PROP.getProperty('cogsJobStep'); // PROP_KEY_COGS_STEP
  if (cogsState === 'FINALIZING') {
    log.warn('Skipping Industry Ledger Phase: Contract COGS calculation is pending.');
    return;
  }
  const START_TIME = new Date().getTime();

  log.info('--- Starting Industry Ledger Phase (BPC Costing & Manufacturing COGS) ---');

  // Read current phase, defaults to 0 (Fetch)
  let phase = parseInt(SCRIPT_PROP.getProperty(INDUSTRY_JOB_PHASE) || '0', 10);

  if (phase === 0) {
    // Phase 0: FETCH ESI DATA (Must complete before next steps)
    try {
      log.info('Phase 0: Running background fetch and cache of ESI Corp Jobs...');
      _getCorporateJobsRaw(true); // Forces a fresh GESI Client call & cache write
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '1'); // Advance to next phase
      phase = 1;
    } catch (e) {
      log.error('Phase 0 (Fetch) FAILED. Check Authorization/Scopes!', e);
      return; // Exit on hard failure
    }
  }

  if (phase === 1) {
    // Phase 1: BPC COSTING (Time-gated)
    if (Date.now() - START_TIME > SOFT_TIME_LIMIT_MS) {
      log.warn('Phase 1 (BPC Costing) skipped: Execution time limit pending. Reschedule.');
      return; // Exit gracefully, property holds '1'
    }
    try {
      log.info('Phase 1: Running BPC Creation Ledger (Stage 1: Calculate WAC)...');
      runBpcCreationLedger(ss);
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '2'); // Advance to next phase
      phase = 2;
    } catch (e) {
      log.error('Phase 1 (BPC Costing) FAILED:', e);
      // Exit, property holds '2', so next run attempts Phase 2.
    }
  }

  if (phase === 2) {
    // Phase 2: MANUFACTURING COGS (Time-gated)
    if (Date.now() - START_TIME > SOFT_TIME_LIMIT_MS) {
      log.warn('Phase 2 (Manufacturing COGS) skipped: Execution time limit pending. Reschedule.');
      return; // Exit gracefully, property holds '2'
    }
    try {
      log.info('Phase 2: Running Manufacturing Ledger Update (Stage 2: COGS)...');
      runIndustryLedgerUpdate(ss);
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '3'); // Advance to cleanup
      phase = 3;
    } catch (e) {
      log.error('Phase 2 (Manufacturing COGS) FAILED:', e);
      // Exit, property holds '3', so next run attempts Phase 3.
    }
  }

  if (phase === 3) {
    // Phase 3: CLEANUP (Final step)
    SCRIPT_PROP.deleteProperty(INDUSTRY_JOB_PHASE);
    log.info('Phase 3: Cleanup complete.');
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
 * SAFE VERSION: Uses console logs instead of UI Alerts to prevent crash.
 */
function resetIndustryLedgerProperties() {

  // Keys defined in IndustryLedger.gs.js
  const INDUSTRY_JOB_KEY = 'processedIndustryJobIds';
  const BPC_JOB_KEY = 'processedBpcJobIds';
  const BPC_WAC_KEY = 'BpcWeightedAverageCost';

  const props = PropertiesService.getScriptProperties();
  const keysToDelete = [INDUSTRY_JOB_KEY, BPC_JOB_KEY, BPC_WAC_KEY];
  let deletedCount = 0;

  try {
    // Deletes the tracking keys
    for (const key of keysToDelete) {
      if (props.getProperty(key) !== null) {
        props.deleteProperty(key);
        deletedCount++;
      }
    }

    // Log success to the console instead of trying to popup a UI alert
    console.log(`✅ Success! Deleted ${deletedCount} Industry Ledger properties.`);
    console.log(`The script will now re-process all delivered jobs and recalculate BPC costs on the next run.`);

  } catch (e) {
    console.error(`Reset Failed: An error occurred while deleting properties: ${e.message}`);
  }
}

// Function runIndustryLedgerUpdate() 
function runIndustryLedgerUpdate() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  // 1. Get SDE Data (Recipes/Material Requirements)
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  if (sdeMatMap.size === 0) { LOG_INDUSTRY.warn("SDE sheets are empty. Skipping."); return; }

  const nameMap = _getSdeNameMap(ss);

  // 2. Find new completed manufacturing jobs (DEFINES 'newJobs')
  const processedJobIds = new Set(JSON.parse(SCRIPT_PROP.getProperty(INDUSTRY_JOB_KEY) || '[]'));
  const newJobs = _getNewCompletedJobs(ss, processedJobIds, [INDUSTRY_ACTIVITY_MANUFACTURING]);
  if (newJobs.length === 0) { LOG_INDUSTRY.info("No new manufacturing jobs to process."); return; }

  // 3. CRITICAL: Collect ALL unique material IDs from the newly defined 'newJobs'
  //    (This block is now correctly placed *after* newJobs is defined)
  const allRequiredMaterialIds = new Set();
  for (const job of newJobs) {
    const materials = sdeMatMap.get(job.blueprint_type_id);
    if (materials) {
      for (const mat of materials) {
        allRequiredMaterialIds.add(mat.materialTypeID);
      }
    }
  }

  // 4. Get Cost Data (Tier 0-3 costMap built here)
  const costMap = _getBlendedCostMap(ss, Array.from(allRequiredMaterialIds));

  if (costMap.size === 0) { LOG_INDUSTRY.warn("Blended_Cost failed to populate any costs. Skipping."); return; }

  // 5. Setup Amortization/BPO Data (Needs costMap defined)
  const amortMap = _getBpoAmortizationMap(ss);
  const bpcWacData = JSON.parse(SCRIPT_PROP.getProperty(BPC_WAC_KEY) || '{}');
  const bpoAttributesMap = _getBpoAttributesMapFromEsi();

  const getBpcCostPerRun = (bpID) => {
    const cost = bpcWacData[bpID];
    return cost ? Number(cost) : 0;
  };

  // 6. Start Processing Jobs
  const ledgerObjects = [];
  const newlyProcessedIds = [];
  const ledgerAPI = ML.forSheet('Material_Ledger');

  // --- MAIN JOB PROCESSING LOOP ---
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

    // D. Write to Ledger
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

  // 7. Upsert and Save State
  if (ledgerObjects.length > 0) {
    const result = ledgerAPI.upsert(['source', 'contract_id'], ledgerObjects);
    LOG_INDUSTRY.info(`Successfully processed and wrote ${result.rows} new manufacturing jobs to the Material_Ledger.`);
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
/**
 * Helper to get the current blended cost for all items, 
 * implementing a Three-Tier Cost Fallback based on required material IDs.
 * * @param {object} ss - SpreadsheetApp object.
 * @param {number[]} requiredMaterialIds - Array of ALL unique material IDs needed for current jobs.
 */
function _getBlendedCostMap(ss, requiredMaterialIds) {
  const sheet = ss.getSheetByName("Blended_Cost");
  const log = LoggerEx.withTag('BLENDED_COST_FALLBACK');

  // --- 1. Setup Tier 2/3 Settings & Acquisition Fee ---
  const marketMedianMap = _getMarketMedianMap(ss); // Tier 2: Local Tracker

  // ⚠️ CRITICAL ACQUISITION FEE LOGIC: (Based on 0 Standings, No Skills)
  const BROKER_FEE_RATE = Number(_getNamedOr_('FEE_RATE', 0.03)); // Default to 3.0%
  const TRANSACTION_TAX_RATE = Number(_getNamedOr_('TAX_RATE', 0.075)); // Default to 7.5%
  const TOTAL_ACQUISITION_FEE = BROKER_FEE_RATE + TRANSACTION_TAX_RATE;
  const ACQUISITION_MULTIPLIER = 1 + TOTAL_ACQUISITION_FEE;
  log.info(`Using ACQUISITION_MULTIPLIER: ${ACQUISITION_MULTIPLIER} (Broker: ${BROKER_FEE_RATE}, Tax: ${TRANSACTION_TAX_RATE})`);

  // Fuzzwork Settings (Tier 3)
  const defaultRegionId = 10000002; // Jita Region ID
  const locationIdRaw = ss.getSheetByName('Location List').getRange('C3').getValue();
  const locationId = (locationIdRaw && locationIdRaw > 0) ? locationIdRaw : defaultRegionId;
  const marketType = 'region';     // Force the broadest search
  const orderType = 'buy';         // Look for Buy Orders
  const orderLevel = 'max';        // Get the MAX price

  const allItemCosts = new Map(); // Final map of type_id -> cost
  const tier3FetchList = new Set(); // List of items requiring API call

  // --- 2. PHASE 1: Read Tier 1 (Blended Cost) ---
  if (sheet && sheet.getLastRow() >= 2) {
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    try {
      const col = _getColIndexMap(headers, ['type_id', 'unit_weighted_average']);
      const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getMaxColumns()).getValues();

      for (const row of data) {
        const type_id = Number(row[col.type_id]);
        let cost = Number(row[col.unit_weighted_average]);

        if (!isNaN(type_id) && cost > 0) {
          allItemCosts.set(type_id, cost); // Tier 1 cost found, NO tax applied
        }
      }
    } catch (e) { log.error(`Error reading Blended_Cost: ${e.message}`); }
  } else {
    log.warn("Blended_Cost sheet is empty or missing. Bypassing Tier 1.");
  }

  // --- 3. PHASE 2: Check ALL required material IDs against known costs ---
  const neededIds = new Set(requiredMaterialIds);

  for (const type_id of neededIds) {
    if (allItemCosts.has(type_id)) {
      continue; // Tier 1 cost is already set
    }

    // Tier 2 Check (Items NOT found in Blended_Cost)
    const marketCost = marketMedianMap.get(type_id) || 0;

    if (marketCost > 0) {
      // TIER 2 FALLBACK: Market Tracker cost found. APPLY ACQUISITION FEE.
      const costWithFee = marketCost * ACQUISITION_MULTIPLIER;
      allItemCosts.set(type_id, costWithFee);
      log.info(`Applied Broker Fee to Tier 2 cost for ${type_id}.`);
    } else {
      // Tier 1 and Tier 2 both failed. ADD TO TIER 3 LIST.
      tier3FetchList.add(type_id);
    }
  }

  // --- 4. PHASE 3: Execute Fuzzwork API Fallback (Tier 3) ---
  if (tier3FetchList.size > 0) {
    const idsArray = Array.from(tier3FetchList);
    log.info(`Attempting Tier 3 fallback for ${idsArray.length} items via Fuzzwork API.`);

    try {
      const rawFuzResults = fuzAPI.requestItems(locationId, marketType, idsArray);

      rawFuzResults.forEach(item => {
        const rawCost = _extractMetric_(item, orderType, orderLevel);

        if (rawCost > 0) {
          // ⚠️ CRITICAL FIX: APPLY ACQUISITION FEE TO FUZZWORK PRICE
          const finalCost = rawCost * ACQUISITION_MULTIPLIER;
          allItemCosts.set(item.type_id, finalCost);
          log.info(`Resolved cost for ${item.type_id} using Fuzzwork (Tier 3) + Fee: ${finalCost}`);
        } else {
          log.warn(`Fuzzwork returned zero cost for material ${item.type_id}. Final cost is 0.`);
        }
      });
    } catch (e) {
      log.error(`Fuzzwork Tier 3 API call failed: ${e.message}`);
    }
  }

  // 5. Final filter: return only items with a positive cost
  const finalCostMap = new Map();
  for (const [type_id, cost] of allItemCosts.entries()) {
    if (cost > 0) { finalCostMap.set(type_id, cost); }
  }

  return finalCostMap;
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
 * FIX: Now reads directly from the robust, cached ESI data array (bypassing the slow spreadsheet sheet).
 */
function _getNewCompletedJobs(ss, processedJobIds, activityIds) {
  // ss parameter is retained for compatibility but not used for sheet reading.

  // 1. Get raw job data (reads from cache/live API via helper)
  // _getCorporateJobsRaw(false) ensures we use the CACHED data from Phase 0.
  const rawJobData = _getCorporateJobsRaw(false);

  // 2. Attempt 2: Lazy Load (Self-Healing)
  // If cache is empty, force a live fetch immediately.
  if (!rawJobData) {
    LOG_INDUSTRY.info("Industry Cache Miss. Triggering self-healing LIVE fetch...");
    try {
      rawJobData = _getCorporateJobsRaw(true); // Force Refresh
    } catch (e) {
      LOG_INDUSTRY.error("Self-healing fetch failed: " + e.message);
    }
  }

  // 3. Final Data Check
  if (!rawJobData || rawJobData.length === 0) {
    LOG_INDUSTRY.warn("Error reading ESI Corp Jobs: No job data found (Cache & Live failed).");
    return [];
  }

  // 2. Define the expected keys (headers) and activity set
  const newJobs = [];
  const activitySet = new Set(activityIds);

  // 3. Process the array of objects directly from the API helper
  for (const jobObj of rawJobData) {
    const job_id = Number(jobObj.job_id);

    // Filter by status, activity, and whether it has been processed
    if (jobObj.status === 'delivered' &&
      activitySet.has(Number(jobObj.activity_id)) &&
      !processedJobIds.has(job_id)) {

      // Data is already an array of objects, just validate and push
      newJobs.push({
        job_id: job_id,
        activity_id: Number(jobObj.activity_id),
        blueprint_type_id: Number(jobObj.blueprint_type_id),
        product_type_id: Number(jobObj.product_type_id),
        runs: Number(jobObj.runs),
        end_date: new Date(jobObj.end_date),
        installer_id: jobObj.installer_id,
        cost: Number(jobObj.cost),
        location_id: Number(jobObj.location_id)
      });
    }
  }

  return newJobs;
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
 * Helper to get NPC Base Prices from SDE_invTypes.
 */
function _getSdeBasePriceMap(ss) {
  const sheet = ss.getSheetByName("SDE_invTypes");
  if (!sheet || sheet.getLastRow() < 2) return new Map();

  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  try {
    // Look for the 'basePrice' column you just added
    const col = _getColIndexMap(headers, ['typeID', 'basePrice']);

    // Read Data
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getMaxColumns()).getValues();
    const priceMap = new Map();

    for (const row of data) {
      const type_id = Number(row[col.typeID]);
      const price = Number(row[col.basePrice]);
      if (!isNaN(type_id) && !isNaN(price)) {
        priceMap.set(type_id, price);
      }
    }
    return priceMap;
  } catch (e) {
    LOG_INDUSTRY.warn(`SDE Base Price lookup failed (Column missing?): ${e.message}`);
    return new Map();
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

      // FIX 1: Update this floor to 10,000 (was 100)
newRuns = Math.max(10000, calculatedRuns); 
    } else {
      // FIX 2: Default to 50,000 (Force update, ignore existing low values)
      newRuns = 50000; 
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
 * Implements a three-tiered pricing fallback: Blended > Tracker Median > SDE Base Price > Fuzzwork API.
 * FIX: Restored SDE Base Price lookup to correctly value NPC-seeded BPOs.
 * FIX: Corrected _getNamedOr_ usage to match Utility.js (2 arguments).
 */
function _getBpoAmortizationMap(ss) {
  const AMORT_SHEET_NAME = "BPO_Amortization";
  const AMORT_HEADERS = ['bp_type_id', 'Amortization_Runs'];
  const amortMap = new Map();
  const log = LoggerEx.withTag('BPO_AMORT');

  // 1. Retrieve essential data maps
  const sdePriceMap = _getSdeBasePriceMap(ss); // RESTORED: Critical for NPC BPO prices
  const blendedCostMap = _getBlendedCostMap(ss);
  const marketMedianMap = _getMarketMedianMap(ss);

  // 2. Setup Settings
  // FIX: _getNamedOr_ in Utility.js only takes 2 arguments (name, fallback).
  // We must NOT pass 'ss' as the first argument.
  const locationId = Number(_getNamedOr_('setting_sell_loc', 60003760)); // Default Jita
  const marketType = _getNamedOr_('setting_market_list', 'region');
  const orderType = 'sell'; // For BPOs, we usually care about Sell price (Acquisition)
  const orderLevel = 'min';

  const sheet = getOrCreateSheet(ss, AMORT_SHEET_NAME, AMORT_HEADERS);
  const lastRow = sheet ? sheet.getLastRow() : 0;
  if (lastRow < 2) { 
    log.error(`Sheet '${AMORT_SHEET_NAME}' has no data rows. Amortization is 0.`); 
    return amortMap; 
  }

  const headers = sheet.getRange(1, 1, 1, sheet.getMaxColumns()).getValues()[0];
  
  try {
    const col = _getColIndexMap(headers, AMORT_HEADERS);
    const data = sheet.getRange(2, 1, lastRow - 1, sheet.getMaxColumns()).getValues();

    const typeIdsToFetch = [];
    const amortizationData = [];

    // --- PHASE 1: IDENTIFY MISSING PRICES ---
    for (const row of data) {
      const bp_type_id = Number(row[col.bp_type_id]);
      const totalRuns = Number(row[col.Amortization_Runs]);
      if (totalRuns <= 0) continue;

      // PRIORITY: 
      // 1. Blended Cost (If you manually bought/tracked the BPO)
      // 2. Market Median (If listed on your tracker)
      // 3. SDE Base Price (The NPC Sell Price - MOST COMMON for T1 BPOs)
      const localValue = blendedCostMap.get(bp_type_id) ||
                         marketMedianMap.get(bp_type_id) ||
                         sdePriceMap.get(bp_type_id) || 0;

      amortizationData.push({ bpId: bp_type_id, runs: totalRuns, localValue: localValue });

      // If still 0, add to Fuzzwork fetch list (Tier 4)
      if (localValue === 0) {
        typeIdsToFetch.push(bp_type_id);
      }
    }

    // --- PHASE 2: FUZZWORK API FALLBACK (Tier 4) ---
    let fuzzworkPrices = new Map();
    if (typeIdsToFetch.length > 0) {
      try {
        const rawFuzResults = fuzAPI.requestItems(locationId, marketType, typeIdsToFetch);
        rawFuzResults.forEach(item => {
          const price = _extractMetric_(item, orderType, orderLevel);
          if (price > 0) fuzzworkPrices.set(item.type_id, price);
        });
        log.info(`Fetched ${fuzzworkPrices.size} fallback BPO prices from Fuzzwork.`);
      } catch (e) {
        log.warn(`Fuzzwork BPO lookup failed: ${e.message}`);
      }
    }

    // --- PHASE 3: CALCULATE SURCHARGE ---
    for (const item of amortizationData) {
      let bpoValue = item.localValue;

      if (bpoValue === 0) {
        bpoValue = fuzzworkPrices.get(item.bpId) || 0;
      }

      if (bpoValue > 0) {
        // Amortization = BPO Cost / Total Lifetime Runs
        const surchargePerRun = bpoValue / item.runs;
        amortMap.set(item.bpId, surchargePerRun);
      } else {
        // Warn only once per batch to avoid log spam
         log.warn(`BPO ${item.bpId}: Zero value found. Surcharge = 0.`);
      }
    }
    
    return amortMap;

  } catch (e) {
    log.error(`Error in _getBpoAmortizationMap: ${e.message}`);
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
 * Custom function to fetch Corporation Industry Jobs with caching.
 * NOW: Checks Cache First -> If Empty, Pulls Live Data.
 * * @param {string} name Character name with ESI Corp Jobs scope.
 * @param {boolean} [include_completed=false] Whether to include completed jobs.
 * @returns {any[][]} Raw data from GESI call.
 * @customfunction
 */
function GESI_CORP_JOBS_CACHED(name, include_completed) {
  const GLOBAL_STATE_KEY = 'GLOBAL_SYSTEM_STATE';

  // START LOGGING & PARAM CHECK
  Logger.log(`[CIJ_SHEET] START: Name='${name}', Completed=${include_completed}`);

  if (!name) {
    Logger.log('[CIJ_SHEET] FAIL: Name is missing.');
    return [['Error: Auth name required']];
  }

  // ROBUST MAINTENANCE CHECK
  const systemState = PropertiesService.getScriptProperties().getProperty(GLOBAL_STATE_KEY) || 'RUNNING';
  if (systemState === 'MAINTENANCE') {
    Logger.log(`[CIJ_SHEET] ABORT: System is in MAINTENANCE mode.`);
    return [['MAINTENANCE_ACTIVE']];
  }

  // 1. ATTEMPT 1: READ FROM CACHE (Fast)
  let rawData = _getCorporateJobsRaw(false);

  // 2. ATTEMPT 2: LIVE FETCH (If Cache Miss)
  if (!rawData) {
    Logger.log('[CIJ_SHEET] Cache Miss. Triggering LIVE fetch.');
    try {
      // Force a live refresh. This may take a few seconds.
      rawData = _getCorporateJobsRaw(true);
    } catch (e) {
      Logger.log(`[CIJ_SHEET] Live fetch failed: ${e.message}`);
      return [['ERROR'], [`Live fetch failed: ${e.message}`]];
    }
  }

  // 3. FINAL DATA CHECK
  if (!rawData || rawData.length === 0) {
    Logger.log('[CIJ_SHEET] WARN: No data found after cache check and live fetch.');
    return [['NO_DATA'], ['No industry jobs found.']];
  }

  // 4. FORMAT OUTPUT (Array of Objects -> Array of Arrays)
  try {
    const headerRow = Object.keys(rawData[0] || {});

    if (headerRow.length === 0) {
      Logger.log('[CIJ_SHEET] ERROR: Raw data object structure is invalid (no headers).');
      return [['ERROR: Invalid Data Structure']];
    }

    // Map the array of objects to an array of arrays for sheet compatibility
    const values = rawData.map(obj => headerRow.map(key => obj[key]));

    Logger.log(`[CIJ_SHEET] SUCCESS: Returning ${values.length} jobs.`);

    // Return the headers and the values
    return [headerRow, ...values];
  } catch (e) {
    Logger.log(`[CIJ_SHEET] ERROR: Formatting failed: ${e.message}`);
    return [['ERROR', `Formatting failed: ${e.message}`]];
  }
}

/**
 * Fetches and Caches the raw array of corporate industry jobs,
 * using sharding to bypass the "Argument too large" limitation.
 */
function _getCorporateJobsRaw(forceRefresh) {
  // NOTE: Assumes getCorpAuthChar() is available globally
  const authToon = getCorpAuthChar();

  const ENDPOINT = 'corporations_corporation_industry_jobs';
  const CACHE_KEY = 'CORP_JOBS_RAW_V1' + ':' + authToon;
  const CACHE_TTL = 3600; // 60 minutes TTL (was 300)

  if (!authToon || authToon === 'YOUR_AUTHORIZED_CHARACTER_NAME') { // Check for accidental use of placeholder
    // This returns null, causing the "Data not available" error.
    return null;
  }

  // 1. Attempt to read from cache (using de-chunking)
  if (!forceRefresh) {
    const cachedJson = _getAndDechunk(CACHE_KEY);
    if (cachedJson) { return JSON.parse(cachedJson); }

    // FIX: Explicitly return null if cache is missed and we are only checking the cache.
    return null;
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






