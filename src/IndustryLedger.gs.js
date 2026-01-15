/**
 * IndustryLedger.gs.js
 *
 * This module is the Industry Ledger Add-on, built for robust COGS accounting.
 * It includes sharding utilities to bypass the Google Apps Script Cache limit.
 *
 * FIX APPLIED: Added strict Activity ID filtering to prevent Invention materials (Datacores)
 * from polluting Manufacturing job costs.
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
// --- LOCAL HELPER: ROBUST NAMED RANGE LOOKUP ---
// ----------------------------------------------------------------------

function _getNamedOr_(arg1, arg2, arg3) {
  let ss, name, fallback;
  if (typeof arg1 === 'object' && arg1 !== null) {
    ss = arg1; name = arg2; fallback = arg3;
  } else {
    ss = SpreadsheetApp.getActiveSpreadsheet(); name = arg1; fallback = arg2;
  }
  try {
    const range = ss.getRangeByName(name);
    if (!range) return fallback;
    const val = range.getValue();
    return (val === '' || val === null || val === undefined) ? fallback : val;
  } catch (e) { return fallback; }
}



// ----------------------------------------------------------------------
// --- CORE UTILITY: DYNAMIC HEADER MAPPING ---
// ----------------------------------------------------------------------

function _getColIndexMap(headers, requiredHeaders) {
  if (!headers || !Array.isArray(headers) || headers.length === 0) throw new Error("Headers must be a non-empty array.");
  const col = {};
  const lowerCaseHeaders = headers.map(h => String(h || '').toLowerCase().trim());
  for (const req of requiredHeaders) {
    const cleanReq = String(req || '').toLowerCase().trim();
    if (cleanReq === '') continue;
    const index = lowerCaseHeaders.indexOf(cleanReq);
    if (index === -1) throw new Error(`CRITICAL: Sheet is missing required column "${cleanReq}".`);
    col[req] = index;
  }
  return col;
}

// ----------------------------------------------------------------------
// --- MASTER ADD-ON INTEGRATION ---
// ----------------------------------------------------------------------

function runIndustryLedgerPhase(ss) {
  const log = LoggerEx.withTag('MASTER_SYNC');
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  const cogsState = SCRIPT_PROP.getProperty('cogsJobStep');
  if (cogsState === 'FINALIZING') {
    log.warn('Skipping Industry Ledger Phase: Contract COGS calculation is pending.');
    return;
  }
  const START_TIME = new Date().getTime();

  log.info('--- Starting Industry Ledger Phase ---');

  let phase = parseInt(SCRIPT_PROP.getProperty(INDUSTRY_JOB_PHASE) || '0', 10);

  if (phase === 0) {
    try {
      log.info('Phase 0: Fetching ESI Corp Jobs...');
      _getCorporateJobsRaw(true);
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '1');
      phase = 1;
    } catch (e) {
      log.error('Phase 0 (Fetch) FAILED.', e);
      return;
    }
  }

  if (phase === 1) {
    if (Date.now() - START_TIME > SOFT_TIME_LIMIT_MS) return;
    try {
      log.info('Phase 1: Running BPC Creation Ledger...');
      runBpcCreationLedger(ss);
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '2');
      phase = 2;
    } catch (e) {
      log.error('Phase 1 FAILED:', e);
    }
  }

  if (phase === 2) {
    if (Date.now() - START_TIME > SOFT_TIME_LIMIT_MS) return;
    try {
      log.info('Phase 2: Running Manufacturing Ledger Update...');
      runIndustryLedgerUpdate(ss);
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '3');
      phase = 3;
    } catch (e) {
      log.error('Phase 2 FAILED:', e);
    }
  }

  if (phase === 3) {
    SCRIPT_PROP.deleteProperty(INDUSTRY_JOB_PHASE);
    log.info('Phase 3: Cleanup complete.');
  }
}

// ----------------------------------------------------------------------
// --- STAGE 1: BPC Cost Calculation ---
// ----------------------------------------------------------------------

function runBpcCreationLedger() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  LOG_INDUSTRY.info("Running BPC Creation Ledger (Stage 1)...");

  const costMap = _getBlendedCostMap(ss);
  const presetRunsMap = _getConfigPresetRuns(ss);
  const { sdeMatMap } = _getSdeMaps(ss);

  const processedJobIds = new Set(JSON.parse(SCRIPT_PROP.getProperty(BPC_JOB_KEY) || '[]'));
  const newBpcJobs = _getNewCompletedJobs(ss, processedJobIds, [INDUSTRY_ACTIVITY_COPYING, INDUSTRY_ACTIVITY_INVENTION]);
  
  if (newBpcJobs.length === 0) {
    LOG_INDUSTRY.info("No new BPC creation jobs.");
    return;
  }

  const bpcCostMap = new Map();
  const newlyProcessedIds = [];

  for (const job of newBpcJobs) {
    let totalMaterialCost = 0;
    let missingCost = false;

    // FIX: Ensure we only charge materials if the JOB ACTIVITY matches the MATERIAL ACTIVITY
    if (job.activity_id === INDUSTRY_ACTIVITY_INVENTION) {
      const materials = sdeMatMap.get(job.blueprint_type_id);
      if (materials) {
        for (const mat of materials) {
          // CRITICAL FILTER: Only include Invention Materials (Activity 8)
          if (mat.activityID !== INDUSTRY_ACTIVITY_INVENTION) continue;

          const matCost = costMap.get(mat.materialTypeID);
          if (matCost === undefined || matCost === null) {
            missingCost = true;
            break;
          }
          totalMaterialCost += (matCost * mat.quantity) * job.runs;
        }
      }
    }

    if (missingCost) continue;

    const presetRuns = presetRunsMap.get(job.blueprint_type_id) || 1;
    const totalActualCost = totalMaterialCost + job.cost;
    const totalRunsProduced = job.runs * presetRuns;

    if (totalRunsProduced === 0) continue;

    const bpID = job.blueprint_type_id;
    if (!bpcCostMap.has(bpID)) bpcCostMap.set(bpID, { totalCost: 0, totalRuns: 0 });

    const currentData = bpcCostMap.get(bpID);
    currentData.totalCost += totalActualCost;
    currentData.totalRuns += totalRunsProduced;

    newlyProcessedIds.push(job.job_id);
  }

  // Retrieve existing history or initialize
  const historyData = JSON.parse(SCRIPT_PROP.getProperty('BpcHistoryData') || '{}');
  const finalWAC = JSON.parse(SCRIPT_PROP.getProperty(BPC_WAC_KEY) || '{}');

  for (const [bpID, data] of bpcCostMap.entries()) {
    // Get existing totals
    const existing = historyData[bpID] || { totalCost: 0, totalRuns: 0 };
    
    // Add new batch to existing totals
    existing.totalCost += data.totalCost;
    existing.totalRuns += data.totalRuns;
    
    // Save back to history object
    historyData[bpID] = existing;
    
    // Calculate TRUE weighted average
    finalWAC[bpID] = existing.totalCost / existing.totalRuns;
  }

  // Save both properties
  SCRIPT_PROP.setProperty('BpcHistoryData', JSON.stringify(historyData));
  SCRIPT_PROP.setProperty(BPC_WAC_KEY, JSON.stringify(finalWAC));

  newlyProcessedIds.forEach(id => processedJobIds.add(id));
  SCRIPT_PROP.setProperty(BPC_JOB_KEY, JSON.stringify(Array.from(processedJobIds).slice(-1000)));
}

// ----------------------------------------------------------------------
// --- STAGE 2: Manufacturing Ledger ---
// ----------------------------------------------------------------------

function runIndustryLedgerUpdate() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  if (sdeMatMap.size === 0) { LOG_INDUSTRY.warn("SDE Sheets empty."); return; }

  const nameMap = _getSdeNameMap(ss);
  const processedJobIds = new Set(JSON.parse(SCRIPT_PROP.getProperty(INDUSTRY_JOB_KEY) || '[]'));
  const newJobs = _getNewCompletedJobs(ss, processedJobIds, [INDUSTRY_ACTIVITY_MANUFACTURING]);
  
  if (newJobs.length === 0) {
    LOG_INDUSTRY.info("No new manufacturing jobs.");
    return;
  }

  // 3. Collect ALL unique material IDs (Filtered by Activity)
  const allRequiredMaterialIds = new Set();
  for (const job of newJobs) {
    const materials = sdeMatMap.get(job.blueprint_type_id);
    if (materials) {
      for (const mat of materials) {
        // FIX: Only fetch costs for Manufacturing Materials
        if (mat.activityID === INDUSTRY_ACTIVITY_MANUFACTURING) {
          allRequiredMaterialIds.add(mat.materialTypeID);
        }
      }
    }
  }

  const costMap = _getBlendedCostMap(ss, Array.from(allRequiredMaterialIds));
  const amortMap = _getBpoAmortizationMap(ss);
  const bpcWacData = JSON.parse(SCRIPT_PROP.getProperty(BPC_WAC_KEY) || '{}');
  const bpoAttributesMap = _getBpoAttributesMapFromEsi();

  const getBpcCostPerRun = (bpID) => {
    const cost = bpcWacData[bpID];
    return cost ? Number(cost) : 0;
  };

  const ledgerObjects = [];
  const newlyProcessedIds = [];
  const ledgerAPI = ML.forSheet('Material_Ledger');

  for (const job of newJobs) {
    const materials = sdeMatMap.get(job.blueprint_type_id);
    const product = sdeProdMap.get(job.blueprint_type_id);

    if (!materials || !product) { LOG_INDUSTRY.warn(`Missing SDE data for job ${job.job_id}. Skipping.`); continue; }

    // Debugging Header
    const debugTag = `[JOB-${job.job_id}]`;

    const bpoItemAttributes = bpoAttributesMap.get(job.blueprint_type_id);
    const meLevel = bpoItemAttributes ? bpoItemAttributes.material_efficiency : 0;
    const materialDiscountFactor = 1 - (meLevel / 100);

    let totalMaterialCostPerRun = 0;
    let missingCost = false;

    for (const mat of materials) {
      // FIX: CRITICAL CHECK - IGNORE INVENTION MATERIALS
      if (mat.activityID !== INDUSTRY_ACTIVITY_MANUFACTURING) continue;

      const matCost = costMap.get(mat.materialTypeID);
      if (!matCost) {
        LOG_INDUSTRY.warn(`${debugTag} Missing cost for material ${mat.materialTypeID}.`);
        missingCost = true;
        break;
      }
      totalMaterialCostPerRun += matCost * (mat.quantity * materialDiscountFactor);
    }

    if (missingCost) continue;

    const totalMaterialCostForAllRuns = totalMaterialCostPerRun * job.runs;
    const totalJobInstallationCost = job.cost;

    let amortizationSurcharge = 0;
    let amortSource = "NONE";

    if (amortMap.has(job.blueprint_type_id)) {
      amortizationSurcharge = amortMap.get(job.blueprint_type_id) * job.runs;
      amortSource = "BPO_SHEET";
    } else {
      const bpcCostPerRun = getBpcCostPerRun(job.blueprint_type_id);
      amortizationSurcharge = bpcCostPerRun * job.runs;
      amortSource = `BPC_WAC (Unit: ${bpcCostPerRun.toFixed(2)})`;
    }

    const totalActualCost = totalMaterialCostForAllRuns + totalJobInstallationCost + amortizationSurcharge;
    const totalUnitsProduced = product.quantity * job.runs;
    
    if (totalUnitsProduced === 0) continue;
    
    const unitManufacturingCost = totalActualCost / totalUnitsProduced;

    // --- DETAILED DEBUG LOGGING ---
    LOG_INDUSTRY.info(`${debugTag} Type: ${job.product_type_id} | Runs: ${job.runs} | Output: ${totalUnitsProduced} units`);
    LOG_INDUSTRY.info(`${debugTag} Materials: ${totalMaterialCostForAllRuns.toFixed(2)}`);
    LOG_INDUSTRY.info(`${debugTag} Install Fee: ${totalJobInstallationCost.toFixed(2)}`);
    LOG_INDUSTRY.info(`${debugTag} Amortization: ${amortizationSurcharge.toFixed(2)} [Source: ${amortSource}]`);
    LOG_INDUSTRY.info(`${debugTag} TOTAL: ${totalActualCost.toFixed(2)} => Unit Cost: ${unitManufacturingCost.toFixed(2)}`);
    // -----------------------------

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

  if (ledgerObjects.length > 0) {
    ledgerAPI.upsert(['source', 'contract_id'], ledgerObjects);
    LOG_INDUSTRY.info(`Processed ${ledgerObjects.length} jobs.`);
  }

  newlyProcessedIds.forEach(id => processedJobIds.add(id));
  SCRIPT_PROP.setProperty(INDUSTRY_JOB_KEY, JSON.stringify(Array.from(processedJobIds).slice(-1000)));
}

// ----------------------------------------------------------------------
// --- DATA HELPERS ---
// ----------------------------------------------------------------------

function _getBlendedCostMap(ss, requiredMaterialIds) {
  const sheet = ss.getSheetByName("Blended_Cost");
  
  // FIX: Using standardized local _getNamedOr_
  const BROKER_FEE_RATE = Number(_getNamedOr_('FEE_RATE', 0.03));
  const TRANSACTION_TAX_RATE = Number(_getNamedOr_('TAX_RATE', 0.075));
  const ACQUISITION_MULTIPLIER = 1 + BROKER_FEE_RATE + TRANSACTION_TAX_RATE;

  const marketMedianMap = _getMarketMedianMap(ss);
  const allItemCosts = new Map();
  const tier3FetchList = new Set();

  // Tier 1: Blended Cost
  if (sheet && sheet.getLastRow() >= 2) {
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    try {
      const col = _getColIndexMap(headers, ['type_id', 'unit_weighted_average']);
      const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getMaxColumns()).getValues();
      data.forEach(row => {
        const cost = Number(row[col.unit_weighted_average]);
        if (cost > 0) allItemCosts.set(Number(row[col.type_id]), cost);
      });
    } catch (e) { console.warn(e.message); }
  }

  // Tier 2: Market Median
  if (requiredMaterialIds) {
    requiredMaterialIds.forEach(id => {
      if (!allItemCosts.has(id)) {
        const mktCost = marketMedianMap.get(id) || 0;
        if (mktCost > 0) allItemCosts.set(id, mktCost * ACQUISITION_MULTIPLIER);
        else tier3FetchList.add(id);
      }
    });
  }

  // Tier 3: Fuzzwork API
  if (tier3FetchList.size > 0 && typeof fuzAPI !== 'undefined') {
    try {
      const locId = _getNamedOr_('setting_sell_loc', 60003760);
      const res = fuzAPI.requestItems(locId, 'region', Array.from(tier3FetchList));
      LOG_INDUSTRY.info(`Attempting Tier 3 fallback for ${tier3FetchList.size} items via Fuzzwork API.`);
      res.forEach(item => {
        const cost = _extractMetric_(item, 'buy', 'max');
        if (cost > 0) {
           const finalCost = cost * ACQUISITION_MULTIPLIER;
           allItemCosts.set(item.type_id, finalCost);
           LOG_INDUSTRY.info(`Resolved cost for ${item.type_id} using Fuzzwork (Tier 3) + Fee: ${finalCost}`);
        }
      });
    } catch (e) { console.error("Fuzzwork Tier 3 failed", e); }
  }

  return allItemCosts;
}

function _getBpoAmortizationMap(ss) {
  const AMORT_SHEET_NAME = "BPO_Amortization";
  const AMORT_HEADERS = ['bp_type_id', 'Amortization_Runs'];
  const amortMap = new Map();

  const sdePriceMap = _getSdeBasePriceMap(ss);
  const blendedCostMap = _getBlendedCostMap(ss);
  const marketMedianMap = _getMarketMedianMap(ss);
  
  const sheet = getOrCreateSheet(ss, AMORT_SHEET_NAME, AMORT_HEADERS);
  if (sheet.getLastRow() < 2) return amortMap;

  const locationId = _getNamedOr_('setting_sell_loc', 60003760);
  const marketType = _getNamedOr_('setting_market_list', 'region');

  const headers = sheet.getRange(1, 1, 1, sheet.getMaxColumns()).getValues()[0];
  try {
    const col = _getColIndexMap(headers, AMORT_HEADERS);
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getMaxColumns()).getValues();
    const typeIdsToFetch = [];
    const buffer = [];

    // Phase 1: Local
    data.forEach(row => {
      const bpId = Number(row[col.bp_type_id]);
      const runs = Number(row[col.Amortization_Runs]);
      if (runs <= 0) return;

      let val = blendedCostMap.get(bpId) || marketMedianMap.get(bpId) || sdePriceMap.get(bpId) || 0;
      buffer.push({ bpId, runs, val });
      if (val === 0) typeIdsToFetch.push(bpId);
    });

    // Phase 2: API
    const apiPrices = new Map();
    if (typeIdsToFetch.length > 0 && typeof fuzAPI !== 'undefined') {
      try {
        const res = fuzAPI.requestItems(locationId, marketType, typeIdsToFetch);
        res.forEach(item => {
          const p = _extractMetric_(item, 'sell', 'min');
          if (p > 0) apiPrices.set(item.type_id, p);
        });
      } catch (e) { console.warn("Amortization API fetch failed", e); }
    }

    // Phase 3: Calc
    buffer.forEach(item => {
      const finalVal = (item.val > 0) ? item.val : (apiPrices.get(item.bpId) || 0);
      if (finalVal > 0) amortMap.set(item.bpId, finalVal / item.runs);
    });

  } catch (e) { LOG_INDUSTRY.error(e.message); }
  
  return amortMap;
}

// --- STANDARD HELPERS ---

function _getSdeMaps(ss) {
  const matSheet = ss.getSheetByName("SDE_industryActivityMaterials");
  const prodSheet = ss.getSheetByName("SDE_industryActivityProducts");
  const res = { sdeMatMap: new Map(), sdeProdMap: new Map() };
  if (!matSheet || !prodSheet) return res;

  try {
    const matHeaders = matSheet.getRange(1, 1, 1, matSheet.getLastColumn()).getValues()[0];
    const prodHeaders = prodSheet.getRange(1, 1, 1, prodSheet.getLastColumn()).getValues()[0];

    const matCol = _getColIndexMap(matHeaders, ['typeID', 'activityID', 'materialTypeID', 'quantity']);
    const prodCol = _getColIndexMap(prodHeaders, ['typeID', 'activityID', 'productTypeID', 'quantity']);

    const matData = matSheet.getRange(2, 1, matSheet.getLastRow() - 1, matSheet.getMaxColumns()).getValues();
    const prodData = prodSheet.getRange(2, 1, prodSheet.getLastRow() - 1, prodSheet.getMaxColumns()).getValues();

    for(const r of matData) {
      const act = Number(r[matCol.activityID]);
      // FIX: Capture both, but store the activity ID so we can filter later!
      if (act === INDUSTRY_ACTIVITY_MANUFACTURING || act === INDUSTRY_ACTIVITY_INVENTION) {
        const bp = Number(r[matCol.typeID]);
        if (!res.sdeMatMap.has(bp)) res.sdeMatMap.set(bp, []);
        // Store Activity ID in the object
        res.sdeMatMap.get(bp).push({ 
          materialTypeID: Number(r[matCol.materialTypeID]), 
          quantity: Number(r[matCol.quantity]),
          activityID: act 
        });
      }
    }
    for(const r of prodData) {
      const act = Number(r[prodCol.activityID]);
      if (act === INDUSTRY_ACTIVITY_MANUFACTURING) {
        res.sdeProdMap.set(Number(r[prodCol.typeID]), { 
          productTypeID: Number(r[prodCol.productTypeID]), 
          quantity: Number(r[prodCol.quantity]) 
        });
      }
    }
  } catch(e) { console.warn("SDE Parse Error", e); }
  return res;
}

function _getSdeNameMap(ss) {
  const sheet = ss.getSheetByName("SDE_invTypes");
  const map = new Map();
  if(!sheet) return map;
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  try {
    const col = _getColIndexMap(headers, ['typeID', 'typeName']);
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getMaxColumns()).getValues();
    for(const r of data) {
      map.set(Number(r[col.typeID]), r[col.typeName]);
    }
  } catch(e) {}
  return map;
}

function _getMarketMedianMap(ss) {
  const sheet = ss.getSheetByName("market price Tracker");
  const map = new Map();
  if (!sheet) return map;
  try {
    const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
    const colId = headers.indexOf('type_id_filtered');
    const colMed = headers.indexOf('Median Sell');
    if (colId === -1 || colMed === -1) return map;
    
    const data = sheet.getRange(2,1,sheet.getLastRow()-1, sheet.getLastColumn()).getValues();
    data.forEach(r => {
      const val = Number(String(r[colMed]).replace(/[^0-9.]/g, ''));
      if (val > 0) map.set(r[colId], val);
    });
  } catch(e){}
  return map;
}

function _getSdeBasePriceMap(ss) {
  const sheet = ss.getSheetByName("SDE_invTypes");
  const map = new Map();
  if (!sheet) return map;
  try {
    const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
    const colId = headers.indexOf('typeID');
    const colP = headers.indexOf('basePrice');
    const data = sheet.getDataRange().getValues();
    for(let i=1; i<data.length; i++) {
      const p = Number(data[i][colP]);
      if(p>0) map.set(Number(data[i][colId]), p);
    }
  } catch(e){}
  return map;
}

// ----------------------------------------------------------------------
// --- JOB FETCHING & CACHING ---
// ----------------------------------------------------------------------

function _getNewCompletedJobs(ss, processedJobIds, activityIds) {
  const rawJobData = _getCorporateJobsRaw(false);
  if (!rawJobData) return []; // Cache miss handled in Phase 0

  const newJobs = [];
  const activitySet = new Set(activityIds);

  for (const jobObj of rawJobData) {
    const job_id = Number(jobObj.job_id);
    if (jobObj.status === 'delivered' &&
      activitySet.has(Number(jobObj.activity_id)) &&
      !processedJobIds.has(job_id)) {

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

function _getCorporateJobsRaw(forceRefresh) {
  const authToon = getCorpAuthChar();
  const ENDPOINT = 'corporations_corporation_industry_jobs';
  const CACHE_KEY = 'CORP_JOBS_RAW_V1' + ':' + authToon;
  const CACHE_TTL = 3600;

  if (!authToon) return null;

  // 1. Always attempt to load from Cache first
  const cachedJson = _getAndDechunk(CACHE_KEY);

  // 2. DECISION MATRIX:
  // IF cache exists AND we are NOT forcing a refresh -> Return Cache (Speed)
  // IF cache is missing (Sheet is empty) -> Fetch (Self-Heal)
  // IF forceRefresh is true (Background Script) -> Fetch (Update)
  if (cachedJson && !forceRefresh) {
    return JSON.parse(cachedJson);
  }

  // 3. Execution (Cache Miss OR Force Refresh)
  try {
    console.log(`[CorpJobs] Fetching live data... (Reason: ${forceRefresh ? "Force Refresh" : "Cache Miss"})`);
    
    const rawObjects = GESI.invokeRaw(ENDPOINT, {
        include_completed: true,
        name: authToon,
        show_column_headings: false,
        version: null
    });

    if (!Array.isArray(rawObjects)) {
      console.warn("[CorpJobs] ESI returned invalid structure.");
      return null;
    }

    // 4. Update Cache (So the next 50 reads are fast)
    _chunkAndPut(CACHE_KEY, JSON.stringify(rawObjects), CACHE_TTL);
    
    return rawObjects;
  } catch (e) {
    console.error(`[CorpJobs] ESI Fetch Failed: ${e.message}`);
    // Optional fallback: If live fetch fails but we had stale cache, we could return it here.
    // For now, we return null to indicate failure.
    return null;
  }
}

function _getBpoAttributesMapFromEsi() {
  const rawObjects = _getCorporateBlueprintsRaw(false);
  const attributesMap = new Map();
  if(!rawObjects) return attributesMap;

  for (const bpObj of rawObjects) {
    attributesMap.set(Number(bpObj.type_id), {
        material_efficiency: Number(bpObj.material_efficiency),
        time_efficiency: Number(bpObj.time_efficiency)
    });
  }
  return attributesMap;
}

function _getCorporateBlueprintsRaw(forceRefresh) {
  const authToon = getCorpAuthChar();
  const ENDPOINT = 'corporations_corporation_blueprints';
  const cacheKey = BPO_RAW_CACHE_KEY + ':' + authToon;

  if (!forceRefresh) {
    const cachedJson = _getAndDechunk(cacheKey);
    if (cachedJson) return JSON.parse(cachedJson);
  }

  try {
    const rawObjects = GESI.invokeRaw(ENDPOINT, { name: authToon, show_column_headings: false, version: null });
    if (Array.isArray(rawObjects)) {
       _chunkAndPut(cacheKey, JSON.stringify(rawObjects), BPO_RAW_CACHE_TTL);
       return rawObjects;
    }
  } catch (e) {}
  return null;
}

function _getConfigPresetRuns(ss) {
  const CONFIG_NAME = "Config_BPC_Runs";
  const CONFIG_HEADERS = ['bp_type_id', 'preset_runs'];
  const presetMap = new Map();
  const sheet = getOrCreateSheet(ss, CONFIG_NAME, CONFIG_HEADERS);
  
  if (sheet.getLastRow() >= 2) {
    const headers = sheet.getRange(1,1,1,sheet.getLastColumn()).getValues()[0];
    const col = _getColIndexMap(headers, CONFIG_HEADERS);
    const data = sheet.getRange(2,1,sheet.getLastRow()-1, sheet.getLastColumn()).getValues();
    data.forEach(r => {
      const id = Number(r[col.bp_type_id]);
      const runs = Number(r[col.preset_runs]);
      if(id>0 && runs>0) presetMap.set(id, runs);
    });
  }
  return presetMap;
}

function _extractMetric_(row, side, level) {
  if (!row || !row[side]) return 0;
  const v = row[side][level];
  const num = Number(v);
  return Number.isFinite(num) ? num : 0;
}

// Reset function manually callable
function resetIndustryLedgerProperties() {
  const props = PropertiesService.getScriptProperties();
  props.deleteProperty(INDUSTRY_JOB_KEY);
  props.deleteProperty(BPC_JOB_KEY);
  props.deleteProperty(BPC_WAC_KEY);
  
  // ADD THIS LINE:
  props.deleteProperty('BpcHistoryData'); 
  
  console.log("Industry Ledger Properties Reset.");
}

/**
 * Custom function to fetch Corporation Industry Jobs with caching.
 * Prevents continuous API calls during sheet recalculations.
 * * @param {string} name Character name with ESI Corp Jobs scope.
 * @param {boolean} [include_completed=false] Whether to include completed jobs.
 * @returns {any[][]} Raw data from GESI call.
 * @customfunction
 */
function GESI_CORP_JOBS_CACHED(name, include_completed) {
    // NOTE: GLOBAL_STATE_KEY must be accessible. Assuming it's defined elsewhere.
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
    
    // 1. Fetch data from the *shared* cache handled by _getCorporateJobsRaw.
    // NOTE: We pass 'false' to force the helper to read from the cache only (no live API call).
    const rawData = _getCorporateJobsRaw(false); 

    if (!rawData || rawData.length === 0) {
        Logger.log('[CIJ_SHEET] WARN: No data found in shared cache. Returning cache instruction.');
        return [['DATA_NOT_CACHED'], ['Run Industry Ledger script to refresh cache.']];
    }

    // 2. Format output for Google Sheets (array of objects -> array of arrays).
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