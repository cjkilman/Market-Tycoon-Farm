/**
 * IndustryLedger.gs.js
 *
 * This module is the Industry Ledger Add-on, built for robust COGS accounting.
 * It includes sharding utilities to bypass the Google Apps Script Cache limit.
 *
 * FIX APPLIED:
 * 1. generateFullBOMData: Now calculates Blueprint Cycles strictly from 'Build Target' / 'Units Per Run'.
 * - Ignores 'Total Runs' column to prevent 100x multiplier errors.
 * - Fixes header mapping for 'Type ID'.
 * 2. generateConsolidatedRequirements: Fixed Hangar column indices (Col B=ID, Col E=Qty).
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

const CORP_ID = '98626262'; // Market-Tycoon Corp ID

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

/**
 * NITRO BOM ENGINE (NUCLEAR OPTION)
 * Ignores 'Units Per Run' from the sheet and forces SDE lookup.
 * Fixes the 9.8B Tritanium bug permanently.
 */
function generateFullBOMData(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('BOM_Engine') : console;
  const clean = (v) => (typeof v === 'number') ? v : parseFloat(String(v).replace(/[^0-9.-]/g, '')) || 0;

  // --- 1. Load Data ---
  const prodSheet = ss.getSheetByName("ProductionList ");
  const sdeMatSheet = ss.getSheetByName("SDE_industryActivityMaterials");
  const sdeProdSheet = ss.getSheetByName("SDE_industryActivityProducts");

  if (!prodSheet || !sdeMatSheet || !sdeProdSheet) return;

  const prodRaw = prodSheet.getDataRange().getValues();
  const pHeaders = prodRaw[4]; 
  const prodData = prodRaw.slice(5);

  const pCol = {
    prodID: pHeaders.indexOf("Type ID"),
    bpID:   pHeaders.indexOf("Blueprint Type ID"),
    me:     pHeaders.indexOf("Material Efficiency (ME)"),
    target: pHeaders.indexOf("Build Target (Qty)")
  };

  // --- 2. BUILD SDE MAP (The "Truth" Database) ---
  // Maps Product ID -> { BlueprintID, YieldQty }
  // This bypasses the need for the sheet to have correct columns.
  const productMetaMap = new Map();
  const sdeProdData = sdeProdSheet.getDataRange().getValues();
  
  // Skip header, assuming Row 1 is header
  for (let i = 1; i < sdeProdData.length; i++) {
    const r = sdeProdData[i];
    // Check for Activity 1 (Manufacturing)
    if (Number(r[1]) === 1) {
      const bpID = Number(r[0]);      // Col A
      const productID = Number(r[2]); // Col C
      const quantity = Number(r[3]);  // Col D (Yield)
      
      productMetaMap.set(productID, { bpID: bpID, qty: quantity });
    }
  }

  const jobMap = new Map();
  
  prodData.forEach(row => {
    const pID = Number(row[pCol.prodID]);
    const buildTarget = clean(row[pCol.target]);
    
    // LOOKUP from SDE (The Fix)
    const meta = productMetaMap.get(pID);
    
    if (meta && buildTarget > 0) {
      const bpID = meta.bpID;
      const unitsPerRun = meta.qty || 1; // Force SDE value (e.g., 100)
      
      // Calculate TRUE cycles
      const runs = Math.ceil(buildTarget / unitsPerRun);
      const me = row[pCol.me] === "" ? 10 : clean(row[pCol.me]);
      
      if (runs > 0) {
        jobMap.set(bpID, { me, runs: (jobMap.get(bpID)?.runs || 0) + runs });
      }
    }
  });

  // --- 3. Process Materials ---
  const sdeMatData = sdeMatSheet.getDataRange().getValues();
  const outputRows = [];
  for (let i = 1; i < sdeMatData.length; i++) {
    const sdeBpID = Number(sdeMatData[i][0]);
    if (sdeMatData[i][1] === 1 && jobMap.has(sdeBpID)) {
      const job = jobMap.get(sdeBpID);
      const baseQty = Number(sdeMatData[i][3]);
      const adjQty = baseQty * ((100 - job.me) / 100);
      
      // Total Req = (Mat Per Run) * (True Cycles)
      const totalReq = Math.ceil(adjQty * job.runs);

      outputRows.push([
          sdeBpID, 
          1, 
          Number(sdeMatData[i][2]), 
          baseQty, 
          job.me, 
          job.runs, 
          adjQty, 
          totalReq
      ]);
    }
  }

  // --- 4. Output ---
  const outSheet = ss.getSheetByName("Full_BOM_Data");
  outSheet.clearContents();
  outSheet.getRange(1, 1, 1, 8).setValues([["BP ID", "Act ID", "Mat ID", "Base Qty", "ME", "Runs", "Adj Qty", "Total Req"]]);
  if (outputRows.length > 0) {
    outSheet.getRange(2, 1, outputRows.length, 8).setValues(outputRows);
    outSheet.getRange(2, 8, outputRows.length, 1).setNumberFormat("#,##0");
  }
  
  LOG.info(`BOM NUCLEAR FIX: Processed ${outputRows.length} lines using SDE yields.`);
}

/**
 * NITRO CONSOLIDATOR: Generates a 100% static requirement and shopping list.
 * Logic: Aggregates BOM, calculates Shopping List/Cost, and outputs static values.
 */
function generateConsolidatedRequirements(ss) {
  const TARGET_SHEET_NAME = 'Consolidated_Requirements';
  const SOURCE_SHEET_NAME = 'Manufaturing Inputs Effective Cost';

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(TARGET_SHEET_NAME);
  const sourceSheet = ss.getSheetByName(SOURCE_SHEET_NAME);

  if (!sheet || !sourceSheet) return;

  const clean = (v) => (typeof v === 'number') ? v : parseFloat(String(v || 0).replace(/[^0-9.-]/g, '')) || 0;

  const rawData = sourceSheet.getDataRange().getValues();
  const headers = rawData[0];
  const dataRows = rawData.slice(1);

  const col = {
    id: headers.indexOf("Type ID"),
    cost: headers.indexOf("Effective Cost"), 
    name: headers.indexOf("Type Name"),
    bufferPct: headers.indexOf("Buffer Status (%)"),
    deficit31d: headers.indexOf("Need to Buy (31d)"),
    daysOnHand: headers.indexOf("Days on Hand")
  };

  const OUT_HEADERS = [
    "Material Name", "Buffer Status", "Total 31-Day Deficit",
    "Projected from Scrap", "Net Need to Buy", "Daily Siphon Target",
    "WAG (Max Buy Price)", "Logistics Action"
  ];

  let results = [];

  const scrapCache = JSON.parse(PropertiesService.getScriptProperties().getProperty('PROJECTED_SCRAP_MINERALS') || '{}');
  
  // FIX: Define acquisitionDays to match the 31-day window
  const acquisitionDays = 31;

  for (let r of dataRows) {
    const name = String(r[col.name] || "").trim();
    if (!name) continue;

    const materialID = Number(r[col.id]);
    const scrapYield = Number(scrapCache[materialID] || 0);
    const rawDeficit = clean(r[col.deficit31d]);

    // The Net Calculation
    const netDeficit = Math.max(0, rawDeficit - scrapYield);
    const buffer = clean(r[col.bufferPct]) || 0;

    if (rawDeficit > 0 || scrapYield > 0) {
      const wagCost = clean(r[col.cost]);
      // Math now works because acquisitionDays is defined
      const dailyTarget = Math.ceil(netDeficit / acquisitionDays);

      let action = "STANDBY";
      if (buffer <= 0.15) action = "CRITICAL: MAX RANGE SAFE SIPHON";
      else if (buffer <= 0.30) action = "ACTIVE: DEPLOY MICRO-HUB ORDERS";
      else action = "PASSIVE: DRIP FEED";

      results.push({
        data: [name, buffer, rawDeficit, scrapYield, netDeficit, dailyTarget, wagCost, action],
        sortKey: buffer
      });
    }
  }

  // Sort lowest buffer to the top
  results.sort((a, b) => a.sortKey - b.sortKey);
  const output = results.map(r => r.data);

  // Write to sheet
  sheet.clearContents();
  sheet.getRange(1, 1, 1, OUT_HEADERS.length).setValues([OUT_HEADERS]).setFontWeight("bold");

  if (output.length > 0) {
    sheet.getRange(2, 1, output.length, OUT_HEADERS.length).setValues(output);
    // Format the WAG column to ISK and Buffer to %
    sheet.getRange(2, 5, output.length, 1).setNumberFormat("#,##0.00 [$ISK]");
    sheet.getRange(2, 2, output.length, 1).setNumberFormat("0.00%");
    sheet.getRange(2, 3, output.length, 2).setNumberFormat("#,##0");
  }
}
function runIndustryLedgerPhase(ss) {
  const log = LoggerEx.withTag('MASTER_SYNC');
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  const cogsState = SCRIPT_PROP.getProperty('cogsJobStep');
  if (cogsState === 'FINALIZING') {
    log.warn('Skipping Industry Ledger Phase: Contract COGS calculation is pending.');
    return;
  }
  const START_TIME = new Date().getTime();

  log.info('--- Starting Rolling Thunder Sync ---');

  let phase = parseInt(SCRIPT_PROP.getProperty(INDUSTRY_JOB_PHASE) || '0', 10);

  // Phase 0: Fetch ESI Jobs
  if (phase === 0) {
    try {
      log.info('Phase 0: Fetching ESI Corp Jobs...');
      _getCorporateJobsRaw(false);
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '1');
      phase = 1;
    } catch (e) { log.error('Phase 0 FAILED.', e); return; }
  }

  // Phase 1: BPC Ledger
  if (phase === 1) {
    if (Date.now() - START_TIME > SOFT_TIME_LIMIT_MS) return;
    try {
      log.info('Phase 1: BPC Creation Ledger...');
      runBpcCreationLedger(ss);
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '2');
      phase = 2;
    } catch (e) { log.error('Phase 1 FAILED:', e); }
  }

  // Phase 2: Manufacturing Ledger
  if (phase === 2) {
    if (Date.now() - START_TIME > SOFT_TIME_LIMIT_MS) return;
    try {
      log.info('Phase 2: Manufacturing Ledger Update...');
      runIndustryLedgerUpdate(ss);
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '3');
      phase = 3;
    } catch (e) { log.error('Phase 2 FAILED:', e); }
  }

  // NEW Phase 3: Reprocessing Forensic Audit
  if (phase === 3) {
    if (Date.now() - START_TIME > SOFT_TIME_LIMIT_MS) return;
    try {
      log.info('Phase 3: Running Reprocessing Audit (MiningHanger)...');
      runReprocessingAudit(ss);
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '4');
      phase = 4;
    } catch (e) { log.error('Phase 3 FAILED:', e); }
  }

  if (phase === 4) {
    SCRIPT_PROP.deleteProperty(INDUSTRY_JOB_PHASE);
    log.info('Phase 4: Rolling Thunder Complete.');
  }
}

/**
 * V12 Ferrari - Blueprint Edition (With Cache Protection)
 */
function fetchAllCorpBlueprints(corporationId) {
  const authToon = getCorpAuthChar();
  const cacheKey = "corp_bps_" + corporationId;
  
  // 1. Check Shard-Chucker
  const cachedJson = _getAndDechunk(cacheKey); 
  if (cachedJson) {
    const parsed = JSON.parse(cachedJson);
    if (parsed.length > 0) {
       console.log(`[CACHE] Serving ${parsed.length} blueprints for ${authToon}.`);
       return parsed;
    }
  }

  console.log(`Ferrari launching... Fetching Corp ${corporationId} via ${authToon}`);
  const client = GESI.getClient().setFunction('corporations_corporation_blueprints');
  let rawObjects = [];

  try {
    const req1 = client.buildRequest({ corporation_id: corporationId, page: 1, name: authToon });
    const resp1 = UrlFetchApp.fetch(req1.url, { method: 'get', headers: req1.headers, muteHttpExceptions: true });

    if (resp1.getResponseCode() !== 200) {
      throw new Error(`ESI Error: ${resp1.getResponseCode()} - ${resp1.getContentText()}`);
    }

    const page1Data = JSON.parse(resp1.getContentText());
    rawObjects = rawObjects.concat(page1Data);

    const headers = resp1.getHeaders();
    const maxPages = Number(headers['X-Pages'] || headers['x-pages']) || 1;
    
    if (maxPages > 1) {
      const requests = [];
      for (let p = 2; p <= maxPages; p++) {
        const req = client.buildRequest({ corporation_id: corporationId, page: p, name: authToon });
        requests.push({ url: req.url, method: 'get', headers: req.headers, muteHttpExceptions: true });
      }
      const responses = UrlFetchApp.fetchAll(requests);
      responses.forEach(res => {
        if (res.getResponseCode() === 200) {
          rawObjects = rawObjects.concat(JSON.parse(res.getContentText()));
        }
      });
    }

    // CRITICAL: Only cache if we actually got results!
    if (rawObjects.length > 0) {
      _chunkAndPut(cacheKey, JSON.stringify(rawObjects), 3600);
      console.log(`[SUCCESS] Cached ${rawObjects.length} blueprints.`);
    }

    return rawObjects;

  } catch (e) {
    console.error(`[CRITICAL] Ferrari Engine stalled: ${e.message}`);
    return []; // Return empty so the sheet update logic knows to stop
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

/**
 * Phase 4: Hangar Audit (Dynamic ID Version)
 */
function syncCorpBlueprintsV12() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const authToon = getCorpAuthChar(ss); // Resolves "Jason Kilman"
  
  // DYNAMIC RESOLUTION: Pull the Corp ID directly from Jason's character data
  const charData = GESI.getCharacterData(authToon);
  if (!charData || !charData.corporation_id) {
    console.error(`[CRITICAL] Could not find Corp ID for ${authToon}. Check GESI Auth.`);
    return;
  }
  
  const corpId = charData.corporation_id;
  console.log(`Auditing Hangar for Corp: ${corpId} (${authToon})`);

  // Use the Ferrari to get data (It now handles dynamic IDs)
  const allBlueprints = fetchAllCorpBlueprints(corpId); 
  
  if (allBlueprints && allBlueprints.length > 0) {
    _updateBpoConfigFromAudit(allBlueprints);
  } else {
    console.error("Audit aborted: No data returned from ESI or Cache.");
  }
}

/**
 * THE BRIDGE: Now with robust header mapping (Case-Insensitive)
 */
function _updateBpoConfigFromAudit(blueprints) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Config_BPC_Runs");
  if (!sheet || !blueprints || blueprints.length === 0) return;

  // 1. Audit the Hangar (runs === -1 are BPOs)
  const auditMap = new Map();
  blueprints.forEach(bp => {
    if (bp.runs === -1) { 
      const id = Number(bp.type_id);
      auditMap.set(id, (auditMap.get(id) || 0) + 1);
    }
  });

  // 2. Map the Sheet using your ROBUST helper
  const rawData = sheet.getDataRange().getValues();
  const headers = rawData[0];
  
  let col;
  try {
    // This helper (already in your script) handles case and spaces
    col = _getColIndexMap(headers, ['bp_type_id', 'available_bpos']);
  } catch (e) {
    console.error("Critical: Sheet headers don't match. Please ensure you have 'bp_type_id' and 'available_bpos' columns.");
    return;
  }

  // Check optional columns (ME/TE) without crashing if they are missing
  const lowerHeaders = headers.map(h => String(h || '').toLowerCase().trim());
  const meIdx = lowerHeaders.indexOf('max_me');
  const teIdx = lowerHeaders.indexOf('max_te');

  // 3. Update memory
  const values = rawData.slice(1).map(row => {
    const bpID = Number(row[col.bp_type_id]);
    row[col.available_bpos] = auditMap.get(bpID) || 0;
    return row;
  });

  // 4. One "Hammer Strike" write back to sheet
  sheet.getRange(2, 1, values.length, headers.length).setValues(values);
  console.log(`[SUCCESS] Hangar Audit: Synchronized ${auditMap.size} BPO types.`);
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

    for (const r of matData) {
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
    for (const r of prodData) {
      const act = Number(r[prodCol.activityID]);
      if (act === INDUSTRY_ACTIVITY_MANUFACTURING) {
        res.sdeProdMap.set(Number(r[prodCol.typeID]), {
          productTypeID: Number(r[prodCol.productTypeID]),
          quantity: Number(r[prodCol.quantity])
        });
      }
    }
  } catch (e) { console.warn("SDE Parse Error", e); }
  return res;
}

function _getSdeNameMap(ss) {
  const sheet = ss.getSheetByName("SDE_invTypes");
  const map = new Map();
  if (!sheet) return map;
  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  try {
    const col = _getColIndexMap(headers, ['typeID', 'typeName']);
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getMaxColumns()).getValues();
    for (const r of data) {
      map.set(Number(r[col.typeID]), r[col.typeName]);
    }
  } catch (e) { }
  return map;
}

function _getMarketMedianMap(ss) {
  const sheet = ss.getSheetByName("market price Tracker");
  const map = new Map();
  if (!sheet) return map;
  try {
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const colId = headers.indexOf('type_id_filtered');
    const colMed = headers.indexOf('Median Sell');
    if (colId === -1 || colMed === -1) return map;

    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
    data.forEach(r => {
      const val = Number(String(r[colMed]).replace(/[^0-9.]/g, ''));
      if (val > 0) map.set(r[colId], val);
    });
  } catch (e) { }
  return map;
}

function _getSdeBasePriceMap(ss) {
  const sheet = ss.getSheetByName("SDE_invTypes");
  const map = new Map();
  if (!sheet) return map;
  try {
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const colId = headers.indexOf('typeID');
    const colP = headers.indexOf('basePrice');
    const data = sheet.getDataRange().getValues();
    for (let i = 1; i < data.length; i++) {
      const p = Number(data[i][colP]);
      if (p > 0) map.set(Number(data[i][colId]), p);
    }
  } catch (e) { }
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
  if (!rawObjects) return attributesMap;

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
  } catch (e) { }
  return null;
}

function _getConfigPresetRuns(ss) {
  const CONFIG_NAME = "Config_BPC_Runs";
  const CONFIG_HEADERS = ['bp_type_id', 'preset_runs'];
  const presetMap = new Map();
  const sheet = getOrCreateSheet(ss, CONFIG_NAME, CONFIG_HEADERS);

  if (sheet.getLastRow() >= 2) {
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const col = _getColIndexMap(headers, CONFIG_HEADERS);
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();
    data.forEach(r => {
      const id = Number(r[col.bp_type_id]);
      const runs = Number(r[col.preset_runs]);
      if (id > 0 && runs > 0) presetMap.set(id, runs);
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