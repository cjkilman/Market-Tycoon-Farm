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

// Ensure you are using the correct SDE Activity IDs
const INDUSTRY_ACTIVITY_MANUFACTURING = 1;
const INDUSTRY_ACTIVITY_TE_RESEARCH = 3;  // Correct SDE ID for TE Research
const INDUSTRY_ACTIVITY_ME_RESEARCH = 4;  // Correct SDE ID for ME Research
const INDUSTRY_ACTIVITY_COPYING = 5;
const INDUSTRY_ACTIVITY_INVENTION = 8;
const INDUSTRY_ACTIVITY_REACTIONS = 11;
const INDUSTRY_JOB_PHASE = 'IndustryJobPhase';
const SOFT_TIME_LIMIT_MS = 280000; // 4 minutes 40 seconds soft limit

// --- CACHE SHARDING CONSTANTS ---
const BPO_RAW_CACHE_KEY = 'BPO_RAW_INVENTORY_V1';
const BPO_RAW_CACHE_TTL = 3600; // 1 hour TTL

const LOG_INDUSTRY = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('IndustryLedger') : console);


// ----------------------------------------------------------------------
// --- LOCAL HELPER: ROBUST NAMED RANGE LOOKUP ---

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
 * HELPER: _getBpFromProduct(productID, sdeProdMap)
 * Inverts the Blueprint Map for a high-speed O(1) lookup.
 * Eliminates sequential .entries() loop performance bottlenecks.
 */
function _getBpFromProduct(productID, sdeProdMap) {
  if (!sdeProdMap) return null;

  // 1. Create a single-use inverted map for fast lookups
  const invertedMap = new Map();
  for (const [bpID, prodObj] of sdeProdMap.entries()) {
    if (prodObj && prodObj.productTypeID) {
      invertedMap.set(Number(prodObj.productTypeID), {
        bpID: bpID,
        yield: Number(prodObj.quantity) || 1
      });
    }
  }

  // 2. Perform an instant lookup instead of a sequential scan
  return invertedMap.get(Number(productID)) || null;
}

/**
 * NITRO BOM ENGINE V3 (HYBRID AUTO-EXPANSION)
 * Keeps the 8-column output but automatically calculates and injects 
 * sub-components for T2 manufacturing. Stops at market composites.
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
    me: pHeaders.indexOf("Material Efficiency (ME)"),
    target: pHeaders.indexOf("Build Target (Qty)")
  };

  // --- 2. BUILD SDE MAPS (Strictly Activity 1 - Manufacturing) ---
  const productMetaMap = new Map();
  const sdeProdData = sdeProdSheet.getDataRange().getValues();

  for (let i = 1; i < sdeProdData.length; i++) {
    // Activity 1 ONLY. This creates the firewall against Reaction Formulas.
    if (Number(sdeProdData[i][1]) === 1) {
      productMetaMap.set(Number(sdeProdData[i][2]), {
        bpID: Number(sdeProdData[i][0]),
        qty: Number(sdeProdData[i][3])
      });
    }
  }

  const bpMaterialsMap = new Map();
  const sdeMatData = sdeMatSheet.getDataRange().getValues();

  for (let i = 1; i < sdeMatData.length; i++) {
    if (Number(sdeMatData[i][1]) === 1) {
      const bpID = Number(sdeMatData[i][0]);
      if (!bpMaterialsMap.has(bpID)) bpMaterialsMap.set(bpID, []);
      bpMaterialsMap.get(bpID).push({
        matID: Number(sdeMatData[i][2]),
        qty: Number(sdeMatData[i][3])
      });
    }
  }

  // --- 3. AUTO-EXPANSION QUEUE (The Ghost Jobs) ---
  const jobMap = new Map(); // Tracks bpID -> {me, runs}

  // Recursive function to drill down through components
  function injectJob(productID, requiredQty, currentME) {
    const meta = productMetaMap.get(productID);

    // FIREWALL: If it's a Composite (Activity 11) or raw mineral, it won't be 
    // in productMetaMap. The script stops here and leaves it as a market purchase.
    if (!meta) return;

    const unitsPerRun = meta.qty || 1;
    const runs = Math.ceil(requiredQty / unitsPerRun);

    if (runs > 0) {
      const bpID = meta.bpID;

      // Add to the master job queue
      const existingRuns = jobMap.get(bpID)?.runs || 0;
      jobMap.set(bpID, { me: currentME, runs: existingRuns + runs });

      // Look at the blueprint's ingredients and drill down
      const materials = bpMaterialsMap.get(bpID) || [];
      materials.forEach(mat => {
        const adjQty = mat.qty * ((100 - currentME) / 100);
        const totalMatReq = Math.ceil(adjQty * runs);

        // Recursively inject. We force ME 10 here for sub-components
        // assuming your component blueprints are fully researched.
        injectJob(mat.matID, totalMatReq, 10);
      });
    }
  }

  // Read ProductionList and trigger the expansion
  prodData.forEach(row => {
    const pID = Number(row[pCol.prodID]);
    const buildTarget = clean(row[pCol.target]);
    const me = row[pCol.me] === "" ? 10 : clean(row[pCol.me]);

    if (pID > 0 && buildTarget > 0) {
      injectJob(pID, buildTarget, me);
    }
  });

  // --- 4. Generate the 8-Column Output ---
  const outputRows = [];
  for (let i = 1; i < sdeMatData.length; i++) {
    const sdeBpID = Number(sdeMatData[i][0]);

    // Match blueprints in our jobMap (both explicit and ghost jobs)
    if (sdeMatData[i][1] === 1 && jobMap.has(sdeBpID)) {
      const job = jobMap.get(sdeBpID);
      const baseQty = Number(sdeMatData[i][3]);
      const adjQty = baseQty * ((100 - job.me) / 100);
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

  // --- 5. Paste to Sheet ---
  const outSheet = ss.getSheetByName("Full_BOM_Data");
  outSheet.clearContents();
  outSheet.getRange(1, 1, 1, 8).setValues([["BP ID", "Act ID", "Mat ID", "Base Qty", "ME", "Runs", "Adj Qty", "Total Req"]]);
  if (outputRows.length > 0) {
    outSheet.getRange(2, 1, outputRows.length, 8).setValues(outputRows);
    outSheet.getRange(2, 8, outputRows.length, 1).setNumberFormat("#,##0");
  }

  LOG.info(`BOM V3 HYBRID: Expanded T2 chains. Processed ${outputRows.length} job lines.`);
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

  if (!sheet || !sourceSheet) {
    console.error("Critical failure: Target or source sheet could not be found.");
    return;
  }

  const clean = (v) => (typeof v === 'number') ? v : parseFloat(String(v || 0).replace(/[^0-9.-]/g, '')) || 0;

  const rawData = sourceSheet.getDataRange().getValues();
  if (!rawData || rawData.length === 0) {
    console.error(`[CRITICAL] Source sheet '${SOURCE_SHEET_NAME}' returned an empty data range.`);
    return;
  }

  // ADAPTIVE FUZZY SCANNER: Sweeps top rows to find where the header row lives
  let headers = [];
  let headerRowIndex = -1;
  let col = { id: -1, cost: -1, name: -1, bufferPct: -1, deficit31d: -1 };

  for (let r = 0; r < Math.min(rawData.length, 12); r++) {
    const checkRow = rawData[r].map(h => String(h || '').toLowerCase().trim());

    const idIdx = checkRow.findIndex(h => h.includes("id") || h.includes("type"));
    const nameIdx = checkRow.findIndex(h => h.includes("name") || h.includes("material"));
    const deficitIdx = checkRow.findIndex(h => h.includes("buy") || h.includes("deficit") || h.includes("need"));

    if (idIdx !== -1 && nameIdx !== -1 && deficitIdx !== -1) {
      headers = rawData[r];
      headerRowIndex = r;

      col.id = idIdx;
      col.name = nameIdx;
      col.deficit31d = deficitIdx;
      col.cost = checkRow.findIndex(h => h.includes("cost") || h.includes("price") || h.includes("wag"));
      col.bufferPct = checkRow.findIndex(h => h.includes("buffer"));
      break;
    }
  }

  if (headerRowIndex === -1) {
    const sampledRows = rawData.slice(0, 5).map((row, idx) => `Row ${idx + 1}: ${JSON.stringify(row.slice(0, 6))}`).join("\n");
    console.error(`Critical failure: Could not locate database headers. Top rows contain:\n${sampledRows}`);
    return;
  }

  const dataRows = rawData.slice(headerRowIndex + 1);

  const OUT_HEADERS = [
    "Material Name", "Buffer Status", "Total 31-Day Deficit",
    "Projected from Scrap", "Net Need to Buy", "Daily Siphon Target",
    "WAG (Max Buy Price)", "Logistics Action"
  ];

  let results = [];
  const acquisitionDays = 31;

  for (let r of dataRows) {
    const name = String(r[col.name] || "").trim();
    if (!name) continue;

    const rawDeficit = clean(r[col.deficit31d]);

    // CACHE BYPASS: Scrap yield is hardcoded to 0 until an inventory tracking sheet is built
    const scrapYield = 0;
    const netDeficit = Math.max(0, rawDeficit - scrapYield);
    const buffer = col.bufferPct !== -1 ? clean(r[col.bufferPct]) : 0;

    // Only process lines that have a real material shortage to buy
    if (rawDeficit > 0) {
      const wagCost = col.cost !== -1 ? clean(r[col.cost]) : 0;

      // Keep precise thresholds for low-volume sub-components
      const dailyTarget = netDeficit < acquisitionDays ?
        Number((netDeficit / acquisitionDays).toFixed(2)) :
        Math.ceil(netDeficit / acquisitionDays);

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

  // Sort lowest safety buffers straight to the top row
  results.sort((a, b) => a.sortKey - b.sortKey);
  const output = results.map(r => r.data);

  // Clear previous values but keep structural grid settings intact
  sheet.clearContents();
  sheet.getRange(1, 1, 1, OUT_HEADERS.length).setValues([OUT_HEADERS]).setFontWeight("bold");

  if (output.length > 0) {
    // 1. Write the payload data matrix
    sheet.getRange(2, 1, output.length, OUT_HEADERS.length).setValues(output);


  } else {
    console.warn("Execution finished: No material rows met the rawDeficit > 0 conditional.");
  }
}

function runIndustryLedgerPhase(ss) {
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const log = LoggerEx.withTag('MASTER_SYNC');
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  if (SCRIPT_PROP.getProperty('cogsJobStep') === 'FINALIZING') {
    log.warn('Skipping: Contract COGS calculation pending.');
    return;
  }

  const START_TIME = Date.now();
  let phase = parseInt(SCRIPT_PROP.getProperty(INDUSTRY_JOB_PHASE) || '0', 10);
  const isTimeUp = () => (Date.now() - START_TIME > SOFT_TIME_LIMIT_MS);

  try {
    // PHASE 0: Fetcher (Write Only)
    if (phase === 0) {
      log.info('Phase 0: Fetching ESI Corp Jobs...');
      _getCorporateJobsRaw(ss, false); //Trust the Cache Time Expire
      phase = 1;
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '1');
    }

    // Initialize jobMap only once we are sure data exists
    // This runs only if we are in Phase 1 or 2
    let jobMap;
    if (phase >= 1) {
      jobMap = _getJobMap(ss);
    }

    // PHASE 1: Processor
    if (phase === 1 && !isTimeUp()) {
      log.info('Phase 1: Running BPC Creation Ledger...');
      runBpcCreationLedger(ss, jobMap, true);
      phase = 2;
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '2');
    }

    // PHASE 2: Processor
    if (phase === 2 && !isTimeUp()) {
      log.info('Phase 2: Manufacturing Ledger Update...');
      runIndustryLedgerUpdate(ss, START_TIME, jobMap);
      phase = 4;
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '4');
    }

    // PHASE 4: Cleanup
    if (phase === 4) {
      log.info('Phase 4: Rolling Thunder Complete.');
      SCRIPT_PROP.deleteProperty(INDUSTRY_JOB_PHASE);
    }
  } catch (e) {
    log.error('Phase ' + phase + ' FAILED.', e);
  }
}

function XRAY_MISSING_JOB() {
  // Put your missing Job ID right here (keep it in quotes)
  const TARGET_JOB_ID = "662047470";

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const log = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('XRAY') : Logger;

  log.log(`--- STARTING X-RAY ON JOB ${TARGET_JOB_ID} ---`);

  // GATE 1: The Cache
  const jobMap = _getJobMap(ss, false);
  const job = jobMap.get(TARGET_JOB_ID);

  if (!job) {
    log.log(`❌ DEAD AT GATE 1: Job not found in the cache map. Check if the ID in the 'ESI Corp Jobs' sheet exactly matches what you pasted.`);
    return;
  }
  log.log(`✅ GATE 1 PASSED: Job found in cache. (Activity: ${job.activity_id}, Status: ${job.status}, Blueprint ID: ${job.blueprint_type_id})`);

  // GATE 2: The Memory Hole
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const processedJobIds = new Set(JSON.parse(SCRIPT_PROP.getProperty('processedIndustryJobIds') || '[]'));

  if (processedJobIds.has(TARGET_JOB_ID)) {
    log.log(`❌ DEAD AT GATE 2: Job ID is sitting in the 'processedJobIds' memory bank. The script already processed this and is ignoring it.`);
    return;
  }
  log.log(`✅ GATE 2 PASSED: Job is recognized as new.`);

  // GATE 3: The Activity Filter
  const targetActivities = [1, 3, 4, 8];
  if (!targetActivities.includes(parseInt(job.activity_id))) {
    log.log(`❌ DEAD AT GATE 3: Activity ID ${job.activity_id} is being actively filtered out.`);
    return;
  }
  log.log(`✅ GATE 3 PASSED: Activity ID is valid.`);

  // GATE 4: The Status Filter
  const validStates = ['active', 'ready', 'delivered', 'cancelled', 'reverted'];
  if (!validStates.includes(job.status)) {
    log.log(`❌ DEAD AT GATE 4: Status '${job.status}' is not in the trackable whitelist.`);
    return;
  }
  log.log(`✅ GATE 4 PASSED: Status is trackable.`);

  // GATE 5: The SDE Lookup
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  const materials = sdeMatMap.get(Number(job.blueprint_type_id));
  const product = sdeProdMap.get(Number(job.blueprint_type_id));

  if (!materials) {
    log.log(`❌ DEAD AT GATE 5A: No 'materials' recipe found in SDE map for Blueprint ID ${job.blueprint_type_id}.`);
    return;
  }
  if (!product && parseInt(job.activity_id) === 1) {
    log.log(`❌ DEAD AT GATE 5B: No 'product' yield found in SDE map for Blueprint ID ${job.blueprint_type_id}.`);
    return;
  }
  log.log(`✅ GATE 5 PASSED: SDE mappings exist.`);

  log.log(`🔥 CONCLUSION: The job perfectly clears all gates. If it is vanishing, it is being overwritten during the ML.forSheet upsert operation.`);
}

/**
 * 🌊 WATERFALL BLUEPRINT STATS ENGINE (FULLY UPGRADED)
 * Sequence: Ledger (Actuals) -> Master Config Engine -> [Yields to ESI if blank]
 */
function _buildWaterfallBlueprintStatsMap(targetBpIds, configMap) {
  // SAFETY: If configMap is missing, initialize it so .get() won't crash
  const map = configMap || new Map();
  const statsMap = new Map();

  // 1. Query Ledger
  const ledgerData = ML.forSheet("Material_Ledger").query({
    type_id: targetBpIds,
    source: ["CONTRACT", "INVENTION", "COPYING"]
  });

  // 2. Pre-aggregate Ledger data (Single-pass)
  const grouped = {};
  ledgerData.forEach(r => {
    const bpId = Number(r.type_id);
    if (!grouped[bpId]) {
      grouped[bpId] = {
        contracts: { weightedMe: 0, weightedTe: 0, totalQty: 0 },
        industry: { weightedMe: 0, weightedTe: 0, totalQty: 0 }
      };
    }

    const qty = Math.abs(Number(r.qty)) || 1;
    const meta = (typeof r.metadata === 'object' && r.metadata !== null) ? r.metadata : {};
    const me = (Number(meta.me) || 0);
    const te = (Number(meta.te) || 0);

    const target = (r.source === "CONTRACT") ? grouped[bpId].contracts : grouped[bpId].industry;
    target.weightedMe += (me * qty);
    target.weightedTe += (te * qty);
    target.totalQty += qty;
  });

  // 3. Resolve: Ledger (Actuals) -> Fallback to Master Config
  targetBpIds.forEach(bpId => {
    const config = map.get(bpId);
    const data = grouped[bpId];

    let me, te;
    let hasValidData = false;

    // 1. Try Ledger First (Priority: Contracts > Industry)
    if (data && data.contracts.totalQty > 0) {
      me = Math.round(data.contracts.weightedMe / data.contracts.totalQty);
      te = Math.round(data.contracts.weightedTe / data.contracts.totalQty);
      hasValidData = true;
    } else if (data && data.industry.totalQty > 0) {
      me = Math.round(data.industry.weightedMe / data.industry.totalQty);
      te = Math.round(data.industry.weightedTe / data.industry.totalQty);
      hasValidData = true;
    }
    // 2. Fallback to Master Config (Using the new camelCase variables)
    else if (config) {
      me = config.maxMe;
      te = config.maxTe;
      hasValidData = true;
    }

    // 3. ONLY set the map if we actually found real data.
    if (hasValidData) {
      statsMap.set(bpId, {
        me: me,
        te: te,
        runs: config ? config.presetRuns : 0
      });
    }
  });

  return statsMap;
}

/**
 * 🧮 CORE INDUSTRY MATH ENGINE (DRY)
 * Centralizes EVE batch formulas, ME discounts, and BPC amortization.
 * Used by both the Projected Cost engine and the Historical Ledger.
 */
function _calculateJobFinancials(params) {
  const {
    bpId,
    activityId,
    runs,
    meLevel,
    materials, // Array of SDE materials
    costMap,
    amortMap,
    internalBpcMap,
    bpcWacData,
    presetRuns, // Needed to normalize bulk BPC contract values
    baseYield,
    actualInstallCost, // Passed from ESI. If undefined, it uses the estimate.
    estInstallRate // e.g., 0.05
  } = params;

  // 1. Amortization (BPC / Setup Costs)
  let amortization = 0;
  if (amortMap.has(bpId)) {
    amortization = amortMap.get(bpId) * runs;
  } else {
    const contractValue = internalBpcMap.get(bpId);
    if (contractValue && contractValue > 0) {
      // Normalizes bulk contract values down to a strict per-run cost
      const pRuns = presetRuns || 1;
      amortization = (contractValue / pRuns) * runs;
    } else {
      amortization = (Number(bpcWacData[bpId]) || 0) * runs;
    }
  }

  // 2. Material Cost (EVE Batch Formula)
  const meMultiplier = activityId === 1 ? (100 - meLevel) / 100 : 1;
  let materialCost = 0;

  materials.forEach(m => {
    if (m.activityID !== activityId) return;
    const baseQty = Number(m.quantity);

    // EVE BATCH FORMULA: Math.ceil( Base * (1 - ME/100) * Runs )
    // Math.max(runs, ...) guarantees at least 1 unit is consumed per run
    const jobQty = Math.max(runs, Math.ceil(baseQty * meMultiplier * runs));
    const unitPrice = (costMap.get(m.materialTypeID) || { landed: 0 }).landed;

    materialCost += (jobQty * unitPrice);
  });

  // 3. Installation Cost
  // If ESI gave us an actual live tax, use it. Otherwise, estimate it.
  const installCost = actualInstallCost !== undefined
    ? Number(actualInstallCost)
    : materialCost * (estInstallRate || 0.05);

  // 4. Final Yield & Unit Cost
  const totalYield = Math.max(baseYield * runs, 1);
  const totalCost = materialCost + installCost + amortization;
  const unitCost = totalCost / totalYield;

  return {
    materialCost: materialCost,
    amortization: amortization,
    installCost: installCost,
    totalCost: totalCost,
    unitCost: unitCost,
    totalYield: totalYield
  };
}

// ----------------------------------------------------------------------
// --- STAGE 1: BPC Cost Calculation (DYNAMIC EXTENSION ENG) ---
// ----------------------------------------------------------------------

/**
 * STAGE 1: BPC Creation Ledger
 * Finalized: Event Sourcing Architecture.
 * FIXED: Uses SDE Name Mapping for ledger item names.
 * FIXED: Pre-fetches Datacore/Decryptor costs to eliminate 0-cost bugs.
 * FIXED: ESI Data Sanitizer added to prevent undefined type_id crashes.
 */
function runBpcCreationLedger(ss, jobMap, skipSummary) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!jobMap) jobMap = _getJobMap(ss);
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  if (!skipSummary) skipSummary = false;

  // Standard Setup
  const configMap = _getMasterBlueprintConfig(ss);
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  const nameMap = _getSdeNameMap(ss);
  const encryptorMatrixMap = _loadEncryptorMatrixMap(ss);
  const internalBpcMap = _buildInternalBpcMap_(ss);
  const bpcWacData = JSON.parse(SCRIPT_PROP.getProperty(BPC_WAC_KEY) || '{}');

  const processedJobIds = new Set(JSON.parse(SCRIPT_PROP.getProperty(BPC_JOB_KEY) || '[]'));
  let newBpcJobs = _getNewCompletedJobs(jobMap, processedJobIds, [INDUSTRY_ACTIVITY_COPYING, INDUSTRY_ACTIVITY_INVENTION]);

  if (newBpcJobs.length === 0) return;

  // Sanitizer
  const validBpcJobs = [];
  for (const job of newBpcJobs) {
    let pid = parseInt(job.product_type_id, 10);
    if (isNaN(pid) || pid <= 0) {
      if (job.activity_id === INDUSTRY_ACTIVITY_COPYING) pid = parseInt(job.blueprint_type_id, 10);
      else if (job.activity_id === INDUSTRY_ACTIVITY_INVENTION) {
        const productInfo = sdeProdMap.get(`${job.activity_id}:${job.blueprint_type_id}`);
        if (productInfo) pid = parseInt(productInfo.typeID || productInfo.productTypeID, 10);
      }
    }
    if (isNaN(pid) || pid <= 0) continue;
    job.product_type_id = pid;
    validBpcJobs.push(job);
  }

  // Batch Processing
  const allRequiredIds = new Set();
  encryptorMatrixMap.forEach(data => allRequiredIds.add(data.typeID));
  validBpcJobs.forEach(job => {
    const materials = sdeMatMap.get(job.blueprint_type_id);
    if (materials) materials.forEach(m => allRequiredIds.add(m.materialTypeID));
  });
  const costMap = _getBlendedCostMap(ss, Array.from(allRequiredIds), true);

  const ledgerAPI = ML.forSheet('Material_Ledger', ss);
  const ledgerObjects = [];
  const newlyProcessedIds = [];

  for (const job of validBpcJobs) {
    const itemName = nameMap.get(job.product_type_id) || "Blueprint " + job.product_type_id;
    let totalMaterialCost = 0;

    if (job.activity_id === INDUSTRY_ACTIVITY_INVENTION) {
      const baseRuns = configMap.has(job.product_type_id) ? configMap.get(job.product_type_id).presetRuns : 1;
      const runMod = (job.licensed_runs / job.runs) - baseRuns;
      const decryptor = encryptorMatrixMap.get(runMod);
      if (decryptor) totalMaterialCost += ((costMap.get(decryptor.typeID)?.landed || 0) * job.runs);
      (sdeMatMap.get(job.blueprint_type_id) || []).filter(m => m.activityID === INDUSTRY_ACTIVITY_INVENTION).forEach(mat => {
        totalMaterialCost += ((costMap.get(mat.materialTypeID)?.landed || 0) * mat.quantity * job.runs);
      });
      totalMaterialCost += ((internalBpcMap.get(job.blueprint_type_id) || Number(bpcWacData[job.blueprint_type_id] || 0)) * job.runs);
    }

    const totalActualCost = totalMaterialCost + (Number(job.cost) || 0);
    if (job.successful_runs > 0) {
      ledgerObjects.push(createLedgerRow(job, (job.activity_id === INDUSTRY_ACTIVITY_INVENTION ? "INVENTION" : "COPYING"), job.successful_runs * job.licensed_runs, totalActualCost / (job.successful_runs * job.licensed_runs), { cost: totalActualCost, runs: job.licensed_runs }, itemName));
    }
    newlyProcessedIds.push(job.job_id);
  }

  if (ledgerObjects.length > 0) ledgerAPI.upsert(['date', 'source', 'type_id', 'contract_id'], ledgerObjects, true, skipSummary);
  newlyProcessedIds.forEach(id => processedJobIds.add(id));
  SCRIPT_PROP.setProperty(BPC_JOB_KEY, JSON.stringify(Array.from(processedJobIds).slice(-1000)));
}

function createLedgerRow(job, source, qty, unitValue, hist, name) {
  if (!job.product_type_id) {
    // This instantly kills the script and logs the custom message
    throw new Error(`CRITICAL ESI FAILURE: Job ID ${job.job_id} returned without a product_type_id. Sync halted.`);
  }
  return {
    date: job.date || job.end_date,
    date: job.completed_date || job.end_date,
    item_name: name,
    qty: qty,
    unit_value: '',
    source: source,
    contract_id: job.job_id,
    char: job.installer_id,
    unit_value_filled: unitValue,
    // Add a last_updated timestamp to force the upsert logic to see a change
    metadata: {
      runs: job.licensed_runs,
      cumulative_cost: hist.cost,
      cumulative_runs: hist.runs,
      me: job.me || 0,
      te: job.te || 0,
      updated_at: new Date().getTime()
    }
  };
}

function purgeCorruptWacEntries() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const rawData = SCRIPT_PROP.getProperty(BPC_WAC_KEY);
  if (!rawData) return;

  const wacData = JSON.parse(rawData);
  const corruptIds = ["31795", "2334", "31723", "12085", "31725", "4400", "1071", "1878", "3832", "3042", "31221", "31379", "19807", "32046", "24428", "2938", "2874"];

  corruptIds.forEach(id => {
    if (wacData.hasOwnProperty(id)) {
      delete wacData[id];
      Logger.log("Purged corrupt WAC for ID: " + id);
    }
  });

  SCRIPT_PROP.setProperty(BPC_WAC_KEY, JSON.stringify(wacData));
}

// ----------------------------------------------------------------------
// --- MATRIX INGESTION LOADER ---
// ----------------------------------------------------------------------
function _loadEncryptorMatrixMap(ss) {
  const sheet = ss.getSheetByName("SDE_Encryptor_Matrix");
  const matrixMap = new Map();
  if (!sheet) return matrixMap;

  const data = sheet.getDataRange().getValues();
  // CSV: typeID(0), typeName(1), probModifier(2), runModifier(3), meModifier(4), teModifier(5)
  for (let i = 1; i < data.length; i++) {
    const typeId = Number(data[i][0]);
    const runMod = parseInt(data[i][3], 10);
    const me = Number(data[i][4]) || 0;
    const te = Number(data[i][5]) || 0;

    matrixMap.set(runMod, {
      typeID: typeId,
      me: me,
      te: te
    });
  }
  return matrixMap;
}

function clearSpecificBpcHistory() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  // 1. Pull the master registries
  const historyData = JSON.parse(SCRIPT_PROP.getProperty('BpcHistoryData') || '{}');
  const finalWAC = JSON.parse(SCRIPT_PROP.getProperty('BpcWacData') || '{}'); // Use your exact BPC_WAC_KEY string

  // 2. Specify the Tech II blueprint Type IDs you messed up last week
  const targetIdsToReset = [/* Put your T2 BPC Type IDs here, e.g., 22444 */];

  targetIdsToReset.forEach(id => {
    delete historyData[id];
    delete finalWAC[id];
    Logger.log(`Cleared historical data cache for Blueprint Type ID: ${id}`);
  });

  // 3. Save the trimmed registries back to cache
  SCRIPT_PROP.setProperty('BpcHistoryData', JSON.stringify(historyData));
  SCRIPT_PROP.setProperty('BpcWacData', JSON.stringify(finalWAC));
}

function INSPECT_PROCESSED_JOB_IDS() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const data = JSON.parse(SCRIPT_PROP.getProperty("INDUSTRY_JOB_KEY") || "[]");
  console.log("Currently tracking " + data.length + " job IDs.");
  console.log(data); // This will print the actual array to your execution log
}

/**
 * ⚙️ MASTER BLUEPRINT CONFIG ENGINE (DRY)
 * Loads Config_BPC_Runs exactly once and returns a comprehensive 
 * configuration object for every tracked blueprint.
 */
let _masterConfigCache = null; // Caches globally to save API/Sheet calls

function _getMasterBlueprintConfig(ss) {
  if (_masterConfigCache) return _masterConfigCache;

  const CONFIG_NAME = "Config_BPC_Runs";
  const CONFIG_HEADERS = ['bp_type_id', 'preset_runs', 'max_me', 'max_te'];
  const configMap = new Map();
  const sheet = getOrCreateSheet(ss, CONFIG_NAME, CONFIG_HEADERS);

  if (sheet.getLastRow() >= 2) {
    const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
    const col = _getColIndexMap(headers, CONFIG_HEADERS);
    const data = sheet.getRange(2, 1, sheet.getLastRow() - 1, sheet.getLastColumn()).getValues();

    data.forEach(r => {
      const id = Number(r[col.bp_type_id]);
      if (id > 0) {
        configMap.set(id, {
          presetRuns: Number(r[col.preset_runs]) || 1,
          maxMe: Number(r[col.max_me]) || 10, // Smart fallback
          maxTe: Number(r[col.max_te]) || 20
        });
      }
    });
  }

  _masterConfigCache = configMap;
  return configMap;
}

/**
 * 🧠 SMART CONFIG RESOLVER
 * Centralizes the baseline ME/TE rules. 
 * Checks the Config CSV first. If missing, it applies the T1/T2 heuristic.
 */
function _resolveSmartConfig(bpId, bpName, ss) {
  const configMap = _getMasterBlueprintConfig(ss);

  // 1. Explicit Config Sheet Override (If you manually set it in the sheet, it wins)
  if (configMap.has(bpId)) {
    const explicit = configMap.get(bpId);
    if (explicit.maxMe > 0) return explicit;
  }

  // 2. The Global Smart Defaults (Define your corp standards here)
  const T1_ME = 10;
  const T2_ME = 2; // Change to 5 if you standardize around specific decryptors

  const isTech2 = String(bpName).trim().endsWith("II");

  return {
    presetRuns: configMap.has(bpId) ? configMap.get(bpId).presetRuns : 1,
    maxMe: isTech2 ? T2_ME : T1_ME,
    maxTe: isTech2 ? 4 : 20,
    source: isTech2 ? "Default T2" : "Default T1" // Tags it for the logger
  };
}

/**
 * STAGE 2: Manufacturing Ledger (CACHE-FREE MASTER ARCHITECTURE)
 * UPGRADE: 1k cache limit completely removed. The Sheet is now the absolute database.
 * UPGRADE: Defensive array initialization prevents 'undefined' crash errors.
 */
function runIndustryLedgerUpdate(ss, startTime, jobMap) {
  // 1. Resolve arguments with fallback defaults
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  if (!jobMap) jobMap = _getJobMap(ss);
  if (!startTime) startTime = Date.now();

  // SAFETY CHECK: Ensure jobMap is actually a Map before proceeding
  if (!jobMap || typeof jobMap.entries !== 'function') {
    const log = (typeof LOG_INDUSTRY !== 'undefined') ? LOG_INDUSTRY : console;
    log.error("CRITICAL: jobMap failed to initialize in Phase 2. Aborting.");
    return;
  }

  // 2. Initial Setup (Note: processedJobIds memory block has been eradicated)
  const { sdeMatMap, sdeProdMap } = _getSdeMaps(ss);
  if (sdeMatMap.size === 0) return;
  
  const nameMap = _getSdeNameMap(ss);
  const configMap = _getMasterBlueprintConfig(ss);
  const costMap = _getBlendedCostMap(ss);
  const amortMap = _getBpoAmortizationMap(ss);
  
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const bpcWacData = JSON.parse(SCRIPT_PROP.getProperty(BPC_WAC_KEY) || '{}');
  
  const bpoAttributesMap = _getBpoAttributesMapFromEsi();
  const internalBpcMap = _buildInternalBpcMap_(ss);
  const ledgerAPI_Local = ML.forSheet('Material_Ledger', ss);

  // --- 3. THE MASTER MEMORY FILTER (SHEET AS DATABASE) ---
  // Defensively load the ledger. If it's empty/null, it defaults to a safe, empty array.
  const existingLedger = ledgerAPI_Local.query({ source: "INDUSTRY" }) || [];
  const safeLedgerArray = Array.isArray(existingLedger) ? existingLedger : [];
  const ledgerIds = new Set(safeLedgerArray.map(j => String(j.contract_id)));

  const targetActivities = [1, 5, 8, 3, 4];
  
  const newJobs = Array.from(jobMap.values()).filter(job => {
    const jid = String(job.job_id);
    
    // THE ULTIMATE GHOST KILLER: 
    // If it's delivered AND already saved in the ledger, ignore it completely.
    if (job.status === 'delivered' && ledgerIds.has(jid)) return false;

    // Otherwise, process it! (Active jobs will recalculate and upsert to show live WIP costs)
    return ['active', 'ready', 'delivered'].includes(job.status) &&
           targetActivities.includes(parseInt(job.activity_id, 10));
  });

  if (newJobs.length === 0) {
    LOG_INDUSTRY.info("No new jobs to process.");
    return;
  }

  const ledgerObjects = [];

  // --- 4. PROCESS THE FILTERED JOBS ---
  for (const job of newJobs) {
    // Time-based safety breaker (240,000ms = 4 minutes)
    if ((Date.now() - (startTime || Date.now()) > 240000)) {
      LOG_INDUSTRY.warn(`Circuit Breaker: Time limit nearing. Saving progress...`);
      break;
    }

    try {
      const materials = sdeMatMap.get(job.blueprint_type_id);
      if (!materials) continue;

      const financials = _calculateJobFinancials({
        bpId: job.blueprint_type_id,
        activityId: parseInt(job.activity_id, 10),
        runs: parseInt(job.runs, 10),
        meLevel: bpoAttributesMap.has(job.blueprint_type_id) ? bpoAttributesMap.get(job.blueprint_type_id).material_efficiency : 0,
        materials: materials,
        costMap: costMap,
        amortMap: amortMap,
        internalBpcMap: internalBpcMap,
        bpcWacData: bpcWacData,
        presetRuns: configMap.has(job.blueprint_type_id) ? configMap.get(job.blueprint_type_id).presetRuns : 1,
        baseYield: (Array.isArray(sdeProdMap.get(`${job.activity_id}:${job.blueprint_type_id}`)) 
            ? sdeProdMap.get(`${job.activity_id}:${job.blueprint_type_id}`).find(p => p.activityID === parseInt(job.activity_id, 10))?.quantity 
            : sdeProdMap.get(`${job.activity_id}:${job.blueprint_type_id}`)?.quantity) || 1,
        actualInstallCost: Number(job.cost),
        estInstallRate: 0.05
      });

      ledgerObjects.push({
        date: job.completed_date || job.end_date,
        type_id: job.product_type_id || job.blueprint_type_id,
        item_name: nameMap.get(job.product_type_id) || `Product ${job.product_type_id}`,
        qty: financials.totalYield,
        unit_value: '',
        source: "INDUSTRY",
        contract_id: job.job_id,
        char: job.installer_id,
        unit_value_filled: financials.unitCost,
        metadata: { 
          me: bpoAttributesMap.has(job.blueprint_type_id) ? bpoAttributesMap.get(job.blueprint_type_id).material_efficiency : 0, 
          te: bpoAttributesMap.has(job.blueprint_type_id) ? bpoAttributesMap.get(job.blueprint_type_id).time_efficiency : 0 
        }
      });

    } catch (err) {
      LOG_INDUSTRY.error(`Skipping Job ${job.job_id}: ${err.message}`);
    }
  }

  // --- 5. PERSISTENCE ---
  if (ledgerObjects.length > 0) {
    ledgerAPI_Local.upsert(['date', 'source', 'type_id', 'contract_id'], ledgerObjects, false);
    LOG_INDUSTRY.info(`Saved ${ledgerObjects.length} jobs to ledger.`);
  }
}

// ----------------------------------------------------------------------
// --- DATA HELPERS ---
// ----------------------------------------------------------------------

// Global memory cache for the duration of the script run
var blendedCache = new Map();
var _tier1Loaded = false;
var _tier15Loaded = false;

function _getBlendedCostMap(ss, requiredMaterialIds, applyFailsafe = false) {

  const log = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('COST_ENGINE') : console;

  // 2. THE TIMEOUT FIX: We MUST pass 'ss' here so getNamedOr doesn't hang the Google servers
  const BROKER_FEE_RATE = Number(_getNamedOr_(ss, 'FEE_RATE', 0.03));
  const TRANSACTION_TAX_RATE = Number(_getNamedOr_(ss, 'TAX_RATE', 0.075));
  const ACQUISITION_MULTIPLIER = 1 + BROKER_FEE_RATE + TRANSACTION_TAX_RATE;

  const createCostObj = (raw, isManufactured = false) => ({
    raw: raw,
    landed: raw * (isManufactured ? 1 : ACQUISITION_MULTIPLIER)
  });

  // 3. TIER 1: BLENDED COST (Load exactly once per script execution)
  if (!_tier1Loaded) {
    const sheet = ss.getSheetByName("Blended_Cost");
    if (sheet && sheet.getLastRow() >= 2) {
      const rawData = sheet.getDataRange().getValues();
      const headers = rawData[0];
      try {
        const col = _getColIndexMap(headers, ['type_id', 'unit_weighted_average']);
        rawData.slice(1).forEach(row => {
          const tid = parseInt(row[col.type_id], 10);
          const rawValue = String(row[col.unit_weighted_average]);
          const cost = parseFloat(rawValue.replace(/[^0-9.]/g, '')) || 0.0;
          if (tid > 0 && cost > 0) blendedCache.set(tid, createCostObj(cost, true));
        });
      } catch (e) { log.warn("Tier 1 Load Failed: " + e.message); }
    }
    _tier1Loaded = true;
  }

  // --- TIER 1.5: MANUFACTURED COMPONENT FALLBACK (Load exactly once per script execution) ---
  if (!_tier15Loaded) {
    const projSheet = ss.getSheetByName("Projected_Build_Costs");
    if (projSheet && projSheet.getLastRow() >= 2) {
      const pRaw = projSheet.getDataRange().getValues();
      const pHeaders = pRaw[0].map(h => String(h).toLowerCase().trim().replace(/_/g, ' '));
      try {
        const idCol = pHeaders.includes('type id') ? pHeaders.indexOf('type id') : pHeaders.findIndex(h => h.includes('id'));
        const costCol = pHeaders.includes('cost') ? pHeaders.indexOf('cost') : pHeaders.findIndex(h => h.includes('cost'));

        if (idCol !== -1 && costCol !== -1) {
          pRaw.slice(1).forEach(row => {
            const tid = parseInt(row[idCol], 10);
            const cost = parseFloat(String(row[costCol]).replace(/[^0-9.]/g, '')) || 0.0;
            if (tid > 0 && cost > 0 && !blendedCache.has(tid)) {
              blendedCache.set(tid, createCostObj(cost, true));
            }
          });
        }
      } catch (e) { log.warn("Tier 1.5 Load Failed: " + e.message); }
    }
    _tier15Loaded = true;
  }

  // 1. Evaluate what we STILL need from the external APIs
  const tier2FetchList = new Set();
  if (requiredMaterialIds) {
    requiredMaterialIds.forEach(id => {
      const tid = parseInt(id, 10);
      if (tid > 0 && !blendedCache.has(tid)) {
        tier2FetchList.add(tid);
      }
    });
  }

  // If our sheets provided everything, return immediately without touching external API tiers
  if (tier2FetchList.size === 0) return blendedCache;

  // 4. TIER 2: MARKET MEDIAN (Only targeting items missing from our sheets)
  const marketMedianMap = _getMarketMedianMap(ss);
  const tier3FetchList = new Set();

  tier2FetchList.forEach(tid => {
    const mktCost = marketMedianMap.get(tid) || 0.0;
    if (mktCost > 0) blendedCache.set(tid, createCostObj(mktCost, false));
    else tier3FetchList.add(tid);
  });

  // 5. TIER 3: HUB FALLBACK
  if (tier3FetchList.size > 0) {
    const typeIDs = Array.from(tier3FetchList);
    const apiResults = hubFallBack(typeIDs, "sell", "min", ss);
    if (apiResults && apiResults.length > 0) {
      for (let i = 0; i < typeIDs.length; i++) {
        const tid = parseInt(typeIDs[i], 10);
        const rawPrice = Array.isArray(apiResults[i]) ? apiResults[i][0] : apiResults[i];
        const cost = parseFloat(rawPrice) || 0.0;
        if (tid > 0 && cost > 0) blendedCache.set(tid, createCostObj(cost, false));
      }
    }
  }

  // 6. FINAL FAILSAFE SWEEP
  if (applyFailsafe) {
    tier2FetchList.forEach(tid => {
      if (!blendedCache.has(tid) || blendedCache.get(tid).raw <= 0) {
        blendedCache.set(tid, createCostObj(1.0, false)); // Failsafe to 1.0 ISK
      }
    });
  }

  return blendedCache;
}

/**
 * Phase 4: Hangar Audit (Dynamic ID Version)
 */
function syncCorpBlueprintsV12() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const authToon = getCorpAuthChar(ss);

  const charData = GESI.getCharacterData(authToon);
  if (!charData || !charData.corporation_id) {
    console.error(`[CRITICAL] Could not find Corp ID for ${authToon}. Check GESI Auth.`);
    return;
  }

  const corpId = charData.corporation_id;
  console.log(`Auditing Hangar for Corp: ${corpId} (${authToon})`);

  // Route execution through your robust look-ahead parallelized cache loader
  const allBlueprints = _getCorporateBlueprintsRaw(true);

  if (allBlueprints && allBlueprints.length > 0) {
    // FIX: Passing the actual variable we just defined
    _updateBpoConfigFromAudit(allBlueprints);
  } else {
    console.error("Audit aborted: No data returned from ESI or Cache.");
  }
}

/**
 * THE REGISTRY BRIDGE: Aggregates ESI hangar assets and commits whole-number 
 * weighted ME/TE stats strictly to the Config_BPC_Runs backend master tab.
 */
function _updateBpoConfigFromAudit(blueprints) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName("Config_BPC_Runs");
  const nameMap = _getSdeNameMap(ss);
  if (!sheet || !blueprints || blueprints.length === 0) return;

  // 1. Group raw hangar items by Type ID and aggregate research percentages
  const auditMap = new Map();
  blueprints.forEach(bp => {
    if (bp.runs === -1) { // Process BPOs only (Ignore BPCs)
      const id = Number(bp.type_id);

      if (!auditMap.has(id)) {
        auditMap.set(id, {
          count: 0,
          totalMe: 0,
          totalTe: 0
        });
      }

      const current = auditMap.get(id);
      current.count += 1;
      current.totalMe += (Number(bp.material_efficiency) || 0);
      current.totalTe += (Number(bp.time_efficiency) || 0);
    }
  });

  // 2. Map headers dynamically to protect against column layout drift
  const rawData = sheet.getDataRange().getValues();
  const headers = rawData[0];

  let col;
  try {
    col = _getColIndexMap(headers, ['bp_type_id', 'available_bpos']);
  } catch (e) {
    console.error("Critical: Config_BPC_Runs sheet missing required identity headers.");
    return;
  }

  const lowerHeaders = headers.map(h => String(h || '').toLowerCase().trim());
  const meColIndex = lowerHeaders.indexOf('max_me');
  const teColIndex = lowerHeaders.indexOf('max_te');

  const dataRows = rawData.slice(1);
  const trackedBpIds = new Set();

  // 3. Loop existing registry records and overwrite with fresh averages
  if (dataRows.length > 0) {
    const updatedFullTableMatrix = dataRows.map(row => {
      const bpID = Number(row[col.bp_type_id]);
      trackedBpIds.add(bpID);

      if (auditMap.has(bpID)) {
        const liveAssetData = auditMap.get(bpID);
        row[col.available_bpos] = liveAssetData.count;

        // Compute whole numbers, then clamp to EVE engine limits (ME: 10, TE: 20)
        const avgMe = Math.round(liveAssetData.totalMe / liveAssetData.count);
        const avgTe = Math.round(liveAssetData.totalTe / liveAssetData.count);

        const weightedMeInt = Math.min(avgMe, 10);
        const weightedTeInt = Math.min(avgTe, 20);

        if (meColIndex !== -1) row[meColIndex] = weightedMeInt;
        if (teColIndex !== -1) row[teColIndex] = weightedTeInt;
      } else {
        row[col.available_bpos] = 0; // Asset is no longer in corporation hangars
      }
      return row;
    });

    sheet.getRange(2, 1, updatedFullTableMatrix.length, headers.length).setValues(updatedFullTableMatrix);
  }

  // 4. Append Delta Check: Inject newly acquired blueprint patterns
  const newRowsToAppend = [];
  for (const [hangarBpId, assetObj] of auditMap.entries()) {
    if (!trackedBpIds.has(hangarBpId)) {
      const bpName = nameMap.get(hangarBpId) || `Blueprint ${hangarBpId}`;

      // Compute whole numbers, then clamp to EVE engine limits
      const rawMe = Math.round(assetObj.totalMe / assetObj.count);
      const rawTe = Math.round(assetObj.totalTe / assetObj.count);

      const finalMeInt = Math.min(rawMe, 10);
      const finalTeInt = Math.min(rawTe, 20);

      const appendRow = new Array(headers.length).fill('');
      appendRow[col.bp_type_id] = hangarBpId;

      const presetRunsIdx = headers.indexOf('preset_runs');
      if (presetRunsIdx !== -1) appendRow[presetRunsIdx] = 1;

      const typeNameIdx = headers.indexOf('type_name');
      if (typeNameIdx !== -1) appendRow[typeNameIdx] = bpName;

      appendRow[col.available_bpos] = assetObj.count;
      if (meColIndex !== -1) appendRow[meColIndex] = finalMeInt;
      if (teColIndex !== -1) appendRow[teColIndex] = finalTeInt;

      const hardCapIdx = headers.indexOf('Hard Run Cap');
      if (hardCapIdx !== -1) appendRow[hardCapIdx] = 300;

      const dailyQuotaIdx = headers.indexOf('Daily Quota');
      if (dailyQuotaIdx !== -1) appendRow[dailyQuotaIdx] = 0;

      newRowsToAppend.push(appendRow);
    }
  }

  if (newRowsToAppend.length > 0) {
    sheet.getRange(sheet.getLastRow() + 1, 1, newRowsToAppend.length, headers.length).setValues(newRowsToAppend);
    console.log(`[REGISTRY ADD] Appended ${newRowsToAppend.length} newly discovered blueprint types to backend reference.`);
  }

  console.log(`[SUCCESS] Master Registry Sync Complete. Updated stats for ${auditMap.size} unique keys.`);
}


function _getBpoAmortizationMap(ss) {
  const AMORT_SHEET_NAME = "BPO_Amortization";
  const AMORT_HEADERS = ['bp_type_id', 'Amortization_Runs'];
  const amortMap = new Map();

  const sdePriceMap = _getSdeBasePriceMap(ss);
  const blendedCostMap = _getBlendedCostMap(ss, null, true);
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
let _SDE_CACHE = null;

function _getSdeMaps(ss) {
  if (_SDE_CACHE !== null) return _SDE_CACHE;

  const matSheet = ss.getSheetByName("SDE_industryActivityMaterials");
  const prodSheet = ss.getSheetByName("SDE_industryActivityProducts");
  const res = { sdeMatMap: new Map(), sdeProdMap: new Map() };

  if (!matSheet || !prodSheet) return res;

  const matHeaders = matSheet.getRange(1, 1, 1, matSheet.getLastColumn()).getValues()[0];
  const prodHeaders = prodSheet.getRange(1, 1, 1, prodSheet.getLastColumn()).getValues()[0];

  const matCol = _getColIndexMap(matHeaders, ['typeID', 'activityID', 'materialTypeID', 'quantity']);
  const prodCol = _getColIndexMap(prodHeaders, ['typeID', 'activityID', 'productTypeID', 'quantity']);

  const matData = matSheet.getRange(2, 1, matSheet.getLastRow() - 1, matSheet.getLastColumn()).getValues();
  const prodData = prodSheet.getRange(2, 1, prodSheet.getLastRow() - 1, prodSheet.getLastColumn()).getValues();

  // Map the Materials
  for (let i = 0; i < matData.length; i++) {
    const r = matData[i];
    const act = Number(r[matCol.activityID]);

    // FIXED: Added COPYING (5), TE_RESEARCH (3), and ME_RESEARCH (4)
    const validActivities = [
      INDUSTRY_ACTIVITY_MANUFACTURING,
      INDUSTRY_ACTIVITY_INVENTION,
      INDUSTRY_ACTIVITY_COPYING,
      INDUSTRY_ACTIVITY_TE_RESEARCH,
      INDUSTRY_ACTIVITY_ME_RESEARCH
    ];

    if (validActivities.includes(act)) {
      const bp = Number(r[matCol.typeID]);
      if (!res.sdeMatMap.has(bp)) res.sdeMatMap.set(bp, []);

      res.sdeMatMap.get(bp).push({
        materialTypeID: Number(r[matCol.materialTypeID]),
        quantity: Number(r[matCol.quantity]),
        activityID: act
      });
    }
  }

  // Map the Products
  for (let i = 0; i < prodData.length; i++) {
    const r = prodData[i];
    const act = Number(r[prodCol.activityID]);

    // Allow Manufacturing (1) AND Invention (8)
    if (act === INDUSTRY_ACTIVITY_MANUFACTURING || act === INDUSTRY_ACTIVITY_INVENTION) {
      // Create a composite key: ActivityID + BlueprintID
      // This allows you to have a product map that knows Invention results AND Mfg results
      const key = `${act}:${Number(r[prodCol.typeID])}`;
      res.sdeProdMap.set(key, {
        productTypeID: Number(r[prodCol.productTypeID]),
        quantity: Number(r[prodCol.quantity])
      });
    }
  }

  _SDE_CACHE = res;
  return res;
}

function _buildInternalBpcMap_(ss) {
  const log = LoggerEx.withTag('BPC_ALLOC');
  // Point to your actual item list sheet
  const SHEET_NAME = 'Item List';
  const bpcMap = new Map();

  const sheet = ss.getSheetByName(SHEET_NAME);
  if (!sheet) return bpcMap;

  try {
    const dataObj = _getData_(ss, SHEET_NAME);
    const h = dataObj.h;

    // Map these to your CSV headers: "Item Name" and "Custom Price"
    // Note: You may need to map "Item Name" to type_id using a lookup if your list 
    // only has names. If you have type_id in your list, use that!
    const cName = h['Item Name'];
    const cVal = h['Custom Price'];

    if (cName == null || cVal == null) return bpcMap;

    dataObj.rows.forEach(row => {
      // If your Item List only has names, you'll need a helper function 
      // like _getTypeIdFromName_(row[cName]) to get the ID
      const type_id = _getTypeIdFromName_(row[cName]);
      const valueStr = String(row[cVal]).replace(/[^\d.]/g, '');
      const internalVal = parseFloat(valueStr) || 0;

      if (type_id > 0 && internalVal > 0) {
        bpcMap.set(type_id, internalVal);
      }
    });

    log.info("[BPC] Loaded " + bpcMap.size + " custom valuations from Item List.");
  } catch (e) {
    log.warn("[BPC] Error loading custom values: " + e.message);
  }

  return bpcMap;
}

// Add a global variable at the top of IndustryLedger.gs
var _cachedSdeNameMap = null;

function _getSdeNameMap(ss) {
  if (_cachedSdeNameMap) return _cachedSdeNameMap;

  const sheet = ss.getSheetByName("SDE_invTypes");
  const map = new Map();
  if (!sheet) return map;

  // OPTIMIZATION: Only grab the columns we actually need.
  // This bypasses the memory overhead of the entire sheet grid.
  const lastRow = sheet.getLastRow();
  if (lastRow < 2) return map;

  const headers = sheet.getRange(1, 1, 1, sheet.getLastColumn()).getValues()[0];
  const colId = headers.indexOf('typeID');
  const colName = headers.indexOf('typeName');

  if (colId === -1 || colName === -1) return map;

  // Use getRange to grab just the two columns. 
  // This is exponentially faster than getDataRange().
  const data = sheet.getRange(2, 1, lastRow - 1, sheet.getLastColumn()).getValues();

  for (let i = 0; i < data.length; i++) {
    const id = Number(data[i][colId]);
    if (id > 0) {
      map.set(id, String(data[i][colName])); // Cast to String to ensure stability
    }
  }

  _cachedSdeNameMap = map;
  return map;
}

function _getMarketMedianMap(ss) {
  if (ss)
    return getMarketPriceMapFor(ss, 'Median Sell');
  return new Map(); // Safety catch!

}



function debugEsiJobs() {
  const authToon = getCorpAuthChar(); // This is your auth character
  const client = GESI.getClient();
  const charData = GESI.getCharacterData(authToon);
  const corpId = charData.corporation_id;

  client.setFunction('corporations_corporation_industry_jobs');
  const req = client.buildRequest({ corporation_id: corpId, include_completed: true, name: authToon });
  const response = UrlFetchApp.fetch(req.url, { method: 'get', headers: req.headers });
  const jobs = JSON.parse(response.getContentText());

  // Find your missing item by ID (or name) in this massive list
  const t2Jobs = jobs.filter(j => j.product_type_id === 31794); // Replace 31789 with the actual TypeID of your Extender II
  Logger.log(JSON.stringify(t2Jobs, null, 2));
}
var _cacheMarketPriceMapFor = null;
function getMarketPriceMapFor(ss, Attribute) {

  if (_cacheMarketPriceMapFor) return _cacheMarketPriceMapFor;
  const map = new Map();
  const NR_Prices = "NR_MARKET_MEDIAN_DATA";

  // 1. Normalize input: "Median Sell" becomes "median_sell"
  // This handles the space vs underscore mismatch automatically
  let targetAttr = (Attribute || "median_sell").toLowerCase().trim().replace(/\s+/g, '_');

  let data;
  try {
    let range = ss.getRangeByName(NR_Prices);
    if (!range) {
      const sheet = ss.getSheetByName("market price Tracker");
      if (!sheet) return map;
      range = sheet.getDataRange();
    }

    data = range.getValues();
    if (data.length < 2) return map;

    // 2. NORMALIZE HEADERS (to lowercase and trim)
    const headers = data[0].map(h => String(h).trim().toLowerCase());

    // 3. FIND COLUMNS
    const colId = headers.indexOf("type_id");
    let colMed = headers.indexOf(targetAttr);

    // Dynamic Fallback: If "median_sell" isn't found, try others from your CSV
    if (colMed === -1) {
      const fallbacks = ["median_sell", "min_sell", "median_buy", "max_buy"];
      for (const f of fallbacks) {
        if (headers.indexOf(f) !== -1) {
          colMed = headers.indexOf(f);
          break;
        }
      }
    }

    if (colId === -1 || colMed === -1) {
      console.warn(`getMarketPriceMapFor: Missing columns. ID: ${colId}, Target: ${targetAttr}. Found: ${headers}`);
      return map;
    }

    // 4. PARSE DATA
    for (let i = 1; i < data.length; i++) {
      const typeId = Number(data[i][colId]);
      // Your CSV has "40,900.00 ISK" - this regex strips the commas and "ISK"
      const rawVal = String(data[i][colMed]).replace(/[^0-9.]/g, "");
      const val = parseFloat(rawVal) || 0;

      if (typeId > 0 && val > 0) {
        map.set(typeId, val);
      }
    }
  } catch (e) {
    console.error(`CRITICAL ERROR in getMarketPriceMapFor: ${e.message}`);
  }
  _cacheMarketPriceMapFor = map;
  return map;
}

// Global memory cache for the SDE Base Prices
var sdeBasePriceCache = null;

function _getSdeBasePriceMap(ss) {
  // 1. THE CACHE INTERCEPT: Return instantly if already loaded
  if (sdeBasePriceCache) {
    return sdeBasePriceCache;
  }

  const sheet = ss.getSheetByName("SDE_invTypes");
  const map = new Map();
  if (!sheet) return map;

  try {
    // 2. THE CLAMP: Lock the exact dimensions
    const lastRow = sheet.getLastRow();
    const lastCol = sheet.getLastColumn();

    if (lastRow < 2 || lastCol < 1) return map;

    // 3. ONE-TOUCH FETCH
    const data = sheet.getRange(1, 1, lastRow, lastCol).getValues();
    const headers = data[0];

    const colId = headers.indexOf('typeID');
    const colP = headers.indexOf('basePrice');

    if (colId === -1 || colP === -1) return map;

    // 4. PARSE
    for (let i = 1; i < data.length; i++) {
      const typeId = Number(data[i][colId]);
      const p = Number(data[i][colP]);

      if (typeId > 0 && p > 0) {
        map.set(typeId, p);
      }
    }
  } catch (e) {
    console.error(`SDE Base Price Load Error: ${e.message}`);
  }

  // 5. SAVE TO CACHE: Store the map before returning
  sdeBasePriceCache = map;

  return map;
}

// ----------------------------------------------------------------------
// --- JOB FETCHING & CACHING ---
// ----------------------------------------------------------------------

function forceOverhaulReset() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  const historyData = JSON.parse(SCRIPT_PROP.getProperty('BpcHistoryData') || '{}');
  const finalWAC = JSON.parse(SCRIPT_PROP.getProperty(BPC_WAC_KEY) || '{}');

  // Specify the blueprint product IDs that need recalculating
  const blueprintsToFix = [22444, 22445];

  // Clear them out of the history registries
  blueprintsToFix.forEach(id => {
    delete historyData[id];
    delete finalWAC[id];
  });

  // FIX: Erase using the correct constant key to release the event log tracking lock
  SCRIPT_PROP.deleteProperty(BPC_JOB_KEY);

  SCRIPT_PROP.setProperty('BpcHistoryData', JSON.stringify(historyData));
  SCRIPT_PROP.setProperty(BPC_WAC_KEY, JSON.stringify(finalWAC));

  console.log("Cache cleared successfully. Ready for full historical ledger reprocessing.");
}


function _getJobMap(ss) {
  const range = ss.getRangeByName("NR_ESI_CORP_JOBS");
  if (!range) throw new Error("Named Range 'NR_ESI_CORP_JOBS' not found!");

  const data = range.getValues();
  // If data is empty or only has headers
  if (data.length <= 1) return new Map();

  const headers = data[0];
  const jobMap = new Map();

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const job = {};
    headers.forEach((h, colIdx) => job[h] = row[colIdx]);

    if (job.job_id) {
      jobMap.set(job.job_id.toString(), job);
    }
  }
  return jobMap;
}

// No 'ss' parameter, no API calls, no forced refreshes.
function _getNewCompletedJobs(jobMap, processedJobIds, activityIds) {
  const acts = activityIds || [1, 8, 11];

  // We filter the map values directly. This is instant memory-speed.
  return Array.from(jobMap.values()).filter(job => {
    const isFinished = job.status === 'delivered' || job.status === 'successful';
    const isTargetActivity = acts.includes(parseInt(job.activity_id));
    const isNew = !processedJobIds.has(job.job_id.toString());

    return isFinished && isTargetActivity && isNew;
  });
}

function _getBpoAttributesMapFromEsi() {
  // This instantly pulls from your chunked cache because forceRefresh is false
  const rawObjects = _getCorporateBlueprintsRaw(false);
  const attributesMap = new Map();

  if (!rawObjects) return attributesMap;

  for (const bpObj of rawObjects) {
    const typeId = Number(bpObj.type_id);
    const me = Number(bpObj.material_efficiency) || 0;
    const te = Number(bpObj.time_efficiency) || 0;

    if (attributesMap.has(typeId)) {
      // We already found a blueprint. If this new one is better, overwrite it.
      const existing = attributesMap.get(typeId);
      if (me > existing.material_efficiency || (me === existing.material_efficiency && te > existing.time_efficiency)) {
        attributesMap.set(typeId, { material_efficiency: me, time_efficiency: te });
      }
    } else {
      // First time seeing this blueprint type
      attributesMap.set(typeId, { material_efficiency: me, time_efficiency: te });
    }
  }

  return attributesMap;
}

/**
 * UPGRADED: Corporate Jobs Fetcher
 * Now uses ESI.forEndpoint for centralized error handling and quota management.
 */
function _getCorporateJobsRaw(ss, forceRefresh = false) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const props = PropertiesService.getScriptProperties();
  const authToon = getCorpAuthChar();
  
  // 1. Quota Gate
  if (ESI.isLocked()) {
    LOG_INDUSTRY.error("ABORT: ESI global quota is locked. Skipping Jobs fetch.");
    return [];
  }

  const log = LoggerEx.withTag('CORP_JOBS');
  const charData = GESI.getCharacterData(authToon);
  if (!charData || !charData.corporation_id) {
    log.error(`Aborted: Could not resolve dynamic Corp ID.`);
    return [];
  }

  // 2. ESI Module Integration
  const authClient = GESI.getClient(authToon);
  const service = ESI.forEndpoint(authClient, 'corporations_corporation_industry_jobs', {
    onLock: () => stopWorker_('Quota Lock Active')
  });

  const result = service.get({ corporation_id: charData.corporation_id });

  if (result.error) {
    log.error(`[ESI_MODULE] Fetch failed: ${result.error}`);
    return [];
  }

  const allJobs = result.data;
  if (!allJobs || allJobs.length === 0) return [];

  // 3. Sheet Writing
  const jobsSheet = ss.getSheetByName("ESI Corp Jobs");
  const STANDARD_HEADERS = ["activity_id", "blueprint_id", "blueprint_location_id", "blueprint_type_id", "completed_character_id", "completed_date", "cost", "duration", "end_date", "facility_id", "installer_id", "job_id", "licensed_runs", "location_id", "output_location_id", "pause_date", "probability", "product_type_id", "runs", "start_date", "status", "successful_runs"];

  const rows = allJobs.map(job => STANDARD_HEADERS.map(h => job[h] ?? null));
  const fullData = [STANDARD_HEADERS, ...rows];

  jobsSheet.getRange(1, 3, Math.max(jobsSheet.getLastRow(), 1), STANDARD_HEADERS.length).clearContent();
  const targetRange = jobsSheet.getRange(1, 3, fullData.length, STANDARD_HEADERS.length);
  targetRange.setValues(fullData);
  ss.setNamedRange("NR_ESI_CORP_JOBS", targetRange);

  return allJobs;
}

/**
 * UPGRADED: Blueprint Ingestion Engine
 */
function _getCorporateBlueprintsRaw(forceRefresh) {
  const log = LoggerEx.withTag('CORP_BPOS');
  const authToon = getCorpAuthChar();
  if (!authToon) return null;

  const charData = GESI.getCharacterData(authToon);
  if (!charData || !charData.corporation_id) return null;

  const corpId = charData.corporation_id;
  const cacheKey = BPO_RAW_CACHE_KEY + ':' + corpId;

  if (!forceRefresh) {
    const cachedJson = _getAndDechunk(cacheKey);
    if (cachedJson) return JSON.parse(cachedJson);
  }

  // ESI Module Integration
  const authClient = GESI.getClient(authToon);
  const service = ESI.forEndpoint(authClient, 'corporations_corporation_blueprints', {
    onLock: () => log.error("Blueprint fetch locked by Quota")
  });

  const result = service.get({ corporation_id: corpId });

  if (result.error) {
    log.error(`ESI Module Failure: ${result.error}`);
    return null;
  }

  const allBlueprints = result.data;
  _chunkAndPut(cacheKey, JSON.stringify(allBlueprints), BPO_RAW_CACHE_TTL);

  return allBlueprints;
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
  props.deleteProperty('processedIndustryJobIds');
  props.deleteProperty('processedConsumptionJobIds');

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
  const rawData = _getCorporateJobsRaw(ss, false);

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