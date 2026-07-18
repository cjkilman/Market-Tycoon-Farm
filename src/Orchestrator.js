/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, ScriptApp, 
  getMasterBatchFromControlTable, withSheetLock, getOrCreateSheet, 
cacheAllCorporateAssetsTrigger, triggerLedgerImportCycle, fuzAPI, _fetchProcessedLootData, 
runLootLedgerDelta, Ledger_Import_CorpJournal, syncContracts, runIndustryLedgerPhase,
  runLootDeltaPhase, runContractLedgerPhase,  LoggerEx, writeDataToSheet, guardedSheetTransaction, atomicSwapAndFlush, deleteTriggersByName, pauseSheet, wakeUpSheet, prepareTempSheet */

// Global variable to track recursion depth for this lock type
var EXECUTION_LOCK_DEPTH_TRY = 0;
var EXECUTION_LOCK_DEPTH_WAIT = 0;

var LOCK_TIMEOUT_MS = 5000;
var LOCK_WAIT_TIMEOUT_MS = 30000;

const finalSheetName = 'Market_Data_Raw';
const tempSheetName = 'Market_Data_Temp';
const MARKET_NAMED_RANGE = 'NR_MARKET_DATA';
const RETRY_DELAY_MS = 30 * 1000;
const PROP_KEY_FINALIZER_STEP = 'marketDataFinalizeStep';


/**
 * Clears all Script Properties for this specific project.
 */
function clearAllScriptProperties() {
  const scriptProperties = PropertiesService.getScriptProperties();
  scriptProperties.deleteAllProperties();
  Logger.log("All Script Properties have been cleared.");
}

/**
 * Clears all User Properties (stored per user per project).
 */
function clearAllUserProperties() {
  const userProperties = PropertiesService.getUserProperties();
  userProperties.deleteAllProperties();
  Logger.log("All User Properties have been cleared.");
}

/**
 * Clears all Document Properties (stored per spreadsheet/doc).
 */
function clearAllDocumentProperties() {
  const documentProperties = PropertiesService.getDocumentProperties();
  documentProperties.deleteAllProperties();
  Logger.log("All Document Properties have been cleared.");
}

// --- TIME GATING CONSTANTS ---
const HOURLY_RUN_INTERVAL_MS = 60 * 60 * 1000;
const JOURNAL_RUN_INTERVAL_MS = 10 * 60 * 1000;
const PROP_KEY_LAST_RUN_TS = 'MAINTENANCE_LAST_RUN_TS_';
const PROP_KEY_HISTORY_DONE = 'HISTORY_PULL_COMPLETED_DATE';

if (typeof GLOBAL_STATE_KEY === 'undefined') {
  var GLOBAL_STATE_KEY = 'GLOBAL_SYSTEM_STATE';
}

// State Machine Constants
const STATE_FLAGS = {
  NEW_RUN: 'NEW_RUN',
  PROCESSING: 'PROCESSING',
  FINALIZING: 'FINALIZING'
};
const PROP_KEY_SETUP_STAGE = 'marketDataSetupStage';

function FORCE_RESTORE_RUNNING_STATE() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // 1. Force the system state back to RUNNING
  SCRIPT_PROP.setProperty('GLOBAL_SYSTEM_STATE', 'RUNNING');
  
  // 2. Force the sheet to wake up (in case it was paused)
  try {
    wakeUpSheet(ss);
  } catch(e) {
    console.error("Wakeup failed, but state is reset. Proceeding.");
  }
  
  // 3. Clear the market engine lock to allow fresh start
  _resetMarketDataJobState(null);
  
  console.log("CRITICAL: System forced back to RUNNING state.");
}

/**
 * Replaces IMPORTRANGE. Fetches static market prices from the external hub.
 * This completely kills the continuous recalculation loop caused by live linking.
 */
function fetchFilteredPricesSync(ss) {
  const LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('PRICE_SYNC') : console;

  // --- CONFIGURATION ---
  const SOURCE_SHEET_ID = "1L37sYZPznkNu3EJy554nmaclXQl6DpvERc_N6ans76M";
  const SOURCE_TAB_NAME = "filtered prices";
  const TARGET_SHEET_NAME = "market price Tracker";
  const RANGE_NAME = "NR_MARKET_MEDIAN_DATA";

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();

  try {
    LOG.info("Connecting to external price database...");

    const sourceBook = SpreadsheetApp.openById(SOURCE_SHEET_ID);
    const sourceSheet = sourceBook.getSheetByName(SOURCE_TAB_NAME);

    if (!sourceSheet) {
      throw new Error(`External tab '${SOURCE_TAB_NAME}' not found!`);
    }

    // Dynamically find the absolute bottom of the data
    const lastRow = sourceSheet.getLastRow();

    // Start at Row 7, Column 5 (E), drop down to lastRow, grab 8 columns across (E through L)
    const rawValues = sourceSheet.getRange(7, 5, lastRow - 6, 8).getValues();

    if (!rawValues || rawValues.length === 0) {
      LOG.warn("Fetch aborted: No data found in the source range.");
      return;
    }

    let targetSheet = ss.getSheetByName(TARGET_SHEET_NAME);
    if (!targetSheet) {
      targetSheet = ss.insertSheet(TARGET_SHEET_NAME);
      LOG.info(`Created new target sheet: ${TARGET_SHEET_NAME}`);
    }

    // Filter data
    const dataToWrite = rawValues.filter(row => row[0] !== "" && row[0] != null);
    if (dataToWrite.length === 0) {
      LOG.warn("No valid rows after cleaning.");
      return;
    }

    // 1. Prepare the Canvas (Wipe and Resize)
    targetSheet.clearContents();
    const maxRows = targetSheet.getMaxRows();

    // If the new payload is bigger than the sheet, add rows FIRST
    if (maxRows < dataToWrite.length) {
      targetSheet.insertRowsAfter(maxRows, dataToWrite.length - maxRows);
    }
    // If the sheet is too big, trim it down to save memory
    else if (maxRows > dataToWrite.length) {
      targetSheet.deleteRows(dataToWrite.length + 1, maxRows - dataToWrite.length);
    }

    // 2. Execute Write
    const finalRange = targetSheet.getRange(1, 1, dataToWrite.length, dataToWrite[0].length);
    finalRange.setValues(dataToWrite);

    // 3. THE SAFE NAMED RANGE UPDATE
    // We do this LAST so the range matches the final sheet dimensions exactly.
    const existing = ss.getNamedRanges().find(nr => nr.getName() === RANGE_NAME);
    if (existing) {
      existing.setRange(finalRange);
      LOG.info(`Updated existing Named Range: ${RANGE_NAME}`);
    } else {
      ss.setNamedRange(RANGE_NAME, finalRange);
      LOG.info(`Created new Named Range: ${RANGE_NAME}`);
    }

    LOG.info(`Price Sync Complete. Wrote ${dataToWrite.length} rows.`);
    ss.toast("External Prices Synced", "Engine Room", 3);

  } catch (e) {
    LOG.error("Failed to sync external prices: " + e.message);
    ss.toast("Price Sync Failed", "Engine Room Error");
  }
}

/**
 * Helper to create a new one-time "retry" trigger.
 */
function scheduleOneTimeTrigger(functionName, delayMs) {
  if (typeof functionName !== 'string' || functionName.trim() === '') {
    throw new Error(`CRITICAL SCHEDULER ERROR: Invalid function name provided.`);
  }

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const systemState = SCRIPT_PROP.getProperty(GLOBAL_STATE_KEY) || 'RUNNING';

  try {
    deleteTriggersByName(functionName);
    if (systemState === 'MAINTENANCE') {
      console.warn(`Blocking trigger for ${functionName}: MAINTENANCE mode.`);
      return;
    }
    ScriptApp.newTrigger(functionName).timeBased().after(delayMs).create();
    console.log(`Created trigger for ${functionName} in ~${Math.round(delayMs / 60000)} min.`);
  } catch (e) {
    console.error(`Failed to create trigger: ${e.message}`);
  }
}

function DEBUG_REMOTE_FILE_STRUCTURE() {
  const sourceId = "1L37sYZPznkNu3EJy554nmaclXQl6DpvERc_N6ans76M";
  const sourceFile = SpreadsheetApp.openById(sourceId);
  const sheets = sourceFile.getSheets();
  
  console.log("Remote File Sheets:");
  sheets.forEach(s => console.log("- " + s.getName()));
  
  const target = sourceFile.getSheetByName("Publish_ESI_Region_market_orders");
  if(target) {
    console.log("Target Sheet Found. Data Range: " + target.getDataRange().getA1Notation());
  } else {
    console.error("Target sheet not found!");
  }
}


/**
 * Grabs Regional Pricing from Market Price Tracker.
 * UPGRADED: Pulls sanitized client data and preserves visual formatting.
 */
function syncESIRegionData(ss) {
  const log = LoggerEx.withTag('REGION_SYNC');
  const sourceId = "1L37sYZPznkNu3EJy554nmaclXQl6DpvERc_N6ans76M";

  // Target your specific Client Tab generated by the Engine
  const sourceSheetName = "Publish_ESI_Region_market_orders"; // <-- Update to match your actual client name
  const targetSheetName = "ESI_Region";
  const NAMED_RANGE_NAME = "ESI_Region_Data";

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const targetSheet = ss.getSheetByName(targetSheetName);

  if (!targetSheet) return;

  try {
    const sourceData = SpreadsheetApp.openById(sourceId)
      .getSheetByName(sourceSheetName)
      .getDataRange()
      .getValues();

    if (sourceData.length < 2) {
      log.warn("Source data is empty. Aborting sync.");
      return;
    }

    const requiredCols = sourceData[0].length;
    const currentCols = targetSheet.getMaxColumns();
    if (currentCols < requiredCols) {
      targetSheet.insertColumnsAfter(currentCols, requiredCols - currentCols);
    }

    // CLEAR & WRITE
    targetSheet.clearContents();
    const newRange = targetSheet.getRange(1, 1, sourceData.length, requiredCols);
    newRange.setValues(sourceData);

    // REAPPLY FORMATTING
    // Assuming schema: [type_id, vol30, vol7, vol5, last_updated, status]
    targetSheet.getRange(2, 1, sourceData.length - 1, 1).setNumberFormat('0');
    targetSheet.getRange(2, 2, sourceData.length - 1, 3).setNumberFormat('#,##0');
    targetSheet.getRange(2, 5, sourceData.length - 1, 1).setNumberFormat('yyyy-mm-dd');

    // UPDATE NAMED RANGE
    ss.setNamedRange(NAMED_RANGE_NAME, newRange);
    log.info(`Named Range '${NAMED_RANGE_NAME}' updated to ${sourceData.length} rows and ${requiredCols} cols.`);

    // THE TRIM
    const lastRow = sourceData.length;
    const currentMax = targetSheet.getMaxRows();
    if (currentMax > lastRow) {
      targetSheet.deleteRows(lastRow + 1, currentMax - lastRow);
    }

    log.info("ESI_Region: Sync, Formatting, & Named Range Update Complete.");
  } catch (e) {
    log.error("ESI_Region Sync Error: " + e.message);
  }
}

/**
 * Dynamically updates the Named Range for the Market Orders sheet.
 * This prevents the "A1:H" shrinkage that causes the 0-velocity bugs.
 */
function updateMarketOrdersNamedRange(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Publish_ESI_Region_market_orders";
  const rangeName = "Region_Radar_Table"; // This is what your VLOOKUP uses

  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    Logger.log("Error: Sheet " + sheetName + " not found.");
    return;
  }

  // 1. Find the boundaries
  const lastRow = sheet.getLastRow();
  // We force it to Column 24 (X) to ensure index 18 and 23 are always inside
  const lastCol = 24;

  // 2. Define the new range (A1 to X[LastRow])
  const newRange = sheet.getRange(1, 1, lastRow, lastCol);

  // 3. Update the Named Range STABLY
  const existingNamedRange = ss.getNamedRanges().find(nr => nr.getName() === rangeName);

  if (existingNamedRange) {
    // This updates the "coordinates" without deleting the object,
    // which prevents the Velocity formula from losing its mind.
    existingNamedRange.setRange(newRange);
  } else {
    ss.setNamedRange(rangeName, newRange);
  }

  Logger.log("SUCCESS: " + rangeName + " now covers A1:X" + lastRow);
}

/**
 * Helper to delete triggers by name.
 */
function deleteTriggersByName(functionName) {
  if (typeof functionName !== 'string' || functionName.trim() === '') return 0;

  let deletedCount = 0;
  try {
    const allTriggers = ScriptApp.getProjectTriggers();
    allTriggers.forEach(trigger => {
      if (trigger.getHandlerFunction() === functionName &&
        trigger.getEventType() === ScriptApp.EventType.CLOCK) {
        try {
          ScriptApp.deleteTrigger(trigger);
          deletedCount++;
        } catch (e) { }
      }
    });
  } catch (e) {
    console.error(`Error deleting triggers: ${e.message}`);
  }
  return deletedCount;
}

/**
 * Internal reset helper.
 */
function _resetMarketDataJobState(error) {
  console.warn(`RESETTING Market Data Job State: ${error ? error.message : 'Manual'}.`);

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const keysToDelete = [
    'marketDataJobStep', 'marketDataRequestIndex', 'marketDataNextWriteRow',
    'marketDataFinalizeStep', 'marketDataSetupStep', 'marketDataJobLeaseUntil',
    'marketDataJobIsActive'
  ];

  try {
    keysToDelete.forEach(k => SCRIPT_PROP.deleteProperty(k));

    // --- NEW: ARM THE PENALTY BOX ---
    if (error) {
      // If triggered by an error, enforce a 30-minute cooldown
      const cooldownDuration = 30 * 60 * 1000; 
      const cooldownUntil = new Date().getTime() + cooldownDuration;
      SCRIPT_PROP.setProperty('MARKET_COOLDOWN', cooldownUntil.toString());
      console.warn(`[COOLDOWN] Market Data engine quarantined until ${new Date(cooldownUntil).toLocaleTimeString()}.`);
    } else {
      // If it's a manual/clean reset, clear any active cooldowns so we can run immediately
      SCRIPT_PROP.deleteProperty('MARKET_COOLDOWN');
    }

  } catch (propError) {
    console.error(`Error deleting properties: ${propError.message}`);
  }

  deleteTriggersByName('updateMarketDataSheet');
  deleteTriggersByName('finalizeMarketDataUpdate');
  console.log("Market data job state reset complete.");
}

/**
 * Wraps a function in a ScriptLock tryLock().
 */
function executeWithTryLock(funcToRun, functionName, timeoutMs = LOCK_TIMEOUT_MS) {
  const lock = LockService.getScriptLock();

  if (typeof funcToRun !== 'function') return false;
  if (!functionName) functionName = 'UnknownFunction';

  if (lock.tryLock(timeoutMs)) {
    try {
      const systemState = PropertiesService.getScriptProperties().getProperty(GLOBAL_STATE_KEY) || 'RUNNING';
      if (systemState === 'MAINTENANCE') {
        console.warn(`Skipping ${functionName}: MAINTENANCE mode.`);
        return null;
      }
      console.log(`--- Starting Execution (TryLock): ${functionName} ---`);
      return funcToRun();
    } catch (e) {
      console.error(`Unhandled exception in ${functionName}: ${e.message}`);
    } finally {
      lock.releaseLock();
      console.log(`Script Lock released for ${functionName}.`);
    }
  } else {
    console.warn(`Skipping execution of ${functionName}: Script Lock was busy.`);
    return null;
  }
}

/**
 * Wraps a function in a ScriptLock waitLock().
 */
function executeWithWaitLock(funcToRun, functionName, timeoutMs = LOCK_WAIT_TIMEOUT_MS) {
  const lock = LockService.getScriptLock();
  try {
    lock.waitLock(timeoutMs);
  } catch (e) {
    console.error(`Could not acquire Script Lock for ${functionName}.`);
    throw e;
  }

  try {
    const systemState = PropertiesService.getScriptProperties().getProperty(GLOBAL_STATE_KEY) || 'RUNNING';
    if (systemState === 'MAINTENANCE') {
      console.warn(`Skipping ${functionName}: MAINTENANCE mode.`);
      return null;
    }

    console.log(`--- Starting Execution (WaitLock): ${functionName} ---`);
    return funcToRun();
  } catch (e) {
    console.error(`Unhandled exception in ${functionName}: ${e.message}`);
    throw e;
  } finally {
    lock.releaseLock();
    console.log(`Script Lock released for ${functionName}.`);
  }
}

/**
 * Manual Quota and Cooldown Reset Function.
 * Call this when the daily quota resets or you want to clear a failed-job cooldown.
 */
function resetSystemQuota() {
 ESI.reset();
}

/**
 * The Master Orchestrator
 * High-frequency entry point. Manages the lifecycle of Market Data, 
 * Maintenance Jobs, and System State (Maintenance/Running).
 */
function masterOrchestrator() {
  const NOW_MS = new Date().getTime();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ORCHESTRATOR') : console;

  // --- 1. PROACTIVE QUOTA COMPLIANCE ---
  if (typeof ESI !== 'undefined' && ESI.isLocked()) {
    LOG.warn("QUOTA DEAD: masterOrchestrator suspended to save execution time.");
    return;
  }

  // --- 3. COOL DOWN CHECK ---
  const cooldownUntil = parseInt(SCRIPT_PROP.getProperty('MARKET_COOLDOWN') || '0', 10);
  if (cooldownUntil > NOW_MS) {
    LOG.warn(`Market Data engine is on cooldown.`);
  }

  // --- 4. PENDING FINALIZATIONS ---
  if (_nudgeCogsFinalizer()) return;

  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');
  if (marketDataStep === STATE_FLAGS.FINALIZING) {
    const lock = LockService.getScriptLock();
    if (lock.tryLock(0)) {
      lock.releaseLock();
      LOG.info(`Finalizing Market Data update.`);
      scheduleOneTimeTrigger("finalizeMarketDataUpdate", 5000);
    }
    return;
  }

  // --- 5. MARKET DATA ENGINE DISPATCH ---
  const lastMarketRun = parseInt(SCRIPT_PROP.getProperty('MARKET_DATA_LAST_RUN_TS') || '0', 10);
  const isMarketOnCooldown = (cooldownUntil > NOW_MS);
  const timeSinceLastRun = NOW_MS - lastMarketRun;
  const RUN_INTERVAL_MS = 28 * 60 * 1000; // 28 minute cycle

  if (!isMarketOnCooldown && timeSinceLastRun > RUN_INTERVAL_MS) {
    const leaseUntil = parseInt(SCRIPT_PROP.getProperty('marketDataJobLeaseUntil') || '0', 10);
    const isJobActive = leaseUntil > NOW_MS;

    if (!isJobActive) {
      LOG.info(`DISPATCHING MARKET DATA JOB (30m Cycle).`);
      
      // Set the lease target BEFORE invoking so concurrent ticks cannot duplicate the run
      SCRIPT_PROP.setProperty('marketDataJobLeaseUntil', (NOW_MS + 300000).toString());
      
      updateMarketDataSheet(NOW_MS); 
      return;
    }
  }

  // --- 6. SMART NUDGE GATE ---
  // Only nudge the worker if the processing step is active AND the script lock is clear.
  // If the lock cannot be acquired immediately, the worker is actively processing chunks.
  if (marketDataStep === STATE_FLAGS.PROCESSING || marketDataStep === STATE_FLAGS.NEW_RUN) {
    const probeLock = LockService.getScriptLock();
    if (probeLock.tryLock(0)) {
      // The lock was free, meaning the chained trigger died! Revive it.
      probeLock.releaseLock();
      LOG.info(`Market Data engine stalled in state (${marketDataStep}). Nudging worker.`);
      updateMarketDataSheet(NOW_MS);
    } else {
      // Lock is busy: The worker is healthy and running its chunk loops right now. Keep quiet.
      LOG.info(`Market Data Active (${marketDataStep}). Worker loop verified running.`);
    }
    return;
  }

  // --- 7. MAINTENANCE & IDLE TASKS ---
  LOG.info(`Market Data Idle. Attempting Maintenance cycle.`);
  executeWithTryLock(runMaintenanceJobs, 'runMaintenanceJobs');
}

function forceResetMaint() {
  const props = PropertiesService.getScriptProperties();
  props.deleteProperty('BOM_MAINTENANCE_LEASE');
  props.deleteProperty('LAST_RUN_generateFullBOMData');
  props.setProperty('MAINTENANCE_QUEUE_INDEX', '0');
  console.log("State cleared. BOM Engine is now next in queue.");
}

// Add this helper to your script
function checkQuotaAndHalt() {
  if (PropertiesService.getScriptProperties().getProperty('DAILY_QUOTA_EXHAUSTED') === 'true') {
    console.warn("[ABORT] Quota Exhausted. Pipeline halting execution.");
    return true; // Return true if we need to halt
  }
  return false;
}

/**
 * Unified Tycoon Engine Pipeline (Standalone UI/Math Version)
 * Runs independent of API status to ensure dashboard math is always available.
 */
function runUnifiedTycoonPipeline(startTime) {
  const log = LoggerEx.withTag('TYCOON_PIPELINE');
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const executionStart = startTime || Date.now();
  const FIVE_MINUTES_MS = 300000;

  log.info('--- Starting Unified Tycoon Calculation Pass (Local Math) ---');

  // Step 1: Projected Costs (Math)
  try {
    log.info('Step 1/3: Projected Cost Table...');
    generateProjectedCostTable(ss);
  } catch (e) {
    log.error('Step 1 Failed.', e);
    return;
  }

  // Step 2: Full BOM (Auto-Expansion Logic)
  try {
    log.info('Step 2/3: Full BOM Data Calculation...');
    generateFullBOMData(ss);
  } catch (e) {
    log.error('Step 2 Failed.', e);
    return;
  }

  // Step 3: Scrap & Reprocessing
  try {
    log.info('Step 3/3: Reprocessed Value Table...');
    generateReprocessedValueTable(ss);
  } catch (e) {
    log.error('Step 3 Failed.', e);
  }

  const totalTimeSec = ((Date.now() - executionStart) / 1000).toFixed(1);
  log.info(`--- Pipeline Complete. Total Execution Time: ${totalTimeSec}s ---`);
}

function runMaintenanceJobs(explicitNowMs) {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  // 1. Priority Lock: Maintenance must yield to active Market Data syncs
  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');
  const manualSync = SCRIPT_PROP.getProperty('MANUAL_SYNC_ACTIVE');
  if (marketDataStep === 'PROCESSING' || marketDataStep === 'NEW_RUN' || marketDataStep === 'FINALIZING' || manualSync === 'TRUE') {
    console.warn("[Maintenance] Aborted: Market Engine or Manual Sync is active.");
    return;
  }

  const NOW_MS = explicitNowMs || new Date().getTime();
  const STANDARD_INTERVAL = 3600000; // 60m default

  // 2. Job Registry with targeted intervals
  /**
   * @typedef {Object} MaintenanceJob
   * @property {string} name - The exact name of the global function to execute.
   * @property {number} interval - Frequency gate (ms). Minimum time required between runs before the job is marked "due".
   * @property {number} [lease] - Concurrency shield (ms). Active lock duration to prevent heavy tycoon tasks 
   * from double-executing or cross-threading if an instance is still processing.
   */
  const JOB_QUEUE = [
    // --- STEP 1: RAW INGESTION (ESI FEEDS) ---
    { name: 'cacheAllCorporateAssetsTrigger', interval: STANDARD_INTERVAL },
    { name: 'syncCorpBlueprintsV12', interval: 2700000, lease: 1200000 }, // Moved up!
    { name: 'TransactionsAndJournalSync', interval: STANDARD_INTERVAL },

    // --- STEP 2: PROCESSING & MATERIAL ALIGNMENT ---
    // Disabled { name: 'runContractLedgerPhase', interval: STANDARD_INTERVAL },
    { name: 'runIndustryLedgerPhase', interval: STANDARD_INTERVAL },
    { name: 'runLootDeltaPhase', interval: STANDARD_INTERVAL },

    // --- STEP 3: HEAVY TYCOON MATRIX MATH (DESTRUCTION & COGS) ---
    { name: 'runUnifiedTycoonPipeline', interval: 2700000, lease: 1800000 },

    // --- STEP 4: CLEANUP & HOUSEKEEPING ---
    { name: 'runDowntimeMaintenance', interval: 86400000 }
  ];

  const QUEUE_INDEX_KEY = 'MAINTENANCE_QUEUE_INDEX';
  let currentIndex = parseInt(SCRIPT_PROP.getProperty(QUEUE_INDEX_KEY) || '0', 10);
  if (currentIndex >= JOB_QUEUE.length) currentIndex = 0;

  let iterations = 0;
  while (iterations < JOB_QUEUE.length) {
    const job = JOB_QUEUE[currentIndex];
    const lastRunKey = 'LAST_RUN_' + job.name;
    const lastRunTs = parseInt(SCRIPT_PROP.getProperty(lastRunKey) || '0', 10);
    const isDue = (NOW_MS - lastRunTs) >= job.interval;

    // 3. Dynamic Lease Management: Now handles BOTH heavy tycoon steps dynamically
    if (job.lease) {
      const leaseKey = job.name + '_LEASE';
      const activeLease = parseInt(SCRIPT_PROP.getProperty(leaseKey) || '0', 10);

      if (isDue) {
        SCRIPT_PROP.deleteProperty(leaseKey);
      } else if (activeLease > NOW_MS) {
        currentIndex = (currentIndex + 1) % JOB_QUEUE.length;
        iterations++;
        continue;
      }
    }

    // 4. Execution Logic
    if (isDue) {
      console.log(`[Maintenance] Dispatching: ${job.name}`);

      if (job.lease) {
        const leaseKey = job.name + '_LEASE';
        SCRIPT_PROP.setProperty(leaseKey, (NOW_MS + job.lease).toString());
      }

      try {
        // GAS Safe: 'this' refers to the global scope. 'window' does not exist.
        const fn = this[job.name];

        if (typeof fn === 'function') {
          fn(); // Execute the job
          SCRIPT_PROP.setProperty(lastRunKey, NOW_MS.toString());
          SCRIPT_PROP.setProperty(QUEUE_INDEX_KEY, ((currentIndex + 1) % JOB_QUEUE.length).toString());
          console.log(`[Maintenance] ${job.name} completed successfully.`);
          return; // One job per Orchestrator tick to save RAM
        } else {
          console.error(`[Maintenance] Critical: Function ${job.name} not found in global scope.`);
        }
      } catch (e) {
        console.error(`[Maintenance] Critical Failure in ${job.name}: ${e.message}`);
      }
    }

    currentIndex = (currentIndex + 1) % JOB_QUEUE.length;
    iterations++;
  }
  console.log("Maintenance Cycle: All jobs are currently within their interval windows.");
}

/**
 * Market Data Worker (Nitro Edition - HYBRID)
 * Phase 1: Surgical Pause (Prevent creation crash)
 * Phase 2: Live Write (No pause, allows dashboard use)
 */
function updateMarketDataSheet(statTime) {
 // Explicitly declare START_TIME as a constant within this function's scope
 const trueStart = new Date().getTime();
 const START_TIME = (typeof statTime === 'number') ? statTime : trueStart;

  if (isSdeJobRunning()) {
    console.warn("ABORT: SDE Update in progress. Parking Market Tycoon.");
    return;
  }

  if (!isEngineRunning_()) {
    console.warn("ABORT: Engine is parked. Market Tycoon skipping fetch.");
    return;
  }

  // --- THE BOUNCER: STRICT SCRIPT LOCK ---
  const scriptLock = LockService.getScriptLock();
  if (!scriptLock.tryLock(1000)) { 
    console.warn("ABORT: updateMarketDataSheet is already running. Bouncing overlapping trigger.");
    return;
  }

  try {
    // 2. STAMP THE LOGICAL START TIME IMMEDIATELY
    const SCRIPT_PROP = PropertiesService.getScriptProperties();
    const PROP_KEY_STEP = 'marketDataJobStep';
    const PROP_KEY_WRITE_INDEX = 'marketDataNextWriteRow';
    const PROP_KEY_CHUNK_SIZE = 'marketDataChunkSize';
    const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
    const PROP_KEY_MARKET_LAST_RUN = 'MARKET_DATA_LAST_RUN_TS';

    SCRIPT_PROP.setProperty(PROP_KEY_MARKET_LAST_RUN, START_TIME.toString());

    const COLUMN_COUNT = 9;
    const START_ROW = 2;
    const DATA_SHEET_HEADERS = ["cacheKey", "type_id", "location_type", "location_id", "sell_min", "buy_max", "sell_volume", "buy_volume", "last_updated"];

    var ss_anchor = SpreadsheetApp.getActiveSpreadsheet();
    
    // 3. CHECK STEP FIRST BEFORE LOADING 20k ROWS
    let currentStep = SCRIPT_PROP.getProperty(PROP_KEY_STEP) || STATE_FLAGS.NEW_RUN;

    // --- Phase 1: NEW_RUN (SURGICAL PAUSE) ---
    if (currentStep === STATE_FLAGS.NEW_RUN) {
      console.log(`State: ${STATE_FLAGS.NEW_RUN}.`);
      
      const masterRequests = getMasterBatchFromControlTable(ss_anchor);
      if (!masterRequests || masterRequests.length === 0) {
        _resetMarketDataJobState(new Error("Control Table empty"));
        return;
      }

      const setupResult = guardedSheetTransaction(() => {
        const result = prepareTempSheet(ss_anchor, tempSheetName, DATA_SHEET_HEADERS);
        if (!result.success) {
          throw new Error(result.error || "Unknown Prep Failure");
        }
        if (result.state) {
          result.state.hideSheet();
        }
        return true;
      }, 60000);

      if (!setupResult.success) {
        console.warn(`[Worker] Sheet prep failed: ${setupResult.error}`);
        scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS);
        return;
      }

      SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, '0');
      SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
      currentStep = 'PROCESSING';
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'PROCESSING');

      scheduleOneTimeTrigger('updateMarketDataSheet', 1000);
      return;
    }

    // --- Phase 2: WRITE (Nitro Mode - LIVE/UNPAUSED) ---
    if (currentStep === 'PROCESSING' || currentStep === 'WRITE') {

      // FETCH EXACTLY ONCE HERE
      const masterRequests_stable = getMasterBatchFromControlTable(ss_anchor);
      if (!masterRequests_stable || masterRequests_stable.length === 0) {
        _resetMarketDataJobState(new Error("Control Table empty during processing"));
        return;
      }

      let allRowsToWrite = [];

      try {
        const marketDataCrates = fuzAPI.getDataForRequests(masterRequests_stable);
        const currentTimeStamp = new Date();
        marketDataCrates.forEach(crate => {
          if (crate && crate.fuzObjects) {
            crate.fuzObjects.forEach(item => {
              if (item && item.type_id != null) {
                allRowsToWrite.push([
                  "", item.type_id,
                  crate.market_type || '', crate.market_id || '',
                  item.sell?.min ?? '', item.buy?.max ?? '',
                  item.sell?.volume ?? 0, item.buy?.volume ?? 0,
                  currentTimeStamp
                ]);
              }
            });
          }
        });

        if (allRowsToWrite.length === 0) {
          console.error("Worker: allRowsToWrite is empty! Aborting write to prevent data wipe.");
          _resetMarketDataJobState(new Error("Zero rows returned from API - Aborted Write"));
          return;
        }
      } catch (e) {
        scheduleOneTimeTrigger('updateMarketDataSheet', RESCHEDULE_DELAY_MS * 2);
        return;
      }

      let writeState = {
        logInfo: console.log, logError: console.error, logWarn: console.warn,
        nextBatchIndex: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_WRITE_INDEX) || '0'),
        ss: ss_anchor,
        metrics: { startTime: START_TIME },
        config: {
          ...(typeof NITRO_CONFIG !== 'undefined' ? NITRO_CONFIG : {}),
          MAX_CELLS_PER_CHUNK: 40000,
          MAX_CHUNK_SIZE: 2000,
          currentChunkSize: parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CHUNK_SIZE) || '1000')
        }
      };

      const writeResult = writeDataToSheet(tempSheetName, allRowsToWrite, START_ROW, 1, writeState);

      if (writeResult.success) {
        console.log("Write SUCCESS. Transitioning to FINALIZING.");
        SCRIPT_PROP.setProperty(PROP_KEY_STEP, STATE_FLAGS.FINALIZING);
        SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
        SCRIPT_PROP.deleteProperty(PROP_KEY_CHUNK_SIZE);
        SCRIPT_PROP.deleteProperty(PROP_KEY_WRITE_INDEX);
        scheduleOneTimeTrigger('finalizeMarketDataUpdate', RESCHEDULE_DELAY_MS);
      }
      else if (writeResult.bailout_reason === "PREDICTIVE_BAILOUT" || (writeResult.error && writeResult.error.includes("timed out"))) {
        const reason = writeResult.error ? writeResult.error : "Predictive Bailout";
        console.warn(`Write phase interrupted. Reason: ${reason}. Rescheduling.`);

        const nextIndex = writeResult.state.nextBatchIndex.toString();
        let nextChunkSize = writeResult.state.config.currentChunkSize;

        if (writeResult.error) {
          nextChunkSize = Math.max(100, Math.floor(nextChunkSize / 2));
        }

        SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, nextIndex);
        SCRIPT_PROP.setProperty(PROP_KEY_CHUNK_SIZE, nextChunkSize.toString());
        Utilities.sleep(1000);
        scheduleOneTimeTrigger('updateMarketDataSheet', 30000);
      }
      else {
        if (writeResult.error && (writeResult.error.includes("Lock Failed") || writeResult.error.includes("Lock timeout"))) {
          console.warn("Lock Conflict detected. Pausing for Sheet to breathe. DO NOT RESET.");
          const nextIndex = (writeResult.state.nextBatchIndex || 0).toString();
          SCRIPT_PROP.setProperty(PROP_KEY_WRITE_INDEX, nextIndex);
          scheduleOneTimeTrigger('updateMarketDataSheet', 30000);
        } else {
          _resetMarketDataJobState(new Error(`Write Failure: ${writeResult.error}`));
        }
      }
    }
  } finally {
    scriptLock.releaseLock();
  }
}

function finalizeMarketDataUpdate() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const finalSheetName = 'Market_Data_Raw';
  const tempSheetName = 'Market_Data_Temp';
  const RESCHEDULE_DELAY_MS = RETRY_DELAY_MS;
  const funcName = 'finalizeMarketDataUpdate';

  // [CRITICAL FIX] Define these variables so guardedSheetTransaction can see them!
  const ss_inner = SpreadsheetApp.getActiveSpreadsheet();
  const repairMap = { 'NR_MARKET_DATA': 'A:I' };

  // Verify we are in the correct state before doing anything
  if (SCRIPT_PROP.getProperty(PROP_KEY_STEP) !== 'FINALIZING') {
    _resetMarketDataJobState(new Error(`Wrong state.`));
    return;
  }

  // Execute the transaction block securely
  const transactionResult = guardedSheetTransaction(() => {
    // === 1. ACTIVATE ANESTHESIA LOCK ===
    SCRIPT_PROP.setProperty('GLOBAL_SYSTEM_STATE', 'MAINTENANCE');
    pauseSheet(ss_inner);

    try {
      // 2. Perform the Atomic Swap (Hot Swap) while the sheet is dead
      const swapRes = atomicSwapAndFlush(ss_inner, finalSheetName, tempSheetName, repairMap);

      // 3. Sync External Prices and Region Data while locked
      fetchFilteredPricesSync(ss_inner);
      syncESIRegionData(ss_inner);
      updateMarketOrdersNamedRange(ss_inner);


      return swapRes;

    } finally {
      // === 5. DEACTIVATE ANESTHESIA (WAKE UP THE SHEET) ===
      wakeUpSheet(ss_inner);
      SCRIPT_PROP.setProperty('GLOBAL_SYSTEM_STATE', 'RUNNING');
      console.log("Anesthesia: System state restored to RUNNING.");
    }
  }, 60000); // 60-second transaction safety timeout

  // Handle the results post-transaction
  const swapSuccess = (transactionResult && transactionResult.success && transactionResult.state && transactionResult.state.success);

  if (swapSuccess) {
    _resetMarketDataJobState(null);
    console.log("SUCCESS: Finalization complete.");
  } else {
    const errorMsg = transactionResult ? (transactionResult.error || (transactionResult.state && transactionResult.state.errorMessage)) : 'Unknown Error';
    console.warn(`[Finalizer] Swap Failed: ${errorMsg}`);
  }
}