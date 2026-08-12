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
 * Generates a clean console report showing exactly when all systems
 * are scheduled to trigger next based on current Script Properties.
 */
function reportOrchestratorTimes() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const now = new Date().getTime();

  // Helper to format milliseconds into a readable countdown
  function formatTime(targetMs) {
    const diff = targetMs - now;
    if (diff <= 0) return "Ready (Will fire next tick)";
    
    const hours = Math.floor(diff / 3600000);
    const mins = Math.floor((diff % 3600000) / 60000);
    const secs = Math.floor((diff % 60000) / 1000);
    
    if (hours > 0) return `${hours}h ${mins}m ${secs}s`;
    return `${mins}m ${secs}s`;
  }

  console.log("========== ORCHESTRATOR DASHBOARD ==========");
  console.log(`Global System State : ${SCRIPT_PROP.getProperty('GLOBAL_SYSTEM_STATE') || 'RUNNING'}`);
  console.log("--------------------------------------------");

  // --- MARKET DATA ENGINE ---
  const marketStep = SCRIPT_PROP.getProperty('marketDataJobStep') || 'IDLE';
  const marketLastRun = parseInt(SCRIPT_PROP.getProperty('MARKET_DATA_LAST_RUN_TS') || '0', 10);
  const marketCooldown = parseInt(SCRIPT_PROP.getProperty('MARKET_COOLDOWN') || '0', 10);
  const marketLease = parseInt(SCRIPT_PROP.getProperty('marketDataJobLeaseUntil') || '0', 10);
  const nextMarketCycle = marketLastRun + (30 * 60 * 1000); // 30 mins

  console.log(">>> MARKET DATA ENGINE");
  console.log(`Current Step        : ${marketStep}`);
  
  if (marketCooldown > now) {
    console.log(`Status              : [PENALTY BOX] Cooldown lifts in ${formatTime(marketCooldown)}`);
  } else if (marketLease > now && marketStep !== 'IDLE' && marketStep !== 'NEW_RUN') {
    console.log(`Status              : [ACTIVE] Job lease expires in ${formatTime(marketLease)}`);
  } else {
    console.log(`Status              : [WAITING] Next cycle due in ${formatTime(nextMarketCycle)}`);
  }

  console.log("--------------------------------------------");

  // --- MAINTENANCE QUEUE ---
  console.log(">>> MAINTENANCE JOBS (60m Intervals)");
  const queueIndex = SCRIPT_PROP.getProperty('MAINTENANCE_QUEUE_INDEX') || '0';
  console.log(`Next Job in Queue Array Index: [${queueIndex}]`);

  const jobQueue = [
    { name: 'cacheAllCorporateAssetsTrigger', interval: 3600000 },
    { name: 'TransactionsAndJournalSync', interval: 3600000 },
    { name: 'runIndustryLedgerPhase', interval: 3600000 },
    { name: 'runLootDeltaPhase', interval: 3600000 },
    { name: 'runUnifiedTycoonPipeline', interval: 3600000 },
    { name: 'runDowntimeMaintenance', interval: 86400000 } // 24h
  ];

  jobQueue.forEach(job => {
    const lastRunTs = parseInt(SCRIPT_PROP.getProperty('LAST_RUN_' + job.name) || '0', 10);
    const nextRunTarget = lastRunTs + job.interval;
    
    // Check for active specific leases (if a job stalled mid-execution)
    const activeLease = parseInt(SCRIPT_PROP.getProperty(job.name + '_LEASE') || '0', 10);
    let leaseTag = "";
    if (activeLease > now) {
      leaseTag = ` (LEASED for ${formatTime(activeLease)})`;
    }

    console.log(`${job.name.padEnd(32)} : ${formatTime(nextRunTarget)}${leaseTag}`);
  });

  console.log("============================================");
}

function checkMarketEngineStatus() {
  const scriptProps = PropertiesService.getScriptProperties();
  const currentStep = scriptProps.getProperty('marketDataJobStep') || 'IDLE';
  const systemState = scriptProps.getProperty('GLOBAL_SYSTEM_STATE') || 'UNKNOWN';
  const lastRunTs = scriptProps.getProperty('MARKET_DATA_LAST_RUN_TS');

  const now = new Date().getTime();
  const cooldownMs = 30 * 60 * 1000; // 30-minute cooldown window

  let lastRunFormatted = 'Never';
  let cooldownRemainingMinutes = 0;
  let isOnCooldown = false;

  if (lastRunTs) {
    const lastRunDate = new Date(Number(lastRunTs));
    lastRunFormatted = lastRunDate.toLocaleString();
    const elapsed = now - Number(lastRunTs);
    if (elapsed < cooldownMs) {
      isOnCooldown = true;
      cooldownRemainingMinutes = Math.ceil((cooldownMs - elapsed) / 60000);
    }
  }

  console.log('=== MARKET ENGINE STATUS ===');
  console.log(`System State       : ${systemState}`);
  console.log(`Current Job Step   : ${currentStep}`);
  console.log(`Last Run Timestamp : ${lastRunFormatted}`);
  console.log(`On Cooldown        : ${isOnCooldown} (${cooldownRemainingMinutes} mins remaining)`);
  console.log('============================');
}

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
  } catch (e) {
    console.error("Wakeup failed, but state is reset. Proceeding.");
  }

  // 3. Clear the market engine lock to allow fresh start
  _resetMarketDataJobState(null);

  console.log("CRITICAL: System forced back to RUNNING state.");
}

/**
 * Replaces IMPORTRANGE. Fetches static market prices from the external hub via Named Range.
 * Optimized: Eliminates grid mutations, minimizes latency, runs silently.
 */
function fetchFilteredPricesSync(ss) {
  const LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('PRICE_SYNC') : console;

  // --- CONFIGURATION ---
  const SOURCE_SHEET_ID = "1L37sYZPznkNu3EJy554nmaclXQl6DpvERc_N6ans76M";
  const REMOTE_RANGE_NAME = "filtered_prices_Data"; 
  const TARGET_SHEET_NAME = "market price Tracker";
  const LOCAL_RANGE_NAME = "NR_MARKET_MEDIAN_DATA";

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();

  try {
    LOG.info("Connecting to external price database...");

    // 1. Fetch Remote Range
    const sourceBook = SpreadsheetApp.openById(SOURCE_SHEET_ID);
    const sourceRange = sourceBook.getRangeByName(REMOTE_RANGE_NAME);

    if (!sourceRange) {
      throw new Error(`External Named Range '${REMOTE_RANGE_NAME}' not found!`);
    }

    // 2. In-Memory Processing
    const rawValues = sourceRange.getValues();
    const dataToWrite = rawValues.filter(row => row[0] !== "" && row[0] != null);
    
    const requiredRows = dataToWrite.length;
    if (requiredRows === 0) {
      LOG.warn("Fetch aborted: No valid data found in remote named range.");
      return;
    }
    const requiredCols = dataToWrite[0].length;

    // 3. Prepare Target Canvas
    let targetSheet = ss.getSheetByName(TARGET_SHEET_NAME);
    if (!targetSheet) {
      targetSheet = ss.insertSheet(TARGET_SHEET_NAME);
      LOG.info(`Created new target sheet: ${TARGET_SHEET_NAME}`);
    }

    const initialMaxRows = targetSheet.getMaxRows();
    const initialMaxCols = targetSheet.getMaxColumns();

    // Expand sheet bounds to prevent out-of-bounds crash (Added Column Protection)
    if (initialMaxRows < requiredRows) {
      targetSheet.insertRowsAfter(initialMaxRows, requiredRows - initialMaxRows);
    }
    if (initialMaxCols < requiredCols) {
      targetSheet.insertColumnsAfter(initialMaxCols, requiredCols - initialMaxCols);
    }

    targetSheet.clearContents();

    // 4. Single Batch Write
    const finalRange = targetSheet.getRange(1, 1, requiredRows, requiredCols);
    finalRange.setValues(dataToWrite);

   // 5. Update Local Named Range (Optimized Single API Call)
    ss.setNamedRange(LOCAL_RANGE_NAME, finalRange);
    LOG.info(`Updated/Created Named Range: ${LOCAL_RANGE_NAME}`);

    LOG.info(`Price Sync Complete. Wrote ${requiredRows} rows.`);

  } catch (e) {
    LOG.error("Failed to sync external prices: " + e.message);
  }
}

function scheduleOneTimeTrigger(functionName, delayMs, force) {
  if (typeof functionName !== 'string' || functionName.trim() === '') {
    throw new Error(`CRITICAL SCHEDULER ERROR: Invalid function name provided.`);
  }

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const systemState = SCRIPT_PROP.getProperty(GLOBAL_STATE_KEY) || 'RUNNING';

  try {
    deleteTriggersByName(functionName);
    if (!force && systemState === 'MAINTENANCE') {
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
  if (target) {
    console.log("Target Sheet Found. Data Range: " + target.getDataRange().getA1Notation());
  } else {
    console.error("Target sheet not found!");
  }
}
/**
 * Grabs Regional Pricing from Market Price Tracker.
 * UPGRADED: Pulls sanitized client data, handles 6-column schema, and safely catches missing ranges.
 * (Formatting block stripped for speed)
 */
function syncESIRegionData(ss) {
  const log = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('REGION_SYNC') : console;
  const sourceId = "1L37sYZPznkNu3EJy554nmaclXQl6DpvERc_N6ans76M";

  // --- CONFIGURATION ---
  const targetSheetName = "ESI_Region";
  const REMOTE_RANGE_NAME = "MarketResultESI_Region_market_orders"; 
  const LOCAL_RANGE_NAME = "ESI_Region_Data";

  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const targetSheet = ss.getSheetByName(targetSheetName);

  if (!targetSheet) {
    log.warn(`Target sheet '${targetSheetName}' not found. Aborting.`);
    return;
  }

  try {
    const sourceBook = SpreadsheetApp.openById(sourceId);
    const remoteRange = sourceBook.getRangeByName(REMOTE_RANGE_NAME);

    // SAFETY CHECK: Catch missing remote range before trying to read values
    if (!remoteRange) {
      throw new Error(`Remote Named Range '${REMOTE_RANGE_NAME}' not found on the source spreadsheet!`);
    }

    const sourceData = remoteRange.getValues();

    if (sourceData.length < 2) {
      log.warn("Source data is empty (less than 2 rows). Aborting sync.");
      return;
    }

    const requiredRows = sourceData.length;
    const requiredCols = sourceData[0].length;
    const currentRows = targetSheet.getMaxRows();
    const currentCols = targetSheet.getMaxColumns();

    // 1. Ensure columns match
    if (currentCols < requiredCols) {
      targetSheet.insertColumnsAfter(currentCols, requiredCols - currentCols);
    } else if (currentCols > requiredCols) {
      targetSheet.deleteColumns(requiredCols + 1, currentCols - requiredCols);
    }

    // 2. Ensure rows match safely using the non-frozen row rule
    const frozenRows = targetSheet.getFrozenRows();
    const safeTargetRows = Math.max(requiredRows, frozenRows + 1);

    if (currentRows < safeTargetRows) {
      targetSheet.insertRowsAfter(currentRows, safeTargetRows - currentRows);
    } else if (currentRows > safeTargetRows) {
      targetSheet.deleteRows(safeTargetRows + 1, currentRows - safeTargetRows);
    }

    // 3. CLEAR & WRITE
    targetSheet.clearContents();
    const newRange = targetSheet.getRange(1, 1, requiredRows, requiredCols);
    newRange.setValues(sourceData);

    // 4. UPDATE LOCAL NAMED RANGE
    ss.setNamedRange(LOCAL_RANGE_NAME, newRange);
    log.info(`Named Range '${LOCAL_RANGE_NAME}' updated to ${requiredRows} rows and ${requiredCols} cols.`);

    // 5. FINAL EXACT TRIM
    const finalMaxRows = targetSheet.getMaxRows();
    if (finalMaxRows > safeTargetRows) {
      targetSheet.deleteRows(safeTargetRows + 1, finalMaxRows - safeTargetRows);
    }

    log.info("ESI_Region: Sync & Named Range Update Complete.");
  } catch (e) {
    log.error("ESI_Region Sync Error: " + e.message);
  }
}

/**
 * Dynamically updates the Named Range for the Market Orders sheet.
 */
function updateMarketOrdersNamedRange(ss) {
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetName = "Publish_ESI_Region_market_orders";
  const rangeName = "Region_Radar_Table";

  const sheet = ss.getSheetByName(sheetName);
  if (!sheet) {
    Logger.log("Error: Sheet " + sheetName + " not found.");
    return;
  }

  const lastRow = sheet.getLastRow();
  const lastCol = 24;

  const newRange = sheet.getRange(1, 1, lastRow, lastCol);
  const existingNamedRange = ss.getNamedRanges().find(nr => nr.getName() === rangeName);

  if (existingNamedRange) {
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

function masterOrchestrator() {
  const NOW_MS = new Date().getTime();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ORCHESTRATOR') : console;

  // 1. Read state variables right at the top
  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');

  // --- 2. PROACTIVE QUOTA COMPLIANCE ---
  if (typeof ESI !== 'undefined' && ESI.isLocked()) {
    LOG.warn("QUOTA DEAD: masterOrchestrator suspended to save execution time.");
    return;
  }

  // --- 3. COOL DOWN CHECK ---
  const cooldownUntil = parseInt(SCRIPT_PROP.getProperty('MARKET_COOLDOWN') || '0', 10);
  if (cooldownUntil > NOW_MS) {
    LOG.warn(`Market Data engine is on cooldown until ${new Date(cooldownUntil).toLocaleTimeString()}.`);
  }

  // --- 4. SMART NUDGE GATE: FINALIZER ---
  // Using explicit strings just in case STATE_FLAGS is not loaded in this context
  if (marketDataStep === 'FINALIZING') {
    const leaseUntil = parseInt(SCRIPT_PROP.getProperty('marketDataJobLeaseUntil') || '0', 10);

    // Added a 10-second grace period to prevent millisecond race conditions
    if (NOW_MS > (leaseUntil + 10000)) {
      LOG.warn(`Finalizer stalled (Lease expired). Reviving finalizer.`);

      // DANGEROUS LINE REMOVED: Do NOT force GLOBAL_SYSTEM_STATE to RUNNING here. 
      // If the finalizer is just slow, waking the sheet will crash the atomic swap. 
      // The finalizer has its own try/finally block to handle wake-ups safely.

      // Grant a 5-minute lease so we do not spawn clones on the next tick
      SCRIPT_PROP.setProperty('marketDataJobLeaseUntil', (NOW_MS + 300000).toString());
      
      if (typeof scheduleOneTimeTrigger === 'function') {
        scheduleOneTimeTrigger("finalizeMarketDataUpdate", 5000);
      }
    } else {
      LOG.info(`Market Data Active (FINALIZING). Finalizer is leased and running.`);
    }
    return; // Exit so we don't accidentally dispatch anything else
  }

  // --- 5. MARKET DATA ENGINE DISPATCH ---
  const lastMarketRun = parseInt(SCRIPT_PROP.getProperty('MARKET_DATA_LAST_RUN_TS') || '0', 10);
  const isMarketOnCooldown = (cooldownUntil > NOW_MS);
  const timeSinceLastRun = NOW_MS - lastMarketRun;

  const RUN_INTERVAL_MS = 30 * 60 * 1000; // 30 minutes

  if (!isMarketOnCooldown && timeSinceLastRun > RUN_INTERVAL_MS) {
    const leaseUntil = parseInt(SCRIPT_PROP.getProperty('marketDataJobLeaseUntil') || '0', 10);
    const isJobActive = leaseUntil > NOW_MS;

    if (!isJobActive) {
      LOG.info(`DISPATCHING MARKET DATA JOB (30m Cycle).`);

      // Set the lease target BEFORE invoking so concurrent ticks cannot duplicate the run
      SCRIPT_PROP.setProperty('marketDataJobLeaseUntil', (NOW_MS + 300000).toString());
      if (typeof updateMarketDataSheet === 'function') updateMarketDataSheet(NOW_MS);
      return;
    }
  }

  // --- 6. SMART NUDGE GATE: WORKER ---
  if (marketDataStep === 'PROCESSING' || marketDataStep === 'NEW_RUN') {
    const leaseUntil = parseInt(SCRIPT_PROP.getProperty('marketDataJobLeaseUntil') || '0', 10);

    if (NOW_MS > (leaseUntil + 10000)) {
      LOG.info(`Market Data engine stalled (Lease expired). Nudging worker.`);
      SCRIPT_PROP.setProperty('marketDataJobLeaseUntil', (NOW_MS + 300000).toString());
      if (typeof updateMarketDataSheet === 'function') updateMarketDataSheet(NOW_MS);
    } else {
      LOG.info(`Market Data Active (${marketDataStep}). Worker loop leased and running.`);
    }
    return;
  }

  // --- 7. MAINTENANCE & IDLE TASKS ---
  LOG.info(`Market Data Idle. Attempting Maintenance cycle.`);
  if (typeof executeWithTryLock === 'function') {
    executeWithTryLock(runMaintenanceJobs, 'runMaintenanceJobs');
  }
}

function UNLOCK_MarketData_Engine() {
  const props = PropertiesService.getScriptProperties();
  
  // Wipe the specific cooldown and step trackers
  props.deleteProperty('marketDataJobStep');
  props.deleteProperty('MARKET_DATA_COOLDOWN'); // (Or whatever your cooldown key is named)
  
  // Make sure the system isn't stuck in Anesthesia mode
  props.setProperty('GLOBAL_SYSTEM_STATE', 'RUNNING');
  
  console.log("Quarantine lifted. System state restored to RUNNING. You are clear to fire.");
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

  syncCorpBlueprintsV12

    try {
    log.info('Step 0: Update Contracts');
    syncCorpBlueprintsV12(ss);
  } catch (e) {
    log.error('Step 0 Failed.', e);
    return;
  }

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

  // FIX 1: Hardcoded the string to prevent ReferenceError
  if (SCRIPT_PROP.getProperty('AssetCache_JobStatus') === 'FINALIZING') {
    console.log("[Maintenance] Asset finalization pending. Forcing execution immediately.");
    if (typeof finalizeAssetCacheJob === 'function') finalizeAssetCacheJob();
    return;
  }

  const NOW_MS = explicitNowMs || new Date().getTime();
  const STANDARD_INTERVAL = 3600000; // 60m default

  const JOB_QUEUE = [
    { name: 'cacheAllCorporateAssetsTrigger', interval: STANDARD_INTERVAL },
    { name: 'TransactionsAndJournalSync', interval: STANDARD_INTERVAL },
    { name: 'runIndustryLedgerPhase', interval: STANDARD_INTERVAL },
    { name: 'runLootDeltaPhase', interval: STANDARD_INTERVAL },
    { name: 'runUnifiedTycoonPipeline', interval: STANDARD_INTERVAL },
    { name: 'runDowntimeMaintenance', interval: 86400000 }
  ];

  const QUEUE_INDEX_KEY = 'MAINTENANCE_QUEUE_INDEX';
  let currentIndex = parseInt(SCRIPT_PROP.getProperty(QUEUE_INDEX_KEY) || '0', 10);
  if (currentIndex >= JOB_QUEUE.length) currentIndex = 0;

  // FIX 2: A safe global dispatcher mapping to avoid 'this' context breakdowns
  const dispatcher = {
    'cacheAllCorporateAssetsTrigger': () => cacheAllCorporateAssetsTrigger(),
    'TransactionsAndJournalSync': () => TransactionsAndJournalSync(),
    'runIndustryLedgerPhase': () => runIndustryLedgerPhase(),
    'runLootDeltaPhase': () => runLootDeltaPhase(),
    'runUnifiedTycoonPipeline': () => runUnifiedTycoonPipeline(),
    'runDowntimeMaintenance': () => runDowntimeMaintenance()
  };

  let iterations = 0;
  while (iterations < JOB_QUEUE.length) {
    const job = JOB_QUEUE[currentIndex];
    const lastRunKey = 'LAST_RUN_' + job.name;
    const lastRunTs = parseInt(SCRIPT_PROP.getProperty(lastRunKey) || '0', 10);
    const isDue = (NOW_MS - lastRunTs) >= job.interval;

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

    if (isDue) {
      console.log(`[Maintenance] Dispatching: ${job.name}`);

      if (job.lease) {
        const leaseKey = job.name + '_LEASE';
        SCRIPT_PROP.setProperty(leaseKey, (NOW_MS + job.lease).toString());
      }

      try {
        const fn = dispatcher[job.name]; // Uses the safe dispatcher

        if (fn) {
          fn();
          SCRIPT_PROP.setProperty(lastRunKey, NOW_MS.toString());
          SCRIPT_PROP.setProperty(QUEUE_INDEX_KEY, ((currentIndex + 1) % JOB_QUEUE.length).toString());
          console.log(`[Maintenance] ${job.name} completed successfully.`);
          return; // One success = exit. Safe.
        } else {
          console.error(`[Maintenance] Critical: Function ${job.name} not found in dispatcher.`);
        }
      } catch (e) {
        console.error(`[Maintenance] Critical Failure in ${job.name}: ${e.message}`);
        // FIX 3: Force the Orchestrator to stop if an inner function throws an error.
        // This prevents the loop from advancing and causing a massive cascading timeout.
        return;
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

      console.time("Timer_ControlTable");
      const masterRequests = getMasterBatchFromControlTable(ss_anchor);
      console.timeEnd("Timer_ControlTable");

      if (!masterRequests || masterRequests.length === 0) {
        _resetMarketDataJobState(new Error("Control Table empty"));
        return;
      }

      console.time("Timer_SheetPrep");
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
      console.timeEnd("Timer_SheetPrep");

      if (!setupResult.success) {
        console.warn(`[Worker] Sheet prep failed: ${setupResult.error}`);
        // FIXED: Using the correctly declared RETRY_DELAY_MS
        scheduleOneTimeTrigger('updateMarketDataSheet', RETRY_DELAY_MS); 
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
        
        // THE FIX: Grant a fresh 5-minute lease so the Orchestrator knows 
        // the Finalizer is expected and doesn't spawn a ghost trigger.
        const NOW_MS = new Date().getTime();
        SCRIPT_PROP.setProperty(PROP_KEY_LEASE, (NOW_MS + 300000).toString());
        
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

function finalizeMarketDataUpdate(ss) {
  if (!ss || typeof ss.getSheetByName !== 'function') {
    ss = SpreadsheetApp.getActiveSpreadsheet();
  }

  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const finalSheetName = 'Market_Data_Raw';
  const tempSheetName = 'Market_Data_Temp';
  
  // FIXED: Restored column width protection (A through I) to prevent auto-width landmines
  const repairMap = { 'NR_MARKET_DATA': 'A1:I' }; 

  if (SCRIPT_PROP.getProperty(PROP_KEY_STEP) !== 'FINALIZING') {
    _resetMarketDataJobState(new Error("Wrong state."));
    return;
  }

  let needsWakeUp = false;

  try {
    // === 1. THE LEAN ATOMIC SWAP (LOCAL ONLY) ===
    const transactionResult = guardedSheetTransaction(() => {
      SCRIPT_PROP.setProperty('GLOBAL_SYSTEM_STATE', 'MAINTENANCE');
      
      // The Finalizer administers the anesthesia ONCE for the entire process
      needsWakeUp = pauseSheet(ss); 

      try {
        // holdAnesthesia = TRUE, requireLock = FALSE
        // This stops the nested deadlock from occurring while guardedSheetTransaction holds the master lock.
        const swapRes = atomicSwapAndFlush(ss, finalSheetName, tempSheetName, repairMap, true, false);
        
        if (!swapRes || !swapRes.success) {
          return { success: false, errorMessage: swapRes ? swapRes.errorMessage : "Atomic swap failed." };
        }

        console.log("[Finalizer] Local atomic swap successful.");
        return { success: true };

      } catch (syncError) {
        console.error("[Finalizer] Critical Failure during swap: " + syncError.message);
        return { success: false, errorMessage: syncError.message };
      }
    }, 30000); // Shorter timeout: Lock releases the moment the internal grid swap is done

    SCRIPT_PROP.setProperty('GLOBAL_SYSTEM_STATE', 'RUNNING');

    // === 2. EVALUATE RESULT & RUN EXTERNAL SYNCS (Still under Anesthesia!) ===
    const success = (transactionResult && transactionResult.success && transactionResult.state && transactionResult.state.success);

    if (success) {
      SCRIPT_PROP.setProperty(PROP_KEY_STEP, 'EXTERNAL_SYNC');
      SCRIPT_PROP.setProperty('MARKET_DATA_LAST_RUN_TS', new Date().getTime().toString());
      _resetMarketDataJobState(null); // Clears job state and lifts cooldowns
      console.log("SUCCESS: Market data successfully swapped and committed.");

      // Run external syncs safely now that the core data is locked in
      // CALCULATIONS ARE STILL FROZEN, completely preventing SheetService timeouts
      try {
        console.log("[Post-Finalize] Running external price and region syncs under Anesthesia...");
        fetchFilteredPricesSync(ss);
        syncESIRegionData(ss);
        updateMarketOrdersNamedRange(ss);
        console.log("[Post-Finalize] All external syncs completed successfully.");
      } catch (externalErr) {
        console.warn("[Post-Finalize] External sync warning (Market data is safe): " + externalErr.message);
      }

    } else {
      const errorMsg = transactionResult ? (transactionResult.error || transactionResult.errorMessage || (transactionResult.state && transactionResult.state.errorMessage)) : 'Unknown Error';
      console.warn("[Finalizer] Failed: " + errorMsg);
      _resetMarketDataJobState(new Error("Finalization transaction failed: " + errorMsg));
    }

  } finally {
    // === 3. WAKE UP THE SHEET ===
    // This finally block guarantees the sheet wakes up exactly ONCE at the very end,
    // even if the external syncs crash or throw an error.
    if (needsWakeUp) {
      try {
        wakeUpSheet(ss);
        console.log("Anesthesia: System state restored to RUNNING and calculations resumed.");
      } catch (wakeError) {
        console.warn("[Finalizer] Sheet wake-up warning: " + wakeError.message);
      }
    }
  }
}

