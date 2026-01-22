/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, ScriptApp, 
  getMasterBatchFromControlTable, withSheetLock, getOrCreateSheet, 
cacheAllCorporateAssetsTrigger, triggerLedgerImportCycle, fuzAPI, _fetchProcessedLootData, 
runLootLedgerDelta, Ledger_Import_CorpJournal, syncContracts, runIndustryLedgerPhase,
  runLootDeltaPhase, runContractLedgerPhase, runAllLedgerImports, LoggerEx, writeDataToSheet, guardedSheetTransaction, atomicSwapAndFlush, deleteTriggersByName, pauseSheet, wakeUpSheet, prepareTempSheet */

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

// ... [Keep scheduleOneTimeTrigger, deleteTriggersByName, _resetMarketDataJobState unchanged] ...
// (Assumed lines 35-100 are standard helpers)

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

// ... [masterOrchestrator logic] ...

function masterOrchestrator() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');
  const PROP_KEY_MARKET_LAST_RUN = 'MARKET_DATA_LAST_RUN_TS';
  const lastMarketRun = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_MARKET_LAST_RUN) || '0', 10);

  const currentMinute = new Date().getMinutes();
  const NOW_MS = new Date().getTime();

  const PROP_KEY_LEASE = 'marketDataJobLeaseUntil';
  const leaseUntil = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_LEASE) || '0', 10);
  let isJobActive = leaseUntil > NOW_MS;

  if (!isJobActive && leaseUntil > 0 && leaseUntil <= NOW_MS) {
    console.warn(`Orchestrator: Expired lease found. Clearing.`);
    SCRIPT_PROP.deleteProperty(PROP_KEY_LEASE);
    isJobActive = false;
  }


  // NEW: Check for pending COGS Finalization and process it immediately.
  if (_nudgeCogsFinalizer()) {
    return;
  }

  if (marketDataStep === STATE_FLAGS.FINALIZING) {
    const lock = LockService.getScriptLock();
    if (lock.tryLock(0)) {
      lock.releaseLock();
      console.log(`Orchestrator: Finalizing Market Data.`);
      scheduleOneTimeTrigger("finalizeMarketDataUpdate", 5000);
    } else {
      console.log(`Orchestrator: Finalizer already running (Lock busy). Skipping.`);
    }
    return;
  }

  console.log(`Orchestrator (min ${currentMinute}): High-Frequency Check.`);

  const timeSinceLastRun = NOW_MS - lastMarketRun;
  const RUN_INTERVAL_MS = 28 * 60 * 1000;

  if (timeSinceLastRun > RUN_INTERVAL_MS) {
    if (isJobActive) {
      console.log(`Orchestrator: Market Data Active. Skipping NEW dispatch.`);
    } else {
      console.log(`Orchestrator: DISPATCHING NEW MARKET DATA JOB (30m Cycle).`);
      const launchResult = updateMarketDataSheet();

      if (launchResult !== null) {
        const FRESH_NOW_MS = new Date().getTime();
        const NEW_LEASE = FRESH_NOW_MS + 300000;
        SCRIPT_PROP.setProperty(PROP_KEY_LEASE, NEW_LEASE.toString());
        console.log("Orchestrator: Lock acquired. Lease set.");
      } else {
        console.warn("Orchestrator: Lock BUSY. Dispatch aborted (will retry in 5m).");
      }
    }
    return;
  }

  if (marketDataStep === STATE_FLAGS.PROCESSING || marketDataStep === STATE_FLAGS.NEW_RUN) {
    console.log(`Orchestrator: Market Data Active (${marketDataStep}). Nudging.`);
    updateMarketDataSheet();
    return;
  }

  console.log(`Orchestrator: Market Data Idle. Attempting Maintenance.`);
  executeWithTryLock(runMaintenanceJobs, 'runMaintenanceJobs');
}


function runMaintenanceJobs() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const QUEUE_INDEX_KEY = 'MAINTENANCE_QUEUE_INDEX';
  const PROP_KEY_CONTRACT_LEASE = 'contractJobLeaseUntil';
  
  // STANDARD INTERVAL (60 Minutes) - Default for Loot, Contracts, Industry
  const STANDARD_INTERVAL_MS = 3600000; 
  
  // JOURNAL INTERVAL (30 Minutes) - Specific override for Ledger Import
  const JOURNAL_INTERVAL_MS = 1800000; 

  const JOB_QUEUE = [
    'runLootDeltaPhase',
    'Ledger_Import_CorpJournal',
    'runContractLedgerPhase',
    'runIndustryLedgerPhase',
    'cacheAllCorporateAssetsTrigger'
  ];

  const marketDataStep = SCRIPT_PROP.getProperty('marketDataJobStep');
  if (marketDataStep === STATE_FLAGS.FINALIZING) return;

  const NOW_MS = new Date().getTime();

  let startIndex = parseInt(SCRIPT_PROP.getProperty(QUEUE_INDEX_KEY) || '0', 10);
  if (startIndex >= JOB_QUEUE.length) startIndex = 0;

  let currentIndex = startIndex;
  let jobExecuted = false;

  // --- LOOP THROUGH ALL JOBS, STARTING AT startIndex ---
  do {
    const currentJobName = JOB_QUEUE[currentIndex];
    const lastRunKey = PROP_KEY_LAST_RUN_TS + currentJobName;
    const lastRunTimestamp = parseInt(SCRIPT_PROP.getProperty(lastRunKey) || '0', 10);

    // --- DYNAMIC INTERVAL CHECK ---
    let requiredInterval = STANDARD_INTERVAL_MS;
    if (currentJobName === 'Ledger_Import_CorpJournal') {
        requiredInterval = JOURNAL_INTERVAL_MS;
    }

    let isJobDue = (NOW_MS - lastRunTimestamp) >= requiredInterval;
    let isLeaseExpired = true; // Assume true unless check proves otherwise

    // 1. LEASE CHECK (Bypasses time check if lease is active)
    if (currentJobName === 'runContractSync') {
      const LEASE_UNTIL = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_CONTRACT_LEASE) || '0', 10);
      isLeaseExpired = (LEASE_UNTIL <= NOW_MS);

      if (!isLeaseExpired) {
        console.warn(`[Maintenance] Skipping ${currentJobName}: Lease active.`);
        // Job is skipped due to lease, advance to next job immediately and continue loop
        currentIndex = (currentIndex + 1) % JOB_QUEUE.length;
        continue;
      }
    }

    // 2. INTERVAL CHECK (Only proceed if the job is due and lease is expired)
    if (isJobDue) {
      console.log(`[Maintenance] Executing: ${currentJobName} (Interval: ${Math.round(requiredInterval/60000)}m)`);

      try {
        const fn = this[currentJobName];
        if (typeof fn === 'function') {
          fn();

          // CRITICAL: Update state only on successful execution
          SCRIPT_PROP.setProperty(lastRunKey, NOW_MS.toString());
          currentIndex = (currentIndex + 1) % JOB_QUEUE.length;
          SCRIPT_PROP.setProperty(QUEUE_INDEX_KEY, currentIndex.toString());
          jobExecuted = true;
          return; // Exit after running one job (Time Slicing)
        } else {
          console.warn(`[Maintenance] Function not found: ${currentJobName}. Advancing pointer.`);
        }
      } catch (e) {
        console.error(`[Maintenance] Failed: ${e.message}. Advancing pointer.`);
      }
    }

    // Advance pointer if job was not due (interval check failed) or failed to run for non-time reasons
    currentIndex = (currentIndex + 1) % JOB_QUEUE.length;

  } while (currentIndex !== startIndex); // Stop after checking the entire queue once.

  if (!jobExecuted) {
    console.log("Maintenance cycle finished: No jobs were due to run.");
  }
}

function updateMarketDataSheet() {
  const funcName = 'updateMarketDataSheet';
  const result = executeWithTryLock(_updateMarketDataSheetWorker, funcName);
  if (result === null) console.warn(`${funcName} skipped (Lock).`);
  return result;
}



function finalizeMarketDataUpdate() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const PROP_KEY_STEP = 'marketDataJobStep';
  const finalSheetName = 'Market_Data_Raw';
  const tempSheetName = 'Market_Data_Temp';

  const funcName = 'finalizeMarketDataUpdate';

  executeWithTryLock(() => {

    if (SCRIPT_PROP.getProperty(PROP_KEY_STEP) !== 'FINALIZING') {
      _resetMarketDataJobState(new Error(`Wrong state.`));
      return;
    }

    var ss_inner = SpreadsheetApp.getActiveSpreadsheet();

    // [CRITICAL FIX] REFRESH CONNECTION
    // The previous 'ss_inner' is dead after the long flush. Get a new one.
    ss_inner = SpreadsheetApp.getActiveSpreadsheet();

    const repairMap = { ['NR_MARKET_DATA']: 'A:G' };

    const transactionResult = guardedSheetTransaction(() => {
      // Uses the new "Hot Swap" (Overwrite) logic, now includes Named Range update
      return atomicSwapAndFlush(ss_inner, finalSheetName, tempSheetName, repairMap);
    }, 60000);



    let swapSuccess = (transactionResult.success && transactionResult.state.success);

    if (swapSuccess) {
      _resetMarketDataJobState(null);
      console.log("SUCCESS: Finalization complete.");

    } else {
      console.warn(`[Finalizer] Swap Failed: ${transactionResult.error || transactionResult.state.errorMessage}`);
    }

  }, funcName);
}