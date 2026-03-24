/** ============================================================================
 * Refresh Script (Trigger Management)
 * ----------------------------------------------------------------------------
 * NOTE: This module contains legacy functions for manual menu use, 
 * primarily intended to trigger asynchronous spreadsheet recalculations 
 * and flag updates. All heavy data synchronization is now handled by 
 * the masterOrchestrator and triggerCacheWarmerWithRetry.
 * ========================================================================== */

/* global LoggerEx, SpreadsheetApp, Utilities, fuzzworkCacheRefresh, withLock_ */

// --- CONFIGURATION ---
// The cell used in the sheet formulas (e.g., =marketStatDataBoth(..., Utility!A1)).
const RECALC_CELL_SPEC = 'Utility!E3';
// ---------------------

function _L_info(tag, obj) {
  try {
    if (typeof LoggerEx !== 'undefined' && LoggerEx.log) LoggerEx.log(tag, obj);
    else console.log(tag, obj);
  } catch (_) {}
}


/**
 * Public function for menu use to trigger a full recalculation cycle.
 * This runs the cache warmer (synchronously, potentially timing out) and 
 * nudges the sheet to re-evaluate formulas.
 * NOTE: For robust background updates, rely on masterOrchestrator schedules.
 */
function Full_Recalculate_Cycle() {
  const log = LoggerEx.withTag('FULL_RECALC');
  log.info('Starting full recalculation cycle...');

  // --- Step 1: Run Cache Warmer (Synchronous Call) ---
  // If the cache warmer takes > 6 minutes, this execution will fail, 
  // but it ensures the cache is hot for the subsequent recalculation.
  try {
    // Assuming fuzzworkCacheRefresh is available in the global scope or another loaded file
    if (typeof fuzzworkCacheRefresh_TimeGated === 'function') {
        // Use the time-gated function built for the orchestrator
        fuzzworkCacheRefresh_TimeGated();
    } else if (typeof fuzzworkCacheRefresh === 'function') {
        fuzzworkCacheRefresh();
    }
    log.info('Fuzzworks queue processed.');
  } catch (e) {
    log.warn('Fuzzworks refresh failed (likely queue empty or network issue).', e.message);
  }

  // --- Step 2: Trigger Asynchronous Sheet Recalculation ---
  // Nudge a specific cell with a new timestamp to force recalculation of volatile formulas.
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    // Using RangeByName is safer for dynamic sheets than A1 notation if the sheet changes name
    const range = ss.getRangeByName(RECALC_CELL_SPEC) || ss.getRange(RECALC_CELL_SPEC);

    if (range) {
      const newValue = new Date().getTime(); // Use a timestamp for a guaranteed change
      
      // NOTE: We rely on the sheet calculating asynchronously after the script finishes.
      range.setValue(newValue);
      log.info(`Recalculation trigger sent to sheet (${RECALC_CELL_SPEC}) with value: ${newValue}`);
    } else {
      log.error(`Recalculation cell target not found: ${RECALC_CELL_SPEC}`);
    }
  } catch (e) {
    log.error('Failed to set sheet recalculation trigger cell.', e.message);
  }

  // --- NEW: Update Need To Buy List ---
  try {
    generateRestockQuery(); // <--- Add this line
    log.info('Need To Buy list regenerated.');
  } catch (e) {
    log.warn('Failed to regenerate restock list inside refresh cycle.', e.message);
  }

  log.info('Full recalculate cycle finished. Script will now exit.');
}



// --- LEGACY FLAG MANAGEMENT (Safe for Menu Use) ---

const REFRESH_DELAY_MS = 300;
const UTILITY_SHEET    = "Utility";
const A1_ALL_RESET     = "B3:D3";
const A1_DYNAMIC       = "B3";
const A1_STATIC        = "C3";
const A1_ESI           = "D3";

/**
 * Helper to acquire document lock
 * FIX: Increased waitLock time to 30,000ms (30 seconds)
 */
function withLock_(fn) {
  const lock = LockService.getDocumentLock();
  lock.waitLock(30000); // 30s wait time for document lock
  try { fn(); } finally { lock.releaseLock(); }
}

function sheet_() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(UTILITY_SHEET);
  if (!sh) throw new Error('Utility sheet missing: "' + UTILITY_SHEET + '"');
  return sh;
}

function resetFlags_() {
  const sh = sheet_();
  sh.getRange(A1_ALL_RESET).setValues([[0, 0, 0]]);
  SpreadsheetApp.flush();
}

function nudge_(a1) {
  const sh = sheet_();
  sh.getRange(a1).setValue(1);
}


/**
 * Public menu function to reset all flags and set dynamic flag.
 */
function refreshData() {
    withLock_(function () { 
        const sh = sheet_();
        
        // 1. Reset all flags to 0 (ensures formulas re-check conditions)
        sh.getRange(A1_ALL_RESET).setValues([[0, 0, 0]]);
        SpreadsheetApp.flush(); // Flush immediately to force reset 
        
        // 2. Set all desired flags to 1
        sh.getRange(A1_DYNAMIC).setValue(1); 
        sh.getRange(A1_STATIC).setValue(1); 
        sh.getRange(A1_ESI).setValue(1); 
        
        // The script relies on the Sheet picking up the last write (set to 1) 
        // after the script exits to trigger formula re-evaluation.
    });
}

function refreshAllData() {
  withLock_(function () {
    resetFlags_();
    nudge_(A1_DYNAMIC);
    nudge_(A1_STATIC); 
    nudge_(A1_ESI); 
  });
}

function refreshDynamicData() {
  withLock_(function () { nudge_(A1_DYNAMIC); });
}

function refreshStaticData() {
  withLock_(function () { nudge_(A1_STATIC); });
}

function refreshESI() {
  withLock_(function () { nudge_(A1_ESI); });
}
