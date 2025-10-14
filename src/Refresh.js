/** ============================================================================
 * Refresh Script (Trigger Management)
 * - This script is designed to be run by a single, User-Installed Time-Driven 
 * trigger (e.g., every 30 minutes).
 * - It orchestrates full data synchronization: 
 * 1. Kicks off the Fuzzworks background fetch cycle.
 * 2. Forces the spreadsheet to recalculate volatile formulas (price lookups).
 * ----------------------------------------------------------------------------
 * NOTE: Assumes the following functions/constants are available in the project:
 * - LoggerEx (for logging)
 * - fuzzworkCacheRefresh() (FuzzApiPrice.V3.gs.js)
 * - GESI functions (if needed, e.g., Ledger_Import_CorpJournal)
 * ========================================================================== */

/* global LoggerEx, SpreadsheetApp, Utilities, fuzzworkCacheRefresh */

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
 * Public function designed to be attached to a User-Installed Time-Driven Trigger.
 * * This function orchestrates the sequential flow: 
 * 1. Executes the Fuzzworks background queue (which fetches prices).
 * 2. Bumps a cell value to force the spreadsheet to recalculate (which pulls new prices from cache).
 * * @customfunction
 */
function Full_Recalculate_Cycle() {
  const log = LoggerEx.withTag('FULL_RECALC');
  log.info('Starting full recalculation cycle...');

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const range = ss.getRangeByName(RECALC_CELL_SPEC);

  if (!range) {
    log.error(`Recalculation cell target not found: ${RECALC_CELL_SPEC}`);
    return;
  }



  // --- Step 2: Force Sheet Recalculation ---
  // Change the value of the designated cell (Utility!A1) to force all formulas 
  // referencing it (like marketStatDataBoth) to re-execute and read the new cache.
  try {
    const currentValue = Number(range.getValue() || 0);
    const newValue = (currentValue % 1000) + 1; // Bump value (e.g., 1 -> 2, 999 -> 1)
    
    range.setValue(newValue);
    log.info(`Recalculation trigger cell (${RECALC_CELL_SPEC}) value updated to: ${newValue}`);
  } catch (e) {
    log.error('Failed to bump sheet recalculation trigger cell.', e.message);
  }

    // --- Step 1: Run Fuzzworks Queue ---
  // The cache miss happens when the sheet recalculates due to volatility. 
  // Running the cache refresh now ensures any queued prices are fetched.
  try {
    fuzzworkCacheRefresh();
    log.info('Fuzzworks queue processed.');
  } catch (e) {
    log.warn('Fuzzworks refresh failed (likely queue empty or network issue).', e.message);
  }

  // --- Step 3: Integrate Other Jobs (Optional, Example) ---
  // You can uncomment and integrate other scheduled jobs here.
  /*
  try {
    Ledger_Import_CorpJournal({ division: 3, sinceDays: 30 });
    log.info('GESI Ledger import completed.');
  } catch (e) {
    log.error('GESI Ledger import failed.', e.message);
  }
  */

  log.info('Full recalculate cycle finished.');
}



// Refresh.js (safe, minimal, same behavior)
// Triggers: Utility!B3 (Dynamic), C3 (Static), D3 (ESI)

const REFRESH_DELAY_MS = 300;
const UTILITY_SHEET    = "Utility";
const A1_ALL_RESET     = "B3:D3";
const A1_DYNAMIC       = "B3";
const A1_STATIC        = "C3";
const A1_ESI           = "D3";

function refreshData() {
  withLock_(function () {
    resetFlags_();
    nudge_(A1_DYNAMIC);
    nudge_(A1_STATIC);
    nudge_(A1_ESI);
  });
}

function refreshAllData() {
  withLock_(function () {
    resetFlags_();
    nudge_(A1_DYNAMIC); // keep legacy behavior: “all” implies dynamic, too
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

/* ---------------- helpers ---------------- */

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
  Utilities.sleep(REFRESH_DELAY_MS);
  const sh = sheet_();
  sh.getRange(a1).setValue(1);
  SpreadsheetApp.flush();
}

function withLock_(fn) {
  const lock = LockService.getDocumentLock();
  lock.waitLock(5000); // 5s
  try { fn(); } finally { lock.releaseLock(); }
}