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