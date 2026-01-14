// ContractItems_Fetchers.gs.js
// Robust, GAS-safe contract sync for EVE (GESI):
//    * Two-phase listing: CHARACTER -> CORPORATION (no mixing of scopes)
//    * Items fetched with HEADERS (boolean; default true in GESI)
//    * Single canonical endpoints: positional arguments
//    * Per-doc cache for auth names; per-user cache for items (scope-partitioned)
//    * LoggerEx integration (marketTracker style)
//    * All major functions now accept an optional 'ss' (Spreadsheet) argument.
//    * Uses executeLocked pattern for top-level locking and retry.
//
/* global GESI, CacheService, SpreadsheetApp, LockService, Utilities, Session, LoggerEx, ML, getOrCreateSheet, PT, _charIdMap, _getData_, _toNumberISK_, executeLocked, scheduleOneTimeTrigger, deleteTriggersByName, _measureSpreadsheetLatency */

// ==========================================================================================
// CONFIG & CONSTANTS
// ==========================================================================================

// Optional override for corp auth character. May be:
//  - a Named Range (e.g., "CORP_AUTH_CHAR"),
//  - a Sheet!A1 range (e.g., "Utility!B3"),
//  - or a literal character name (e.g., "CJ Kilman").
// If omitted/blank/invalid, we default to GESI.name, then to first authed name.
var CORP_AUTH_CHARACTER = "setting_director";

// NEW PERSISTENT PROPERTY KEY
const _CORP_AUTH_CHAR_PROP = 'GESI_PERSISTED_CORP_AUTH_CHAR';

// Rolling lookback (days) for finished item_exchange contracts.
var CONTRACT_LOOKBACK_DAYS = 30;

// Maximum number of RAW sheet rows (excluding header) to read and process for CPU safety.
const MAX_RAW_ROWS_TO_PROCESS = 50000;

// --- CRITICAL CONSTANTS (Must be defined in the module file) ---
const CONTRACTS_RAW_SHEET = "Contracts (RAW)";
const CONTRACT_ITEMS_RAW_SHEET = "Contract Items (RAW)";
const CONTRACT_RAW_COLUMNS = 16; // Number of columns in Contracts (RAW)
const ITEMS_RAW_COLUMNS = 6;     // Number of columns in Contract Items (RAW)
const CONTRACT_STATUSES = ["finished", "completed", "outstanding"]; // Fetch all relevant states
const PROP_KEY_COGS_STEP = 'cogsJobStep';
const STATE_FLAGS_COGS = { FINALIZING: 'FINALIZING' };
const PROP_KEY_LAST_CONTRACT_ID = 'lastProcessedContractId';

const PROP_KEY_CONTRACT_LEASE = 'contractJobLeaseUntil';
const CONTRACT_LEASE_DURATION_MS = 60 * 60 * 1000; // 1 hour lease in milliseconds

// LEDGER SHEET CONSTANTS
const LEDGER_BUY_SHEET = 'Material_Ledger';
const LEDGER_SALE_SHEET = 'Sales_Ledger';
const LEDGER_CORP_SALE_SOURCE = 'CORP_SALE'; // New source label for corp sales
const CORP_JOURNAL_RESUME_PROP = 'CORP_JOURNAL_DIV_RESUME'; // Property for resume logic

// NEW: Property to store the transaction ID of the most recently fetched (newest) record.
const CORP_JOURNAL_LAST_ID = 'CORP_JOURNAL_LAST_TRANSACTION_ID';

// --- Raw_loot (rolling 30d total) -> Material_Ledger (post deltas) ------------
const RAW_LOOT_SHEET = 'Raw_loot';
const SNAP_KEY = 'raw_loot:snapshot:v2'; // doc properties key

// NEW EXTERNAL LOOT SOURCE CONFIG (Replaces IMPORTRANGE formula)
const EXTERNAL_LOOT_SHEET_ID = "1qESXdN_BabqiJmwHS7fHkmQxntEkU7_Zfh6mhUwLfIg";
const EXTERNAL_LOOT_RANGE = "Raw_loot!A:D";


// ENDPOINTS (canonical; let GESI handle versioning)
var EP_LIST_CHAR = "characters_character_contracts";
var EP_LIST_CORP = "corporations_corporation_contracts";

var EP_ITEMS_CHAR = "characters_character_contracts_contract_items";
var EP_ITEMS_CORP = "corporations_corporation_contracts_contract_items";


// Contract list (headerless) column order fallback, if needed
var GESI_CONTRACT_COLS = [
  "acceptor_id", "assignee_id", "availability", "buyout", "collateral", "contract_id",
  "date_accepted", "date_completed", "date_expired", "date_issued", "days_to_complete",
  "end_location_id", "for_corporation", "issuer_corporation_id", "issuer_id", "price",
  "reward", "start_location_id", "status", "title", "type", "volume", "character_name"
];

// Cache TTLs (seconds)
var GESI_TTL = (GESI_TTL != null && typeof GESI_TTL === 'object') ? GESI_TTL : {};
GESI_TTL.chars = (GESI_TTL.chars != null) ? GESI_TTL.chars : 21600; // 6h (document cache)

// UPDATED: Set Contracts and Items to 60 minutes (3600 seconds)
GESI_TTL.contracts = (GESI_TTL.contracts != null) ? GESI_TTL.contracts : 3600;    // 60m
GESI_TTL.items = (GESI_TTL.items != null) ? GESI_TTL.items : 3600;    // 60m

// ADDED: Module-level cache variable to store the authenticated character name
// only once per script execution.
var _cachedAuthChar = null;
// ADDED: Cache for Named Ranges to avoid slow API lookups
var _cachedNamedRanges = {};
// NEW: Cache for authenticated GESI names (expensive call)
var _cachedAuthNames = null;
// NEW: Cache for character ID map
var _cachedCharIdMap = null;

// ==========================================================================================
// UTILITIES (GAS-SAFE)
// ==========================================================================================

/**
 * Utility function to read all data from a sheet (rows 2+) and return header map.
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss
 * @param {string} sheetName
 */
function _getData_(ss, sheetName) { // ADDED ss ARGUMENT
  var sh = ss.getSheetByName(sheetName); // USE ss ARGUMENT
  if (!sh) throw new Error('Missing sheet: ' + sheetName);
  var vals = sh.getDataRange().getValues();

  // ROBUSTNESS FIX: Check for empty data before accessing header row (A1 is excluded from getDataRange if only headers exist)
  if (vals.length < 1) {
    return { sh: sh, header: [], rows: [], h: {} };
  }

  var header = vals[0] || [];
  var rows = vals.slice(1);
  var h = {};
  for (var i = 0; i < header.length; i++) { h[String(header[i]).trim()] = i; } // 0-based index map

  return { sh: sh, header: header, rows: rows, h: h };
}

/**
 * Replaces the content of a sheet (from row 2 down) with new rows.
 * Assumes the sheet is already created and headers are set, and lock is held.
 */
function _rewriteData_(sh, header, rows) {
  // We rely on the caller to hold the lock.

  var needed = (rows.length || 0) + 1;
  var lastHad = sh.getMaxRows();

  // 1. Ensure sheet has enough rows (insert if necessary)
  if (lastHad < needed) sh.insertRowsAfter(lastHad, needed - lastHad);

  // 2. Write new data (starting at row 2)
  if (rows.length) {
    sh.getRange(2, 1, rows.length, header.length).setValues(rows);
  }

  // 3. Clear old excess data
  var lastNow = rows.length + 1;
  var extra = Math.max(0, lastHad - lastNow);
  if (extra > 0) {
    sh.getRange(lastNow + 1, 1, extra, sh.getMaxColumns()).clearContent();
  }
}
/**
 * Resets the Loot Delta Snapshot. 
 * Forces the next run to treat ALL current loot in the external sheet as 'New'
 * and import it to the ledger.
 * WARNING: This may cause duplicates if the data is already in the ledger!
 */
function resetLootSnapshot() {
  const PROP_KEY = 'raw_loot:snapshot:v2'; // Must match SNAP_KEY in GESI Extentions.js
  const props = PropertiesService.getDocumentProperties();

  const lock = LockService.getScriptLock();
  if (lock.tryLock(5000)) {
    try {
      props.deleteProperty(PROP_KEY);
      Logger.log("✅ Loot Snapshot Reset. Next run will import all external quantities as new deltas.");
      if (typeof SpreadsheetApp !== 'undefined') SpreadsheetApp.getUi().alert("Success: Loot Snapshot Reset.");
    } catch (e) {
      Logger.log("❌ Error resetting snapshot: " + e.message);
    } finally {
      lock.releaseLock();
    }
  } else {
    Logger.log("⚠️ Could not acquire lock. Try again.");
  }
}
/**
 * Reads external loot sheet, filters for non-null items, and sorts the result.
 * This completely replaces the slow QUERY(IMPORTRANGE()) formula.
 * @returns {Object|null} { header: string[], rows: any[][], h: Object } or null on failure.
 */
function _fetchProcessedLootData() {
  const log = LoggerEx.withTag('LOOT_SYNC');

  try {
    // 1. Open external sheet
    const externalSs = SpreadsheetApp.openById(EXTERNAL_LOOT_SHEET_ID);
    const sourceSheetName = EXTERNAL_LOOT_RANGE.split('!')[0];
    const sourceRange = EXTERNAL_LOOT_RANGE.split('!')[1];

    if (!externalSs) {
      log.error('External Loot Sheet not found.', { id: EXTERNAL_LOOT_SHEET_ID });
      return null;
    }

    // 2. Read the entire required range from the external source
    const externalSheet = externalSs.getSheetByName(sourceSheetName);
    if (!externalSheet) {
      log.error('External Loot Sheet not found.', { name: sourceSheetName });
      return null;
    }

    // Use the specified range (A:D in this case)
    const values = externalSheet.getRange(sourceRange).getValues();

    if (values.length < 2) { // Need at least header + 1 row
      log.warn('External loot source returned insufficient data (less than 1 data row).');
      return { sh: externalSheet, header: values[0] || [], rows: [], h: {} };
    }

    const header = values[0];
    let rows = values.slice(1);

    // Determine which column is Col1 (the first column, index 0)
    const Col1_Index = 0;

    // 3. Filter: WHERE Col1 IS NOT NULL
    const filteredRows = rows.filter(row => row[Col1_Index] != null && row[Col1_Index] !== "");

    // 4. Sort: Order By Col1 DESC
    filteredRows.sort((a, b) => {
      const valA = a[Col1_Index];
      const valB = b[Col1_Index];

      // Simple descending comparison (assumes sortable data type)
      if (valA > valB) return -1;
      if (valA < valB) return 1;
      return 0;
    });

    log.info('Successfully fetched, filtered, and sorted external loot data.', { rows: filteredRows.length });

    // 5. Return in the same structure as _getData_
    const h = {};
    for (let i = 0; i < header.length; i++) { h[String(header[i]).trim()] = i; } // 0-based index map

    return { sh: externalSheet, header: header, rows: filteredRows, h: h };

  } catch (e) {
    log.error('Failed to fetch and process external loot data:', e);
    return null; // Return null on any catastrophic failure (Sheet/File not found)
  }
}


function _isoDate(d) {
  return Utilities.formatDate(new Date(d), "UTC", "yyyy-MM-dd");
}

function _toIntOrNull(v) {
  if (v == null) return null;
  var s = String(v).trim().replace(/[^\d]/g, '');
  if (!s) return null;
  var n = parseInt(s, 10);
  return (Number(n) === n && isFinite(n)) ? n : null;
}

/** Reads a config value from a Named Range, falling back to a default value. */
function _getNamedOr_(name, fallback) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const r = ss.getRangeByName(name);
    if (!r) return fallback;
    const v = String(r.getValue()).trim();
    return v !== '' ? v : fallback;
  } catch (e) { return fallback; }
}


// Lookback days resolver (Named Range "LOOKBACK_DAYS" -> default)
function getLookbackDays(ss) { // ADDED ss ARGUMENT
  ss = ss || SpreadsheetApp.getActiveSpreadsheet(); // Fallback to ensure 'ss' is defined
  var v = null;
  try {
    var nr = ss.getRangeByName('LOOKBACK_DAYS');
    if (nr) v = nr.getValue();
  } catch (_) { }
  var n = parseInt(v, 10);
  if (!(Number(n) === n && isFinite(n))) n = CONTRACT_LOOKBACK_DAYS;
  if (n < 1) n = 1;
  if (n > 365) n = 365;
  return n;
}

/* Per-DOCUMENT cache for authenticated character names */
function getCharNamesFast() {
  // NEW: Return cached value if available during this execution
  if (_cachedAuthNames) {
    return _cachedAuthNames;
  }

  // Directly call the global GESI function. GESI handles its own caching
  // via ScriptProperties or other mechanisms.
  var namesFn =
    (GESI && typeof GESI.getAuthenticatedCharacterNames === 'function')
      ? GESI.getAuthenticatedCharacterNames
      : (typeof getAuthenticatedCharacterNames === 'function'
        ? getAuthenticatedCharacterNames
        : null);

  if (!namesFn) throw new Error('getAuthenticatedCharacterNames not found (GESI or global).');

  const names = namesFn() || [];
  _cachedAuthNames = names; // Cache for rest of execution
  return names;
}

// Resolve corp auth character (override -> GESI.name -> NamedRange/Utility -> first authed)
function getCorpAuthChar(ss) { // ADDED ss ARGUMENT
  // --- PHASE 1: FASTEST EXIT (In-Memory Cache / Persistent Property) ---
  if (_cachedAuthChar) {
    return _cachedAuthChar;
  }

  const props = PropertiesService.getScriptProperties();
  const persistedChar = props.getProperty(_CORP_AUTH_CHAR_PROP);

  if (persistedChar) {
    _cachedAuthChar = persistedChar;
    return persistedChar;
  }

  // --- PHASE 2: EXPENSIVE RESOLUTION (Sheet I/O / API Calls) ---
  const SAFE_CONSOLE_SHIM = {
    log: console.log,
    info: console.log, // <-- CRITICAL FIX: Ensures log.info() is callable
    warn: console.warn,
    error: console.error,
    startTimer: () => ({ stamp: () => { } })
  };
  const GESI_LOG = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('GESI_AUTH') : SAFE_CONSOLE_SHIM);
  const t = GESI_LOG.startTimer('getCorpAuthChar_SlowPath');

  try {
    var log = LoggerEx.withTag('GESI');
    const spreadsheet = ss || SpreadsheetApp.getActiveSpreadsheet(); // Fallback if ss is null/undefined
    log.info("Checking for Authized Corp Character (SLOW PATH)");

    // *** NEW: Spreadsheet Latency Check ***
    if (typeof _measureSpreadsheetLatency !== 'undefined') {
      const ssLatency = _measureSpreadsheetLatency();
      log.info(`[PERF] Spreadsheet Latency at start of SLOW PATH: ${ssLatency}ms`);
    }
    // *** END NEW ***

    var desired = "";

    // Helper function optimized for speed by caching Named Range lookups
    function _resolve(sh, spec) {
      if (!spec) return null;
      spec = String(spec).trim();

      // 1. Check value cache first (Fastest)
      if (_cachedNamedRanges[spec] !== undefined) {
        return _cachedNamedRanges[spec] != null ? String(_cachedNamedRanges[spec]).trim() : null;
      }

      var got = null;

      // 2. Perform expensive Sheet API calls

      // 2a. Try Named range lookup
      try {
        var nr = sh.getRangeByName(spec);
        if (nr) got = nr.getValue();
      } catch (_) {
        // Ignore
      }

      // 2b. Try Sheet!A1 reference lookup if 2a failed
      if (got == null && spec.indexOf('!') > 0) {
        var cut = spec.indexOf('!');
        var shn = spec.slice(0, cut);
        var a1 = spec.slice(cut + 1);

        if (sh) {
          try {
            got = sh.getSheetByName(shn).getRange(a1).getValue();
          } catch (_) { }
        }
      }

      // 3. Cache the resulting value and return
      // Use null to indicate "not found" or "no value" explicitly in the cache
      const resultValue = got != null && got !== "" ? got : null;
      _cachedNamedRanges[spec] = resultValue;

      return resultValue != null ? String(resultValue).trim() : null;
    }

    // 1. Try config override (fast, no GESI)
    if (typeof CORP_AUTH_CHARACTER !== 'undefined' && CORP_AUTH_CHARACTER != null) {
      desired = _resolve(spreadsheet, CORP_AUTH_CHARACTER);
    }

    // Convert null result from _resolve back to empty string for subsequent checks
    if (desired === null) desired = "";

    // 2. Try GESI's internal default (fast, usually a PropertyService lookup)
    if (!desired && GESI && GESI.getMainCharacter) {
      desired = String(GESI.getMainCharacter()).trim();
    }

    // 3. Try secondary config location (fast, no GESI)
    if (!desired) {
      desired = _resolve(spreadsheet, 'CORP_AUTH_CHAR');
      if (desired === null) desired = ""; // Ensure result is not null
    }

    // --- OPTIMIZED FALLBACK LOGIC ---

    // 4. Verification/Fallback: Only execute the slow GESI check if needed.
    // This check relies on the newly optimized getCharNamesFast()
    var names = getCharNamesFast();
    var fallback = names[0] || "";

    if (!desired) {
      // Case 1: No name found via fast methods (Steps 1-3). Use the GESI fallback.
      desired = fallback;

    } else {
      // Case 2: A name was found via config.
      // We still need to verify this name is authenticated by GESI.

      // If the name from the config (desired) is not in the official list, revert to default.
      if (names.indexOf(desired) === -1) {
        log.warn('Corp auth override not in authenticated names; falling back', { wanted: desired, using: fallback, list: names });
        desired = fallback;
      }
    }


    log.debug('corp auth character', { using: desired });

    // --- PHASE 3: CACHE AND PERSIST ---
    if (desired) {
      _cachedAuthChar = desired;
      props.setProperty(_CORP_AUTH_CHAR_PROP, desired); // Persist for future fast runs
    }

    t.stamp('Auth_Resolved'); // <-- ADDED STAMP

    return desired;
  } catch (e) {
    // If an error occurs (e.g. network/GESI), rely on GESI.name fallback
    LoggerEx.withTag('GESI').error('getCorpAuthChar failed during slow path:', e);
    t.stamp('Auth_Failed'); // <-- ADDED STAMP
    return (GESI && GESI.name) || '';
  }
}


/** Build Char name -> ID map (Implementation) */
function _charIdMap(ss) {
  // --- IMPLEMENTATION OF NAME-TO-ID MAP (Based on Corp Members) ---
  if (_cachedCharIdMap) {
    return _cachedCharIdMap;
  }

  const log = LoggerEx.withTag('CHAR_MAP');
  const authToon = getCorpAuthChar(ss);

  if (!authToon) {
    log.warn('No authorized character found for building character map.');
    _cachedCharIdMap = {};
    return {};
  }

  const charIdMap = {};

  try {
    // 1. Get all member IDs for the corporation tied to the authenticated character.
    // FIX: Use GESI.invokeRaw with parameter object for robust script execution.
    const memberIdsRaw = GESI.invokeRaw(
      'corporations_corporation_members',
      {
        name: authToon,
        show_column_headings: false,
        version: null
      }
    );

    const memberIds = Array.isArray(memberIdsRaw) ? memberIdsRaw.filter(Number.isFinite) : [];

    if (memberIds.length === 0) {
      log.warn('No member IDs returned from GESI.corporations_corporation_members.');
      // Throw an error to ensure the subsequent fallback logic is executed
      throw new Error("No ESI member IDs found.");
    }

    // 2. Resolve those IDs to Names.
    // FIX: Use GESI.invokeRaw with the standard 'universe_names' alias.
    // IMPLEMENTING USER'S EXPLICIT INSTRUCTION: ids: [memberIds]
    const nameResolutions = GESI.invokeRaw('universe_names',
      {
        ids: memberIds, // Implementing user's explicit instruction
        show_column_headings: false,
        version: null
      }
    );
    LoggerEx.info(JSON.stringify(nameResolutions));
    // 3. Build the final Name -> ID map
    if (Array.isArray(nameResolutions)) {
      for (const entry of nameResolutions) {
        if (entry && entry.category === 'character' && entry.name && entry.id) {
          // The map is NAME -> ID
          charIdMap[entry.name] = entry.id;
        }
      }
    }

  } catch (e) {
    log.error('Error building character ID map:', e.message);

    // CRITICAL STABILITY FALLBACK (to handle external API failures)
    const fallbackIdRaw = _getNamedOr_('CORP_AUTH_CHAR_ID', null);
    const fallbackId = parseInt(fallbackIdRaw, 10);

    if (authToon && fallbackId && Number.isFinite(fallbackId)) {
      charIdMap[authToon] = fallbackId;
      log.warn(`[CHAR_MAP] ESI call failed. Using configured ID ${fallbackId} from Named Range 'CORP_AUTH_CHAR_ID' for ${authToon}.`);
    } else {
      log.warn(`[CHAR_MAP] ESI call failed. No valid fallback ID found in Named Range 'CORP_AUTH_CHAR_ID'.`);
    }

    _cachedCharIdMap = charIdMap;
    return charIdMap;
  }

  log.info(`Built character ID map for ${Object.keys(charIdMap).length} members.`);
  _cachedCharIdMap = charIdMap;
  return charIdMap;
}



// ==========================================================================================
// NORMALIZERS
//==========================================================================================

// File: cjkilman/market-tycoon-farm/Market-Tycoon-Farm-dev/src/GESI Extentions.js

// Normalize CONTRACT LIST results -> [{ ch, c }]
// Normalize CONTRACT LIST results -> [{ ch, c }]
function _normalizeCharContracts(res, names, idNameMap) { // NOTE: idNameMap is now required
  const LOG = Logger;
  var tuples = [];

  if (!res || !res.length) {
    LOG.warn('CHAR_NORM: No contract results found for normalization.');
    return tuples;
  }

  LOG.log(`CHAR_NORM: Starting normalization for ${names.length} authenticated tokens.`);

  // *** FIX 1: Per-Char Arrays (Primary GESI Output) ***
  if (Array.isArray(res[0]) && (res[0].length === 0 || (res[0].length > 0 && typeof res[0][0] === 'object'))) {
    LOG.log('CHAR_NORM: Using Object Array Normalization Logic.');
    for (var a = 0; a < names.length; a++) {
      var arr = res[a] || [];
      var fetchingCharName = names[a] || '';

      LOG.log(`CHAR_NORM: Processing token holder: ${fetchingCharName} (Found ${arr.length} contracts)`);

      for (var b = 0; b < arr.length; b++) {
        var cA = arr[b];
        if (!cA || typeof cA !== 'object') continue;
        if (cA.for_corporation === true) {
          continue;
        }
        // CRITICAL FIX: Ensure the ESI IDs are strings for lookup consistency
        const acceptorId = String(cA.acceptor_id);
        const issuerId = String(cA.issuer_id);

        // Default to the token holder's name (safety)
        let chA = fetchingCharName;

        // CRITICAL FIX: Prioritize attribution to the known ESI ID party
        if (idNameMap[acceptorId]) {
          // Priority 1: Contract is linked to an authenticated character via the Acceptor role.
          chA = idNameMap[acceptorId];
        } else if (idNameMap[issuerId]) {
          // Priority 2: Linked via the Issuer role.
          chA = idNameMap[issuerId];
        }

        // Final Push with the resolved name
        LOG.log(`CHAR_NORM: Contract ID ${cA.contract_id}: Resolved to ${chA}. Issuer ESI: ${issuerId}. Acceptor ESI: ${acceptorId}.`);
        tuples.push({ ch: String(chA), c: cA });
      }
    }
    return tuples;
  }

  // ... (The rest of the normalization logic should be reviewed but is not the source of the critical bug) ...

  LOG.log(`CHAR_NORM: Finished normalization. Generated ${tuples.length} tuples.`);
  return tuples;
}


// Corp list: same mapping, but force auth name (corp lists usually lack char names)
function _normalizeCorpContracts(res, corpAuthName) {
  var tuples = [];
  if (!res || !res.length) return tuples;

  // Headerless rows (Positional Array Output)
  if (Array.isArray(res[0]) && typeof res[0][0] !== 'string') {
    for (var r = 0; r < res.length; r++) {
      var row = res[r], c = {};
      var n = Math.min(row.length, GESI_CONTRACT_COLS.length);
      for (var k = 0; k < n; k++) c[GESI_CONTRACT_COLS[k]] = row[k];
      // FIX: Force attribution to corpAuthName
      tuples.push({ ch: String(corpAuthName), c: c });
    }
    return tuples;
  }

  // Tabular (Header row)
  if (Array.isArray(res[0]) && typeof res[0][0] === 'string') {
    var hdr = res[0];
    for (var i = 1; i < res.length; i++) {
      var row2 = res[i]; if (!Array.isArray(row2)) continue;
      var c2 = {};
      for (var j = 0; j < hdr.length; j++) c2[String(hdr[j]).trim()] = row2[j];
      // FIX: Force attribution to corpAuthName
      tuples.push({ ch: String(corpAuthName), c: c2 });
    }
    return tuples;
  }

  // Flat objects (Most common GESI format)
  if (typeof res[0] === 'object') {
    for (var m = 0; m < res.length; m++) {
      var cB = res[m]; if (!cB || typeof cB !== 'object') continue;
      // FIX: Force attribution to corpAuthName, regardless of GESI data
      tuples.push({ ch: String(corpAuthName), c: cB });
    }
  }
  return tuples;
}

// ITEMS: we always request headers; normalize header/object -> {is_included, is_singleton, quantity, type_id}
function normalizeItemRows(rows) {
  if (!rows || !rows.length) return [];
  // Tabular with header row
  if (Array.isArray(rows[0]) && typeof rows[0][0] === 'string') {
    var hdr = rows[0];
    var idx = {};
    for (var h = 0; h < hdr.length; h++) idx[String(hdr[h]).trim()] = h;
    var out = [];
    for (var r = 1; r < rows.length; r++) {
      var a = rows[r]; if (!Array.isArray(a)) continue;
      out.push({
        is_included: !!a[idx.is_included],
        is_singleton: !!a[idx.is_singleton],
        quantity: Number(a[idx.quantity] || 0),
        type_id: Number(a[idx.type_id] || 0)
      });
    }
    return out;
  }
  // Objects (some GESI builds)
  if (rows[0] && !Array.isArray(rows[0]) && typeof rows[0] === 'object') {
    var out2 = [];
    for (var i = 0; i < rows.length; i++) {
      var x = rows[i] || {};
      out2.push({
        is_included: !!x.is_included,
        is_singleton: !!x.is_singleton,
        quantity: Number(x.quantity || x.qty || 0),
        type_id: Number(x.type_id || x.typeId || 0)
      });
    }
    return out2;
  }
  return [];
}


function _fetchCharContractItems(charName, contractId) {
  var cid = _toIntOrNull(contractId);
  if (cid == null) throw new Error('contract_id must be an integer');
  return GESI.characters_character_contracts_contract_items(
    cid,
    charName,
    true
  );
}

function _fetchCorpContractItems(charName, contractId) {
  var cid = _toIntOrNull(contractId);
  if (cid == null) throw new Error('contract_id must be an integer');
  return GESI.corporations_corporation_contracts_contract_items(
    cid,
    charName,
    true
  );
}


// Per-USER cached items (partition by scope + auth name; HEADERS shape)
function getContractItemsCached(charName, contractId, force, forCorp) {
  if (force === void 0) force = false;
  var log = LoggerEx.withTag('GESI');

  var cid = _toIntOrNull(contractId);
  if (cid == null) {
    log.warn('getContractItemsCached: invalid contract_id', { char: charName, contractId: contractId });
    return [];
  }

  var authName = forCorp ? getCorpAuthChar() : String(charName);
  var scope = forCorp ? 'corp' : 'char';

  var c = CacheService.getUserCache();
  var k = 'gesi:items:' + (forCorp ? ('CORP:' + authName) : authName) + ':' + cid + ':' + scope + ':hdr';
  if (!force) {
    var hit = c.get(k);
    if (hit) return JSON.parse(hit);
  }

  var items = forCorp
    ? _fetchCorpContractItems(authName, cid)
    : _fetchCharContractItems(authName, cid);

  c.put(k, JSON.stringify(items || []), GESI_TTL.items);
  return items || [];
}

// ==========================================================================================
// RAW SHEET FUNCTIONS (mirror the same signature/shape as invoke fetchers)
// ==========================================================================================
function raw_characters_character_contract_items(contract_id, name, show_column_headings) {
  if (show_column_headings == null) show_column_headings = true;
  var cid = _toIntOrNull(contract_id);
  var cache = CacheService.getUserCache();
  var key = "gesiCharContractItmz:" + name + ":" + cid + ":" + (show_column_headings ? 1 : 0);
  var hit = cache.get(key);
  if (hit !== null) return JSON.parse(hit);

  var data = GESI.invoke(
    EP_ITEMS_CHAR,
    [String(name)],
    { contract_id: cid, show_column_headings: !!show_column_headings }
  ) || [];

  cache.put(key, JSON.stringify(data), 3600);
  return data;
}

function raw_corporations_corporation_contracts_contract_items(contract_id, name, show_column_headings) {
  if (show_column_headings == null) show_column_headings = true;
  var cid = _toIntOrNull(contract_id);
  var cache = CacheService.getUserCache();
  var key = "gesiCorpContractItmz:" + name + ":" + cid + ":" + (show_column_headings ? 1 : 0);
  var hit = cache.get(key);
  if (hit !== null) return JSON.parse(hit);

  var data = GESI.invoke(
    EP_ITEMS_CORP,
    [String(name)],
    { contract_id: cid, show_column_headings: !!show_column_headings }
  ) || [];

  cache.put(key, JSON.stringify(data), 3600);
  return data;
}

function _pickCharForContract(candidates, contractRow, idMap) {
  // candidates: array of { ch, c } for the same contract_id
  // contractRow: any one of those (for acceptor_id, etc.)
  // idMap: { name -> character_id } from CharIDMap()
  if (!candidates || !candidates.length) return '';

  var acc = String(contractRow.acceptor_id || '').trim();
  if (acc && idMap) {
    for (var i = 0; i < candidates.length; i++) {
      var ch = candidates[i].ch || '';
      if (ch && idMap[ch] && String(idMap[ch]) === acc) return ch;
    }
  }
  // fallback: first seen
  return candidates[0].ch || '';
}


// ==========================================================================================
// START LEDGER FUNCTIONS
// ==========================================================================================

/**
 * --- Raw_loot -> Material_Ledger (UUID Delta Mode) ---
 * Generates unique Transaction IDs for every delta.
 * CRITICAL FIX: Only saves the snapshot if the ledger write succeeds.
 */
function _runLootDeltaImport(ss, lootData, asOfDate, sourceLabel, writeNegatives) {
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const dateStr = asOfDate ? _isoDate(asOfDate) : _isoDate(Date.now());
  const source = sourceLabel || 'LOOT';
  const allowNeg = !!writeNegatives;
  const charName = getCorpAuthChar(ss);
  const log = LoggerEx.withTag('LOOT_DELTA');

  if (!lootData || lootData.rows.length === 0) {
    log.log('loot_import', { status: 'Skipped: No fresh external loot data available.' });
    return 0;
  }
  const loot = lootData;
  const MaterialLedger = ML.forSheet(LEDGER_BUY_SHEET);

  const h = loot.h;
  const cTid = h['type_id'], cQty = h['total_quantity'], cBuy = h['weighted_average_buy'], cVal = h['weighted_average_value'];

  if ([cTid, cQty, cBuy, cVal].some(v => v == null)) {
    throw new Error(`'${RAW_LOOT_SHEET}' must have headers: type_id, total_quantity, weighted_average_buy, weighted_average_value`);
  }

  // 1. Calculate Deltas
  const curr = new Map();
  for (const r of loot.rows) {
    const tid = Number(r[cTid]) || 0;
    if (!tid) continue;
    const qty = Number(String(r[cQty]).replace(/[^\d.\-]/g, '')) || 0;
    const sBuy = String(r[cBuy] == null ? '' : r[cBuy]).replace(/[^\d.\-]/g, '').replace(/,/g, '');
    const buy = isFinite(Number(sBuy)) ? Number(sBuy) : 0;
    const sVal = String(r[cVal] == null ? '' : r[cVal]).replace(/[^\d.\-]/g, '').replace(/,/g, '');
    const val = isFinite(Number(sVal)) ? Number(sVal) : 0;
    curr.set(tid, { qty, val, buy });
  }

  const props = PropertiesService.getDocumentProperties();
  const prevRaw = props.getProperty(SNAP_KEY);
  const prev = prevRaw ? JSON.parse(prevRaw) : {};

  const allTids = new Set([...curr.keys(), ...Object.keys(prev).map(x => Number(x) || 0)]);
  const outRows = [];

  for (const tid of allTids) {
    const cur = curr.get(tid) || { qty: 0, val: 0, buy: 0 };
    const p = prev[String(tid)] || { qty: 0, val: 0 };
    const dq = cur.qty - (Number(p.qty) || 0);

    if (dq === 0) continue;
    if (!allowNeg && dq < 0) continue;

    let unit = (isFinite(cur.val) && cur.qty > 0) ? (cur.val / cur.qty) : (cur.buy || 0);
    if (!(unit > 0)) unit = cur.buy || 0;

    // GENERATE UUID for every delta event
    outRows.push({
      date: dateStr,
      type_id: tid,
      qty: dq,
      unit_value_filled: unit,
      source: source,
      char: charName,
      contract_id: Utilities.getUuid() // Unique Transaction ID
    });
  }

  if (outRows.length === 0) {
    log.log('loot_import', { status: 'Skipped ledger update: No deltas found.', processed: allTids.size, date: dateStr });
    return 0;
  }

  // 2. Write to Ledger (Using UUID key = Append)
  // FIX: Check the result object to ensure success BEFORE saving snapshot.
  const result = MaterialLedger.upsert(['contract_id'], outRows);
  const count = result.rows || 0;

  // 3. Safe Snapshot Save
  if (result.status === "SUCCESS" || count > 0) {
    const nextSnap = {};
    for (const [tid, cur] of curr.entries()) {
      nextSnap[String(tid)] = { qty: cur.qty, val: cur.val };
    }
    props.setProperty(SNAP_KEY, JSON.stringify(nextSnap));

    log.log('loot_import', { appended: count, status: "SUCCESS", date: dateStr });
  } else {
    log.warn('loot_import', { status: "WRITE_FAILED_SNAPSHOT_NOT_SAVED", error: result.errorMerssage });
  }

  return count;
}


/** ===== JOURNAL -> Material_Ledger & Sales_Ledger (Optimized Cache Handoff) =====================
 * Imports corporate market transactions (buy and sell) for Division 3.
 * Fixes Timeout: Phase 1 caches data for Phase 2, skipping the second API fetch.
 * Fixes "Argument too large": Now uses CHUNKED CACHING to handle large sell volumes.
 */
function Ledger_Import_CorpJournal(ss, opts) {
  const log = LoggerEx.withTag('CORP_TXN');
  const t = log.startTimer('Ledger_Import_CorpJournal_Setup');
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const CACHE = CacheService.getScriptCache(); // Use Script Cache for handoff

  const PHASE_KEY = 'CORP_JOURNAL_PHASE';
  const CACHE_KEY_SELLS = 'CORP_JOURNAL_HANDOFF_SELLS';
  const CACHE_KEY_ANCHOR = 'CORP_JOURNAL_HANDOFF_ANCHOR';
  const CACHE_TTL = 1200; // 20 minutes (plenty for the handoff)

  opts = opts || {};
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();

  const GESI_FUNC_NAME = 'corporations_corporation_wallets_division_transactions';
  const BUY_SOURCE = String(opts.sourceName || 'JOURNAL').toUpperCase();
  const SINCE_DAYS = Math.max(0, Number(opts.sinceDays || 30));
  const MS_PER_DAY = 86400000;
  const cutoff = new Date(Date.now() - SINCE_DAYS * MS_PER_DAY);
  const TARGET_DIVISION = 3;
  const authToon = getCorpAuthChar(ss);

  const rawFromId = SCRIPT_PROP.getProperty(CORP_JOURNAL_LAST_ID);
  let currentFromId =  null;
  if (isNaN(currentFromId)) currentFromId = null;

  const currentPhase = SCRIPT_PROP.getProperty(PHASE_KEY) || 'BUYS';
  log.info(`Starting Corp Journal Import. Current Phase: ${currentPhase}`);

  t.stamp('Setup complete.');

  // --- VARIABLES FOR PROCESSING ---
  let buyRows = [];
  let sellRows = [];
  let allCorpTransactions = [];
  let newestTransactionId = null;
  let dataLoadedFromCache = false;

  // --- PHASE 2 SHORTCUT: TRY LOADING FROM CACHE (CHUNKED SUPPORT) ---
  if (currentPhase === 'SELLS') {
    let cachedSellData = CACHE.get(CACHE_KEY_SELLS);

    // If main key is missing, check for CHUNKED keys
    if (!cachedSellData) {
      const chunkCountStr = CACHE.get(CACHE_KEY_SELLS + '_CHUNKS');
      if (chunkCountStr) {
        const count = parseInt(chunkCountStr, 10);
        const parts = [];
        let missing = false;
        for (let i = 0; i < count; i++) {
          const part = CACHE.get(CACHE_KEY_SELLS + '_' + i);
          if (part) parts.push(part);
          else { missing = true; break; }
        }
        if (!missing) cachedSellData = parts.join('');
      }
    }

    const cachedAnchor = CACHE.get(CACHE_KEY_ANCHOR);

    if (cachedSellData) {
      log.info("Phase 2 Shortcut: Loaded SELL data from cache. Skipping API fetch.");
      sellRows = JSON.parse(cachedSellData);
      newestTransactionId = cachedAnchor; // Restore the anchor we found in Phase 1
      dataLoadedFromCache = true;
    }
  }

  // --- FETCH DATA (Only if NOT loaded from cache) ---
  if (!dataLoadedFromCache) {
    let fetchMore = true;
    log.log(`Fetching Corp Transactions for Division ${TARGET_DIVISION} (since ${SINCE_DAYS} days)...`);

    do {
      try {
        let from_id_arg = null;
        const previousFromId = currentFromId;
        if (currentFromId) {
          from_id_arg = currentFromId;
        }
        const rawEntries = GESI.invokeRaw(
          GESI_FUNC_NAME,
          {
            division: TARGET_DIVISION, from_id: from_id_arg, name: authToon,
            show_column_headings: false, version: null
          }
        );
        if (!Array.isArray(rawEntries) || rawEntries.length === 0) {
          fetchMore = false; break;
        }
        allCorpTransactions.push(...rawEntries);
        const oldestEntry = rawEntries[rawEntries.length - 1];
        const oldestDate = new Date(oldestEntry.date);
        const oldestEntryId = oldestEntry.transaction_id;

        if (isNaN(oldestDate.getTime())) {
          log.error("Invalid Date found in ESI response.");
          fetchMore = false; break;
        }
        if (previousFromId && previousFromId === oldestEntryId) {
          log.log(`Pagination exhausted: Oldest ID ${oldestEntryId} repeated.`);
          fetchMore = false; break;
        }
        if (oldestDate.getTime() < cutoff.getTime()) {
          log.log(`Oldest entry date (${oldestDate}) past cutoff.`);
          fetchMore = false;
        } else {
          currentFromId = oldestEntryId;
        }
        Utilities.sleep(50);
      } catch (e) {
        log.error(`Error fetching Division ${TARGET_DIVISION} at from_id ${currentFromId}.`, e);
        fetchMore = false; throw e;
      }
    } while (fetchMore);

    SCRIPT_PROP.deleteProperty(CORP_JOURNAL_RESUME_PROP);

    // Capture Newest ID for Anchoring
    if (allCorpTransactions.length > 0) {
      newestTransactionId = String(allCorpTransactions[0].transaction_id);
    }

    // --- PROCESS DATA INTO ROWS ---
    for (const e of allCorpTransactions) {
      const d = new Date(e.date);
      if (isNaN(d.getTime()) || d.getTime() < cutoff.getTime()) continue;

      const isBuy = e.is_buy === true;
      const typeId = Number(e.type_id || 0);
      const qty = Number(e.quantity || 0);
      const price = Number(e.unit_price || e.price || 0);
      const contractId = String(e.transaction_id || e.id || 0);

      const row = {
        date: d,
        type_id: typeId,
        qty: isBuy ? qty : -qty,
        unit_value: '',
        source: BUY_SOURCE,
        contract_id: contractId,
        char: authToon, 
        unit_value_filled: price
      };

      if (isBuy) buyRows.push(row); else sellRows.push(row);
    }
  } // End Fetch Block

  // --- PHASED EXECUTION LOGIC ---
  const MaterialLedger = ML.forSheet(LEDGER_BUY_SHEET);
  const SalesLedger = ML.forSheet(LEDGER_SALE_SHEET);
  const keys = ['source', 'contract_id'];

  let buyCount = 0;
  let sellCount = 0;

  // 1. PHASE: BUYS
  if (currentPhase === 'BUYS') {
    if (buyRows.length > 0) {
      log.info(`Processing ${buyRows.length} BUY transactions...`);
      const buyResult = MaterialLedger.upsert(keys, buyRows);
      buyCount = buyResult.rows; 
      log.log(`Buy side processed for ${LEDGER_BUY_SHEET}`, { appended_or_updated: buyCount, processed: buyRows.length });
    } else {
      log.info("No BUY transactions to process.");
    }

    // CACHE HANDOFF: Save 'sellRows' and 'newestTransactionId' for Phase 2
    if (sellRows.length > 0) {
      try {
        const jsonStr = JSON.stringify(sellRows);
        const MAX_BYTES = 90000; // Safe Chunk Size (Limit is 100KB)

        if (jsonStr.length <= MAX_BYTES) {
           CACHE.put(CACHE_KEY_SELLS, jsonStr, CACHE_TTL);
           log.info(`Cached ${sellRows.length} SELL rows directly.`);
        } else {
           // CHUNK IT
           const chunks = [];
           for (let i = 0; i < jsonStr.length; i += MAX_BYTES) {
             chunks.push(jsonStr.substring(i, i + MAX_BYTES));
           }
           CACHE.put(CACHE_KEY_SELLS + '_CHUNKS', String(chunks.length), CACHE_TTL);
           chunks.forEach((chunk, idx) => {
             CACHE.put(CACHE_KEY_SELLS + '_' + idx, chunk, CACHE_TTL);
           });
           log.info(`Cached ${sellRows.length} SELL rows in ${chunks.length} chunks.`);
        }
        
        if (newestTransactionId) CACHE.put(CACHE_KEY_ANCHOR, newestTransactionId, CACHE_TTL);
      } catch (e) {
        log.warn("Failed to cache SELL rows. Phase 2 will perform full fetch.", e);
      }
    }

    // TRANSITION
    SCRIPT_PROP.setProperty(PHASE_KEY, 'SELLS');
    log.info("Phase 'BUYS' complete. Saved state 'SELLS'. Exiting.");
    return { status: "PARTIAL_SUCCESS", phase: "BUYS", buyCount: buyCount };
  }

  // 2. PHASE: SELLS
  if (currentPhase === 'SELLS') {
    if (sellRows.length > 0) {
      log.info(`Processing ${sellRows.length} SELL transactions...`);
      const sellResult = SalesLedger.upsert(keys, sellRows);
      sellCount = sellResult.rows; 
      log.log(`Sell side processed for ${LEDGER_SALE_SHEET}`, { appended_or_updated: sellCount, processed: sellRows.length });
    } else {
      log.info("No SELL transactions to process.");
    }

    // COMPLETE CYCLE: UPDATE ANCHOR & RESET
    if (newestTransactionId) {
      SCRIPT_PROP.setProperty(CORP_JOURNAL_LAST_ID, String(newestTransactionId));
      log.log(`Saved new transaction anchor: ${newestTransactionId}`);
    }

    SCRIPT_PROP.deleteProperty(PHASE_KEY);
    CACHE.remove(CACHE_KEY_SELLS);
    CACHE.remove(CACHE_KEY_ANCHOR);
    
    // Clean up chunks if they exist
    const chunkCountStr = CACHE.get(CACHE_KEY_SELLS + '_CHUNKS');
    if (chunkCountStr) {
      const count = parseInt(chunkCountStr, 10);
      for(let i=0; i<count; i++) CACHE.remove(CACHE_KEY_SELLS + '_' + i);
      CACHE.remove(CACHE_KEY_SELLS + '_CHUNKS');
    }

    log.info("Phase 'SELLS' complete. Cycle finished.");
  }

  return {
    appended_or_updated_buy: buyCount,
    appended_or_updated_sell: sellCount,
    sheets: { buy: LEDGER_BUY_SHEET, sell: LEDGER_SALE_SHEET }
  };
}

/**
 * NEW: Helper function to run all loot delta processing steps
 * Assumes lock is held by caller.
 */
function runLootDeltaPhase(ss) {
  const log = LoggerEx.withTag('MASTER_SYNC');
  let lootData = null;
  try {
    log.info('Running _fetchProcessedLootData (External Data Sync)...');
    lootData = _fetchProcessedLootData();
  } catch (e) {
    log.error('_fetchProcessedLootData FAILED', e.message);
  }

  try {
    if (lootData) {
      log.info('Executing loot delta calculation and import...');
      // Assumes _runLootDeltaImport no longer uses internal withSheetLock
      _runLootDeltaImport(ss, lootData, null, null, false);
    } else {
      log.warn('Skipping loot delta import: Loot data could not be fetched/processed.');
    }
  } catch (e) {
    log.error('Loot Delta Phase FAILED', e.message);
  }
}

/**
 * Resets the Contract Sync Anchor and Locks.
 * Forces the next 'runContractLedgerPhase' to re-scan ALL contracts
 * within the lookback window (e.g., 30 days) and re-process them.
 */
function resetContractSync() {
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    Logger.log("⚠️ Reset failed: Script Lock busy. Try again.");
    return;
  }

  try {
    const SCRIPT_PROP = PropertiesService.getScriptProperties();

    // Keys defined in GESI Extentions.js
    const ANCHOR_KEY = 'lastProcessedContractId'; // PROP_KEY_LAST_CONTRACT_ID
    const LEASE_KEY = 'contractJobLeaseUntil';    // PROP_KEY_CONTRACT_LEASE
    const COGS_FLAG = 'cogsJobStep';              // PROP_KEY_COGS_STEP

    // 1. Delete the Anchor (Forces re-scan of old contracts)
    SCRIPT_PROP.deleteProperty(ANCHOR_KEY);

    // 2. Clear Lease (Unblocks execution if stuck)
    SCRIPT_PROP.deleteProperty(LEASE_KEY);

    // 3. Clear COGS Flag (Resets finalizer state)
    SCRIPT_PROP.deleteProperty(COGS_FLAG);

    Logger.log("✅ Contract Sync Reset Complete.");
    Logger.log("Next run will process ALL contracts in the lookback window.");

    if (typeof SpreadsheetApp !== 'undefined') {
      SpreadsheetApp.getUi().alert("Contract Sync Reset. The next run will be a full re-scan.");
    }

  } catch (e) {
    Logger.log("❌ Error resetting contract sync: " + e.message);
  } finally {
    lock.releaseLock();
  }
}

function syncContracts(ss, charIdMap) {
  var log = LoggerEx.withTag('GESI');

  // Headers are correctly defined by the original function's structure.
  var hdrC = ["char", "contract_id", "type", "status", "issuer_id", "acceptor_id", "date_issued", "date_expired", "price", "reward", "collateral", "volume", "title", "availability", "start_location_id", "end_location_id"];
  var hdrI = ["char", "contract_id", "type_id", "quantity", "is_included", "is_singleton"];
  const LAST_CID = parseInt(PropertiesService.getScriptProperties().getProperty(PROP_KEY_LAST_CONTRACT_ID) || '0', 10);
  let maxContractId = 0;
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();

  // Necessary variables for filtering/processing
  var MS_PER_DAY = 86400000;
  var lookbackDays = getLookbackDays(ss);
  var lookIso = _isoDate(Date.now() - lookbackDays * MS_PER_DAY);
  var authToon = getCorpAuthChar(ss);
  var myCharId = charIdMap[authToon] || null; // CRITICAL: Get the ESI ID for sale/buy checks
  var allNames = getCharNamesFast();
  const idNameMap = {};
  Object.entries(charIdMap).forEach(([name, id]) => {
    // We map IDs for ALL corp members so we can resolve names of unauthed acceptors,
    // but we only fetch FOR the authed list above.
    idNameMap[String(id)] = name;
  });

  // FINAL DATA ARRAYS (Combined and Segregated)
  var outC_Combined = []; // For writing to Contracts (RAW)
  var outI_Combined = []; // For writing to Contract Items (RAW)
  var outC_Buy = [];      // For Material Ledger
  var outC_Sale = [];     // For Sales Ledger

  const seenCids = new Set();

  // ---------------- PHASE 1 & 2: MULTI-STATUS FETCH ----------------
  CONTRACT_STATUSES.forEach(status => {

    // --- 1 & 2: Fetch Char & Corp Data ---
    const allCharContractsRaw = [];
    allNames.forEach(charName => {
      try {
        const contracts = GESI.invokeRaw(EP_LIST_CHAR, {
          name: charName,
          status: status,
          show_column_headings: false,
          version: null
        });
        // CRITICAL FIX: Always push the result array (empty or full) to maintain alignment
        // with the index position of charName in allNames.
        allCharContractsRaw.push(contracts || []);
        if (contracts && contracts.length > 0) {
          LoggerEx.debug("charName: " + charName + " contracts: " + JSON.stringify(contracts));
        }

      } catch (e) {
        // LOG THE SPECIFIC CHARACTER THAT FAILED
        log.error(`[SyncContracts] FAILED to fetch contracts for character: ${charName}. Error: ${e.message}`);

        // OPTION A: Fail hard (re-throw) if you want the job to stop
        //throw new Error(`Auth Error for ${charName}: ${e.message}`);

        // OPTION B: (Recommended) Push empty array and continue, so other chars still sync
        allCharContractsRaw.push([]);
      }

    });
    const resCorp = GESI.invokeRaw(EP_LIST_CORP, {
      status: status,
      name: authToon,
      show_column_headings: false,
      version: null
    });

    // --- 3. NORMALIZE & AGGREGATE RESULTS ---
    var tuplesChar = _normalizeCharContracts(allCharContractsRaw, allNames, idNameMap);
    var tuplesCorp = _normalizeCorpContracts(resCorp, authToon);
    var allTuples = [...tuplesChar, ...tuplesCorp];

    // --- 4. FILTER and SEGREGATE DATA ---
    for (const tuple of allTuples) {
      const c = tuple.c;
      const cid = _toIntOrNull(c.contract_id);

      if (LAST_CID > 0 && cid <= LAST_CID) {
        // ESI usually returns newest contracts first. If we hit an old ID, we stop processing this fetch.
        log.info(`Stopping contract sync: Reached old Contract ID ${cid} (Last processed ID was ${LAST_CID}).`);
        return; // Use 'return' inside forEach or break the loop if this were not a forEach.
      }

      if (!cid || seenCids.has(cid)) continue;
      if (cid > maxContractId) maxContractId = cid;
      // --- Determine Buy or Sale Status ---
      const isSale = (c.status === 'finished' || c.status === 'completed') &&
        (c.issuer_id === myCharId);

      // Standardized Contract Row (16 columns)
      const contractRow = [
        tuple.ch, cid, c.type || '', c.status || '', c.issuer_id || 0, c.acceptor_id || 0,
        _isoDate(c.date_issued), _isoDate(c.date_expired), c.price || 0, c.reward || 0,
        c.collateral || 0, c.volume || 0, c.title || '', c.availability || '',
        c.start_location_id || 0, c.end_location_id || 0
      ];

      // Standardized Item Row (6 columns)
      const isCorp = (tuple.ch === authToon);
      const itemsRaw = getContractItemsCached(tuple.ch, cid, true, isCorp) || [];
      const items = normalizeItemRows(itemsRaw);

      // --- PUSH TO SEGREGATED AND COMBINED ARRAYS ---
      if (isSale) {
        outC_Sale.push(contractRow);
        // NOTE: Sales ledger is complex. We push the full row for later allocation.
      } else {
        // Assume the Material Ledger handles Buys (Item Exchange where we are acceptor)
        outC_Buy.push(contractRow);
      }

      // Push raw contract/item data to combined arrays for raw sheet overwrite
      outC_Combined.push(contractRow);
      for (const item of items) {
        outI_Combined.push([tuple.ch, cid, item.type_id || 0, item.quantity || 0, item.is_included ? 'TRUE' : 'FALSE', item.is_singleton ? 'TRUE' : 'FALSE']);
      }

      seenCids.add(cid);
    }
  });

  // ---------------- WRITE RAW SHEETS (MONOLITHIC OVERWRITE) ----------------
  const shC = getOrCreateSheet(ss, CONTRACTS_RAW_SHEET, hdrC);
  const shI = getOrCreateSheet(ss, CONTRACT_ITEMS_RAW_SHEET, hdrI);

  // Dimension Safety Logic
  const dataC = (outC_Combined.length > 0) ? outC_Combined : [Array(CONTRACT_RAW_COLUMNS).fill('')];
  const dataI = (outI_Combined.length > 0) ? outI_Combined : [Array(ITEMS_RAW_COLUMNS).fill('')];

  _rewriteData_(shC, hdrC, dataC);
  _rewriteData_(shI, hdrI, dataI);

  log.log('syncContracts done', { total_synced: outC_Combined.length, buy_count: outC_Buy.length, sale_count: outC_Sale.length });

  // ---------------- FINAL RETURN (The Refactored Output) ----------------
  // Returns the separate data streams. The Monolithic Ledger functions (Material/Sales) 
  // must now be refactored to accept and process this object structure.
  return {
    contracts: outC_Combined.length,
    buyData: { contracts: outC_Buy, items: outI_Combined },
    saleData: { contracts: outC_Sale, items: outI_Combined },
    maxContractId: maxContractId // New: Return the highest ID found
  };
}


// File: cjkilman/market-tycoon-farm/Market-Tycoon-Farm-dev/src/GESI Extentions.js

/**
 * Refactored: processes segregated buy-side contract data into the Material_Ledger.
 *
 * @param {object} ss - Spreadsheet object.
 * @param {object} charIdMap - Character ID lookup map.
 * @param {object} buyData - { contracts: any[][], items: any[][] }
 */
function contractsToMaterialLedger(ss, charIdMap, buyData) {

  //TODO: make contractsToMaterialLedger capable of  replacing contractsToSalesLedger

  if (!buyData || !buyData.contracts || buyData.contracts.length === 0 || !buyData.items || buyData.items.length === 0) {
    log.log('contracts->ledger', { status: 'Skipped: In-memory data is empty.' });
    return 0;
  }

  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const log = LoggerEx.withTag('GESI');
  const MaterialLedger = ML.forSheet(LEDGER_BUY_SHEET);

  // Define header indices based on the column positions written by syncContracts
  const hC_Names = ["char", "contract_id", "type", "status", "acceptor_id", "date_issued"];
  const hI_Names = ["char", "contract_id", "type_id", "quantity", "is_included"];

  const ix = (arr, name) => arr.indexOf(name);
  const colC = { char: ix(hC_Names, "char"), contract_id: ix(hC_Names, "contract_id"), date_issued: ix(hC_Names, "date_issued") };
  const colI = { contract_id: ix(hI_Names, "contract_id"), type_id: ix(hI_Names, "type_id"), quantity: ix(hI_Names, "quantity"), is_included: ix(hI_Names, "is_included") };

  // Input Check (replaces slow lastRow checks)
  if (!buyData || !buyData.contracts || buyData.contracts.length === 0 || !buyData.items || buyData.items.length === 0) {
    log.log('contracts->ledger', { status: 'Skipped: In-memory data is empty.' });
    return 0;
  }

  // 1. Retrieve the list of ALL currently authenticated characters (the "Active Filter")
  const LOGGED_IN_CHARS = new Set(getCharNamesFast());

  // 2. Build map of all Buy Contract IDs from the in-memory contracts
  const buyCids = new Set(buyData.contracts.map(c => c[colC.contract_id]));

  // 3. Build map of Contract Items (filtering down to only BUY CIDs from the combined item list)
  const itemsByCid = {};
  for (const rowI of buyData.items) {
    const cid = rowI[colI.contract_id];

    // Only include items belonging to the contracts we are processing (buyCids)
    if (!buyCids.has(cid)) continue;

    if (!itemsByCid[cid]) itemsByCid[cid] = [];

    // Note: Items were written as 'TRUE'/'FALSE' strings by syncContracts
    itemsByCid[cid].push({
      type_id: rowI[colI.type_id],
      qty: Number(rowI[colI.quantity] || 0),
      is_included: String(rowI[colI.is_included]).toUpperCase() === 'TRUE'
    });
  }

  const outRows = [];

  // 4. Process Buy Contracts (C) and build ledger rows
  for (const rowC of buyData.contracts) {
    const contractChar = String(rowC[colC.char] || "");

    // CRITICAL FILTER: Only process contracts belonging to a currently logged-in GESI user.
    if (!LOGGED_IN_CHARS.has(contractChar)) continue;

    const cid2 = rowC[colC.contract_id];
    const issued = rowC[colC.date_issued] ? _isoDate(rowC[colC.date_issued]) : "";
    const items = itemsByCid[cid2] || [];

    for (const it of items) {
      if (!it.is_included || it.qty <= 0) continue;
      outRows.push({
        date: issued,
        type_id: it.type_id,
        qty: it.qty,
        source: "CONTRACT",
        contract_id: cid2,
        char: contractChar
      });
    }
  }

  if (outRows.length === 0) { log.log('contracts->ledger', { status: 'Skipped: No qualifying deltas.' }); return 0; }

  // --- Final Write Operation ---
  // FIX: PAUSE SHEET CALCULATIONS TO PREVENT TIMEOUT
  var needsWakeUp = pauseSheet(ss);

  try {
    const keys = ['source', 'char', 'contract_id', 'type_id'];
    const upsertResult = MaterialLedger.upsert(keys, outRows);
    const count = upsertResult.rows; // FIX: Extract the count from the object

    log.log('contracts->ledger', { appended_or_updated: count, processed_rows: outRows.length });
    return count;

  } catch (e) {
    log.error('contractsToMaterialLedger WRITE FAILED', e.message);
    throw e;
  } finally {
    // FIX: RESUME SHEET CALCULATIONS, REGARDLESS OF SUCCESS OR FAILURE
    if (needsWakeUp) {
      wakeUpSheet(ss);
    }
  }
}

/**
 * Schedules a new job to run the heavy COGS finalization step later.
 */
function triggerContractUnitCostsFinalization() {
  const LOG = LoggerEx.withTag('COGS_TRIGGER');
  const FINALIZER_FUNC = '_runRebuildContractUnitCostsWorker';

  // Schedule the worker to run soon after the main ledger phase exits.
  scheduleOneTimeTrigger(FINALIZER_FUNC, 5000); // 5 seconds delay
  LOG.info(`Scheduled heavy COGS finalization: ${FINALIZER_FUNC}`);
}

/**
 * Worker function that executes the expensive COGS allocation logic.
 * NOW CHECKS DEPENDENCIES: Loot, Journal, and Contracts must have run at least once.
 */
function _runRebuildContractUnitCostsWorker() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const funcName = '_runRebuildContractUnitCostsWorker';

  const workerFunc = () => {
    const log = LoggerEx.withTag('COGS_WORKER');
    log.info('Running contract unit costs worker.');

    // --- 1. DEPENDENCY CHECK (NEW) ---
    // Check if the upstream data sources have run at least once successfully.
    // These keys match what runMaintenanceJobs uses in Orchestrator.js.
    const tsPrefix = 'MAINTENANCE_LAST_RUN_TS_';
    const lastLoot = parseInt(SCRIPT_PROP.getProperty(tsPrefix + 'runLootDeltaPhase') || '0', 10);
    const lastJournal = parseInt(SCRIPT_PROP.getProperty(tsPrefix + 'Ledger_Import_CorpJournal') || '0', 10);
    const lastContracts = parseInt(SCRIPT_PROP.getProperty(tsPrefix + 'runContractLedgerPhase') || '0', 10);

    if (lastLoot === 0 || lastJournal === 0 || lastContracts === 0) {
      log.warn('ABORTING COGS: Dependencies missing. Loot, Journal, and Contracts must complete at least once.');
      log.info(`Debug Status: Loot=${lastLoot > 0}, Journal=${lastJournal > 0}, Contracts=${lastContracts > 0}`);

      // CRITICAL: We clear the flag so the Orchestrator doesn't get stuck in an infinite "Nudge" loop
      // trying to run this worker when it is destined to fail.
      SCRIPT_PROP.deleteProperty(PROP_KEY_COGS_STEP);
      return;
    }
    // --------------------------------

    // 2. CHECK STATE: Ensure we are in the correct state for finalization
    if (SCRIPT_PROP.getProperty(PROP_KEY_COGS_STEP) !== STATE_FLAGS_COGS.FINALIZING) {
      log.warn('COGS worker called outside FINALIZING state. Aborting.');
      return;
    }

    // 3. EXECUTE CORE WORK
    rebuildContractUnitCosts(ss);

    // 4. CLEAR FLAG: Clear the finalize flag on successful completion
    SCRIPT_PROP.deleteProperty(PROP_KEY_COGS_STEP);
    log.info('COGS Finalization flag removed on success.');
  };

  // Use executeWithTryLock from Orchestrator.js to manage the lock
  executeWithTryLock(workerFunc, funcName);
}

/**
 * Checks for and restarts the COGS unit cost worker if the flag is still set.
 * Assumes scheduleOneTimeTrigger is defined.
 */
function _nudgeCogsFinalizer() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  // NOTE: Assuming PROP_KEY_COGS_STEP and STATE_FLAGS_COGS are defined or globally available.

  if (SCRIPT_PROP.getProperty('cogsJobStep') === 'FINALIZING') {
    const lock = LockService.getScriptLock();

    // Check if the worker's lock is currently available (meaning the worker is not running).
    if (lock.tryLock(0)) {
      lock.releaseLock();
      console.log(`Orchestrator: COGS Finalizer flag found. Re-queuing worker.`);
      // Assumes _runRebuildContractUnitCostsWorker and scheduleOneTimeTrigger are global.
      scheduleOneTimeTrigger("_runRebuildContractUnitCostsWorker", 5000);
      return true;
    } else {
      console.log(`Orchestrator: COGS Finalizer flag set but worker lock is busy. Skipping nudge.`);
    }
  }
  return false;
}

// --- REMOVED withSheetLock wrapper ---
/**
 * New function: contractsToSalesLedger
 * Assumes lock is held by caller.
 */
function contractsToSalesLedger(ss, charIdMap) {

  //TODO: make contractsToMaterialLedger capable of  replacing contractsToSalesLedger

  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const log = LoggerEx.withTag('GESI');
  const charName = getCorpAuthChar(ss);
  const myCharId = charIdMap[charName] || null;
  const SalesLedger = ML.forSheet(LEDGER_SALE_SHEET);

  const shC = ss.getSheetByName(CONTRACTS_RAW_SHEET);
  const shI = ss.getSheetByName(CONTRACT_ITEMS_RAW_SHEET);
  if (!shC || !shI) throw new Error("Run syncContracts() first to populate RAW sheets.");
  if (shC.getLastRow() <= 1 || shI.getLastRow() <= 1) { log.log('contracts->sales_ledger', { status: 'Skipped: RAW sheets empty.' }); return 0; }

  const C = shC.getRange(1, 1, Math.min(shC.getLastRow(), MAX_RAW_ROWS_TO_PROCESS + 1), shC.getLastColumn()).getValues();
  const hC = C.shift();
  const I = shI.getRange(1, 1, Math.min(shI.getLastRow(), MAX_RAW_ROWS_TO_PROCESS + 1), shI.getLastColumn()).getValues();
  const hI = I.shift();
  if (C.length === 0 || I.length === 0) { log.log('contracts->sales_ledger', { status: 'Skipped: No raw data found.' }); return 0; }

  const ix = (arr, name) => arr.indexOf(name);
  const colC = { char: ix(hC, "char"), contract_id: ix(hC, "contract_id"), type: ix(hC, "type"), status: ix(hC, "status"), issuer_id: ix(hC, "issuer_id"), date_issued: ix(hC, "date_issued"), price: ix(hC, "price") };
  const colI = { contract_id: ix(hI, "contract_id"), type_id: ix(hI, "type_id"), quantity: ix(hI, "quantity"), is_included: ix(hI, "is_included") };

  const itemsByCid = {};
  // ... (logic to populate itemsByCid remains the same) ...
  for (let r = 0; r < I.length; r++) { const rowI = I[r]; const cid = rowI[colI.contract_id]; if (!itemsByCid[cid]) itemsByCid[cid] = []; itemsByCid[cid].push({ type_id: rowI[colI.type_id], qty: Number(rowI[colI.quantity] || 0), is_included: !!rowI[colI.is_included] }); }

  const outRows = [];
  // ... (logic to populate outRows based on C and itemsByCid remains the same) ...
  for (let q = 0; q < C.length; q++) { const rowC = C[q]; /* ... filtering logic ... */ const cid2 = rowC[colC.contract_id]; const issued = rowC[colC.date_issued] ? _isoDate(rowC[colC.date_issued]) : ""; const items = itemsByCid[cid2] || []; const price = Number(rowC[colC.price] || 0); for (const it of items) { if (!it.is_included || it.qty <= 0) continue; let unit_price_filled = it.qty > 0 ? price / it.qty : 0; outRows.push({ date: issued, type_id: it.type_id, qty: -it.qty, unit_value: '', unit_value_filled: unit_price_filled, source: "SALE", contract_id: cid2, char: rowC[colC.char] || "" }); } }


  if (outRows.length === 0) { log.log('contracts->sales_ledger', { status: 'Skipped: No qualifying deltas.' }); return 0; }

  // --- Code previously inside withSheetLock now runs directly ---
  const keys = ['source', 'char', 'contract_id', 'type_id'];
  const count = SalesLedger.upsert(keys, outRows);
  log.log('contracts->sales_ledger', { appended_or_updated: count, processed_rows: outRows.length });
  return count; // Return actual count
}

// ==========================================================================================
// CONTRACT UNIT COST ALLOCATION LOGIC (FINAL STEP)
// ==========================================================================================

// NOTE: This logic assumes getOrCreateSheet, _getNamedOr_, and _getData_ are defined globally.

// ==========================================================================================
// CONTRACT UNIT COST ALLOCATION HELPERS
// ==========================================================================================

/**
 * Helper to build the reference price map (Tier 1 & 2 prices).
 * Reads the 'market price Tracker' sheet. (Resolves missing dependency)
 */
function _buildRefPriceMap_(ss) {
  const log = LoggerEx.withTag('CONTRACT_ALLOC');
  const TRACKER_SHEET_NAME = 'market price Tracker';
  const HEADERS = ['type_id_filtered', 'Median Buy', 'Median Sell'];

  // Assumes _getData_ is robust (returns {rows, h})
  const dataObj = _getData_(ss, TRACKER_SHEET_NAME);
  if (dataObj.rows.length === 0) {
    log.warn(`[RefPrice] Tracker sheet is empty. Cannot allocate costs.`);
    return new Map();
  }

  const h = dataObj.h;
  const refMap = new Map();

  const cTid = h[HEADERS[0]];
  const cBuy = h[HEADERS[1]];
  const cSell = h[HEADERS[2]];

  dataObj.rows.forEach(row => {
    const type_id = Number(row[cTid]);
    const medianBuyStr = String(row[cBuy]).replace(/[^0-9.]/g, '');
    const medianSellStr = String(row[cSell]).replace(/[^0-9.]/g, '');

    const buy = parseFloat(medianBuyStr) || 0;
    const sell = parseFloat(medianSellStr) || 0;

    if (type_id > 0 && (buy > 0 || sell > 0)) {
      refMap.set(type_id, { buy, sell });
    }
  });

  log.info(`[RefPrice] Built price map for ${refMap.size} items.`);
  return refMap;
}

/**
 * Reads the user's primary market configuration from the spreadsheet cells.
 * Uses 'Location List'!C3 for Location ID and 'Market Overview'!C8 for Location Type.
 */
function _getPrimaryMarketConfig(ss) {
  const log = LoggerEx.withTag('MARKET_CONFIG');
  let locationId = 0;
  let locationType = 'Region'; // Default safe assumption

  try {
    // 1. Location ID from 'Location List'!C3 
    const locSheet = ss.getSheetByName('Location List');
    if (locSheet) {
      locationId = Number(locSheet.getRange('C3').getValue());
    }

    // 2. Location Type from 'Market Overview'!C8 
    const marketSheet = ss.getSheetByName('Market Overview');
    if (marketSheet) {
      locationType = String(marketSheet.getRange('C8').getValue()).trim() || locationType;
    }

    if (!locationId || isNaN(locationId)) {
      // Fallback to Amarr Region ID if user's ID is missing (better than Jita)
      locationId = 10000043;
      log.warn(`Location ID from 'Location List'!C3 was invalid. Defaulting to ${locationId}.`);
    }

  } catch (e) {
    log.error(`Error reading market config: ${e.message}`);
  }

  return { locationId, locationType };
}



/**
 * Robustly cleans and converts a price string to a positive number.
 */
function _cleanPrice_(value) {
  if (value == null || value === 0 || value === '') return 0;
  // Aggressively strip all non-digit, non-decimal characters (e.g., commas, ISK).
  const cleaned = String(value).replace(/[^\d.]/g, '');
  const num = Number(cleaned);
  return (Number.isFinite(num) && num > 0) ? num : 0;
}

/**
 * Helper to fetch prices for items missing from the initial _buildRefPriceMap_
 * Checks Tier 2 (Tracker) and Tier 3 (Fuzzwork API, using dynamic location).
 *
 * NOTE: Assumes _getPrimaryMarketConfig is available and functional.
 */
function _getContractPriceFallbackMap(ss, missingTids) {
  const log = LoggerEx.withTag('CONTRACT_FALLBACK');
  const fallbackMap = new Map();

  if (missingTids.length === 0) return fallbackMap;

  // --- Tier 2: Read Local Market Tracker for Missing Items ---
  const TRACKER_SHEET_NAME = "market price Tracker";
  const ID_HEADER = 'type_id_filtered';
  const BUY_HEADER = 'Median Buy';
  const requiredTids = new Set(missingTids);
  const tidsForFuzzwork = [];

  try {
    // Read data for Tier 2 check (local tracker)
    const allData = _getData_(ss, TRACKER_SHEET_NAME);
    const h = allData.h;
    const cTid = h[ID_HEADER];
    const cBuy = h[BUY_HEADER];

    if (cTid != null && cBuy != null) {
      for (const row of allData.rows) {
        const type_id = Number(row[cTid]);

        if (requiredTids.has(type_id)) {
          // Normalize price string
          const priceStr = String(row[cBuy]).replace(/ISK/gi, '').replace(/,/g, '').trim();
          const buyPrice = Number(priceStr);

          if (type_id > 0 && buyPrice > 0) {
            fallbackMap.set(type_id, { buy: buyPrice, sell: 0 });
            requiredTids.delete(type_id); // Item resolved locally
          }
        }
      }
    }

    // --- Tier 3: Prepare the remaining TIDs for API Call ---
    requiredTids.forEach(tid => tidsForFuzzwork.push(tid));

    if (tidsForFuzzwork.length > 0) {
      log.info(`Attempting Tier 3 Fuzzwork fallback for ${tidsForFuzzwork.length} missing items.`);

      // Get the dynamically configured market location
      const { locationId, locationType } = _getPrimaryMarketConfig(ss);

      // FIX: ONE-STEP CALL using fuzAPI.requestItems
      // Assumes this returns an array of item objects directly.
      const rawFuzResults = fuzAPI.requestItems(locationId, locationType, tidsForFuzzwork);

      // Process the resulting array of item objects directly.
      if (Array.isArray(rawFuzResults)) {
        rawFuzResults.forEach(item => {
          const tid = item.type_id;
          // Use item.buy.max to get the highest buy order (best acquisition cost)
          const maxBuyPrice = item.buy?.max || 0;

          if (tid > 0 && maxBuyPrice > 0) {
            fallbackMap.set(tid, { buy: maxBuyPrice, sell: 0 });
            log.debug(`Resolved Tier 3 cost for ${tid}: ${maxBuyPrice}`);
          }
        });
      }
    }

  } catch (e) {
    log.error(`Contract Price Fallback FAILED: ${e.message}`);
  }

  return fallbackMap;
}

/**
 * NEW: Wraps _getContractPriceFallbackMap to cache expensive Tier 3 prices.
 */
function _getContractPricesCached(ss, missingTids) {
  const log = LoggerEx.withTag('CONTRACT_CACHE');
  const cache = CacheService.getScriptCache();
  const cacheKey = 'CONTRACT_FALLBACK_PRICES_V1'; // Static key for all items
  const CACHE_TTL = 3600; // Cache these fallback prices for 1 hour

  // 1. Check for cached fallback map
  const cachedJson = cache.get(cacheKey);
  let cachedFallbackMap = new Map();

  if (cachedJson) {
    // Rebuild the Map from the cached JSON array
    const parsedArray = JSON.parse(cachedJson);
    cachedFallbackMap = new Map(parsedArray);
    log.info(`[CONTRACT_CACHE] Loaded ${cachedFallbackMap.size} fallback prices from cache.`);
  }

  // 2. Identify TIDs *still* missing after checking local and cache
  const finalMissingTids = missingTids.filter(tid => !cachedFallbackMap.has(tid));

  if (finalMissingTids.length > 0) {
    log.info(`[CONTRACT_CACHE] Running API for ${finalMissingTids.length} uncached items.`);

    // 3. Run the slow, API-dependent function only for items still missing
    const newFallbackMap = _getContractPriceFallbackMap(ss, finalMissingTids);

    if (newFallbackMap.size > 0) {
      // 4. Merge new results with the cache
      newFallbackMap.forEach((v, k) => cachedFallbackMap.set(k, v));

      // 5. Store the entire merged map back into the cache
      const jsonToCache = JSON.stringify(Array.from(cachedFallbackMap.entries()));
      cache.put(cacheKey, jsonToCache, CACHE_TTL);
      log.info(`[CONTRACT_CACHE] Cached and merged ${newFallbackMap.size} new prices. Total cached: ${cachedFallbackMap.size}.`);
    }
  }

  // 6. Return the consolidated map for lookup
  return cachedFallbackMap;
}

/**
 * Helper to build a map of contract prices for allocation reference.
 * Reads the Contracts (RAW) sheet. (Resolves missing dependency)
 */
function _buildContractPriceMap_(ss) {
  const log = LoggerEx.withTag('CONTRACT_ALLOC');
  const RAW_SHEET = "Contracts (RAW)";
  const HEADERS = ['contract_id', 'price', 'collateral', 'reward', 'char'];

  const dataObj = _getData_(ss, RAW_SHEET);
  if (dataObj.rows.length === 0) {
    log.warn(`[PriceMap] Contracts RAW sheet is empty. Skipping.`);
    return new Map();
  }

  const h = dataObj.h;
  const priceMap = new Map();

  const cContractId = h[HEADERS[0]];
  const cPrice = h[HEADERS[1]];
  const cCollateral = h[HEADERS[2]];
  const cReward = h[HEADERS[3]];
  const cChar = h[HEADERS[4]];

  dataObj.rows.forEach(row => {
    const contract_id = String(row[cContractId]);
    const price = Number(String(row[cPrice]).replace(/[^\d.]/g, '')) || 0;
    const collateral = Number(String(row[cCollateral]).replace(/[^\d.]/g, '')) || 0;
    const reward = Number(String(row[cReward]).replace(/[^\d.]/g, '')) || 0;
    const char = String(row[cChar]);

    if (contract_id) {
      priceMap.set(contract_id, { price, collateral, reward, char });
    }
  });

  log.info(`[PriceMap] Built price map for ${priceMap.size} contracts.`);
  return priceMap;
}

// ==========================================================================================
// UNIT COST ALLOCATION FUNCTION
// ==========================================================================================


/**
 * Recalculates unit costs for contracts in the Material/Sales Ledger.
 * This is the final step in the COGS pipeline, now using tiered fallbacks.
 */
function rebuildContractUnitCosts(ss) {
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const log = LoggerEx.withTag('CONTRACT_UNIT_COST');
  const LEDGER_BUY_SHEET = 'Material_Ledger';

  // --- 1. BUILD REQUIRED LOOKUP MAPS ---
  const allocMode = String(_getNamedOr_('setting_contract_alloc_mode', 'REF')).toUpperCase();
  const refMap = _buildRefPriceMap_(ss); // Tier 1: Local Tracker Prices
  const priceMap = _buildContractPriceMap_(ss);

  // --- 2. READ RAW DATA (Contract Items) & COLLECT ALL UNIQUE TIDS ---
  const ci = _getData_(ss, 'Contract Items (RAW)');
  if (ci.rows.length === 0) {
    log.warn('rebuildContractUnitCosts: Skipping, RAW contract data is empty.');
    return 0;
  }

  const itemsByCid = new Map();
  const allUniqueContractTids = new Set();
  const cICol = {
    contract_id: ci.h['contract_id'],
    type_id: ci.h['type_id'],
    quantity: ci.h['quantity'],
    is_included: ci.h['is_included']
  };

  ci.rows.forEach(row => {
    const cid = String(row[cICol.contract_id]);
    if (!itemsByCid.has(cid)) itemsByCid.set(cid, []);

    const qty = Number(row[cICol.quantity] || 0);
    if (String(row[cICol.is_included]).toUpperCase() === 'TRUE' && qty > 0) {
      const tid = Number(row[cICol.type_id]);
      itemsByCid.get(cid).push({ tid: tid, qty: qty });
      allUniqueContractTids.add(tid);
    }
  });

  // 3. Generate Fallback Map for Tids missing from Tier 1 (refMap)
  const tidsMissingTier1 = Array.from(allUniqueContractTids).filter(tid => !refMap.has(tid));
  const fallbackMap = _getContractPricesCached(ss, tidsMissingTier1);


  // --- 4. CALCULATE UNIT COSTS (The Core Logic) ---
  const outRows = [];
  let processedItems = 0;

  for (const [cid, items] of itemsByCid.entries()) {
    const contractMeta = priceMap.get(cid);
    if (!contractMeta) continue;

    const totalContractValue = contractMeta.price + contractMeta.collateral - contractMeta.reward;
    let totalReferenceValue = 0;

    // Pass 1: Calculate total reference value (needed for allocation factor)
    for (const { tid, qty } of items) {

      // TIERED PRICING: Check Tier 1 (refMap) then Fallback Map
      let refPriceObj = refMap.get(tid) || fallbackMap.get(tid);
      const refPrice = refPriceObj?.buy || 0;

      if (refPrice > 0) {
        totalReferenceValue += refPrice * qty;
      }
    }

    const pricePerRefUnit = (totalReferenceValue > 0) ? (totalContractValue / totalReferenceValue) : 0;
    const simpleVolumeSplit = (items.length > 0) ? (totalContractValue / items.length) : 0;

    // Pass 2: Apply allocation logic
    for (const { tid, qty } of items) {
      let unitCost = 0;

      // TIERED PRICING: Get the best available price for this item
      let refPriceObj = refMap.get(tid) || fallbackMap.get(tid);
      const refPrice = refPriceObj?.buy || 0;

      // FIX: Using local variable 'allocMode' consistently (was fixed from ALLOC_MODE)
      if (allocMode === 'REF' && totalReferenceValue > 0) {

        if (refPrice > 0) {
          // Cost = Reference Price * Allocation Factor
          unitCost = refPrice * pricePerRefUnit;
        } else {
          // Item was unpriced even with fallbacks; fall back to simple volume split.
          unitCost = simpleVolumeSplit / qty;
        }
      } else {
        // FALLBACK/VOLUME Mode: Cost = TotalValue / TotalItems (Simple averaging)
        unitCost = simpleVolumeSplit / qty;
      }

      // --- WRITE NEW LEDGER ROW OBJECT (for MaterialLedger.upsert) ---
      outRows.push({
        source: "CONTRACT",
        char: contractMeta.char,
        contract_id: cid,
        type_id: tid,

        // CRITICAL FIX: Pass the quantity back so it isn't overwritten with 0
        qty: qty,

        unit_value_filled: unitCost,
      });
      processedItems++;
    }
  }

  if (processedItems === 0) {
    log.log('rebuildContractUnitCosts', { status: 'Skipped: No items found for costing.' });
    return 0;
  }

  // --- 5. SHEET WRITE OPERATION ---
  const MaterialLedger = ML.forSheet(LEDGER_BUY_SHEET);
  const keys = ['source', 'char', 'contract_id', 'type_id'];

  // FIX: PAUSE SHEET CALCULATIONS TO PREVENT TIMEOUT
  // var needsWakeUp = pauseSheet(ss);

  let count = 0;
  try {
    const upsertResult = MaterialLedger.upsert(keys, outRows);
    count = upsertResult.rows; // FIX: Extract the count from the object

    log.log('rebuildContractUnitCosts', { appended_or_updated: count, processed: processedItems });
    return count;

  } catch (e) {
    log.error('rebuildContractUnitCosts WRITE FAILED', e.message);
    throw e;
  } finally {
    // Moved to ML upsert
    // if (needsWakeUp) {
    //  wakeUpSheet(ss);
    //}
  }
}


/**
 * Resets the anchor for the Ledger_Import_CorpJournal function,
 * forcing a re-fetch of the full 30-day history on the next run.
 * * NOTE: This clears the two script properties that track the ESI cursor.
 */
function resetCorpJournalImport() {
  // Use a Script Lock to prevent any maintenance job from running during the reset
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) { // Wait up to 5 seconds to acquire the lock
    Logger.log("Reset failed: Script Lock busy. Try again shortly.");
    return;
  }

  try {
    const SCRIPT_PROP = PropertiesService.getScriptProperties();
    // Safety check for LoggerEx vs console
    const LOG = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('RESET_TOOL') : console;

    // Keys are defined in GESI Extentions.js
    const LAST_ID_KEY = 'CORP_JOURNAL_DIV_RESUME'; // Mapped from CORP_JOURNAL_RESUME_PROP
    const RESUME_KEY = 'CORP_JOURNAL_LAST_TRANSACTION_ID'; // Mapped from CORP_JOURNAL_LAST_ID

    // 1. Delete the last transaction ID anchor
    SCRIPT_PROP.deleteProperty(LAST_ID_KEY);

    // 2. Delete the resume anchor (for safety against partial API fetches)
    SCRIPT_PROP.deleteProperty(RESUME_KEY);

    if (LOG.info) LOG.info("Corporate Journal Import anchors reset successfully.");
    else LOG.log("Corporate Journal Import anchors reset successfully.");

    // 3. Provide an alert (SAFEGUARDED against headless context)
    try {
      if (typeof SpreadsheetApp !== 'undefined') {
        SpreadsheetApp.getUi().alert('Corporate Journal Import reset successful. Next run will fetch the full 30-day history.');
      }
    } catch (uiError) {
      // Ignore UI errors (happens when running from Editor or Trigger)
      console.warn("UI Alert skipped (Context does not support UI): " + uiError.message);
    }

  } catch (e) {
    Logger.log("Failed to reset Corporate Journal Import anchors: " + e.message);
  } finally {
    lock.releaseLock();
  }
}

/**
 * MONOLITH BREAKER: Consumes segregated data and runs ledger posts conditionally.
 * Solves the timeout by skipping the unnecessary 2-minute sales write.
 */
/**
 * MONOLITH BREAKER: Consumes segregated data and runs ledger posts conditionally.
 * Implements a 1-hour lease to manage long execution cycles.
 */
function runContractLedgerPhase(ss) {
  const log = LoggerEx.withTag('MASTER_SYNC');
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  // --- 1. ACQUIRE LEASE ---
  const NOW_MS = new Date().getTime();
  const LEASE_UNTIL = NOW_MS + CONTRACT_LEASE_DURATION_MS;
  SCRIPT_PROP.setProperty(PROP_KEY_CONTRACT_LEASE, String(LEASE_UNTIL));
  log.info(`Contract Ledger Phase LEASE acquired until ${new Date(LEASE_UNTIL).toISOString()}`);

  const charIdMap = _charIdMap(ss);
  let syncResult = {};

  try {
    // --- STEP 1: SYNC RAW DATA AND SEGREGATE ---
    log.info('Running syncContracts (Fetch RAW data and Segregate)...');

    syncResult = syncContracts(ss, charIdMap);

    const contractsWritten = syncResult.contracts;

    // --- STEP 2: TOTAL COUNT GUARD ---
    if (contractsWritten === 0) {
      log.info('Skipping contract ledger processing: No new contracts were synced.');

      // Release the lease immediately if no work was found.
      SCRIPT_PROP.deleteProperty(PROP_KEY_CONTRACT_LEASE);
      return;
    }

    // --- STEP 3 & 4: POST BUY-SIDE / SALES LEDGER ---
    if (syncResult.buyData.contracts.length > 0) {
      log.info('Running contractsToMaterialLedger (Contract Buys)...');
      contractsToMaterialLedger(ss, charIdMap, syncResult.buyData);
    } else {
      log.info('contractsToMaterialLedger skipped: No buy contracts found.');
    }

    if (syncResult.saleData.contracts.length > 0) {
      log.info('Running contractsToSalesLedger (Contract Sells)...');
      contractsToSalesLedger(ss, charIdMap);
    } else {
      log.warn('contractsToSalesLedger skipped: No sales contracts found.');
    }
    // --- STEP 6: SAVE LAST PROCESSED ID (NEW) ---
    if (syncResult.maxContractId && syncResult.maxContractId > 0) {
      SCRIPT_PROP.setProperty(PROP_KEY_LAST_CONTRACT_ID, String(syncResult.maxContractId));
      log.info(`Saved new last processed Contract ID: ${syncResult.maxContractId}`);
    }
    // --- STEP 5: COST ALLOCATION (COGS) ---
    try {
      log.info('Decoupling COGS finalization to asynchronous trigger.');
      triggerContractUnitCostsFinalization();
    } catch (e) {
      log.error('rebuildContractUnitCosts FAILED', e.message);
    }

    // --- RELEASE LEASE ON SUCCESS ---
    SCRIPT_PROP.deleteProperty(PROP_KEY_CONTRACT_LEASE);

  } catch (e) {
    log.error('runContractLedgerPhase FAILED', e.message);
    // On hard failure, release the lease immediately so the orchestrator can re-try sooner.
    SCRIPT_PROP.deleteProperty(PROP_KEY_CONTRACT_LEASE);
    throw e;
  }
}

function triggerContractUnitCostsFinalization() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const LOG = LoggerEx.withTag('COGS_TRIGGER');
  const FINALIZER_FUNC = '_runRebuildContractUnitCostsWorker';

  // 1. Set the finalize flag before scheduling
  SCRIPT_PROP.setProperty(PROP_KEY_COGS_STEP, STATE_FLAGS_COGS.FINALIZING);

  // 2. Schedule the worker to run soon after the main ledger phase exits.
  scheduleOneTimeTrigger(FINALIZER_FUNC, 5000); // 5 seconds delay
  LOG.info(`Scheduled heavy COGS finalization: ${FINALIZER_FUNC}. Flag set.`);
}

/**
 * NEW: Master orchestrator function to run all ledger imports.
 * ASSUMES executeLocked() has already acquired the lock before calling this.
 */
function runAllLedgerImports() {
  const log = LoggerEx.withTag('MASTER_SYNC');
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  log.info('--- Starting Full Ledger Import Cycle (Lock Acquired) ---');

  // --- PHASE 1: CORE LEDGERS (Journal & Loot) ---
  try {
    // Assumes runLootDeltaPhase and _runLootDeltaImport no longer use internal lock
    runLootDeltaPhase(ss);
  } catch (e) {
    log.error('runLootDeltaPhase FAILED', e.message);
    // Continue to next phase even if loot fails
  }

  try {
    log.info('Running Ledger_Import_CorpJournal...');
    // Assumes Ledger_Import_CorpJournal no longer uses internal lock
    Ledger_Import_CorpJournal(ss, { division: 3, sinceDays: 30 });
  } catch (e) {
    log.error('Ledger_Import_CorpJournal FAILED', e.message);
    // Continue to next phase even if journal fails
  }

  // --- PHASE 2: CONTRACTS ---
  try {
    // Assumes runContractLedgerPhase and its sub-functions no longer use internal lock
    runContractLedgerPhase(ss);
  } catch (e) {
    log.error('runContractLedgerPhase FAILED', e.message);
    // Log error, but cycle is complete
  }


  try {
    // Assumes runContractLedgerPhase and its sub-functions no longer use internal lock
    runIndustryLedgerPhase(ss);
  } catch (e) {
    log.error('runIndustryLedgerPhase FAILED', e.message);
    // Log error, but cycle is complete
  }

  log.info('--- Full Ledger Import Cycle Complete ---');
  // NOTE: No lock release here, executeLocked handles it.
  return true;
}

/**
 * Triggered function (e.g., hourly) to attempt running the full ledger import cycle.
 * Uses executeLocked with retry logic.
 */
function triggerLedgerImportCycle() {
  const FUNC_NAME = 'runAllLedgerImports'; // The function containing the actual work
  const RETRY_DELAY_MS = 10 * 60 * 1000; // 10 minutes retry

  console.log(`Trigger received for ${FUNC_NAME}. Attempting to acquire lock...`);

  // --- Potential: Add a custom failure handler if needed ---
  // function myContractFailureHandler(error) { ... }
  // const success = executeLocked(runAllLedgerImports, FUNC_NAME, myContractFailureHandler);

  // Attempt to run the main function under lock
  const success = executeWithTryLock(runAllLedgerImports, FUNC_NAME);

  // If skipped due to lock, schedule a one-time retry
  if (!success) {
    console.warn(`Scheduling one-time retry for ${FUNC_NAME} as it was skipped due to lock.`);
    // Ensure scheduleOneTimeTrigger exists and works as expected
    scheduleOneTimeTrigger(FUNC_NAME, RETRY_DELAY_MS);
  } else {
    console.log(`${FUNC_NAME} execution initiated successfully (or was already running nested).`);
  }
}