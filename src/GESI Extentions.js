// ContractItems_Fetchers.gs.js
// Robust, GAS-safe contract sync for EVE (GESI):
//   • Two-phase listing: CHARACTER → CORPORATION (no mixing of scopes)
//   • Items fetched with HEADERS (boolean; default true in GESI)
//   • Single canonical endpoints: positional arguments
//   • Per-doc cache for auth names; per-user cache for items (scope-partitioned)
//   • LoggerEx integration (marketTracker style)
//   • All major functions now accept an optional 'ss' (Spreadsheet) argument.
//
/* global GESI, CacheService, SpreadsheetApp, LockService, Utilities, Session, LoggerEx, ML, withSheetLock, getOrCreateSheet, PT, _charIdMap, _getData_, _toNumberISK_ */

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

// SHEET NAMES
var CONTRACTS_RAW_SHEET = "Contracts (RAW)";
var CONTRACT_ITEMS_RAW_SHEET = "Contract Items (RAW)";

// LEDGER SHEET CONSTANTS
const LEDGER_BUY_SHEET = 'Material_Ledger';
const LEDGER_SALE_SHEET = 'Sales_Ledger';
const LEDGER_CORP_SALE_SOURCE = 'CORP_SALE'; // New source label for corp sales
const CORP_JOURNAL_RESUME_PROP = 'CORP_JOURNAL_DIV_RESUME'; // Property for resume logic

// NEW: Property to store the transaction ID of the most recently fetched (newest) record.
const CORP_JOURNAL_LAST_ID = 'CORP_JOURNAL_LAST_TRANSACTION_ID';

// --- Raw_loot (rolling 30d total) → Material_Ledger (post deltas) ------------
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
var GESI_TTL = (typeof GESI_TTL === 'object' && GESI_TTL) || {};
GESI_TTL.chars = (GESI_TTL.chars != null) ? GESI_TTL.chars : 21600; // 6h (document cache)
GESI_TTL.contracts = (GESI_TTL.contracts != null) ? GESI_TTL.contracts : 900;   // 15m (unused here)
GESI_TTL.items = (GESI_TTL.items != null) ? GESI_TTL.items : 900;   // 15m (user cache)

// ADDED: Module-level cache variable to store the authorized character name 
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
  // We rely on the caller (syncContracts, importAndCleanRawLoot) to hold the lock.
  
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


// Lookback days resolver (Named Range "LOOKBACK_DAYS" → Utility!B2 → default)
function getLookbackDays(ss) { // ADDED ss ARGUMENT
  ss = ss || SpreadsheetApp.getActiveSpreadsheet(); // Fallback to ensure 'ss' is defined
  var v = null;
  try {
    var nr = ss.getRangeByName('LOOKBACK_DAYS');
    if (nr) v = nr.getValue();
  } catch (_) { }
  if (v == null) {
    var util = ss.getSheetByName('Utility');
    if (util) { try { v = util.getRange(2, 2).getValue(); } catch (_) { } }
  }
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

// Resolve corp auth character (override → GESI.name → NamedRange/Utility → first authed)
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
  
  try {
    var log = LoggerEx.withTag('GESI');
    const spreadsheet = ss || SpreadsheetApp.getActiveSpreadsheet(); // Fallback if ss is null/undefined
    log.info("Checking for Authized Corp Character (SLOW PATH)");
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
    
    return desired;
  } catch (e) {
    // If an error occurs (e.g. network/GESI), rely on GESI.name fallback
    LoggerEx.withTag('GESI').error('getCorpAuthChar failed during slow path:', e);
    return (GESI && GESI.name) || '';
  }
}

/** Build Char name → ID map (Implementation) */
function _charIdMap(ss) { // ADDED ss ARGUMENT
  // --- IMPLEMENTATION OF NAME-TO-ID MAP (Based on Corp Members) ---
  if (_cachedCharIdMap) {
      return _cachedCharIdMap;
  }

  const log = LoggerEx.withTag('CHAR_MAP');
  // NOTE: getCorpAuthChar must be defined before this line
  const authToon = getCorpAuthChar(ss); // Get the authorized char name

  if (!authToon) {
      log.warn('No authorized character found for building character map.');
      _cachedCharIdMap = {};
      return {};
  }
  
  const charIdMap = {};
  
  try {
      // 1. Get all member IDs for the corporation tied to the authenticated character.
      // Assumes GESI wraps this ESI call correctly and takes the authToon name.
      // This returns an array of character IDs (numbers).
      const memberIdsRaw = GESI.corporations_corporation_members([authToon]);
      
      const memberIds = Array.isArray(memberIdsRaw) ? memberIdsRaw.filter(Number.isFinite) : [];

      if (memberIds.length === 0) {
          log.warn('No member IDs returned from GESI.corporations_corporation_members.');
          _cachedCharIdMap = {};
          return {};
      }

      // 2. Resolve those IDs to Names.
      // Using the generic ID-to-name lookup endpoint via GESI.
      const ID_TO_NAME_ENDPOINT = 'universe_names_id_to_name'; 
      
      // GESI handles batching for GESI.invoke
      const nameResolutions = GESI.invoke(ID_TO_NAME_ENDPOINT, memberIds, { show_column_headings: false }); 
      
      // 3. Build the final Name -> ID map
      if (Array.isArray(nameResolutions)) {
        for (const entry of nameResolutions) {
            // Check for required properties and filter by category 'character'
            if (entry && entry.category === 'character' && entry.name && entry.id) {
                // The map is NAME -> ID
                charIdMap[entry.name] = entry.id;
            }
        }
      }

  } catch (e) {
      log.error('Error building character ID map:', e);
      _cachedCharIdMap = {}; // Fail safe
      return {};
  }
  
  log.info(`Built character ID map for ${Object.keys(charIdMap).length} members.`);
  _cachedCharIdMap = charIdMap;
  return charIdMap;
}

// ==========================================================================================
// NORMALIZERS
//==========================================================================================

// Normalize CONTRACT LIST results → [{ ch, c }]
function _normalizeCharContracts(res, names) {
  var tuples = [];
  if (!res || !res.length) return tuples;

  // per-char arrays (aligned to names) — handle this FIRST
  if (Array.isArray(res[0]) && res[0].length && typeof res[0][0] === 'object') {
    for (var a = 0; a < names.length; a++) {
      var arr = res[a] || [];
      for (var b = 0; b < arr.length; b++) {
        var cA = arr[b]; if (!cA || typeof cA !== 'object') continue;
        var chA = cA.character_name || cA.char || names[a] || '';
        tuples.push({ ch: String(chA), c: cA });
      }
    }
    return tuples;
  }

  // tabular (header row)
  if (Array.isArray(res[0]) && typeof res[0][0] === 'string') {
    var hdr = res[0];
    for (var i = 1; i < res.length; i++) {
      var row2 = res[i]; if (!Array.isArray(row2)) continue;
      var c2 = {};
      for (var j = 0; j < hdr.length; j++) c2[String(hdr[j]).trim()] = row2[j];
      var ch2 = c2.character_name || '';
      tuples.push({ ch: String(ch2), c: c2 });
    }
    return tuples;
  }

  // headerless rows
  if (Array.isArray(res[0]) && typeof res[0][0] !== 'string') {
    for (var r = 0; r < res.length; r++) {
      var row = res[r];
      var c = {};
      var n = Math.min(row.length, GESI_CONTRACT_COLS.length);
      for (var k = 0; k < n; k++) c[GESI_CONTRACT_COLS[k]] = row[k];
      var ch = c.character_name || (names && names.length === 1 ? names[0] : '');
      tuples.push({ ch: String(ch), c: c });
    }
    return tuples;
  }

  // flat objects
  if (typeof res[0] === 'object') {
    for (var m = 0; m < res.length; m++) {
      var cB = res[m]; if (!cB || typeof cB !== 'object') continue;
      var chB = cB.character_name || cB.char || '';
      tuples.push({ ch: String(chB), c: cB });
    }
  }
  return tuples;
}


// Corp list: same mapping, but force auth name (corp lists usually lack char names)
function _normalizeCorpContracts(res, corpAuthName) {
  var tuples = [];
  if (!res || !res.length) return tuples;

  if (Array.isArray(res[0]) && typeof res[0][0] !== 'string') {
    for (var r = 0; r < res.length; r++) {
      var row = res[r], c = {};
      var n = Math.min(row.length, GESI_CONTRACT_COLS.length);
      for (var k = 0; k < n; k++) c[GESI_CONTRACT_COLS[k]] = row[k];
      tuples.push({ ch: String(corpAuthName), c: c });
    }
    return tuples;
  }

  if (Array.isArray(res[0]) && typeof res[0][0] === 'string') {
    var hdr = res[0];
    for (var i = 1; i < res.length; i++) {
      var row2 = res[i]; if (!Array.isArray(row2)) continue;
      var c2 = {};
      for (var j = 0; j < hdr.length; j++) c2[String(hdr[j]).trim()] = row2[j];
      tuples.push({ ch: String(corpAuthName), c: c2 });
    }
    return tuples;
  }

  if (typeof res[0] === 'object') {
    for (var m = 0; m < res.length; m++) {
      var cB = res[m]; if (!cB || typeof cB !== 'object') continue;
      tuples.push({ ch: String(cB.character_name || cB.char || corpAuthName), c: cB });
    }
  }
  return tuples;
}

// ITEMS: we always request headers; normalize header/object → {is_included, is_singleton, quantity, type_id}
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
    : _fetchCorpContractItems(authName, cid);

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
  // idMap: { name → character_id } from CharIDMap()
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
 * NEW: Master orchestrator function to run all ledger imports
 */
function runAllLedgerImports() {
  const log = LoggerEx.withTag('MASTER_SYNC');
  const ss = SpreadsheetApp.getActiveSpreadsheet(); // RETRIEVED ONCE HERE

  log.info('--- Starting Full Ledger Import Cycle ---');

  // --- PHASE 1: CORE LEDGERS (Journal & Loot) ---
  
  // 0. Run Loot Delta Phase (Fetch external data and calculate deltas)
  try {
      runLootDeltaPhase(ss);
  } catch (e) {
      log.error('runLootDeltaPhase FAILED', e.message);
  }

  // 1. Import Corporate Journal Transactions (Buys/Sells)
  try {
    log.info('Running Ledger_Import_CorpJournal (Fetch Wallet TXNs)...');
    Ledger_Import_CorpJournal(ss, { division: 3, sinceDays: 30 }); // PASS ss
  } catch (e) {
    log.error('Ledger_Import_CorpJournal FAILED', e.message);
  }

  // --- PHASE 2: CONTRACTS (The entire contract flow) ---
  
  // 3. Contract Phase Runner (Includes sync, filtering, processing, and costing)
  try {
    runContractLedgerPhase(ss);
  } catch (e) {
    log.error('runContractLedgerPhase FAILED', e.message);
  }


  log.info('--- Full Ledger Import Cycle Complete ---');
}


/** ===== JOURNAL → Material_Ledger & Sales_Ledger (Buy and Sell Sides) =====================
 * Imports corporate market transactions (buy and sell) for Division 3 only.
 * Buy transactions go to Material_Ledger (default).
 * Sell transactions go to Sales_Ledger.
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss
 */
function Ledger_Import_CorpJournal(ss, opts) { // ADDED ss ARGUMENT
  const log = LoggerEx.withTag('CORP_TXN');
  // ADDED: Start timer to profile execution time
  const t = log.startTimer('Ledger_Import_CorpJournal_Setup'); 
  
  opts = opts || {};
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();

  if (typeof ML === 'undefined' || typeof ML.upsertBy !== 'function') {
    throw new Error("ML API not found. Ensure MaterialLedger.gs.js is included and the ML object is global.");
  }

  const GESI_FUNC_NAME = 'corporations_corporation_wallets_division_transactions'; 

  const BUY_SOURCE = String(opts.sourceName || 'JOURNAL').toUpperCase();
  const SINCE_DAYS = Math.max(0, Number(opts.sinceDays || 30)); // Get sinceDays from opts

  // --- Date Cutoff Calculation ---
  const MS_PER_DAY = 86400000;
  // FIX: Corrected typo from 'MS_PER DAY' to 'MS_PER_DAY'
  const cutoff = new Date(Date.now() - SINCE_DAYS * MS_PER_DAY); // Calculate cutoff date

  // --- Division Selection: FIXED TO DIVISION 3 ---
  const TARGET_DIVISION = 3;
  const authToon = getCorpAuthChar(ss); // PASS ss
  // --- End Division Selection ---

  // --- ANCHORING SETUP ---
  // Read last known transaction ID from PropertiesService
  const props = PropertiesService.getScriptProperties();
  const rawFromId = props.getProperty(CORP_JOURNAL_LAST_ID); 
  
  // FIX: Use parseInt (corrected spelling) to convert the saved ID (string) to an integer.
  // If rawFromId is null, parseInt returns NaN, so we convert NaN back to null.
  let currentFromId = rawFromId ? parseInt(rawFromId, 10) : null;
  if (isNaN(currentFromId)) {
      currentFromId = null;
  }
  // NOTE: If the ID is used in the URL, it must be the string form. ESI typically handles 
  // conversion of large numbers passed via the options object.

  t.stamp('Setup complete, starting API calls.'); // Profiling step 1

  const allCorpTransactions = [];
  let fetchMore = true;

  log.log(`Fetching Corp Transactions for Division ${TARGET_DIVISION} (since ${SINCE_DAYS} days)...`);
  
  do {
    try {
      
      // If we have an anchor, use it to start fetching OLDER data.
      let from_id_arg = null;
      // Capture the from_id *before* the fetch to check against later (infinite loop prevention)
      const previousFromId = currentFromId; 
      
      if (currentFromId) {
        from_id_arg = currentFromId; 
        log.log(`...fetching transactions before ID: ${currentFromId}`);
      } else {
        log.log(`...fetching most recent page (no anchor set).`);
      }
      
      // FIX: Use GESI.invokeRaw (User's preferred stable method)
      const rawEntries = GESI.invokeRaw(
        GESI_FUNC_NAME, 
        {
          division: TARGET_DIVISION, // Bundled required path parameter
          from_id: from_id_arg, // Bundled optional query parameter (Number or null)
          name: authToon, // Bundled optional auth name
          show_column_headings: false, // Use raw output for object parsing
          version: null // Let GESI handle it
        }
      ); 

      if (!Array.isArray(rawEntries) || rawEntries.length === 0) {
        fetchMore = false;
        break;
      }
      
      allCorpTransactions.push(...rawEntries);

      // Check the date of the oldest entry in the current page. If it's older than 
      // our cutoff, stop fetching.
      const oldestEntry = rawEntries[rawEntries.length - 1];
      const oldestDate = new Date(oldestEntry.date);
      const oldestEntryId = oldestEntry.transaction_id;

      // FIX: Add validation check for Invalid Date before comparing getTime()
      if (isNaN(oldestDate.getTime())) {
          log.error("Invalid Date found in ESI response. Stopping pagination.", { entry: oldestEntry });
          fetchMore = false;
          break;
      }
      
      // FIX: INFINITE LOOP PREVENTION 
      // If the oldest entry ID found is the same as the as the ID we just requested (meaning we hit the end 
      // of the data and the server sent us the same last page again), we stop.
      if (previousFromId && previousFromId === oldestEntryId) {
           log.log(`Pagination exhausted: Oldest ID ${oldestEntryId} repeated. Stopping loop.`);
           fetchMore = false;
           break;
      }


      if (oldestDate.getTime() < cutoff.getTime()) {
        log.log(`Oldest entry date (${oldestDate}) is past cutoff. Stopping pagination.`);
        fetchMore = false;
      } else {
        // Set the transaction_id of the oldest entry in the current page 
        // as the starting point for the next, older page.
        currentFromId = oldestEntryId;
      }
      
      // Small sleep to be polite to the ESI server
      Utilities.sleep(100); 

    } catch (e) {
      log.error(`Error fetching Division ${TARGET_DIVISION} at from_id ${currentFromId}.`, e);
      fetchMore = false; // Stop the loop on error
      // Re-throw the error after logging so the Apps Script runtime fails visibly
      throw e; 
    }
  } while (fetchMore);
  
  
  // Resume property is not needed and is removed/cleared.
  props.deleteProperty(CORP_JOURNAL_RESUME_PROP);


  // --- PROCESSING LOGIC: Convert Raw Transactions to Ledger Rows ---
  const buyRows = [];
  const sellRows = [];
  const charName = authToon; // Corp transactions use the auth character name

  for (const e of allCorpTransactions) {
      const dt = e.date || e.timestamp || e.time;
      const d = (typeof PT !== 'undefined' && PT.parseDateSafe) ? PT.parseDateSafe(dt) : new Date(dt);
      
      // Check date against cutoff again, in case the server sent a slightly older first page
      if (!(d instanceof Date) || isNaN(d.getTime()) || d.getTime() < cutoff.getTime()) continue; 
      
      const isBuy = e.is_buy === true;
      const typeId = Number(e.type_id || 0);
      const qty = Number(e.quantity || 0);
      const price = Number(e.unit_price || e.price || 0);
      const contractId = String(e.transaction_id || e.id || 0);
      
      if (!(typeId > 0 && qty > 0 && price > 0)) continue;

      const row = {
          date: d,
          type_id: typeId,
          // item_name: e.type_name || '', // ML will likely fill this later
          qty: isBuy ? qty : -qty, // Positive for buy (inflow), negative for sell (outflow)
          unit_value: '',
          source: BUY_SOURCE, // Reusing BUY_SOURCE for transaction reference
          contract_id: contractId,
          char: charName,
          unit_value_filled: price,
      };

      if (isBuy) {
          buyRows.push(row);
      } else {
          sellRows.push(row);
      }
  }
  // --- END PROCESSING LOGIC ---

  // FIX: Optimization Check: Skip processing/writing if no NEW transactions were found.
  // This is a comprehensive check that should remain.
  const isNoNewData = rawFromId && allCorpTransactions.length > 0 && 
                      (rawFromId === String(allCorpTransactions[0].transaction_id)); 
                      
  if (allCorpTransactions.length === 0 || isNoNewData) {
      log.log('CORP_TXN', { status: 'Skipped ledger write: No new transactions found.' });
      return { 
        appended_or_updated_buy: 0, 
        appended_or_updated_sell: 0, 
        processed: allCorpTransactions.length,
        sheets: { buy: opts.sheet || LEDGER_BUY_SHEET, sell: LEDGER_SALE_SHEET },
        locked: false
      };
  }

  // --- BEGIN TRANSACTION ---
  const result = withSheetLock(function () {
    // Key: source + contract_id (transaction ID is unique per transaction type)
    const keys = ['source', 'contract_id'];
    let buyCount = 0;
    let sellCount = 0;
    
    const MAT_LEDGER_SHEET = opts.sheet || LEDGER_BUY_SHEET; // Capture Material Ledger name for logging
    
    // 1. Set up Ledger instances
    const MaterialLedger = ML.forSheet(MAT_LEDGER_SHEET); 
    const SalesLedger = ML.forSheet(LEDGER_SALE_SHEET);
    
    // 2. Process Buy Side (Inflow to Material_Ledger)
    if (buyRows.length > 0) {
      buyCount = MaterialLedger.upsert(keys, buyRows);
      // FIX: Use MAT_LEDGER_SHEET string directly instead of missing MaterialLedger.sheetName() method
      log.log(`Buy side processed for ${MAT_LEDGER_SHEET}`, { appended_or_updated: buyCount, processed: buyRows.length });
    }

    // 3. Process Sell Side (Outflow to Sales_Ledger)
    if (sellRows.length > 0) {
      sellCount = SalesLedger.upsert(keys, sellRows);
      // FIX: Use LEDGER_SALE_SHEET string directly instead of missing SalesLedger.sheetName() method
      log.log(`Sell side processed for ${LEDGER_SALE_SHEET}`, { appended_or_updated: sellCount, processed: sellRows.length });
    }

    return { 
      appended_or_updated_buy: buyCount, 
      appended_or_updated_sell: sellCount, 
      processed: allCorpTransactions.length,
      sheets: { buy: MAT_LEDGER_SHEET, sell: LEDGER_SALE_SHEET }, // Use captured MAT_LEDGER_SHEET
      locked: true
    };
  });
  // --- END TRANSACTION ---

  // --- ANCHORING WRITE LOGIC (Outside Lock) ---
  // Only save the newest ID if we actually fetched new data (and the array isn't empty)
  if (allCorpTransactions.length > 0) {
        // The newest transaction is the first element in the array (ESI returns newest first)
        const newestTransactionId = allCorpTransactions[0].transaction_id;
        
        // Save the newest ID to act as the starting anchor (from_id) for the NEXT run.
        props.setProperty(CORP_JOURNAL_LAST_ID, String(newestTransactionId));
        log.log(`Saved new transaction anchor: ${newestTransactionId}`);
    }


  return result;
}
/**
 * --- Raw_loot (rolling 30d total) → Material_Ledger (post deltas) ------------
 * Internal helper for calculating loot deltas and updating the snapshot.
 */
function _runLootDeltaImport(ss, lootData, asOfDate, sourceLabel, writeNegatives) { // ADDED ss ARGUMENT
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const dateStr = asOfDate ? _isoDate(asOfDate) : _isoDate(Date.now());
  const source = sourceLabel || 'LOOT';
  const allowNeg = !!writeNegatives;
  const charName = getCorpAuthChar(ss); // Removed redundant check
  const log = LoggerEx.withTag('LOOT_DELTA');

  // NOTE: lootData is passed directly from _fetchProcessedLootData (no longer read via _getData_)
  if (!lootData || lootData.rows.length === 0) {
    log.log('loot_import', { status: 'Skipped: No fresh external loot data available.' });
    return 0;
  }
  const loot = lootData;


  return withSheetLock(function () {
    // 1. Set up Ledger instance
    const MaterialLedger = ML.forSheet(LEDGER_BUY_SHEET);

    // Read Raw_loot (30-day rolling totals)
    // const loot = _getData_(ss, RAW_LOOT_SHEET); // REMOVED: Replaced by lootData parameter
    const h = loot.h;
    const cTid = h['type_id'],
      cQty = h['total_quantity'],
      cBuy = h['weighted_average_buy'],
      cVal = h['weighted_average_value'];
    if ([cTid, cQty, cBuy, cVal].some(v => v == null)) {
      throw new Error(`'${RAW_LOOT_SHEET}' must have headers: type_id, total_quantity, weighted_average_buy, weighted_average_value`);
    }

    // Build current snapshot map: tid → {qty,val,buy}
    const curr = new Map();
    for (const r of loot.rows) {
      const tid = Number(r[cTid]) || 0;
      if (!tid) continue;
      const qty = Number(String(r[cQty]).replace(/[^\d.\-]/g, '')) || 0;

      // Inline ISK conversion for weighted_average_buy
      const sBuy = String(r[cBuy] == null ? '' : r[cBuy]).replace(/[^\d.\-]/g, '').replace(/,/g, '');
      const buy = isFinite(Number(sBuy)) ? Number(sBuy) : 0;

      // Inline ISK conversion for weighted_average_value
      const sVal = String(r[cVal] == null ? '' : r[cVal]).replace(/[^\d.\-]/g, '').replace(/,/g, '');
      const val = isFinite(Number(sVal)) ? Number(sVal) : 0;

      curr.set(tid, {
        qty,
        val,
        buy
      });
    }

    // Load previous snapshot
    const props = PropertiesService.getDocumentProperties();
    const prevRaw = props.getProperty(SNAP_KEY);
    const prev = prevRaw ? JSON.parse(prevRaw) : {};

    // UNION of tids (handles day-31 disappearances)
    const allTids = new Set([
      ...curr.keys(),
      ...Object.keys(prev).map(x => Number(x) || 0)
    ]);

    const outRows = [];

    for (const tid of allTids) {
      const cur = curr.get(tid) || {
        qty: 0,
        val: 0,
        buy: 0
      };
      const p = prev[String(tid)] || {
        qty: 0,
        val: 0
      };

      const dq = cur.qty - (Number(p.qty) || 0);
      const dv = cur.val - (Number(p.val) || 0);

      if (dq === 0) continue;
      if (!allowNeg && dq < 0) {
        log.debug('Skipping negative delta due to pruning or loss', { tid, dq });
        continue;
      }

      // Price calculation: Use absolute delta value / delta quantity, or fall back to weighted average buy price
      let unit = (isFinite(dv / dq) && Math.abs(dv) > 0) ? Math.abs(dv / dq) : (cur.buy || 0);
      if (!(unit > 0)) unit = cur.buy || 0;

      outRows.push({
        date: dateStr,
        type_id: tid,
        qty: dq,
        unit_value_filled: unit,
        source: source,
        char: charName
      });
    }

    // ** EARLY EXIT OPTIMIZATION **
    if (outRows.length === 0) {
      log.log('loot_import', { status: 'Skipped ledger update: No deltas found.', processed: allTids.size, date: dateStr });
      return 0;
    }

    const keys = ['source', 'char', 'type_id', 'date'];
    const count = MaterialLedger.upsert(keys, outRows);

    const nextSnap = {};
    for (const [tid, cur] of curr.entries()) {
      nextSnap[String(tid)] = {
        qty: cur.qty,
        val: cur.val
      };
    }
    props.setProperty(SNAP_KEY, JSON.stringify(nextSnap));

    log.log('loot_import', {
      appended_or_updated: count,
      processed: allTids.size,
      date: dateStr,
    });

    return count;
  });
}

/**
 * NEW: Helper function to run all loot delta processing steps
 */
function runLootDeltaPhase(ss) {
  const log = LoggerEx.withTag('MASTER_SYNC');

  // 0. Processed Loot Data (Replaces importAndCleanRawLoot)
  let lootData = null;
  try {
    log.info('Running _fetchProcessedLootData (External Data Sync)...');
    // Fetch and process data directly, bypassing local sheet write/read cycle
    lootData = _fetchProcessedLootData();
  } catch (e) {
    log.error('_fetchProcessedLootData FAILED', e.message);
    // Continue execution, but lootData is null
  }

  // 1. Import Loot Deltas (Requires a stable Materia_Ledger state)
  try {
    if (lootData) {
        log.info('Executing loot delta calculation and import...');
        // Note: setting writeNegatives to false here for safety against pruning
        _runLootDeltaImport(ss, lootData, null, null, false); // PASS ss, set writeNegatives=false
    } else {
        log.warn('Skipping loot delta import: Loot data could not be fetched/processed.');
    }
  } catch (e) {
    log.error('Loot Delta Phase FAILED', e.message);
  }
}

// ==========================================================================================
// SYNC: Two-phase (CHAR first → CORP). No scope mixing.
// ==========================================================================================
function syncContracts(ss, charIdMap) { // ADDED charIdMap ARGUMENT
  var log = LoggerEx.withTag('GESI');
  var hdrC = ["char", "contract_id", "type", "status", "issuer_id", "acceptor_id", "date_issued", "date_expired", "price", "reward", "collateral", "volume", "title", "availability", "start_location_id", "end_location_id"];
  var hdrI = ["char", "contract_id", "type_id", "quantity", "is_included", "is_singleton"];

  ss = ss || SpreadsheetApp.getActiveSpreadsheet(); // Ensure ss is defined
  var names = getCharNamesFast();
  var corpAuth = getCorpAuthChar(ss); // PASS ss
  log.log('chars', names);

  var MS_PER_DAY = 86400000;
  var lookbackDays = getLookbackDays(ss); // PASS ss
  var lookIso = _isoDate(Date.now() - lookbackDays * MS_PER_DAY);

  var outC = [];
  var outI = [];
  
  const userCache = CacheService.getUserCache(); // Initialize cache here

  // ---------------- PHASE 1: CHARACTER CONTRACTS ----------------
  var tListChar = log.startTimer('contracts:list:char');
  
  // --- CACHE CHECK (CHAR LIST) ---
  const charListCacheKey = 'gesi:contract_list:char:' + names.join(':');
  let resChar = userCache.get(charListCacheKey);
  
  if (resChar) {
    resChar = JSON.parse(resChar);
    log.info('contracts:list:char fetched from cache.');
  } else {
    // NOTE: This call is slow as it fetches ALL contracts for ALL characters
    resChar = GESI.invokeMultiple(EP_LIST_CHAR, names, { status: "all" }) || [];
    userCache.put(charListCacheKey, JSON.stringify(resChar), GESI_TTL.contracts);
  }
  tListChar.stamp('listed');

  var tuplesChar = _normalizeCharContracts(resChar, names);
  log.log('normalized char contracts', tuplesChar.length);

  var seenChar = {}; // track char contract_ids to avoid double insert if corp list repeats
  // Group all char contracts by contract_id first
  var byCid = Object.create(null);
  for (var t = 0; t < tuplesChar.length; t++) {
    var c1 = tuplesChar[t].c;
    var type1 = String(c1.type || '').toLowerCase();
    var stat1 = String(c1.status || '').toLowerCase();
    
    // --- REINSTATED CRITICAL FILTERS ---
    if (type1 !== 'item_exchange' || stat1 !== 'finished') continue;

    var issued1 = c1.date_issued ? String(c1.date_issued).slice(0, 10) : '';
    if (issued1 && issued1 < lookIso) continue;
    // --- END REINSTATED CRITICAL FILTERS ---

    var cid1 = _toIntOrNull(c1.contract_id);
    if (cid1 == null) continue;

    if (!byCid[cid1]) byCid[cid1] = [];
    byCid[cid1].push(tuplesChar[t]); // keep all sightings for this cid
  }

  // var idMap = _charIdMap(ss) || null; // REMOVED redundant map creation
  var cids = Object.keys(byCid);
  for (var g = 0; g < cids.length; g++) {
    var cid = cids[g];
    var group = byCid[cid];             // [{ ch, c }, ...] same contract_id
    var cRow = group[0].c;             // representative row (dates, price, etc.)
    var ch1 = _pickCharForContract(group, cRow, charIdMap); // USED PASSED MAP

    var cidNum = _toIntOrNull(cid);
    if (cidNum == null) continue;

    // Fetch items once (character scope)
    var items1Raw = getContractItemsCached(ch1, cidNum, false, false) || [];
    var items1 = normalizeItemRows(items1Raw);

    // Write contract & items under the chosen character (usually the acceptor)
    outC.push([
      ch1, cidNum, cRow.type, cRow.status, cRow.issuer_id, cRow.acceptor_id,
      cRow.date_issued || "", cRow.date_expired || "", cRow.price || 0, cRow.reward || 0, cRow.collateral || 0,
      cRow.volume || 0, cRow.title || "", cRow.availability || "", cRow.start_location_id || "", cRow.end_location_id || ""
    ]);

    for (var j1 = 0; j1 < items1.length; j1++) {
      var it1 = items1[j1];
      // Item contracts should only include items that were 'included' AND have a quantity
      if (!it1.is_included || !it1.quantity) continue;
      outI.push([ch1, cidNum, it1.type_id, it1.quantity, true, !!it1.is_singleton]);
    }

    seenChar['' + cidNum] = true; // so corp phase won’t re-add it
    Utilities.sleep(150);
  }

  // ---------------- PHASE 2: CORPORATION CONTRACTS ----------------
  var tListCorp = log.startTimer('contracts:list:corp');
  
  // --- CACHE CHECK (CORP LIST) ---
  const corpListCacheKey = 'gesi:contract_list:corp:' + corpAuth;
  let resCorp = userCache.get(corpListCacheKey);
  
  if (resCorp) {
    resCorp = JSON.parse(resCorp);
    log.info('contracts:list:corp fetched from cache.');
  } else {
    // NOTE: This call is slow as it fetches ALL contracts for the corporation
    resCorp = GESI.invoke(EP_LIST_CORP, [corpAuth], { status: "all" }) || [];
    userCache.put(corpListCacheKey, JSON.stringify(resCorp), GESI_TTL.contracts);
  }
  tListCorp.stamp('listed');

  var tuplesCorp = _normalizeCorpContracts(resCorp, corpAuth);
  log.log('normalized corp contracts', tuplesCorp.length);

  for (var u = 0; u < tuplesCorp.length; u++) {
    var ch2 = tuplesCorp[u].ch || corpAuth; // always corp auth name
    var c2 = tuplesCorp[u].c;

    var type2 = String(c2.type || '').toLowerCase();
    var stat2 = String(c2.status || '').toLowerCase();
    
    // --- REINSTATED CRITICAL FILTERS ---
    if (type2 !== 'item_exchange' || stat2 !== 'finished') continue;

    var issued2 = c2.date_issued ? String(c2.date_issued).slice(0, 10) : '';
    if (issued2 && issued2 < lookIso) continue;
    // --- END REINSTATED CRITICAL FILTERS ---

    var cid2 = _toIntOrNull(c2.contract_id);
    if (cid2 == null) { log.warn('corp: invalid contract_id; skip', { char: ch2, raw: c2.contract_id }); continue; }

    // If this ID already appeared in CHAR phase, skip (keep scopes cleanly separated)
    if (seenChar['' + cid2]) continue;

    var items2Raw = getContractItemsCached(ch2, cid2, false, true) || [];
    var items2 = normalizeItemRows(items2Raw);

    outC.push([
      ch2, cid2, c2.type, c2.status, c2.issuer_id, c2.acceptor_id,
      c2.date_issued || "", c2.date_expired || "", c2.price || 0, c2.reward || 0, c2.collateral || 0,
      c2.volume || 0, c2.title || "", c2.availability || "", c2.start_location_id || "", c2.end_location_id || ""
    ]);

    for (var j2 = 0; j2 < items2.length; j2++) {
      var it2 = items2[j2];
      // Item contracts should only include items that were 'included' AND have a quantity
      if (!it2.is_included || !it2.quantity) continue;
      outI.push([ch2, cid2, it2.type_id, it2.quantity, true, !!it2.is_singleton]);
    }

    seenChar['' + cid2] = true; // so corp phase won’t re-add it
    Utilities.sleep(150);
  }

  // ---------------- WRITE SHEETS ----------------
  return withSheetLock(function () { // WRAPPED ENTIRE WRITE IN LOCK
    // Replacing custom _sheetSafe with getOrCreateSheet (assuming robust implementation)
    const shC = getOrCreateSheet(ss, CONTRACTS_RAW_SHEET, hdrC);
    const shI = getOrCreateSheet(ss, CONTRACT_ITEMS_RAW_SHEET, hdrI);
    
    // NOTE: Assuming _rewriteFast exists in Utility.js or globally for this usage.
    _rewriteData_(shC, hdrC, outC); // Lock removed internally
    _rewriteData_(shI, hdrI, outI); // Lock removed internally
  
    log.log('syncContracts done', { contracts: outC.length, items: outI.length, lookback_days: lookbackDays, lookIso: lookIso });
    
    // Return the number of contracts written
    return outC.length; // <--- ADDED RETURN VALUE
  }); // END LOCK
}


/**
 * NEW: Helper function to run all contract processing steps
 */
function runContractLedgerPhase(ss) {
    const log = LoggerEx.withTag('MASTER_SYNC');
    
    // --- STEP 1: RESOLVE MAP (Required for filtering/validation) ---
    let charIdMap = {};
    try {
        charIdMap = _charIdMap(ss) || {};
    } catch (e) {
        log.error('Char ID Map RESOLUTION FAILED during contract phase', e.message);
        // Continue, but charIdMap will be empty
    }
    
    // --- STEP 2: SYNC RAW CONTRACT DATA (Fetch API) ---
    log.info('Running syncContracts (Fetch RAW data)...');
    let contractsWritten = 0;
    try {
        // Pass the resolved map to syncContracts for internal filtering
        contractsWritten = syncContracts(ss, charIdMap); 
    } catch (e) {
        log.error('syncContracts FAILED', e.message);
        // If sync fails, we assume 0 contracts were written (contractsWritten=0)
    }
    
    
    // --- STEP 3: CONDITIONAL CHECK ---
    if (contractsWritten === 0) {
        log.info('Skipping contract ledger processing: No new contracts were synced.');
        return;
    }
    
    // --- STEPS 4, 5, 6: EXECUTE PROCESSING AND COSTING ---
    log.info(`Processing ${contractsWritten} newly synced contracts...`);
    try {
      log.info('Running contractsToMaterialLedger (Contract Buys)...');
      contractsToMaterialLedger(ss, charIdMap); // PASS ss and charIdMap
      log.info('Running contractsToSalesLedger (Contract Sells)...');
      contractsToSalesLedger(ss, charIdMap); // PASS ss and charIdMap
    } catch (e) {
      log.error('Contract Ledger Processing FAILED', e.message);
    }
  
    // 6. Recalculate Unit Costs (Must run after contract ledgers are updated)
    try {
      log.info('Running rebuildContractUnitCosts (Allocate Prices)...');
      rebuildContractUnitCosts(ss); // PASS ss
    } catch (e) {
      log.error('rebuildContractUnitCosts FAILED', e.message);
    }
}
