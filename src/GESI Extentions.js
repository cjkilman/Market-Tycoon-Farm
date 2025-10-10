// ContractItems_Fetchers.gs.js
// Robust, GAS-safe contract sync for EVE (GESI):
//   • Two-phase listing: CHARACTER → CORPORATION (no mixing of scopes)
//   • Items fetched with HEADERS (boolean; default true in GESI)
//   • Single canonical endpoints; positional args only (no named objects)
//   • Per-doc cache for auth names; per-user cache for items (scope-partitioned)
//   • LoggerEx integration (marketTracker style)
//
// Assumes the GESI library + LoggerEx are available.
/* global GESI, CacheService, SpreadsheetApp, LockService, Utilities, Session, LoggerEx, ML, withSheetLock, getOrCreateSheet, PT, _charIdMap */

// ==========================================================================================
// CONFIG & CONSTANTS
// ==========================================================================================

// Optional override for corp auth character. May be:
//  - a Named Range (e.g., "CORP_AUTH_CHAR"),
//  - a Sheet!A1 range (e.g., "Utility!B3"),
//  - or a literal character name (e.g., "CJ Kilman").
// If omitted/blank/invalid, we default to GESI.name, then to first authed name.
var CORP_AUTH_CHARACTER = "setting_director";

// Rolling lookback (days) for finished item_exchange contracts.
// You may also define a Named Range "LOOKBACK_DAYS" or put the value in Utility!B2.
var CONTRACT_LOOKBACK_DAYS = 30;

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
var _cachedNamedRanges = {};

// ... (Your existing Utility functions: _isoDate, _toIntOrNull, _sheet, _rewrite, etc.) ...

// ==========================================================================================
// UTILITIES (GAS-SAFE)
// ==========================================================================================


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




function _setValues(sh, startRow, rows) {
  if (!rows || !rows.length) return;
  sh.getRange(startRow, 1, rows.length, rows[0].length).setValues(rows);
}

// Lookback days resolver (Named Range "LOOKBACK_DAYS" → Utility!B2 → default)
function getLookbackDays() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
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
  // Directly call the global GESI function. GESI handles its own caching 
  // via ScriptProperties or other mechanisms.
  var namesFn =
    (GESI && typeof GESI.getAuthenticatedCharacterNames === 'function')
      ? GESI.getAuthenticatedCharacterNames
      : (typeof getAuthenticatedCharacterNames === 'function'
        ? getAuthenticatedCharacterNames
        : null);

  if (!namesFn) throw new Error('getAuthenticatedCharacterNames not found (GESI or global).');

  return namesFn() || [];
}

// Resolve corp auth character (override → GESI.name → NamedRange/Utility → first authed)
function getCorpAuthChar() {
  // ADDED: Return cached value if available
  if (_cachedAuthChar) {
    return _cachedAuthChar;
  }

  var log = LoggerEx.withTag('GESI');
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  var desired = "";

  // Helper function optimized for speed by caching Named Range lookups
  function _resolve(sh, spec) {
    if (!spec) return "";
    spec = String(spec).trim();
    var got = null;

    // 1. Named range lookup (Use internal cache if available)
    if (_cachedNamedRanges[spec] !== undefined) {
      // Hit the cache (can be null if the range doesn't exist)
      var nr = _cachedNamedRanges[spec];
    } else {
      // MISS: Do the slow lookup, then cache the result (or null)
      try {
        var nr = sh.getRangeByName(spec);
        _cachedNamedRanges[spec] = nr;
      } catch (_) {
        _cachedNamedRanges[spec] = null;
      }
    }

    if (nr) got = nr.getValue();

    // 2. Sheet!A1 reference lookup
    if (!got && spec.indexOf('!') > 0) {
      var cut = spec.indexOf('!');
      var shn = spec.slice(0, cut);
      var a1 = spec.slice(cut + 1);

      // NOTE: We rely on the caller passing the active Spreadsheet object 'sh'
      if (sh) { try { got = sh.getSheetByName(shn).getRange(a1).getValue(); } catch (_) { } }
    }

    if (!got) got = spec;
    return String(got).trim();
  }

  // 1. Try config override (fast, no GESI)
  if (typeof CORP_AUTH_CHARACTER !== 'undefined' && CORP_AUTH_CHARACTER != null) {
    desired = _resolve(ss, CORP_AUTH_CHARACTER);
  }

  // 2. Try GESI's internal default (fast, usually a PropertyService lookup)
  if (!desired && GESI && GESI.getMainCharacter) {
    desired = String(GESI.getMainCharacter()).trim();
  }

  // 3. Try secondary config location (fast, no GESI)
  if (!desired) {
    desired = _resolve(ss, 'CORP_AUTH_CHAR');
    if (!desired) desired = _resolve(ss, 'Utility!B3');
  }

  // 4. Last resort: Fetch authenticated names (SLOWEST PATH)
  if (!desired) {
    // We only execute this expensive block if the desired name is not found in fast lookups.
    var names = getCharNamesFast();
    var fallback = names[0] || "";

    // Check if the desired name (if one was set earlier) is valid, otherwise use fallback.
    if (!desired || names.indexOf(desired) === -1) {
      if (desired) log.warn('Corp auth override not in authenticated names; falling back', { wanted: desired, using: fallback, list: names });
      desired = fallback;
    }
  }


  log.debug('corp auth character', { using: desired });

  // ADDED: Cache the result before returning
  _cachedAuthChar = desired;
  return desired;
}

// ==========================================================================================
// NORMALIZERS
// ==========================================================================================

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
      tuples.push({ ch: String(corpAuthName), c: cB });
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
// SYNC: Two-phase (CHAR first → CORP). No scope mixing.
// ==========================================================================================
function syncContracts() {
  var log = LoggerEx.withTag('GESI');
  var hdrC = ["char", "contract_id", "type", "status", "issuer_id", "acceptor_id", "date_issued", "date_expired", "price", "reward", "collateral", "volume", "title", "availability", "start_location_id", "end_location_id"];
  var hdrI = ["char", "contract_id", "type_id", "quantity", "is_included", "is_singleton"];

  var names = getCharNamesFast();
  var corpAuth = getCorpAuthChar();
  log.log('chars', names);

  var MS_PER_DAY = 86400000;
  var lookbackDays = getLookbackDays();
  var lookIso = _isoDate(Date.now() - lookbackDays * MS_PER_DAY);

  var outC = [];
  var outI = [];
  
  const ss = SpreadsheetApp.getActiveSpreadsheet();

  // ---------------- PHASE 1: CHARACTER CONTRACTS ----------------
  var tListChar = log.startTimer('contracts:list:char');
  var resChar = GESI.invokeMultiple(EP_LIST_CHAR, names, { status: "all" }) || [];
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
    if (type1 !== 'item_exchange' || stat1 !== 'finished') continue;

    var issued1 = c1.date_issued ? String(c1.date_issued).slice(0, 10) : '';
    if (issued1 && issued1 < lookIso) continue;

    var cid1 = _toIntOrNull(c1.contract_id);
    if (cid1 == null) continue;

    if (!byCid[cid1]) byCid[cid1] = [];
    byCid[cid1].push(tuplesChar[t]); // keep all sightings for this cid
  }

  var idMap = (typeof _charIdMap === 'function') ? _charIdMap() : null;
  var cids = Object.keys(byCid);
  for (var g = 0; g < cids.length; g++) {
    var cid = cids[g];
    var group = byCid[cid];             // [{ ch, c }, ...] same contract_id
    var cRow = group[0].c;             // representative row (dates, price, etc.)
    var ch1 = _pickCharForContract(group, cRow, idMap);

    var cidNum = _toIntOrNull(cid);
    if (cidNum == null) continue;

    // Fetch items once (character scope)
    var items1Raw = getContractItemsCached(ch1, cidNum, false, false) || [];
    var items1 = normalizeItemRows(items1Raw);

    // Write contract & items under the chosen character (usually the acceptor)
    outC.push([
      ch1, cidNum, cRow.type, cRow.status, cRow.issuer_id, cRow.acceptor_id,
      cRow.date_issued || "", cRow.date_expired || "", cRow.price || 0, cRow.reward || 0, cRow.collateral || 0,
      cRow.volume || 0, cRow.title || "", c2.start_location_id || "", cRow.end_location_id || ""
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
  var resCorp = GESI.invoke(EP_LIST_CORP, [corpAuth], { status: "all" }) || [];
  tListCorp.stamp('listed');

  var tuplesCorp = _normalizeCorpContracts(resCorp, corpAuth);
  log.log('normalized corp contracts', tuplesCorp.length);

  for (var u = 0; u < tuplesCorp.length; u++) {
    var ch2 = tuplesCorp[u].ch || corpAuth; // always corp auth name
    var c2 = tuplesCorp[u].c;

    var type2 = String(c2.type || '').toLowerCase();
    var stat2 = String(c2.status || '').toLowerCase();
    if (type2 !== 'item_exchange' || stat2 !== 'finished') continue;

    var issued2 = c2.date_issued ? String(c2.date_issued).slice(0, 10) : '';
    if (issued2 && issued2 < lookIso) continue;

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

    Utilities.sleep(150);
  }

  // ---------------- WRITE SHEETS ----------------
  // Replacing custom _sheetSafe with getOrCreateSheet (assuming robust implementation)
  const shC = getOrCreateSheet(ss, CONTRACTS_RAW_SHEET, hdrC);
  const shI = getOrCreateSheet(ss, CONTRACT_ITEMS_RAW_SHEET, hdrI);
  
  // NOTE: Assuming _rewriteFast exists in Utility.js or globally for this usage.
  _rewriteFast(shC, hdrC, outC);
  _rewriteFast(shI, hdrI, outI);

  log.log('syncContracts done', { contracts: outC.length, items: outI.length, lookback_days: lookbackDays, lookIso: lookIso });
}

// ==========================================================================================
// RAW → Material_Ledger (Import/Buy Side)
// ==========================================================================================
function contractsToMaterialLedger() {
  const log = LoggerEx.withTag('GESI');
  // NOTE: This assumes a helper function _charIdMap() exists globally or in another module
  const myCharId = (typeof _charIdMap === 'function') ? _charIdMap()[getCorpAuthChar()] : null;

  return withSheetLock(function () {
    // 1. Explicitly set the sheet to ensure correct target
    ML.setSheet('Material_Ledger');

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const shC = ss.getSheetByName(CONTRACTS_RAW_SHEET);
    const shI = ss.getSheetByName(CONTRACT_ITEMS_RAW_SHEET);
    if (!shC || !shI) throw new Error("Run syncContracts() first to populate RAW sheets.");

    const C = shC.getDataRange().getValues();
    const hC = C.shift();
    const I = shI.getDataRange().getValues();
    const hI = I.shift();

    const ix = (arr, name) => arr.indexOf(name);

    const colC = {
      char: ix(hC, "char"),
      contract_id: ix(hC, "contract_id"),
      type: ix(hC, "type"),
      status: ix(hC, "status"),
      acceptor_id: ix(hC, "acceptor_id"),
      date_issued: ix(hC, "date_issued"),
    };
    const colI = {
      contract_id: ix(hI, "contract_id"),
      type_id: ix(hI, "type_id"),
      quantity: ix(hI, "quantity"),
      is_included: ix(hI, "is_included"),
    };

    const itemsByCid = {};
    for (let r = 0; r < I.length; r++) {
      const rowI = I[r];
      const cid = rowI[colI.contract_id];
      if (!itemsByCid[cid]) itemsByCid[cid] = [];
      itemsByCid[cid].push({
        type_id: rowI[colI.type_id],
        qty: Number(rowI[colI.quantity] || 0),
        is_included: !!rowI[colI.is_included],
      });
    }

    const outRows = [];
    for (let q = 0; q < C.length; q++) {
      const rowC = C[q];
      const ctype = String(rowC[colC.type] || "").toLowerCase();
      const status = String(rowC[colC.status] || "").toLowerCase();
      const acceptorId = rowC[colC.acceptor_id];
      const charName = rowC[colC.char] || "";

      // Filter for finished contracts where the character is the acceptor (buy)
      if (ctype !== "item_exchange" || status !== "finished" || (myCharId && acceptorId !== myCharId) || (charName && acceptorId && myCharId && charName !== getCorpAuthChar())) {
        continue;
      }

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
          char: rowC[colC.char] || ""
        });
      }
    }

    // Use ML.upsertBy to handle the de-duplication and writing
    const keys = ['source', 'char', 'contract_id', 'type_id'];
    const count = ML.upsertBy(keys, outRows);

    log.log('contracts→ledger', {
      appended_or_updated: count,
      processed_rows: outRows.length
    });
    return count;
  });
}

// ============================================================================
// RAW → Sales_Ledger (Export/Sell Side)
// ============================================================================

function contractsToSalesLedger() {
  const log = LoggerEx.withTag('GESI');
  // NOTE: This assumes a helper function _charIdMap() exists globally or in another module
  const myCharId = (typeof _charIdMap === 'function') ? _charIdMap()[getCorpAuthChar()] : null;

  return withSheetLock(function () {
    // 1. Explicitly set the sheet to ensure correct target
    ML.setSheet('Sales_Ledger');

    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const shC = ss.getSheetByName(CONTRACTS_RAW_SHEET);
    const shI = ss.getSheetByName(CONTRACT_ITEMS_RAW_SHEET);
    if (!shC || !shI) throw new Error("Run syncContracts() first to populate RAW sheets.");

    const C = shC.getDataRange().getValues();
    const hC = C.shift();
    const I = shI.getDataRange().getValues();
    const hI = I.shift();

    const ix = (arr, name) => arr.indexOf(name);

    const colC = {
      char: ix(hC, "char"),
      contract_id: ix(hC, "contract_id"),
      type: ix(hC, "type"),
      status: ix(hC, "status"),
      issuer_id: ix(hC, "issuer_id"),
      date_issued: ix(hC, "date_issued"),
      price: ix(hC, "price"),
    };
    const colI = {
      contract_id: ix(hI, "contract_id"),
      type_id: ix(hI, "type_id"),
      quantity: ix(hI, "quantity"),
      is_included: ix(hI, "is_included"),
    };

    const itemsByCid = {};
    for (let r = 0; r < I.length; r++) {
      const rowI = I[r];
      const cid = rowI[colI.contract_id];
      if (!itemsByCid[cid]) itemsByCid[cid] = [];
      itemsByCid[cid].push({
        type_id: rowI[colI.type_id],
        qty: Number(rowI[colI.quantity] || 0),
        is_included: !!rowI[colI.is_included],
      });
    }

    const outRows = [];
    for (let q = 0; q < C.length; q++) {
      const rowC = C[q];
      const ctype = String(rowC[colC.type] || "").toLowerCase();
      const status = String(rowC[colC.status] || "").toLowerCase();
      const issuerId = rowC[colC.issuer_id];
      const charName = rowC[colC.char] || "";
      const price = Number(rowC[colC.price] || 0);

      // Filter for finished contracts where the character is the issuer (sell)
      if (ctype !== "item_exchange" || status !== "finished" || (myCharId && issuerId !== myCharId) || (charName && issuerId && myCharId && charName !== getCorpAuthChar())) {
        continue;
      }

      const cid2 = rowC[colC.contract_id];
      const issued = rowC[colC.date_issued] ? _isoDate(rowC[colC.date_issued]) : "";
      const items = itemsByCid[cid2] || [];

      for (const it of items) {
        if (!it.is_included || it.qty <= 0) continue;

        let unit_price_filled = 0;
        if (it.qty > 0) {
          unit_price_filled = price / it.qty; // Simple price allocation per unit
        }

        // Use a NEGATIVE quantity to denote a sale/outgoing item
        outRows.push({
          date: issued,
          type_id: it.type_id,
          qty: -it.qty,
          unit_value: '',
          unit_value_filled: unit_price_filled,
          source: "SALE",
          contract_id: cid2,
          char: charName
        });
      }
    }

    const keys = ['source', 'char', 'contract_id', 'type_id'];
    const count = ML.upsertBy(keys, outRows);

    log.log('contracts→sales_ledger', {
      appended_or_updated: count,
      processed_rows: outRows.length
    });
    return count;
  });
}

/***********************
 * Contract costing → unit_cost_alloc + Ledger fill (Original was removed; keep for reference if needed)
 ***********************/

/* Settings: can be overridden by Named Ranges:
   - setting_contract_alloc_mode: "REF" | "QTY"
   - setting_contract_ref_source: "MEDIAN_BUY" | "CURRENT_BUY" | "MEDIAN_SELL" | "CURRENT_SELL"
*/
function _getNamedOr_(name, fallback) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const r = ss.getRangeByName(name);
    if (!r) return fallback;
    const v = String(r.getValue()).trim();
    return v !== '' ? v : fallback;
  } catch (e) { return fallback; }
}




/** Build type_id → refPrice map from 'market price Tracker' */
function _buildRefPriceMap_() {
  const refSrc = String(_getNamedOr_('setting_contract_ref_source', 'MEDIAN_BUY')).toUpperCase();
  const { rows, h } = _getData_(MARKET_PRICE_SHEET);
  const cTid = h['type_id_filtered'], cMB = h['Median Buy'], cMS = h['Median Sell'], cCB = h['Current Buy'], cCS = h['Current Sell'];
  if ([cTid, cMB, cMS, cCB, cCS].some(v => v == null)) throw new Error(`'${MARKET_PRICE_SHEET}' missing expected headers`);
  const pick = refSrc === 'CURRENT_BUY' ? cCB : refSrc === 'MEDIAN_SELL' ? cMS : refSrc === 'CURRENT_SELL' ? cCS : cMB;

  const map = new Map();
  for (const r of rows) {
    const tid = Number(r[cTid]); if (!tid) continue;
    // fallback chain: chosen → Median Buy → Current Buy → Median Sell → Current Sell
    const a = _toNumberISK_(r[pick]);
    const b = a || _toNumberISK_(r[cMB]) || _toNumberISK_(r[cCB]) || _toNumberISK_(r[cMS]) || _toNumberISK_(r[cCS]);
    if (b > 0) map.set(tid, b);
  }
  return map;
}

/** contract_id → price (finished item_exchange), ignore 0-price */
function _buildContractPriceMap_() {
  const { rows, h } = _getData_(CONTRACTS_RAW_SHEET);
  const cCID = h['contract_id'], cType = h['type'], cStatus = h['status'], cPrice = h['price'];
  if ([cCID, cType, cStatus, cPrice].some(v => v == null)) throw new Error(`'${CONTRACTS_RAW_SHEET}' missing headers: contract_id,type,status,price`);
  const map = new Map();
  for (const r of rows) {
    if (String(r[cType]) !== 'item_exchange') continue;
    if (String(r[cStatus]) !== 'finished') continue;
    const cid = String(r[cCID]); const price = _toNumberISK_(r[cPrice]);
    if (price > 0) map.set(cid, price); // ignore zero-price
  }
  return map;
}

function _toNumberISK_(v) {
  var s = String(v == null ? '' : v).replace(/[^\d.\-]/g, '').replace(/,/g, '');
  var n = Number(s);
  return isFinite(n) ? n : 0;
}

/** Main: compute unit_cost_alloc + fill Material_Ledger.unit_value_filled */
function rebuildContractUnitCosts() {
  const log = LoggerEx.withTag('GESI');
  const allocMode = String(_getNamedOr_('setting_contract_alloc_mode', 'REF')).toUpperCase(); // 'REF' | 'QTY'
  const refMap = _buildRefPriceMap_();
  const priceMap = _buildContractPriceMap_();

  // Read Contract Items (RAW)
  const ci = _getData_(CONTRACT_ITEMS_RAW_SHEET);
  const iCID = ci.h['contract_id'], iTID = ci.h['type_id'], iQ = ci.h['quantity'], iInc = ci.h['is_included'];
  if ([iCID, iTID, iQ, iInc].some(v => v == null)) throw new Error(`'${CONTRACT_ITEMS_RAW_SHEET}' missing headers: contract_id,type_id,quantity,is_included`);

  // Read Contracts (RAW) to get contract dates and character names
  const c = _getData_(CONTRACTS_RAW_SHEET);
  const cID = c.h['contract_id'], cChar = c.h['char'], cDate = c.h['date_issued'];
  if ([cID, cChar, cDate].some(v => v == null)) throw new Error(`'${CONTRACTS_RAW_SHEET}' missing headers: contract_id, char, date_issued`);
  const contractMeta = new Map();
  for (const r of c.rows) {
    const cid = String(r[cID]);
    contractMeta.set(cid, {
      char: String(r[cChar]),
      date: r[cDate] ? _isoDate(r[cDate]) : ''
    });
  }

  const outRows = [];

  // Group items by contract
  const itemsByCid = new Map();
  ci.rows.forEach(r => {
    const cid = String(r[iCID]);
    const included = String(r[iInc]).toUpperCase() === 'TRUE';
    if (!included) return;
    const tid = Number(r[iTID]) || 0;
    const qty = Number(r[iQ]) || 0;
    if (qty <= 0) return;
    if (!itemsByCid.has(cid)) itemsByCid.set(cid, []);
    itemsByCid.get(cid).push({
      tid,
      qty
    });
  });

  // Calculate unit costs and build ledger rows
  for (const [cid, items] of itemsByCid.entries()) {
    const price = priceMap.get(cid) || 0;
    const meta = contractMeta.get(cid);
    if (price <= 0 || !meta) continue;

    // Determine allocation denominator
    let denQty = 0, denRef = 0, missingRef = false;
    for (const { tid, qty } of items) {
      denQty += qty;
      const ref = refMap.get(tid) || 0;
      if (ref > 0) denRef += qty * ref;
      else missingRef = true;
    }
    const useRef = (allocMode === 'REF' && !missingRef && denRef > 0);

    for (const { tid, qty } of items) {
      let unit = 0;
      if (useRef) {
        const ref = refMap.get(tid) || 0;
        unit = (denRef > 0 && ref > 0) ? (price * ref / denRef) : 0;
      } else {
        unit = denQty > 0 ? price / denQty : 0;
      }
      if (unit <= 0) continue;

      outRows.push({
        date: meta.date,
        type_id: tid,
        qty: qty,
        unit_value_filled: unit,
        source: 'CONTRACT',
        contract_id: cid,
        char: meta.char
      });
    }
  }

  // Use ML.upsertBy() to write all rows to the ledger
  ML.setSheet('Material_Ledger');
  const keys = ['source', 'char', 'contract_id', 'type_id'];
  const count = ML.upsertBy(keys, outRows);

  log.log('rebuildContractUnitCosts', {
    appended_or_updated: count,
    processed: outRows.length
  });

  return count;
}

/** Build Char name → ID map (stub) */
function _charIdMap() {
  // In a real app, this would read from a dedicated sheet like 'CharIDMap'
  // Headers: [char, character_id]
  return {}; // Placeholder
}

// --- ADD (UTILITIES) ---------------------------------------------------------
function _hdrMap_(arr) { var m = {}; for (var i = 0; i < arr.length; i++) m[String(arr[i]).trim()] = i; return m; }

function _getSheet_(name) { // keep if not already defined
  var sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(name);
  if (!sh) throw new Error('Missing sheet: ' + name);
  return sh;
}

function _getData_(sheetName) {
  var sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(sheetName); // Use direct SpreadsheetApp.getActiveSpreadsheet()
  if (!sh) throw new Error('Missing sheet: ' + sheetName);
  var vals = sh.getDataRange().getValues();
  var header = vals[0] || [];
  var rows = vals.slice(1);
  return { sh: sh, header: header, rows: rows, h: _hdrMap_(header) };
}


function _ensureColumn_(sh, headerRow, name) {
  var hdr = sh.getRange(headerRow, 1, 1, sh.getLastColumn()).getValues()[0];
  var idx = -1;
  for (var i = 0; i < hdr.length; i++) { if (String(hdr[i]).trim() === String(name)) { idx = i; break; } }
  if (idx === -1) { idx = hdr.length; sh.getRange(headerRow, idx + 1).setValue(name); }
  return idx; // 0-based
}
// ---------------------------------------------------------------------------

// --- Raw_loot (rolling 30d total) → Material_Ledger (post deltas) ------------
const RAW_LOOT_SHEET = 'Raw_loot';
const SNAP_KEY = 'raw_loot:snapshot:v2'; // doc properties key

function importRawLootDeltasToLedger(asOfDate, sourceLabel, writeNegatives) {
  const log = LoggerEx.withTag('LOOT_IMPORT');
  const dateStr = asOfDate ? _isoDate(asOfDate) : _isoDate(Date.now());
  const source = sourceLabel || 'LOOT';
  const allowNeg = !!writeNegatives;
  const charName = (typeof getCorpAuthChar === 'function') ? getCorpAuthChar() : (GESI && GESI.name) || '';

  return withSheetLock(function () {
    // 1. Explicitly set the sheet to ensure correct target
    ML.setSheet('Material_Ledger');

    // Read Raw_loot (30-day rolling totals)
    const loot = _getData_(RAW_LOOT_SHEET);
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
      // Sanitize input values robustly before using them
      const qty = Number(String(r[cQty]).replace(/[^\d.\-]/g, '')) || 0;
      const buy = _toNumberISK_(r[cBuy]);
      const val = _toNumberISK_(r[cVal]);
      curr.set(tid, {
        qty,
        val,
        buy
      });
    }

    // Load previous snapshot
    const props = PropertiesService.getDocumentProperties();
    const prevRaw = props.getProperty(SNAP_KEY);
    const prev = prevRaw ? JSON.parse(prevRaw) : {}; // { tid: {qty,val} }

    // UNION of tids (handles items that disappeared from loot sheet, e.g., day-31)
    const allTids = new Set([
      ...curr.keys(),
      ...Object.keys(prev).map(x => Number(x) || 0).filter(Number)
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
      if (!allowNeg && dq < 0) continue; // ignore pruning drops unless allowed

      // Unit for this delta: prefer dv/dq; fallback to current buy (if dv/dq results in 0 or NaN)
      let unit = (isFinite(dv / dq) && Math.abs(dv) > 0) ? Math.abs(dv / dq) : (cur.buy || 0);
      if (!(unit > 0)) unit = cur.buy || 0;

      // Use the date and type_id as the unique identifier for loot entries
      const contractId = dateStr;

      outRows.push({
        date: dateStr,
        type_id: tid,
        qty: dq,
        unit_value_filled: unit,
        source: source,
        // Since we are applying the delta once per day, we can use the date as the contract_id
        contract_id: contractId,
        char: charName
      });
    }

    // Use ML.upsertBy() to update or append the rows
    // Key: source + char + type_id + contract_id (which is the date)
    const keys = ['source', 'char', 'type_id', 'contract_id'];
    const count = ML.upsertBy(keys, outRows);

    // Save new snapshot = current totals (present tids only)
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

function resetRawLootSnapshot() {
  PropertiesService.getDocumentProperties().deleteProperty(SNAP_KEY);
}


/** ===== JOURNAL → Material_Ledger (Import/Buy Side) =====================
 * Depends on: GESI, getCorpAuthChar(), getOrCreateSheet (Utility.js), PT (Project Time)
 */

function Ledger_Import_Journal_Default() {
  // Pass opts as an object, including the necessary division and sinceDays for the corp journal import logic.
  return Ledger_Import_CorpJournal({ division: 3, sinceDays: 30 });
}

function Ledger_Import_Journal(opts) {
  const log = LoggerEx.withTag('CHAR_TXN');
  opts = opts || {};
  var SHEET_NAME = opts.sheet || 'Material_Ledger';
  var SINCE_DAYS = Math.max(0, Number(opts.sinceDays || 30));
  var SOURCE = String(opts.sourceName || 'JOURNAL').toUpperCase();

  if (typeof ML === 'undefined' || typeof ML.upsertBy !== 'function') {
    throw new Error("ML API not found. Ensure MaterialLedger.gs.js is included and the ML object is global.");
  }

  // NOTE: Assuming GESI_GetMarketTransactionsForAllChars or GESI_GetWalletJournalForAllChars is available
  var GESI_GET_TXN_FUNC = (typeof GESI_GetMarketTransactionsForAllChars === 'function')
    ? GESI_GetMarketTransactionsForAllChars
    : (typeof GESI_GetWalletJournalForAllChars === 'function' ? GESI_GetWalletJournalForAllChars : null);

  if (!GESI_GET_TXN_FUNC) {
    throw new Error("Required GESI transaction helper (GetMarketTransactionsForAllChars or GetWalletJournalForAllChars) not found.");
  }

  // 1) Fetch/normalize entries OUTSIDE the lock (slow stuff)
  var nowMs = Date.now();
  var cutoff = new Date(nowMs - SINCE_DAYS * 86400000);

  var rawEntries = GESI_GET_TXN_FUNC({ since: cutoff }) || [];

  // Normalize to ML-compliant row objects
  var outRows = [];
  for (var i = 0; i < rawEntries.length; i++) {
    var e = rawEntries[i] || {};
    var dt = e.date || e.timestamp || e.time;
    var d = (typeof PT !== 'undefined' && PT.parseDateSafe) ? PT.parseDateSafe(dt) : new Date(dt);
    if (!(d instanceof Date) || isNaN(d.getTime()) || d.getTime() < cutoff.getTime()) continue; // Apply date filter here

    var isBuy = (e.is_buy === true) || /buy/i.test(String(e.ref_type || ''));
    if (!isBuy) continue;

    var typeId = Number(e.type_id || e.typeID || 0);
    var qty = Number(e.quantity || e.qty || 0);
    var price = Number(e.unit_price || e.price || e.amount_per_unit || 0);
    if (!(typeId > 0 && qty > 0 && price > 0)) continue;

    var contractId = String(e.transaction_id || e.id || e.journal_ref_id || e.context_id);
    var charName = e.char_name || e.char || e.character || '';

    outRows.push({
      date: d,
      type_id: typeId,
      item_name: e.type_name || '',
      qty: qty,
      unit_value: '',
      source: SOURCE,
      contract_id: contractId,
      char: charName,
      unit_value_filled: price,
    });
  }

  // 2) Lock + Sheet I/O + Write via ML API
  return withSheetLock(function () {
    ML.setSheet(SHEET_NAME);

    // Key: source + contract_id + char (transaction ID + character name is unique)
    const keys = ['source', 'contract_id', 'char'];
    const count = ML.upsertBy(keys, outRows);

    log.log('journal_import', { appended_or_updated: count, processed: outRows.length, sheet: ML.sheetName() });
    return count;
  });
}

/** ===== JOURNAL → Material_Ledger & Sales_Ledger (Buy and Sell Sides) =====================
 * Imports corporate market transactions (buy and sell) for Division 3 only.
 * Buy transactions go to Material_Ledger (default).
 * Sell transactions go to Sales_Ledger.
 */
function Ledger_Import_CorpJournal(opts) {
  const log = LoggerEx.withTag('CORP_TXN');
  // ADDED: Start timer to profile execution time
  const t = log.startTimer('Ledger_Import_CorpJournal_Setup');

  opts = opts || {};

  if (typeof ML === 'undefined' || typeof ML.upsertBy !== 'function') {
    throw new Error("ML API not found. Ensure MaterialLedger.gs.js is included and the ML object is global.");
  }

  const GESI_FUNC_NAME = 'corporations_corporation_wallets_division_transactions';

  if (typeof GESI === 'undefined' || typeof GESI.invoke !== 'function') { // Check for GESI.invoke
    throw new Error("GESI Library/invoke not found. Ensure GESI is added as a library to your script.");
  }

  const BUY_SOURCE = String(opts.sourceName || 'JOURNAL').toUpperCase();
  const SINCE_DAYS = Math.max(0, Number(opts.sinceDays || 30)); // Get sinceDays from opts

  // --- Date Cutoff Calculation ---
  const MS_PER_DAY = 86400000;
  const cutoff = new Date(Date.now() - SINCE_DAYS * MS_PER_DAY); // Calculate cutoff date

  // --- Division Selection: FIXED TO DIVISION 3 ---
  const TARGET_DIVISION = 3;
  const authToon = getCorpAuthChar(); // Fetched and cached
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
  // NOTE: If currentFromId exists, we use it to start the fetch for the next page of transactions (older data).
  // ESI uses the ID of the newest record you HAVE to get records older than it.

  t.stamp('Setup complete, starting API calls.'); // Profiling step 1

  const allCorpTransactions = [];
  let fetchMore = true;

  log.log(`Fetching Corp Transactions for Division ${TARGET_DIVISION} (since ${SINCE_DAYS} days)...`);

  do {
    try {

      // The single-object parameter method, using null if the ID is not available (first page).
      const rawEntries = GESI.invokeRaw(
        GESI_FUNC_NAME,
        {
          division: TARGET_DIVISION, // Bundled required path parameter
          from_id: currentFromId, // Bundled optional query parameter
          name: authToon, // Bundled optional auth name
          show_column_headings: false, // No Headers needed with invokeRaw
          version: null // let GESI handle it
        }
      );

      if (!rawEntries || rawEntries.length === 0) {
        fetchMore = false;
        break;
      }

      allCorpTransactions.push(...rawEntries);

      // Check the date of the oldest entry in the current page. If it's older than 
      // our cutoff, stop fetching.
      const oldestEntry = rawEntries[rawEntries.length - 1];
      const oldestDate = new Date(oldestEntry.date);

      // FIX: Add validation check for Invalid Date before comparing getTime()
      if (isNaN(oldestDate.getTime())) {
        log.error("Invalid Date found in ESI response. Stopping pagination.", { entry: oldestEntry });
        fetchMore = false;
        break;
      }

      if (oldestDate.getTime() < cutoff.getTime()) {
        log.log(`Oldest entry date (${oldestDate}) is past cutoff. Stopping pagination.`);
        fetchMore = false;
      } else {
        // Set the transaction_id of the oldest entry in the current page 
        // as the starting point for the next, older page.
        currentFromId = oldestEntry.transaction_id;
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

  const buyRows = [];
  const sellRows = [];

  for (const e of allCorpTransactions) {
    if (!e || typeof e !== 'object') continue;

    const d = new Date(e.date);
    // FINAL FILTER: Only process transactions newer than the cutoff.
    if (isNaN(d.getTime()) || d.getTime() < cutoff.getTime()) continue;

    const typeId = Number(e.type_id || 0);
    const qty = Number(e.quantity || 0);
    const price = Number(e.unit_price || 0);

    // Skip if essential data is missing or invalid
    if (!(typeId > 0 && qty > 0 && price > 0)) continue;

    const contractId = String(e.transaction_id);
    const isBuy = (e.is_buy === true);

    const row = {
      date: d,
      type_id: typeId,
      item_name: '',
      qty: qty,
      unit_value: '',
      source: isBuy ? BUY_SOURCE : LEDGER_CORP_SALE_SOURCE,
      contract_id: contractId,
      char: authToon, // use Corp's Authed Toon
      unit_value_filled: price,
    };

    if (isBuy) {
      // Buy: Positive quantity (item inflow)
      buyRows.push(row);
    } else {
      // Sell: Negative quantity (item outflow), needs sign reversal
      row.qty = -Math.abs(row.qty);
      sellRows.push(row);
    }
  }

  t.stamp('Data normalization complete, starting write.'); // Profiling step 2

  // --- BEGIN TRANSACTION ---
  const result = withSheetLock(function () {
    const keys = ['source', 'contract_id'];
    let buyCount = 0;
    let sellCount = 0;

    // 1. Process Buy Side (Inflow to Material_Ledger)
    if (buyRows.length > 0) {
      ML.setSheet(opts.sheet || LEDGER_BUY_SHEET);
      buyCount = ML.upsertBy(keys, buyRows);
      log.log(`Buy side processed for ${ML.sheetName()}`, { appended_or_updated: buyCount, processed: buyRows.length });
    }

    // 2. Process Sell Side (Outflow to Sales_Ledger)
    if (sellRows.length > 0) {
      ML.setSheet(LEDGER_SALE_SHEET);
      sellCount = ML.upsertBy(keys, sellRows);
      log.log(`Sell side processed for ${ML.sheetName()}`, { appended_or_updated: sellCount, processed: sellRows.length });
    }

    return {
      appended_or_updated_buy: buyCount,
      appended_or_updated_sell: sellCount,
      processed: allCorpTransactions.length,
      sheets: { buy: opts.sheet || LEDGER_BUY_SHEET, sell: LEDGER_SALE_SHEET },
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
 * Build a map: type_id → typeName from a Named Range.
 * Expects columns: [typeID, groupID, typeName, volume].
 * Skips header/blank lines automatically.
 */
function _typeNameMapFromSDE_(rangeName) {
  rangeName = rangeName || 'sde_typeid_name';
  const ss = SpreadsheetApp.getActive();
  const rng = ss.getRangeByName(rangeName);
  if (!rng) throw new Error('Named range not found: ' + rangeName);

  const vals = rng.getValues();        // 2D array
  const map = new Map();
  for (let i = 0; i < vals.length; i++) {
    const row = vals[i];
    // Column 0: typeID, Column 2: typeName
    const rawId = row[0];
    const name = String(row[2] || '').trim();
    // Parse typeID robustly (handles scientific notation)
    const tid = Number(String(rawId).replace(/[^\d+Ee\.-]/g, ''));
    if (Number.isFinite(tid) && name) map.set(Math.floor(tid), name);
  }
  return map;
}

