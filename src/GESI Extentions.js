// ContractItems_Fetchers.gs.js
// Robust, GAS-safe contract sync for EVE (GESI):
//   • Two-phase listing: CHARACTER → CORPORATION (no mixing of scopes)
//   • Items fetched with HEADERS (boolean; default true in GESI)
//   • Single canonical endpoints; positional args only (no named objects)
//   • Per-doc cache for auth names; per-user cache for items (scope-partitioned)
//   • LoggerEx integration (marketTracker style)
//
// Assumes the GESI library + LoggerEx are available.
/* global GESI, CacheService, SpreadsheetApp, LockService, Utilities, Session, LoggerEx */




// TODO: Impliment Market Journal Costs Source: GESI
// TODO: Impliment Loot Transfers Source: importrange calculation
/* 
date  type_id Quantity  unit_price
45908.33971 178 100 5.79
45908.33971	179	100	2.21
45908.33971	180	200	4.94

*/
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
var CONTRACTS_RAW_SHEET      = "Contracts (RAW)";
var CONTRACT_ITEMS_RAW_SHEET = "Contract Items (RAW)";
var MATERIAL_LEDGER_SHEET    = "Material_Ledger";

// ENDPOINTS (canonical; let GESI handle versioning)
var EP_LIST_CHAR = "characters_character_contracts";
var EP_LIST_CORP = "corporations_corporation_contracts";

var EP_ITEMS_CHAR = "characters_character_contracts_contract_items";
var EP_ITEMS_CORP = "corporations_corporation_contracts_contract_items";

// Contract list (headerless) column order fallback, if needed
var GESI_CONTRACT_COLS = [
  "acceptor_id","assignee_id","availability","buyout","collateral","contract_id",
  "date_accepted","date_completed","date_expired","date_issued","days_to_complete",
  "end_location_id","for_corporation","issuer_corporation_id","issuer_id","price",
  "reward","start_location_id","status","title","type","volume","character_name"
];

// Cache TTLs (seconds)
var GESI_TTL = (typeof GESI_TTL === 'object' && GESI_TTL) || {};
GESI_TTL.chars     = (GESI_TTL.chars     != null) ? GESI_TTL.chars     : 21600; // 6h (document cache)
GESI_TTL.contracts = (GESI_TTL.contracts != null) ? GESI_TTL.contracts : 900;   // 15m (unused here)
GESI_TTL.items     = (GESI_TTL.items     != null) ? GESI_TTL.items     : 900;   // 15m (user cache)

// ==========================================================================================
// UTILITIES (GAS-SAFE)
// ==========================================================================================

if (typeof getObjType !== "function") {
  function getObjType(o) {
    var t = Object.prototype.toString.call(o);
    return t.slice(8, -1);
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

function _toBoolStrict(v) {
  if (typeof v === 'boolean') return v;
  if (typeof v === 'number') return v !== 0;
  if (typeof v === 'string') {
    var s = v.trim().toLowerCase();
    if (s === 'true' || s === 't' || s === '1' || s === 'yes') return true;
    if (s === 'false' || s === 'f' || s === '0' || s === 'no' || s === '') return false;
  }
  return false;
}

function _sheet(name, header) {
  var ss = SpreadsheetApp.getActive();
  var lock = LockService.getDocumentLock();
  for (var a = 0; a < 5; a++) {
    try {
      lock.waitLock(5000);
      var sh = ss.getSheetByName(name);
      if (!sh) {
        sh = ss.insertSheet(name);
        if (header && header.length) sh.getRange(1,1,1,header.length).setValues([header]);
      } else if (header && header.length) {
        sh.getRange(1,1,1,header.length).setValues([header]);
      }
      lock.releaseLock();
      return sh;
    } catch (e) {
      try { lock.releaseLock(); } catch(_) {}
      Utilities.sleep(250 * Math.pow(2, a));
      if (a === 4) throw e;
    }
  }
}

function _rewrite(sh, header, rows) {
  var lock = LockService.getDocumentLock();
  lock.waitLock(5000);
  try {
    if (header && header.length) sh.getRange(1,1,1,header.length).setValues([header]);
    var needed = (rows.length || 0) + 1;
    var have = sh.getMaxRows();
    if (have < needed) sh.insertRowsAfter(have, needed - have);
    if (rows.length) {
      sh.getRange(2, 1, rows.length, header.length).setValues(rows);
    }
    var lastNow = rows.length + 1;
    var lastHad = sh.getLastRow();
    var extra = Math.max(0, lastHad - lastNow);
    if (extra > 0) {
      sh.getRange(lastNow + 1, 1, extra, sh.getMaxColumns()).clearContent();
    }
  } finally {
    lock.releaseLock();
  }
}

function _sheetSafe(name, header){ return _sheet(name, header); }
function _rewriteFast(sh, header, rows){ return _rewrite(sh, header, rows); }

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
  } catch(_) {}
  if (v == null) {
    var util = ss.getSheetByName('Utility');
    if (util) { try { v = util.getRange(2, 2).getValue(); } catch(_) {} }
  }
  var n = parseInt(v, 10);
  if (!(Number(n) === n && isFinite(n))) n = CONTRACT_LOOKBACK_DAYS;
  if (n < 1) n = 1;
  if (n > 365) n = 365;
  return n;
}

/* Per-DOCUMENT cache for authenticated character names */
function getCharNamesFast() {
  var c = CacheService.getDocumentCache();
  var k = 'gesi:chars';
  var hit = c.get(k);
  if (hit) return JSON.parse(hit);

  var namesFn =
    (GESI && typeof GESI.getAuthenticatedCharacterNames === 'function')
      ? GESI.getAuthenticatedCharacterNames
      : (typeof getAuthenticatedCharacterNames === 'function'
          ? getAuthenticatedCharacterNames
          : null);
  if (!namesFn) throw new Error('getAuthenticatedCharacterNames not found (GESI or global).');

  var names = namesFn() || [];
  c.put(k, JSON.stringify(names), GESI_TTL.chars);
  return names;
}

// Resolve corp auth character (override → GESI.name → NamedRange/Utility → first authed)
function getCorpAuthChar() {
  var log = LoggerEx.withTag('GESI');
  var ss  = SpreadsheetApp.getActiveSpreadsheet();
  var names = getCharNamesFast();
  var desired = "";

  function _resolve(spec) {
    if (!spec) return "";
    spec = String(spec).trim();
    var got = null;
    // Named range?
    try {
      var nr = ss.getRangeByName(spec);
      if (nr) got = nr.getValue();
    } catch(_) {}
    // Sheet!A1?
    if (!got && spec.indexOf('!') > 0) {
      var cut = spec.indexOf('!');
      var shn = spec.slice(0, cut);
      var a1  = spec.slice(cut + 1);
      var sh  = ss.getSheetByName(shn);
      if (sh) { try { got = sh.getRange(a1).getValue(); } catch(_) {} }
    }
    if (!got) got = spec;
    return String(got).trim();
  }

  if (typeof CORP_AUTH_CHARACTER !== 'undefined' && CORP_AUTH_CHARACTER != null) {
    desired = _resolve(CORP_AUTH_CHARACTER);
  }

  if (!desired && GESI && GESI.name) desired = String(GESI.name).trim();

  if (!desired) {
    desired = _resolve('CORP_AUTH_CHAR');
    if (!desired) desired = _resolve('Utility!B3');
  }

  if (names.indexOf(desired) === -1) {
    var fallback = names[0] || "";
    if (desired) log.warn('Corp auth override not in authenticated names; falling back', { wanted: desired, using: fallback, list: names });
    desired = fallback;
  }

  log.debug('corp auth character', { using: desired });
  return desired;
}

// ==========================================================================================
// NORMALIZERS
// ==========================================================================================

// Normalize CONTRACT LIST results → [{ ch, c }]
function _normalizeCharContracts(res, names) {
  var tuples = [];
  if (!res || !res.length) return tuples;

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

  // per-char arrays (aligned to names)
  if (Array.isArray(res[0])) {
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
  var scope    = forCorp ? 'corp' : 'char';

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
  var hdrC = ["char","contract_id","type","status","issuer_id","acceptor_id","date_issued","date_expired","price","reward","collateral","volume","title","availability","start_location_id","end_location_id"];
  var hdrI = ["char","contract_id","type_id","quantity","is_included","is_singleton"];

  var names = getCharNamesFast();
  var corpAuth = getCorpAuthChar();
  log.log('chars', names);

  var MS_PER_DAY = 86400000;
  var lookbackDays = getLookbackDays();
  var lookIso = _isoDate(Date.now() - lookbackDays * MS_PER_DAY);

  var outC = [];
  var outI = [];

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

  var issued1 = c1.date_issued ? String(c1.date_issued).slice(0,10) : '';
  if (issued1 && issued1 < lookIso) continue;

  var cid1 = _toIntOrNull(c1.contract_id);
  if (cid1 == null) continue;

  if (!byCid[cid1]) byCid[cid1] = [];
  byCid[cid1].push(tuplesChar[t]); // keep all sightings for this cid
}

var idMap = (typeof _charIdMap === 'function') ? _charIdMap() : null;
var cids = Object.keys(byCid);
for (var g = 0; g < cids.length; g++) {
var cid   = cids[g];
 var group = byCid[cid];             // [{ ch, c }, ...] same contract_id
  var cRow  = group[0].c;             // representative row (dates, price, etc.)
  var ch1   = _pickCharForContract(group, cRow, idMap);

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
    if (!it1.is_included || !it1.quantity) continue;
    outI.push([ch1, cidNum, it1.type_id, it1.quantity, true, !!it1.is_singleton]);
  }

  seenChar[''+cidNum] = true; // so corp phase won’t re-add it
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
    var c2  = tuplesCorp[u].c;

    var type2 = String(c2.type || '').toLowerCase();
    var stat2 = String(c2.status || '').toLowerCase();
    if (type2 !== 'item_exchange' || stat2 !== 'finished') continue;

    var issued2 = c2.date_issued ? String(c2.date_issued).slice(0,10) : '';
    if (issued2 && issued2 < lookIso) continue;

    var cid2 = _toIntOrNull(c2.contract_id);
    if (cid2 == null) { log.warn('corp: invalid contract_id; skip', { char: ch2, raw: c2.contract_id }); continue; }

    // If this ID already appeared in CHAR phase, skip (keep scopes cleanly separated)
    if (seenChar[''+cid2]) continue;

    var items2Raw = getContractItemsCached(ch2, cid2, false, true) || [];
    var items2 = normalizeItemRows(items2Raw);

    outC.push([
      ch2, cid2, c2.type, c2.status, c2.issuer_id, c2.acceptor_id,
      c2.date_issued || "", c2.date_expired || "", c2.price || 0, c2.reward || 0, c2.collateral || 0,
      c2.volume || 0, c2.title || "", c2.availability || "", c2.start_location_id || "", c2.end_location_id || ""
    ]);

    for (var j2 = 0; j2 < items2.length; j2++) {
      var it2 = items2[j2];
      if (!it2.is_included || !it2.quantity) continue;
      outI.push([ch2, cid2, it2.type_id, it2.quantity, true, !!it2.is_singleton]);
    }

    Utilities.sleep(150);
  }

  // ---------------- WRITE SHEETS ----------------
  var shC = _sheetSafe(CONTRACTS_RAW_SHEET, hdrC);
  var shI = _sheetSafe(CONTRACT_ITEMS_RAW_SHEET, hdrI);
  _rewriteFast(shC, hdrC, outC);
  _rewriteFast(shI, hdrI, outI);

  log.log('syncContracts done', { contracts: outC.length, items: outI.length, lookback_days: lookbackDays, lookIso: lookIso });
}

// ==========================================================================================
// RAW → Material_Ledger (optional helper to normalize inflow)
// ==========================================================================================
function contractsToMaterialLedger() {
  var log = LoggerEx.withTag('GESI');

  var hdrML = ["date","type_id","item_name","qty","unit_value","source","contract_id","char"];
  var shML = _sheetSafe(MATERIAL_LEDGER_SHEET, hdrML);

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var shC = ss.getSheetByName(CONTRACTS_RAW_SHEET);
  var shI = ss.getSheetByName(CONTRACT_ITEMS_RAW_SHEET);
  if (!shC || !shI) throw new Error("Run syncContracts() first to populate RAW sheets.");

  var C = shC.getDataRange().getValues(); var hC = C.shift();
  var I = shI.getDataRange().getValues(); var hI = I.shift();

  function ix(arr, name){ return arr.indexOf(name); }

  var colC = {
    char: ix(hC,"char"),
    contract_id: ix(hC,"contract_id"),
    type: ix(hC,"type"),
    status: ix(hC,"status"),
    acceptor_id: ix(hC,"acceptor_id"),
    date_issued: ix(hC,"date_issued")
  };
  var colI = {
    char: ix(hI,"char"),
    contract_id: ix(hI,"contract_id"),
    type_id: ix(hI,"type_id"),
    quantity: ix(hI,"quantity"),
    is_included: ix(hI,"is_included")
  };

  var itemsByCid = {};
  for (var r = 0; r < I.length; r++) {
    var rowI = I[r];
    if (!rowI[colI.is_included]) continue;
    var cid = rowI[colI.contract_id];
    if (!itemsByCid[cid]) itemsByCid[cid] = [];
    itemsByCid[cid].push({
      char: rowI[colI.char],
      type_id: rowI[colI.type_id],
      qty: Number(rowI[colI.quantity] || 0)
    });
  }

  var out = [];
  for (var q = 0; q < C.length; q++) {
    var rowC = C[q];
    var ctype  = String(rowC[colC.type]||"").toLowerCase();
    var status = String(rowC[colC.status]||"").toLowerCase();
    if (ctype !== "item_exchange" || status !== "finished") continue;

    var cid2 = rowC[colC.contract_id];
    var issued = rowC[colC.date_issued] ? _isoDate(rowC[colC.date_issued]) : "";
    var items = itemsByCid[cid2] || [];
    for (var s = 0; s < items.length; s++) {
      var it = items[s];
      if (it.qty <= 0) continue;
      out.push([ issued, it.type_id, "", it.qty, "", "CONTRACT", cid2, rowC[colC.char] || "" ]);
    }
  }

// --- de-dup against existing rows ---
var have = shML.getLastRow() > 1 ? shML.getRange(2,1,shML.getLastRow()-1,hdrML.length).getValues() : [];
var seen = Object.create(null);
for (var i = 0; i < have.length; i++) {
  var k = have[i][6] + '|' + have[i][1] + '|' + have[i][7]; // contract_id|type_id|char
  seen[k] = true;
}
var fresh = [];
for (var j = 0; j < out.length; j++) {
  var key = out[j][6] + '|' + out[j][1] + '|' + out[j][7];
  if (seen[key]) continue;
  seen[key] = true;
  fresh.push(out[j]);
}

if (fresh.length) {
  var start = Math.max(2, shML.getLastRow()+1);
  _setValues(shML, start, fresh);
}
log.log('contracts→ledger', { appended: fresh.length, total_ledger_rows: shML.getLastRow()-1 });
}
