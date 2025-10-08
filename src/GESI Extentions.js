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


function _sheet(name, header) {
  var ss = SpreadsheetApp.getActive();
  var lock = LockService.getDocumentLock();
  for (var a = 0; a < 5; a++) {
    try {
      lock.waitLock(5000);
      var sh = ss.getSheetByName(name);
      if (!sh) {
        sh = ss.insertSheet(name);
        if (header && header.length) sh.getRange(1, 1, 1, header.length).setValues([header]);
      } else if (header && header.length) {
        sh.getRange(1, 1, 1, header.length).setValues([header]);
      }
      lock.releaseLock();
      return sh;
    } catch (e) {
      try { lock.releaseLock(); } catch (_) { }
      Utilities.sleep(250 * Math.pow(2, a));
      if (a === 4) throw e;
    }
  }
}

function _rewrite(sh, header, rows) {
  var lock = LockService.getDocumentLock();
  lock.waitLock(5000);
  try {
    if (header && header.length) sh.getRange(1, 1, 1, header.length).setValues([header]);
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

function _sheetSafe(name, header) { return _sheet(name, header); }
function _rewriteFast(sh, header, rows) { return _rewrite(sh, header, rows); }

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
  var ss = SpreadsheetApp.getActiveSpreadsheet();
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
    } catch (_) { }
    // Sheet!A1?
    if (!got && spec.indexOf('!') > 0) {
      var cut = spec.indexOf('!');
      var shn = spec.slice(0, cut);
      var a1 = spec.slice(cut + 1);
      var sh = ss.getSheetByName(shn);
      if (sh) { try { got = sh.getRange(a1).getValue(); } catch (_) { } }
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
      cRow.volume || 0, cRow.title || "", cRow.availability || "", cRow.start_location_id || "", cRow.end_location_id || ""
    ]);

    for (var j1 = 0; j1 < items1.length; j1++) {
      var it1 = items1[j1];
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

  var hdrML = ["date", "type_id", "item_name", "qty", "unit_value", "source", "contract_id", "char"];
  var shML = _sheetSafe(LEDGER_SHEET, hdrML);

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var shC = ss.getSheetByName(CONTRACTS_RAW_SHEET);
  var shI = ss.getSheetByName(CONTRACT_ITEMS_RAW_SHEET);
  if (!shC || !shI) throw new Error("Run syncContracts() first to populate RAW sheets.");

  var C = shC.getDataRange().getValues(); var hC = C.shift();
  var I = shI.getDataRange().getValues(); var hI = I.shift();

  function ix(arr, name) { return arr.indexOf(name); }

  var colC = {
    char: ix(hC, "char"),
    contract_id: ix(hC, "contract_id"),
    type: ix(hC, "type"),
    status: ix(hC, "status"),
    acceptor_id: ix(hC, "acceptor_id"),
    date_issued: ix(hC, "date_issued")
  };
  var colI = {
    char: ix(hI, "char"),
    contract_id: ix(hI, "contract_id"),
    type_id: ix(hI, "type_id"),
    quantity: ix(hI, "quantity"),
    is_included: ix(hI, "is_included")
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
    var ctype = String(rowC[colC.type] || "").toLowerCase();
    var status = String(rowC[colC.status] || "").toLowerCase();
    if (ctype !== "item_exchange" || status !== "finished") continue;

    var cid2 = rowC[colC.contract_id];
    var issued = rowC[colC.date_issued] ? _isoDate(rowC[colC.date_issued]) : "";
    var items = itemsByCid[cid2] || [];
    for (var s = 0; s < items.length; s++) {
      var it = items[s];
      if (it.qty <= 0) continue;
      out.push([issued, it.type_id, "", it.qty, "", "CONTRACT", cid2, rowC[colC.char] || ""]);
    }
  }

  // --- de-dup against existing rows ---
  var have = shML.getLastRow() > 1 ? shML.getRange(2, 1, shML.getLastRow() - 1, hdrML.length).getValues() : [];
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
    var start = Math.max(2, shML.getLastRow() + 1);
    _setValues(shML, start, fresh);
  }
  log.log('contracts→ledger', { appended: fresh.length, total_ledger_rows: shML.getLastRow() - 1 });
}


/******************************************************
 * Contract → unit costs table (for Material_Ledger fill)
 * - Lists finished item_exchange in the lookback window
 * - Pulls items per contract (character, then corp)
 * - Allocates NET price across included items
 * - Returns rows: [contract_id, type_id, qty, unit_cost, price, total_qty, issuer_id, acceptor_id, date_issued]
 *
 * Dependencies:
 *   - GESI lib (contracts + contract_items)
 *   - raw_characters_character_contract_items / raw_corporations_corporation_contracts_contract_items (already in this file)
 * Optional:
 *   - Sheet "CharIDMap"  headers: [char | character_id]  (for STRICT_ACCEPTOR_CHECK)
 ******************************************************/

/* ===== Config ===== */
var LOOKBACK_DAYS = (typeof LOOKBACK_DAYS !== 'undefined') ? LOOKBACK_DAYS : 90;   // rolling window
var CONTRACT_ALLOC_MODE = (typeof CONTRACT_ALLOC_MODE !== 'undefined') ? CONTRACT_ALLOC_MODE : "qty"; // "qty" | "ref"
var STRICT_ACCEPTOR_CHECK = (typeof STRICT_ACCEPTOR_CHECK !== 'undefined') ? STRICT_ACCEPTOR_CHECK : false;

/* ===== Helpers ===== */
function _daysAgo(days) {
  var d = new Date();
  d.setUTCDate(d.getUTCDate() - Math.max(0, +days || 0));
  return d;
}

function _getCharIdFromMap_(name) {
  try {
    var ss = SpreadsheetApp.getActive();
    var sh = ss.getSheetByName("CharIDMap");
    if (!sh) return null;
    var vals = sh.getDataRange().getValues();
    var hdr = vals.shift();
    var cChar = hdr.indexOf("char"), cId = hdr.indexOf("character_id");
    if (cChar < 0 || cId < 0) return null;
    for (var i = 0; i < vals.length; i++) {
      if (String(vals[i][cChar]).trim() === String(name).trim()) return +vals[i][cId] || null;
    }
  } catch (_) { }
  return null;
}

/**
 * Try both scopes and return merged finished item_exchange contracts in lookback.
 * Row shape (subset of ESI): {contract_id, date_issued, date_accepted, type, status, price, issuer_id, acceptor_id}
 */
function _listFinishedItemExchange_(name, lookbackDays) {
  var since = _daysAgo(lookbackDays || LOOKBACK_DAYS);

  var out = [];
  var log = LoggerEx.withTag('GESI');
  // CHARACTER scope
  try {
    var rowsC = GESI.characters_character_contracts(name, true, "v1");
    // rowsC includes header; find columns
    if (Array.isArray(rowsC) && rowsC.length > 1) {
      var h = rowsC[0].map(String);
      var idx = {};
      ["contract_id", "date_issued", "date_accepted", "type", "status", "price", "issuer_id", "acceptor_id"].forEach(function (k) { idx[k] = h.indexOf(k); });
      for (var i = 1; i < rowsC.length; i++) {
        var r = rowsC[i];
        if (!r || !r.length) continue;
        if (String(r[idx.type]) !== "item_exchange") continue;
        if (String(r[idx.status]) !== "finished") continue;
        var di = r[idx.date_issued]; if (di && new Date(di).getTime() < since.getTime()) continue;
        out.push({
          scope: "char",
          contract_id: +r[idx.contract_id],
          date_issued: r[idx.date_issued],
          date_accepted: r[idx.date_accepted],
          type: r[idx.type],
          status: r[idx.status],
          price: +r[idx.price] || 0,
          issuer_id: +r[idx.issuer_id] || null,
          acceptor_id: +r[idx.acceptor_id] || null,
          name: name
        });
      }
    }
  } catch (e) {
    log.warn("[contracts:list] char scope failed %s", e);
  }

  // CORPORATION scope (optional; uses configured name or GESI.name)
  try {
    var rowsCo = GESI.corporations_corporation_contracts(name, true, "v1");
    if (Array.isArray(rowsCo) && rowsCo.length > 1) {
      var h2 = rowsCo[0].map(String);
      var idy = {};
      ["contract_id", "date_issued", "date_accepted", "type", "status", "price", "issuer_id", "acceptor_id"].forEach(function (k) { idy[k] = h2.indexOf(k); });
      for (var j = 1; j < rowsCo.length; j++) {
        var rc = rowsCo[j];
        if (!rc || !rc.length) continue;
        if (String(rc[idy.type]) !== "item_exchange") continue;
        if (String(rc[idy.status]) !== "finished") continue;
        var di2 = rc[idy.date_issued]; if (di2 && new Date(di2).getTime() < since.getTime()) continue;
        out.push({
          scope: "corp",
          contract_id: +rc[idy.contract_id],
          date_issued: rc[idy.date_issued],
          date_accepted: rc[idy.date_accepted],
          type: rc[idy.type],
          status: rc[idy.status],
          price: +rc[idy.price] || 0,
          issuer_id: +rc[idy.issuer_id] || null,
          acceptor_id: +rc[idy.acceptor_id] || null,
          name: name
        });
      }
    }
  } catch (e2) {
    log.warn("[contracts:list] corp scope failed %s", e2);
  }

  return out;
}

/** Allocation mode hook: change how we apportion price across item rows. */
function _allocateUnitCosts_(items, netPriceISK, mode) {
  // items: [{type_id, qty, is_included}]
  var inc = items.filter(function (it) { return it.is_included !== false; }); // treat missing as included
  var totalQty = inc.reduce(function (s, it) { return s + (it.qty || 0); }, 0);

  if (mode === "ref") {
    // Optional: apportion by reference price weight (e.g., hub buy). Implement your own lookup.
    // For now, fallback to quantity if ref is missing.
    var wSum = 0;
    inc.forEach(function (it) { it._ref = _refPriceForTypeId_(it.type_id) || 0; wSum += (it._ref * (it.qty || 0)); });
    if (wSum > 0) {
      return inc.map(function (it) {
        var share = (it._ref * (it.qty || 0)) / wSum;
        var cost = (share * netPriceISK) / Math.max(1, (it.qty || 0));
        return { type_id: it.type_id, qty: it.qty || 0, unit_cost: cost, total_qty: totalQty };
      });
    }
  }

  // Default: simple per-quantity allocation
  if (totalQty <= 0) return inc.map(function (it) { return { type_id: it.type_id, qty: it.qty || 0, unit_cost: null, total_qty: 0 }; });
  var unitPool = netPriceISK / totalQty;
  return inc.map(function (it) {
    return { type_id: it.type_id, qty: it.qty || 0, unit_cost: unitPool, total_qty: totalQty };
  });
}

// TODO: Replace this with your real reference-price function if using CONTRACT_ALLOC_MODE="ref"
function _refPriceForTypeId_(type_id) { return 0; }

/**
 * Public: Build a sheet-ready table of per-item unit costs for recent finished contracts.
 * @param {string} [name=GESI.name] - authed character (used for both char + corp scopes)
 * @param {number} [lookbackDays=LOOKBACK_DAYS]
 * @param {boolean} [withHeader=true]
 * @returns {any[][]} rows: [contract_id, type_id, qty, unit_cost, price, total_qty, issuer_id, acceptor_id, date_issued]
 */
function CONTRACT_unit_costs_table(name, lookbackDays, withHeader) {
  if (!name) name = GESI && GESI.name;
  if (!name) throw new Error("name is required (GESI.name is blank)");
  if (withHeader == null) withHeader = true;

  var meCharId = _getCharIdFromMap_(name); // optional
  var contracts = _listFinishedItemExchange_(name, lookbackDays);

  // Strict acceptor filter (optional)
  if (STRICT_ACCEPTOR_CHECK && meCharId) {
    contracts = contracts.filter(function (c) { return +c.acceptor_id === +meCharId; });
  }

  var table = [];
  if (withHeader) {
    table.push(["contract_id", "type_id", "qty", "unit_cost", "price", "total_qty", "issuer_id", "acceptor_id", "date_issued"]);
  }

  for (var i = 0; i < contracts.length; i++) {
    var c = contracts[i];
    // fetch items using your cached RAW helpers
    var itemsRows;
    if (c.scope === "char") {
      itemsRows = raw_characters_character_contract_items(c.contract_id, c.name, true);
    } else {
      itemsRows = raw_corporations_corporation_contracts_contract_items(c.contract_id, c.name, true);
    }
    if (!Array.isArray(itemsRows) || itemsRows.length < 2) continue;

    // header map
    var hdr = itemsRows[0].map(String);
    var ix = {
      type_id: hdr.indexOf("type_id"),
      quantity: hdr.indexOf("quantity"),
      is_included: hdr.indexOf("is_included")
    };
    var items = [];
    for (var r = 1; r < itemsRows.length; r++) {
      var row = itemsRows[r];
      items.push({
        type_id: +row[ix.type_id],
        qty: +row[ix.quantity] || 0,
        is_included: (ix.is_included >= 0) ? !!row[ix.is_included] : true
      });
    }

    var alloc = _allocateUnitCosts_(items, +c.price || 0, CONTRACT_ALLOC_MODE);
    for (var k = 0; k < alloc.length; k++) {
      var a = alloc[k];
      table.push([c.contract_id, a.type_id, a.qty, a.unit_cost, c.price, a.total_qty, c.issuer_id, c.acceptor_id, c.date_issued]);
    }
  }

  return table.length ? table : (withHeader ? [["contract_id", "type_id", "qty", "unit_cost", "price", "total_qty", "issuer_id", "acceptor_id", "date_issued"], ["No data", "", "", "", "", "", "", "", ""]] : [["No data"]]);
}


/***********************
 * Contract costing → unit_cost_alloc + Ledger fill
 * - Ignores zero-price contracts (treat as transfers)
 * - REF mode: split by qty × ref_price (from 'market price Tracker')
 *   ↪ Fallback to QTY if any included row in the contract is missing a ref
 * - QTY mode: flat per-unit (price / Σ qty_included)
 * - Batch reads/writes only (no per-cell loops)
 ***********************/

const LEDGER_SHEET = 'Material_Ledger';
const MARKET_PRICE_SHEET = 'market price Tracker';

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
  const allocMode = String(_getNamedOr_('setting_contract_alloc_mode', 'REF')).toUpperCase(); // 'REF' | 'QTY'
  const refMap = _buildRefPriceMap_();
  const priceMap = _buildContractPriceMap_();

  // Read Contract Items
  const ci = _getData_(CONTRACT_ITEMS_RAW_SHEET);
  const iCID = ci.h['contract_id'], iTID = ci.h['type_id'], iQ = ci.h['quantity'], iInc = ci.h['is_included'];
  if ([iCID, iTID, iQ, iInc].some(v => v == null)) throw new Error(`'${CONTRACT_ITEMS_RAW_SHEET}' missing headers: contract_id,type_id,quantity,is_included`);
  const unitIdx = _ensureColumn_(ci.sh, 1, 'unit_cost_alloc');
  const unitCol = new Array(ci.rows.length).fill('');

  // Group by contract (priced only), included rows
  const byCid = new Map();
  ci.rows.forEach((r, ri) => {
    const cid = String(r[iCID]); const price = priceMap.get(cid) || 0;
    if (price <= 0) return; // ignored
    const included = String(r[iInc]).toUpperCase() === 'TRUE'; if (!included) return;
    const tid = Number(r[iTID]) || 0; const qty = Number(r[iQ]) || 0; if (qty <= 0) return;
    (byCid.get(cid) || byCid.set(cid, []).get(cid)).push({ ri, tid, qty });
  });

  // Denominators per contract
  const denoms = new Map();
  for (const [cid, rows] of byCid.entries()) {
    let denQty = 0, denRef = 0, missingRef = 0;
    for (const { tid, qty } of rows) {
      denQty += qty;
      const ref = refMap.get(tid) || 0;
      if (ref > 0) denRef += qty * ref; else missingRef++;
    }
    const useRef = (allocMode === 'REF' && missingRef === 0 && denRef > 0);
    denoms.set(cid, { denQty, denRef, useRef });
  }

  // Compute units
  for (const [cid, rows] of byCid.entries()) {
    const price = priceMap.get(cid) || 0; if (price <= 0) continue;
    const { denQty, denRef, useRef } = denoms.get(cid);
    if (!denQty) continue;
    for (const { ri, tid } of rows) {
      let unit = 0;
      if (useRef) {
        const ref = refMap.get(tid) || 0;
        unit = (denRef > 0 && ref > 0) ? (price * ref / denRef) : 0;
      } else {
        unit = price / denQty;
      }
      unitCol[ri] = unit;
    }
  }

  // Write back unit_cost_alloc
  ci.sh.getRange(2, unitIdx + 1, unitCol.length, 1).setValues(unitCol.map(v => [v]));

  // Fill ledger
  fillLedgerUnitValuesFromItems_();
}

/** Fill Material_Ledger.unit_value_filled from CI.unit_cost_alloc for source="CONTRACT" */
function fillLedgerUnitValuesFromItems_() {
  const ci = _getData_(CONTRACT_ITEMS_RAW_SHEET);
  const iCID = ci.h['contract_id'], iTID = ci.h['type_id'], iUnit = ci.h['unit_cost_alloc'];
  if ([iCID, iTID, iUnit].some(v => v == null)) throw new Error(`'${CONTRACT_ITEMS_RAW_SHEET}' missing headers: contract_id,type_id,unit_cost_alloc`);

  // index contractId|typeId → unit
  const key = (cid, tid) => `${cid}§${tid}`;
  const unitMap = new Map();
  for (const r of ci.rows) {
    const u = Number(r[iUnit]); if (!isFinite(u) || u <= 0) continue;
    unitMap.set(key(String(r[iCID]), Number(r[iTID]) || 0), u);
  }

  const lg = _getData_(LEDGER_SHEET);
  const s = lg.h['source'], c = lg.h['contract_id'], t = lg.h['type_id'], u0 = lg.h['unit_value'];
  if ([s, c, t].some(v => v == null)) throw new Error(`'${LEDGER_SHEET}' missing headers: source, contract_id, type_id`);
  const outIdx = _ensureColumn_(lg.sh, 1, 'unit_value_filled');

  const out = new Array(lg.rows.length);
  for (let i = 0; i < lg.rows.length; i++) {
    const row = lg.rows[i]; const src = String(row[s]).toUpperCase().trim();
    if (src !== 'CONTRACT') { out[i] = [u0 != null ? row[u0] : '']; continue; }
    const unit = unitMap.get(key(String(row[c]), Number(row[t]) || 0)) || '';
    out[i] = [unit];
  }
  if (out.length) lg.sh.getRange(2, outIdx + 1, out.length, 1).setValues(out);
}


// --- ADD (UTILITIES) ---------------------------------------------------------
function _hdrMap_(arr) { var m = {}; for (var i = 0; i < arr.length; i++) m[String(arr[i]).trim()] = i; return m; }

function _getSheet_(name) { // keep if not already defined
  var sh = SpreadsheetApp.getActiveSpreadsheet().getSheetByName(name);
  if (!sh) throw new Error('Missing sheet: ' + name);
  return sh;
}

function _getData_(sheetName) {
  var sh = _getSheet_(sheetName);
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
  const dateStr = asOfDate ? _isoDate(asOfDate) : _isoDate(Date.now());
  const source = sourceLabel || 'LOOT';
  const allowNeg = !!writeNegatives;
  const charName = (typeof getCorpAuthChar === 'function') ? getCorpAuthChar() : (GESI && GESI.name) || '';

  const lock = LockService.getDocumentLock(); lock.waitLock(5000);
  try {
    // Ensure ledger sheet + required columns exist
    const hdrML = ["date", "type_id", "item_name", "qty", "unit_value", "source", "contract_id", "char"];
    let shML = SpreadsheetApp.getActive().getSheetByName(LEDGER_SHEET);
    if (!shML) shML = _sheetSafe(LEDGER_SHEET, hdrML);

    let lgAll = _getData_(LEDGER_SHEET);
    const needCols = ['date', 'type_id', 'item_name', 'qty', 'unit_value', 'source', 'contract_id', 'char'];
    needCols.forEach(col => { if (lgAll.h[col] == null) _ensureColumn_(lgAll.sh, 1, col); });
    // re-read to refresh header map/indexes if we just added columns
    const lg = _getData_(LEDGER_SHEET);
    const H = lg.h, rows = lg.rows, SH = lg.sh;

    const iDate = H['date'], iTid = H['type_id'], iQty = H['qty'], iUnit = H['unit_value'],
      iSrc = H['source'];

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
      const tid = Number(r[cTid]) || 0; if (!tid) continue;
      const qty = Number(String(r[cQty]).replace(/[^\d.\-]/g, '')) || 0;
      const buy = _toNumberISK_(r[cBuy]);
      const val = _toNumberISK_(r[cVal]); // usually ≈ qty*buy
      curr.set(tid, { qty, val, buy });
    }

    // Load previous snapshot
    const props = PropertiesService.getDocumentProperties();
    const prevRaw = props.getProperty(SNAP_KEY);
    const prev = prevRaw ? JSON.parse(prevRaw) : {}; // { tid: {qty,val} }

    // Index existing same-day rows (for upserts)
    const key = (d, tid, src) => `${d}|${tid}|${src}`;
    const existing = new Map(); // key → {ri, qty, unit}
    for (let r = 0; r < rows.length; r++) {
      const d = rows[r][iDate];
      const src = String(rows[r][iSrc] || '');
      if (_isoDate(d) !== dateStr || src !== source) continue;
      const tid = Number(rows[r][iTid]) || 0; if (!tid) continue;
      existing.set(key(dateStr, tid, source), {
        ri: r,
        qty: Number(rows[r][iQty]) || 0,
        unit: _toNumberISK_(rows[r][iUnit])
      });
    }

    // UNION of tids (handles day-31 disappearances)
    const allTids = new Set([
      ...curr.keys(),
      ...Object.keys(prev).map(x => Number(x) || 0)
    ]);

    const updates = [];   // {ri, newQty, newUnit}
    const appends = [];   // rows to append: [date,type_id,item_name,qty,unit_value,source,contract_id,char]

    for (const tid of allTids) {
      const cur = curr.get(tid) || { qty: 0, val: 0, buy: 0 };
      const p = prev[String(tid)] || { qty: 0, val: 0 };

      const dq = cur.qty - (Number(p.qty) || 0);
      const dv = cur.val - (Number(p.val) || 0);

      if (dq === 0) continue;
      if (!allowNeg && dq < 0) continue; // ignore pruning drops unless allowed

      // Unit for this delta: prefer dv/dq; fallback to current buy
      let unit = (isFinite(dv / dq) && Math.abs(dv) > 0) ? Math.abs(dv / dq) : (cur.buy || 0);
      if (!(unit > 0)) unit = cur.buy || 0;

      const k = key(dateStr, tid, source);
      const hit = existing.get(k);

      if (hit) {
        // upsert: combine with prior same-day row; keep weighted unit
        const oldQty = Number(hit.qty) || 0;
        const oldVal = (Number(hit.qty) || 0) * (Number(hit.unit) || 0);
        const newQty = oldQty + dq;
        const newVal = oldVal + (dq * unit);
        const newUnit = (newQty !== 0) ? (newVal / newQty) : 0;
        updates.push({ ri: hit.ri, newQty, newUnit });
      } else {
        appends.push([dateStr, tid, "", dq, unit, source, "", charName]);
      }
    }

    // Apply updates (one call per row)
    for (const u of updates) {
      const row1 = u.ri + 2; // body index -> sheet row
      SH.getRange(row1, iQty + 1, 1, 2).setValues([[u.newQty, u.newUnit]]);
    }

    // Append new rows
    if (appends.length) {
      const start = Math.max(2, SH.getLastRow() + 1);
      _setValues(SH, start, appends);
    }

    // Save new snapshot = current totals (present tids only)
    const nextSnap = {};
    for (const [tid, cur] of curr.entries()) {
      nextSnap[String(tid)] = { qty: cur.qty, val: cur.val };
    }
    props.setProperty(SNAP_KEY, JSON.stringify(nextSnap));
  } finally {
    lock.releaseLock();
  }
}

function resetRawLootSnapshot() {
  PropertiesService.getDocumentProperties().deleteProperty(SNAP_KEY);
}


/** ===== JOURNAL → Material_Ledger (no duplicate helpers) =====================
 * Depends on: GESI, getCorpAuthChar(), getOrCreateSheet (Utility.js), PT (Project Time)
 * Source table shape written:
 * [date, type_id, item_name, qty, unit_value, source, contract_id, char, unit_value_filled]
 */

function Ledger_Import_Journal_Default() {
  return Ledger_Import_Journal({ division: 3, sinceDays: 30, maxPages: 8 });
}

function Ledger_Import_Journal(opts) {
  opts = opts || {};
  var division = Number(opts.division || 3);
  var sinceDays = Number(opts.sinceDays || 14);
  var maxPages = Math.max(1, Number(opts.maxPages || 5));
  var includeSells = !!opts.includeSells;       // false = buys only
  var sourceLabel = String(opts.sourceLabel || 'JOURNAL');
  var cellsPerChunk = Math.max(4000, Number(opts.cellsPerChunk || 7000)); // batch writes

  // Auth character (your helper)
  var charName = '';
  try { charName = String(getCorpAuthChar() || ''); } catch (_e) { charName = ''; }

  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var SHEET = 'Material_Ledger';
  var HEAD = ['date', 'type_id', 'item_name', 'qty', 'unit_value', 'source', 'contract_id', 'char', 'unit_value_filled'];
  var shOut = getOrCreateSheet(ss, SHEET, HEAD);   // from Utility.js

  // Build once per run
  var TYPE_NAME = _typeNameMapFromSDE_('sde_typeid_name');

  // Build existing keys (source="JOURNAL") → Set(contract_id)
  var hdr = shOut.getRange(1, 1, 1, HEAD.length).getValues()[0];
  var iSrc = hdr.indexOf('source');
  var iKey = hdr.indexOf('contract_id');
  var last = shOut.getLastRow();
  var existing = new Set();
  if (last >= 2 && iSrc > -1 && iKey > -1) {
    var srcCol = shOut.getRange(2, iSrc + 1, last - 1, 1).getValues();
    var keyCol = shOut.getRange(2, iKey + 1, last - 1, 1).getValues();
    for (var r = 0; r < srcCol.length; r++) {
      if (String(srcCol[r][0] || '').toUpperCase() !== 'JOURNAL') continue;
      var k = String(keyCol[r][0] || '').trim();
      if (k) existing.add(k);
    }
  }

  // GESI client
  var client = GESI.getClient().setFunction('corporations_corporation_wallets_division_transactions');
  if (typeof client.setCharacter === 'function' && charName) {
    client.setCharacter(charName);
  }

  var cutoffMs = Date.now() - sinceDays * 86400000;
  var toAppend = [];
  var fromId = null, pages = 0;

  while (pages < maxPages) {
    var args = { division: division };
    if ((!client.setCharacter) && charName) args.character = charName; // fallback
    if (fromId != null) args.from_id = fromId;

    // Fetch one page (newest first)
    var rows = client.executeRaw(args);
    if (!Array.isArray(rows) || rows.length === 0) break;
    pages++;

    var minTxnId = null;

    for (var i = 0; i < rows.length; i++) {
      var r = rows[i];

      // date cutoff
      var dRaw = r.date || '';
      var dObj = PT.parseDateSafe(dRaw);   // from project Time
      if (isNaN(dObj)) continue;
      if (dObj.getTime() < cutoffMs) { minTxnId = null; break; }

      var isBuy = !!r.is_buy;
      if (!isBuy && !includeSells) continue;

      var qty = Number(r.quantity || 0) || 0;
      var price = Number(r.unit_price || 0) || 0;
      var tid = Number(r.type_id || 0) || 0;
      if (!isFinite(qty) || !isFinite(price) || !isFinite(tid) || qty === 0 || price === 0) continue;

      // journal_ref_id preferred; fallback to transaction_id
      var refJ = Number(r.journal_ref_id || 0) || 0;
      var refT = Number(r.transaction_id || 0) || 0;
      var ref = refJ ? String(refJ) : (refT ? String(refT) : '');
      if (!ref || existing.has(ref)) continue;

      // Normalize to project-local midnight (Apps Script project tz)
      var day = PT.projectDate(dObj.getFullYear(), dObj.getMonth(), dObj.getDate(), 0, 0, 0);

      const friendly = TYPE_NAME.get(tid) || '';
      toAppend.push([
        day,                 // date
        tid,                 // type_id
        friendly,            // item_name ← filled from sde_typeid_name
        isBuy ? qty : -qty,  // qty
        price,               // unit_value
        sourceLabel,         // source ("JOURNAL")
        ref,                 // contract_id (journal_ref/txn id)
        charName,            // char
        price                // unit_value_filled
      ]);

      existing.add(ref);
      if (!minTxnId || (r.transaction_id && r.transaction_id < minTxnId)) {
        minTxnId = r.transaction_id;
      }
    }

    if (!minTxnId) break;
    fromId = Number(minTxnId) - 1; // older page
    if (rows.length < 1000) break; // GESI paginates at 1000
    Utilities.sleep(120); // be gentle
  }

  if (!toAppend.length) {
    return { appended: 0, pages: pages, note: 'No new rows' };
  }

  // Batched append
  var COLS = HEAD.length;
  var rowsPerBatch = Math.max(50, Math.floor(cellsPerChunk / COLS));
  var start = shOut.getLastRow() + 1;

  for (var off = 0; off < toAppend.length; off += rowsPerBatch) {
    var seg = toAppend.slice(off, off + rowsPerBatch);
    shOut.getRange(start + off, 1, seg.length, COLS).setValues(seg);
  }

  // Cheap formats only (won’t bloat)
  try {
    var n = toAppend.length;
    shOut.getRange(start, 1, n, 1).setNumberFormat('yyyy-mm-dd');
    shOut.getRange(start, 2, n, 1).setNumberFormat('0');       // type_id
    shOut.getRange(start, 4, n, 1).setNumberFormat('#,##0');   // qty
    shOut.getRange(start, 5, n, 1).setNumberFormat('#,##0.00');// unit_value
    shOut.getRange(start, 9, n, 1).setNumberFormat('#,##0.00');// unit_value_filled
  } catch (_fmtErr) { }

  return { appended: toAppend.length, pages: pages };
}

function ML_fillEffectiveCost() {
  var sh = SpreadsheetApp.getActive().getSheetByName('Material_Ledger');
  var last = sh.getLastRow(); if (last < 2) return;
  var vals = sh.getRange(2, 1, last-1, 9).getValues(); // A..I
  var out = new Array(vals.length);
  var state = new Map(); // type_id -> {Q, C}

  for (var i = 0; i < vals.length; i++) {
    var tid = +vals[i][1];                 // B type_id
    var qty = +vals[i][3];                 // D qty
    var price = +(vals[i][4] || vals[i][8]); // E unit_value or I unit_value_filled
    if (!Number.isFinite(tid) || !Number.isFinite(qty) || !Number.isFinite(price)) { out[i]=['']; continue; }

    var s = state.get(tid) || { Q:0, C:price };
    if (qty > 0) { s.C = (s.Q*s.C + qty*price) / (s.Q + qty); s.Q += qty; }
    else if (qty < 0) { s.Q += qty; } // sells don’t change avg cost
    state.set(tid, s);
    out[i] = [s.C];
  }

  // Ensure header, then write K
  sh.getRange(1, 11).setValue('effective_cost');
  sh.getRange(2, 11, out.length, 1).setValues(out);
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

