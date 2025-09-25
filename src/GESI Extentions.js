// ContractItems_Fetchers.gs.js
// Minimal, drop-in hardening for Character/Corp contract item fetchers used as
// custom functions in Google Sheets. Fixes "Loading…" loops by:
//  - de-duplicating requests per recalculation (in‑memory memo)
//  - partitioning cache keys by (name|contract_id|version|headerFlag)
//  - merging results with exactly ONE header row when requested
//
// Assumes the GESI library is available.
/* global GESI, CacheService, Logger */

// ---------- Per-execution memos (avoid duplicate calls during one recalculation) ----------
var _ccciMemo = Object.create(null); // characters
var _cociMemo = Object.create(null); // corp

// ---------- Safe helper: getObjType (fallback if not present in project) ----------
if (typeof getObjType !== "function") {
  /** @param {*} o */
  function getObjType(o) {
    var t = Object.prototype.toString.call(o);
    return t.slice(8, -1); // e.g., "Number", "String", "Object"
  }
}

// ==========================================================================================
// Character contract items
// ==========================================================================================

/**
 * RAW: Character contract items with proper cache partitioning.
 * @param {number} contract_id
 * @param {string} [name=GESI.name]
 * @param {boolean} [show_column_headings=true]
 * @param {string} [version="v1"]
 * @returns {any[][]}
 */
function raw_characters_character_contract_items(
  contract_id,
  name /* = GESI.name */,
  show_column_headings /* = true */,
  version /* = "v1" */
) {
  if (contract_id == null) throw new Error("contract_id is required");
  if (!name) name = GESI && GESI.name; // default to GESI.name
  if (!name) throw new Error("name is required");
  if (isNaN(contract_id)) throw new Error("contract_id is type " + getObjType(contract_id));
  if (show_column_headings == null) show_column_headings = true;
  if (version == null) version = "v1";

  var key = "gesiCharContractItmz:" + name + ":" + contract_id + ":" + version + ":" + (show_column_headings ? 1 : 0);

  if (_ccciMemo[key]) return _ccciMemo[key];

  var cache = CacheService.getUserCache();
  var jsonText = cache.get(key);
  if (jsonText === null) {
    var data = GESI.characters_character_contracts_contract_items(
      parseInt(contract_id, 10), name, show_column_headings, version
    );
    jsonText = JSON.stringify(data);
    cache.put(key, jsonText, 3600); // 1 hour TTL
  }

  var out = JSON.parse(jsonText);
  _ccciMemo[key] = out;
  return out;
}

/**
 * ARRAY: Merge multiple (contract_id, name) pairs with EXACTLY one header row
 *        when show_column_headings=true; none otherwise.
 * @param {number[]|number} contract_ids
 * @param {string[]|string} names
 * @param {boolean} [show_column_headings=true]
 * @param {string} [version="v1"]
 * @returns {any[][]}
 */
function array_characters_character_contract_items(
  contract_ids, names, show_column_headings, version
) {
  if (contract_ids == null) throw new Error("contract_id is required");
  if (!Array.isArray(contract_ids)) contract_ids = [contract_ids];
  if (!Array.isArray(names)) names = [names];
  if (show_column_headings == null) show_column_headings = true;
  if (version == null) version = "v1";

  // Build unique (name|id) pairs from the two arrays (zip by index)
  var maxLen = Math.max(contract_ids.length, names.length);
  var pairSet = Object.create(null); // use object as set for speed
  for (var i = 0; i < maxLen; i++) {
    var id = parseInt(contract_ids[i], 10);
    var nm = names[i] == null ? "" : String(names[i]).trim();
    if (!Number.isFinite(id) || !nm) continue; // skip invalid/blank
    pairSet[nm + "|" + id] = true;
  }

  var pairs = Object.keys(pairSet);
  if (pairs.length === 0) return [["No data"]];

  var out = [];
  var headerAdded = false;
  var wantHeader = !!show_column_headings;

  for (var p = 0; p < pairs.length; p++) {
    var pair = pairs[p];
    var cut = pair.lastIndexOf("|");
    var name = pair.slice(0, cut);
    var id = parseInt(pair.slice(cut + 1), 10);

    // Always fetch with header; we will keep it only once if requested
    var rows = raw_characters_character_contract_items(id, name, true, version);
    if (!Array.isArray(rows) || rows.length === 0) continue;

    var start = (wantHeader && !headerAdded) ? 0 : 1; // include header exactly once
    for (var r = start; r < rows.length; r++) out.push(rows[r]);

    if (wantHeader && !headerAdded) headerAdded = true;
  }

  return out.length ? out : [["No data"]];
}

// ==========================================================================================
// Corp contract items
// ==========================================================================================

/**
 * RAW: Corp contract items with proper cache partitioning.
 * @param {number} contract_id
 * @param {string} [name=GESI.name]
 * @param {boolean} [show_column_headings=true]
 * @param {string} [version="v1"]
 * @returns {any[][]}
 */
function raw_corporations_corporation_contracts_contract_items(
  contract_id,
  name /* = GESI.name */,
  show_column_headings /* = true */,
  version /* = "v1" */
) {
  if (contract_id == null) throw new Error("contract_id is required");
  if (!name) name = GESI && GESI.name; // default
  if (!name) throw new Error("name is required");
  if (isNaN(contract_id)) throw new Error("contract_id is type " + getObjType(contract_id));
  if (show_column_headings == null) show_column_headings = true;
  if (version == null) version = "v1";

  var key = "gesiCorpContractItmz:" + name + ":" + contract_id + ":" + version + ":" + (show_column_headings ? 1 : 0);

  if (_cociMemo[key]) return _cociMemo[key];

  var cache = CacheService.getUserCache();
  var jsonText = cache.get(key);
  if (jsonText === null) {
    var data = GESI.corporations_corporation_contracts_contract_items(
      parseInt(contract_id, 10), name, show_column_headings, version
    );
    jsonText = JSON.stringify(data);
    cache.put(key, jsonText, 3600);
  }

  var out = JSON.parse(jsonText);
  _cociMemo[key] = out;
  return out;
}

/**
 * ARRAY: Merge multiple corp contract_ids with EXACTLY one header when requested.
 * @param {number[]|number} contract_ids
 * @param {string} [name=GESI.name]
 * @param {boolean} [show_column_headings=true]
 * @param {string} [version="v1"]
 * @returns {any[][]}
 */
function array_corporations_corporation_contracts_contract_items(
  contract_ids, name, show_column_headings, version
) {
  if (contract_ids == null) throw new Error("contract_id is required");
  if (!Array.isArray(contract_ids)) contract_ids = [contract_ids];
  if (!name) name = GESI && GESI.name;
  if (!name) throw new Error("name is required");
  if (show_column_headings == null) show_column_headings = true;
  if (version == null) version = "v1";

  // sanitize + de-dup IDs
  var i, id;
  var ids = [];
  for (i = 0; i < contract_ids.length; i++) {
    id = parseInt(contract_ids[i], 10);
    if (Number.isFinite(id)) ids.push(id);
  }
  var seen = Object.create(null), uniqIds = [];
  for (i = 0; i < ids.length; i++) {
    id = ids[i];
    if (!seen[id]) { seen[id] = true; uniqIds.push(id); }
  }
  if (!uniqIds.length) return [["No data"]];

  var out = [];
  var headerAdded = false;
  var wantHeader = !!show_column_headings;

  for (i = 0; i < uniqIds.length; i++) {
    id = uniqIds[i];
    // Always fetch with header; keep it once if requested
    var rows = raw_corporations_corporation_contracts_contract_items(id, name, true, version);
    if (!Array.isArray(rows) || rows.length === 0) continue;

    var start = (wantHeader && !headerAdded) ? 0 : 1;
    for (var r = start; r < rows.length; r++) out.push(rows[r]);
    if (wantHeader && !headerAdded) headerAdded = true;
  }

  return out.length ? out : [["No data"]];
}

/***** CONFIG *****/
const CONTRACT_LOOKBACK_DAYS = 90;

/* Sheet names (feel free to tweak) */
const CONTRACTS_RAW_SHEET = "Contracts (RAW)";
const CONTRACT_ITEMS_RAW_SHEET = "Contract Items (RAW)";
const MATERIAL_LEDGER_SHEET = "Material_Ledger";

/* Caching TTLs (seconds) */
const TTL = Object.assign(typeof TTL === 'object' ? TTL : {}, {
  chars: 21600,  // 6h (per DOCUMENT)
  items: 900,   // 15m (per USER, per contract)
});

/* Optional strict inbound detection:
   When TRUE, we only ledger items if the contract's acceptor_id matches our char's ID.
   Provide a map sheet named "CharIDMap" with headers: char | character_id
   When FALSE, we treat included items on finished item_exchange as inbound. */
const STRICT_ACCEPTOR_CHECK = false;

/***** HELPERS *****/
const LOG_GESI = LoggerEx.tag('GESI');

function _sheet(name, header) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let sh = ss.getSheetByName(name);
  if (!sh) {
    sh = ss.insertSheet(name);
    if (header && header.length) {
      sh.getRange(1, 1, 1, header.length).setValues([header]);
    }
  }
  return sh;
}
function _rewrite(sh, header, rows) {
  sh.clearContents();
  if (header && header.length) sh.getRange(1, 1, 1, header.length).setValues([header]);
  if (rows && rows.length) sh.getRange(2, 1, rows.length, rows[0].length).setValues(rows);
}
function _setValues(sh, startRow, rows) {
  if (!rows.length) return;
  sh.getRange(startRow, 1, rows.length, rows[0].length).setValues(rows);
}
function _isoDate(d) {
  return Utilities.formatDate(new Date(d), "UTC", "yyyy-MM-dd");
}

/* Per-DOCUMENT cache for authenticated character names */
function getCharNamesFast() {
  const c = CacheService.getDocumentCache();
  const k = 'gesi:chars';
  const hit = c.get(k);
  if (hit) return JSON.parse(hit);
  const names = getAuthenticatedCharacterNames() || [];
  c.put(k, JSON.stringify(names), TTL.chars);
  return names;
}

/* Per-USER cache for per-contract items (personalized) */
function getContractItemsCached(charName, contractId, force = false) {
  const c = CacheService.getUserCache();
  const k = `gesi:items:${charName}:${contractId}`;
  if (!force) {
    const hit = c.get(k);
    if (hit) return JSON.parse(hit);
  }
  const items = invoke("characters_character_contract_id_items", [charName], { contract_id: contractId }) || [];
  c.put(k, JSON.stringify(items), TTL.items);
  return items;
}

/* Optional: read name→character_id map from a tiny sheet "CharIDMap" (char | character_id) */
function _charIdMap() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName("CharIDMap");
  if (!sh) return {};
  const vals = sh.getDataRange().getValues();
  vals.shift(); // header
  const map = {};
  vals.forEach(r => { if (r[0] && r[1]) map[String(r[0])] = String(r[1]); });
  return map;
}

/***** 1) SYNC CONTRACTS (RAW) *****/
function syncContracts() {
  const hdrC = ["char", "contract_id", "type", "status", "issuer_id", "acceptor_id", "date_issued", "date_expired", "price", "reward", "collateral", "volume", "title", "availability", "start_location_id", "end_location_id"];
  const hdrI = ["char", "contract_id", "type_id", "quantity", "is_included", "is_singleton"];

  const shC = _sheet(CONTRACTS_RAW_SHEET, hdrC);
  const shI = _sheet(CONTRACT_ITEMS_RAW_SHEET, hdrI);

  const names = getCharNamesFast();
  LOG_GESI.info('chars', names);

  const lookIso = _isoDate(Date.now() - CONTRACT_LOOKBACK_DAYS * 86400000);

  LOG_GESI.time('contracts:list');
  const lists = invokeMultiple("characters_character_contracts", names, { status: "all" }) || [];
  LOG_GESI.timeEnd('contracts:list');

  const outC = [];
  const outI = [];

  for (let i = 0; i < names.length; i++) {
    const ch = names[i];
    const list = lists[i] || [];
    LOG_GESI.debug('char', ch, 'contracts', list.length);

    for (const c of list) {
      if (c.type !== "item_exchange" || c.status !== "finished") continue;
      if (c.date_issued && c.date_issued.slice(0, 10) < lookIso) continue;

      outC.push([
        ch,
        c.contract_id, c.type, c.status, c.issuer_id, c.acceptor_id,
        c.date_issued || "", c.date_expired || "", c.price || 0, c.reward || 0, c.collateral || 0,
        c.volume || 0, c.title || "", c.availability || "", c.start_location_id || "", c.end_location_id || ""
      ]);

      // Items are per-contract; cache per user
      const items = getContractItemsCached(ch, c.contract_id);
      for (const it of (items || [])) {
        if (!it.is_included || !it.quantity) continue;   // only included stack(s)
        outI.push([ch, c.contract_id, it.type_id, it.quantity, !!it.is_included, !!it.is_singleton]);
      }
      Utilities.sleep(150); // be kind to ESI
    }
    LOG_GESI.info('char', ch, 'kept contracts so far', outC.length, 'item rows', outI.length);
  }

  _rewrite(shC, hdrC, outC);
  _rewrite(shI, hdrI, outI);
  LOG_GESI.info('syncContracts done', { contracts: outC.length, items: outI.length });
}

/***** 2) RAW → Material_Ledger (normalized inflow) *****/
function contractsToMaterialLedger() {
  const hdrML = ["date", "type_id", "item_name", "qty", "unit_value", "source", "contract_id", "char"];
  const shML = _sheet(MATERIAL_LEDGER_SHEET, hdrML);

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const shC = ss.getSheetByName(CONTRACTS_RAW_SHEET);
  const shI = ss.getSheetByName(CONTRACT_ITEMS_RAW_SHEET);
  if (!shC || !shI) throw new Error("Run syncContracts() first to populate RAW sheets.");

  const C = shC.getDataRange().getValues(); const hC = C.shift();
  const I = shI.getDataRange().getValues(); const hI = I.shift();

  const ixC = (name) => hC.indexOf(name);
  const ixI = (name) => hI.indexOf(name);

  const colC = {
    char: ixC("char"),
    contract_id: ixC("contract_id"),
    type: ixC("type"),
    status: ixC("status"),
    acceptor_id: ixC("acceptor_id"),
    date_issued: ixC("date_issued"),
  };
  const colI = {
    char: ixI("char"),
    contract_id: ixI("contract_id"),
    type_id: ixI("type_id"),
    quantity: ixI("quantity"),
    is_included: ixI("is_included"),
  };

  // Join items by contract_id (included only)
  const itemsByCid = {};
  for (const r of I) {
    if (!r[colI.is_included]) continue;
    const cid = r[colI.contract_id];
    if (!itemsByCid[cid]) itemsByCid[cid] = [];
    itemsByCid[cid].push({
      char: r[colI.char],
      type_id: r[colI.type_id],
      qty: Number(r[colI.quantity] || 0)
    });

  }

  const idMap = STRICT_ACCEPTOR_CHECK ? _charIdMap() : null;
  const out = [];

  for (const r of C) {
    const ctype = String(r[colC.type] || "").toLowerCase();
    const status = String(r[colC.status] || "").toLowerCase();
    if (ctype !== "item_exchange" || status !== "finished") continue;

    // Inbound detection
    let inbound = true;
    if (STRICT_ACCEPTOR_CHECK) {
      const ch = r[colC.char];
      const myId = idMap && idMap[ch];
      const acc = String(r[colC.acceptor_id] || "");
      inbound = myId && acc && (String(myId) === acc);
    }

    if (!inbound) continue;

    const cid = r[colC.contract_id];
    const issued = r[colC.date_issued] ? _isoDate(r[colC.date_issued]) : "";

    const items = itemsByCid[cid] || [];
    for (const it of items) {
      if (it.qty <= 0) continue;
      out.push([
        issued,            // date
        it.type_id,        // type_id
        "",                // item_name (fill later via Items map)
        it.qty,            // qty
        "",                // unit_value (leave blank; valuation modes FREE/WAVG/MEDIAN handle later)
        "CONTRACT",        // source
        cid,               // contract_id
        r[colC.char] || "" // char
      ]);
    }
  }

  // De-dup on (contract_id|type_id|char)
  const key = (row) => `${row[6]}|${row[1]}|${row[7]}`;
  const have = shML.getLastRow() > 1 ? shML.getRange(2, 1, shML.getLastRow() - 1, hdrML.length).getValues() : [];
  const seen = new Set(have.map(key));
  const fresh = out.filter(row => (seen.has(key(row)) ? false : (seen.add(key(row)), true)));

  if (fresh.length) {
    const start = Math.max(2, shML.getLastRow() + 1);
    _setValues(shML, start, fresh);
  }
  LOG_GESI.info('contracts→ledger', { appended: fresh.length, total_ledger_rows: shML.getLastRow() - 1 });
}


