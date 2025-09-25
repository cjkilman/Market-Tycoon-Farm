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

// ContractPricer.gs.js — evaluates a contract against hub reference prices
var ContractPricer = (function(){
const CONTRACT_ITEMS_SHEET = 'contracts_items'; // raw ESI: contract_id | type_id | quantity | is_included
const TRACKER_SHEET = 'Minerals Tracker';
const OUT_SHEET = 'Contract Pricing';


function price(contractId, hubRef, mode){
const ss=SpreadsheetApp.getActive();
const ci = ss.getSheetByName(CONTRACT_ITEMS_SHEET);
if(!ci) throw new Error('Missing '+CONTRACT_ITEMS_SHEET);
const cv = ci.getDataRange().getValues(); const ch = cv.shift().map(String);
const cID=ch.indexOf('contract_id'), cT=ch.indexOf('type_id'), cQ=ch.indexOf('quantity'), cInc=ch.indexOf('is_included');


const items = cv.filter(r=>String(r[cID])==String(contractId) && (r[cInc]===true || r[cInc]==='TRUE'))
.map(r=>({type_id:r[cT], qty:Number(r[cQ])||0}));


const tr=ss.getSheetByName(TRACKER_SHEET); if(!tr) throw new Error('Run MineralTracker.build() first');
const tv=tr.getDataRange().getValues(); const th=tv.shift().map(String);
const tType=th.indexOf('type_id'), tHub=th.indexOf('Hub');
const tSellMin=th.indexOf('sell_min (now)'), tMedSell=th.indexOf('median_sell_24h');


const rows=[]; let total=0;
for (const it of items){
// pick ref
const refRow = tv.find(r=>r[tType]==it.type_id && r[tHub]==hubRef);
if(!refRow){ rows.push(['', it.type_id, '', it.qty, null, null, 'no ref']); continue; }
let refPrice = null;
if (mode==='median_sell') refPrice = refRow[tMedSell];
else /* sell_min */ refPrice = refRow[tSellMin];


const line = refPrice!=null ? refPrice * it.qty : null;
if(line!=null) total += line;
rows.push(['', it.type_id, '', it.qty, refPrice, line, '']);
}


// write out
const out = ss.getSheetByName(OUT_SHEET) || ss.insertSheet(OUT_SHEET);
out.getRange('B1').setValue(contractId);
out.getRange('B2').setValue(hubRef);
out.getRange('B3').setValue(mode||'sell_min');
out.getRange('B4').setValue(0.08);


const hdr=['line','type_id','Item Name','Qty','Ref price','Line value','Notes'];
const data=[hdr].concat(rows);
out.getRange(6,1,data.length,data[0].length).setValues(data);


// Totals & markdown
const last = 6 + rows.length;
out.getRange(last+1,4,1,2).setValues([["Subtotal:", total]]);
out.getRange(last+2,4,1,2).setValues([["Fair offer (1 - markdown_pct):", total * (1 - Number(out.getRange('B4').getValue()||0))]]);
}


return { price: price };
})();
