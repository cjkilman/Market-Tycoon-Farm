/**
 * FuzzApiPrice.js — coalesced fetch + script-scope cache + negative caching
 * - ScriptCache + ScriptLock (aligned)
 * - One aggregate per (location_type, location_id, type_id) containing both buy & sell
 * - Claim + recheck so only one concurrent caller fetches a given id
 * - Bytes check before caching (CacheService limit ≈ 100KB/key)
 * - Negative caching: store JSON 'null' for not-found ids with longer TTL
 * - Helpers + wrappers:
 *    fuzzPriceDataByHub(type_ids, market_hub="Jita", order_type="sell", order_level=null)
 *    fuzzApiPriceDataJitaSell(type_ids, market_hub=60003760, order_type=null, order_level=null)
 *    marketStatData(type_ids, location_type, location_id, order_type, order_level)
 *    marketStatDataCache(type_ids, location_type, location_id, order_type, order_level)
 *    marketStatDataBoth(type_ids, location_type, location_id, order_level)
 *    marketStatDataBothCache(type_ids, location_type, location_id, order_level)
 */

const FUZ_CACHE_VER = 'v5';
const FUZ_NEG_TTL = 6 * 60 * 60;  // 6 hours for negative (not-found)
const CLAIM_TTL_S = 45;          // was 20s — give slow calls room
const WAIT_FOR_CLAIM_MS = 6000;  // non-claimers will wait up to ~6s
const WAIT_STEP_MS = 500;        // poll every 500ms          // 20s claim lifetime (short)
const FETCH_BATCH = 700;          // ids per POST

function ttlForScope(lt) {
  lt = String(lt || '').toLowerCase();
  return lt === 'region' ? 30 * 60 : lt === 'system' ? 20 * 60 : 10 * 60; // seconds
}

function cacheScope() {
  return CacheService.getScriptCache();
}

// add near the top
function toNumOrNull(x) {
  if (x === null || x === undefined) return null;
  if (typeof x === 'number') return Number.isFinite(x) ? x : null;
  if (typeof x === 'string') {
    const s = x.trim();
    if (s === '') return null;                 // ← prevent Number('') -> 0
    const n = Number(s);
    return Number.isFinite(n) ? n : null;
  }
  return null;
}

function withDocLock(fn, ms = 1200) {         // keep small (< 1.5s)
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(ms)) return fn();         // skip claiming; proceed
  try { return fn(); } finally { lock.releaseLock(); }
}

function _fuzKey(location_type, location_id, type_id) {
  return ['fuz', FUZ_CACHE_VER, location_type, location_id, type_id].join(':');
}
function _claimKey(location_type, location_id, type_id) {
  return _fuzKey(location_type, location_id, type_id) + ':claim';
}

function _L_warn(tag, obj) { try { console.warn(tag, obj); } catch (_) { } }
function _L_info(tag, obj) { try { console.log(tag, obj); } catch (_) { } }

function _parseCached_(s) {
  if (s == null) return undefined;              // true miss
  s = String(s).trim();
  if (s === '' || s === 'undefined') return undefined; // our “cleared” writes or junk
  if (s === 'null') return null;                // NEG sentinel
  // Fast filter: only JSON objects/arrays should reach JSON.parse
  const c = s.charCodeAt(0);                    // '{' = 123, '[' = 91
  if (c !== 123 && c !== 91) return undefined;
  try {
    const obj = JSON.parse(s);
    return (obj && typeof obj === 'object') ? obj : undefined;
  } catch (_) {
    return undefined;                           // treat malformed as miss
  }
}

/** Run fn with simple backoff on common transient errors. */
function withRetries(fn, triesOrOpts, baseMs) {
  // defaults
  var tries = 3, base = 300;
  // retry on: 429/420/5xx + common network terms + our data-shape tripwires
  var retryPattern = /(?:\b(429|420|5\d\d)\b|dns|socket|ssl|handsh|timeout|temporar|rate|quota|Service invoked|empty-200|bad[-\s]?json)/i;

  if (typeof triesOrOpts === 'number') {
    tries = triesOrOpts;
    if (baseMs != null) base = baseMs;
  } else if (triesOrOpts && typeof triesOrOpts === 'object') {
    // old callsites: {max, base, retryPattern?}
    if (Number(triesOrOpts.max))  tries = Number(triesOrOpts.max);
    if (Number(triesOrOpts.base)) base  = Number(triesOrOpts.base);
    if (triesOrOpts.retryPattern instanceof RegExp) retryPattern = triesOrOpts.retryPattern;
  }

  var lastErr;
  for (var i = 0; i < tries; i++) {
    try {
      var res = fn();
      // If caller passed muteHttpExceptions:true, non-200 won't throw.
      // Force a retry for 429/420/5xx by throwing here.
      if (res && typeof res.getResponseCode === 'function') {
        var code = res.getResponseCode();
        if (code === 429 || code === 420 || (code >= 500 && code < 600)) {
          throw new Error('HTTP ' + code);
        }
      }
      return res;
    } catch (e) {
      lastErr = e;
      var s = String((e && e.message) || e);
      if (!retryPattern.test(s) || i === tries - 1) throw e;
      // exp backoff + jitter
      Utilities.sleep(base * Math.pow(2, i) + Math.floor(Math.random() * 200));
    }
  }
  throw lastErr;
}


const _FUZZ_FIELDS = [
  'min', 'max', 'avg', 'median', 'volume',
  'weightedAverage', 'orderCount', 'fivePercent'
];

function sanitizeAgg_(row) {
  if (!row || typeof row !== 'object') return null;

  const normalizeSide = (side) => {
    if (!side || typeof side !== 'object') return null;
    const out = Object.create(null);
    for (const k of _FUZZ_FIELDS) {
      out[k] = toNumOrNull(side[k]);
    }
    if (out.avg == null && out.weightedAverage != null) out.avg = out.weightedAverage;
    for (const k in out) { if (out[k] !== null) return out; }
    return null;
  };

  const buy = normalizeSide(row.buy);
  const sell = normalizeSide(row.sell);
  if (!buy && !sell) return null;
  return { buy: buy || {}, sell: sell || {} };
}

/**
 * Batch-fetch aggregates from Fuzzwork using JSON POST.
 * Returns: { id -> rawRow | null }  (null on error/missing)
 */
/**
 * Batch-fetch aggregates from Fuzzwork with robust fallbacks.
 * Prefers JSON POST, then falls back to form POST, then GET.
 * Returns: { id -> rawRow | null }  (null on error/missing)
 */
/**
 * Batch-fetch aggregates from Fuzzwork with robust fallbacks.
 * Prefers JSON POST, then form POST, then GET.
 * Returns: { id -> (rawRow | null | undefined) }
 *   - object: good row from server
 *   - null:   server positively says "no data / not found" for that id
 *   - undefined: fetch failed for the whole batch → DO NOT CACHE
 */
function fetchFuzzAggsInBatches_(ids, location_type, location_id) {
  if (!Array.isArray(ids)) ids = [ids];
  ids = ids.map(Number).filter(Number.isFinite);
  const out = Object.create(null);
  if (!ids.length) return out;

  const lt = String(location_type || 'station').toLowerCase();
  if (!/^(region|system|station)$/.test(lt)) throw new Error('bad location_type: ' + location_type);

  const url = 'https://market.fuzzwork.co.uk/aggregates/';
  const BATCH = Math.max(1, Math.min(FETCH_BATCH || 700, 1000));
  const SLEEP = 100;

  // helper: parse JSON safely, return null on empty-200, throw on non-JSON
  function _parseOrNull_(txt) {
    if (!txt || !String(txt).trim()) return null; // empty-200
    try {
      return JSON.parse(txt);
    } catch (_) {
      throw new Error('bad-json');
    }
  }

  for (let i = 0; i < ids.length; i += BATCH) {
    const slice = ids.slice(i, i + BATCH);
    const typesCsv = slice.join(',');
    let obj = null;
    let succeeded = false;

    // 1) JSON POST
    try {
      const payloadJson = JSON.stringify({ [lt]: location_id, types: typesCsv });
      const resp = withRetries(() => UrlFetchApp.fetch(url, {
        method: 'post',
        contentType: 'application/json',
        payload: payloadJson,
        muteHttpExceptions: true,
        validateHttpsCertificates: true
      }), { max: 4, base: 300 });

      if (resp && resp.getResponseCode && resp.getResponseCode() === 200) {
        const parsed = _parseOrNull_(resp.getContentText());
        if (parsed === null) throw new Error('empty-200');
        obj = parsed;
        succeeded = true;
      } else {
        _L_warn('fuz.non200.json', { code: resp && resp.getResponseCode && resp.getResponseCode() });
      }
    } catch (e) {
      _L_warn('fuz.fetch.json.fail', { msg: String(e && e.message || e), lt, location_id, count: slice.length });
    }

    // 2) form POST fallback
    if (!succeeded) {
      try {
        const payload = lt + '=' + encodeURIComponent(location_id) + '&types=' + encodeURIComponent(typesCsv);
        const resp = withRetries(() => UrlFetchApp.fetch(url, {
          method: 'post',
          contentType: 'application/x-www-form-urlencoded',
          payload: payload,
          muteHttpExceptions: true,
          validateHttpsCertificates: true
        }), { max: 4, base: 300 });

        if (resp && resp.getResponseCode && resp.getResponseCode() === 200) {
          const parsed = _parseOrNull_(resp.getContentText());
          if (parsed === null) throw new Error('empty-200');
          obj = parsed;
          succeeded = true;
        } else {
          _L_warn('fuz.non200.form', { code: resp && resp.getResponseCode && resp.getResponseCode() });
        }
      } catch (e) {
        _L_warn('fuz.fetch.form.fail', { msg: String(e && e.message || e), lt, location_id, count: slice.length });
      }
    }

    // 3) GET fallback
    if (!succeeded) {
      try {
        const qs = '?' + lt + '=' + encodeURIComponent(location_id) + '&types=' + encodeURIComponent(typesCsv);
        const resp = withRetries(() => UrlFetchApp.fetch(url + qs, {
          method: 'get',
          muteHttpExceptions: true,
          validateHttpsCertificates: true
        }), { max: 4, base: 300 });

        if (resp && resp.getResponseCode && resp.getResponseCode() === 200) {
          const parsed = _parseOrNull_(resp.getContentText());
          if (parsed === null) throw new Error('empty-200');
          obj = parsed;
          succeeded = true;
        } else {
          _L_warn('fuz.non200.get', { code: resp && resp.getResponseCode && resp.getResponseCode() });
        }
      } catch (e) {
        _L_warn('fuz.fetch.get.fail', { msg: String(e && e.message || e), lt, location_id, count: slice.length });
      }
    }

    if (succeeded && obj && typeof obj === 'object') {
      // Success → fill each id either with row or null (not-found)
      for (const id of slice) {
        out[id] = Object.prototype.hasOwnProperty.call(obj, String(id)) ? obj[id] : null;
      }
    } else {
      // Failure → mark as undefined: caller will SKIP caching for these
      for (const id of slice) out[id] = undefined;
    }

    if (SLEEP) Utilities.sleep(SLEEP);
  }

  return out;
}




function postFetch(type_ids, location_id, location_type = "station") {
  const ids = [];
  if (Array.isArray(type_ids)) {
    for (let i = 0; i < type_ids.length; i++) {
      const n = Number(type_ids[i]);
      if (Number.isFinite(n)) ids.push(n);
    }
  } else {
    const n1 = Number(type_ids);
    if (Number.isFinite(n1)) ids.push(n1);
  }
  if (!ids.length) return {};

  const seen = Object.create(null), uniq = [];
  for (let i = 0; i < ids.length; i++) { const v = ids[i]; if (!seen[v]) { seen[v] = 1; uniq.push(v); } }

  const cache = cacheScope();
  const ttlSec = ttlForScope(location_type);

  const dataKeys = uniq.map(id => _fuzKey(location_type, location_id, id));
  const got = cache.getAll(dataKeys) || {};
  const result = Object.create(null);
  const misses = [];

  for (let i = 0; i < uniq.length; i++) {
    const id = uniq[i];
    const parsed = _parseCached_(got[dataKeys[i]]);
    if (parsed === undefined) {
      misses.push(id);
    } else {
      result[id] = parsed;
    }
  }
  if (!misses.length) return result;

  let claimables = [];
  withDocLock(() => {
    const nowData = cache.getAll(misses.map(id => _fuzKey(location_type, location_id, id))) || {};
    const nowClaim = cache.getAll(misses.map(id => _claimKey(location_type, location_id, id))) || {};
    for (let i = 0; i < misses.length; i++) {
      const id = misses[i];
      const dk = _fuzKey(location_type, location_id, id);
      const ck = _claimKey(location_type, location_id, id);
      if (!nowData[dk] && !nowClaim[ck]) claimables.push(id);
    }
    if (claimables.length) {
      const toPut = {};
      const stamp = String(Date.now());
      for (const id of claimables) toPut[_claimKey(location_type, location_id, id)] = stamp;
      cache.putAll(toPut, CLAIM_TTL_S);
    }
  });

  const fetchList = claimables;
  let fetched = Object.create(null);

  // If we didn't claim, give the claimer a short window to finish.
  if (!fetchList.length) {
    const tries = Math.floor(WAIT_FOR_CLAIM_MS / WAIT_STEP_MS);
    for (let t = 0; t < tries; t++) {
      Utilities.sleep(WAIT_STEP_MS);
      const recheck = cache.getAll(misses.map(id => _fuzKey(location_type, location_id, id))) || {};
      let ready = true;
      for (let i = 0; i < misses.length; i++) {
        const dk = _fuzKey(location_type, location_id, misses[i]);
        if (recheck[dk] == null) { ready = false; break; }
      }
      if (ready) break;  // claimer populated; proceed to read below
    }
  } else {
    // We are the claimer → do the network
    fetched = fetchFuzzAggsInBatches_(fetchList, location_type, location_id);
  }


  const toPutPos = {};
  const toPutNeg = {};
  const re = cache.getAll(misses.map(id => _fuzKey(location_type, location_id, id))) || {};

  for (let i = 0; i < misses.length; i++) {
    const id = misses[i];
    const dk = _fuzKey(location_type, location_id, id);

    let row = _parseCached_(re[dk]);
    if (row === undefined) {
      row = sanitizeAgg_(fetched[id]);
    }

    if (row !== undefined) {
      if (row) {
        const s = JSON.stringify(row);
        if (Utilities.newBlob(s).getBytes().length < 95000) {
          toPutPos[dk] = s;
        }
        result[id] = row;
      } else {
        toPutNeg[dk] = 'null';
        result[id] = null;
      }
    }
  }

  if (Object.keys(toPutPos).length || Object.keys(toPutNeg).length) {
    withDocLock(() => {
      if (Object.keys(toPutPos).length) {
        const entries = Object.entries(toPutPos), CHUNK = 80;
        for (let i = 0; i < entries.length; i += CHUNK) {
          cache.putAll(Object.fromEntries(entries.slice(i, i + CHUNK)), ttlSec);
        }
      }
      if (Object.keys(toPutNeg).length) {
        const entries = Object.entries(toPutNeg), CHUNK = 200;
        for (let i = 0; i < entries.length; i += CHUNK) {
          cache.putAll(Object.fromEntries(entries.slice(i, i + CHUNK)), FUZ_NEG_TTL);
        }
      }
    });
  }

  return result;
}

/////////////////////
// Sheet Helpers   //
/////////////////////

function _flattenTypeIds_(rangeOrArray) {
  if (Array.isArray(rangeOrArray)) {
    const out = [];
    for (let i = 0; i < rangeOrArray.length; i++) {
      const v = Array.isArray(rangeOrArray[i]) ? rangeOrArray[i][0] : rangeOrArray[i];
      const n = Number(v);
      if (Number.isFinite(n)) out.push(n);
    }
    return out;
  }
  const n1 = Number(rangeOrArray);
  return Number.isFinite(n1) ? [n1] : [];
}

function _canonSide_(s, fallback) {
  s = (s == null ? fallback : s) || 'sell';
  s = String(s).toLowerCase();
  if (s === 'both') return 'both';
  return (s === 'buy' || s === 'sell') ? s : 'sell';
}

function _canonField_(f, priceDefault) {
  if (f == null) f = priceDefault || 'avg';
  f = String(f).toLowerCase().trim();
  if (f === 'weighted' || f === 'wa' || f === 'weightedaverage') return 'weightedAverage';
  if (f === 'price' || f === 'mean' || f === 'average' || f === 'avg') return 'avg';
  if (f === '5p' || f === 'five' || f === 'fivepercent' || f === 'five_percent') return 'fivePercent';
  if (f === 'orders' || f === 'ordercount' || f === 'numorders') return 'orderCount';
  if (f === 'min' || f === 'low') return 'min';
  if (f === 'max' || f === 'high') return 'max';
  if (f === 'median') return 'median';
  if (f === 'vol' || f === 'volume') return 'volume';
  return f;
}

function _hubToStationId_(hub) {
  if (hub == null || hub === '') return 60003760; // Jita 4-4 CNAP
  const n = Number(hub);
  if (Number.isFinite(n)) return n;
  const s = String(hub).toLowerCase();
  if (s.indexOf('jita') > -1) return 60003760;
  if (s.indexOf('amarr') > -1) return 60008494;
  if (s.indexOf('dodixie') > -1) return 60011866;
  if (s.indexOf('rens') > -1) return 60004588;
  if (s.indexOf('hek') > -1) return 60005686;
  return 60003760;
}

function _readAggsFromCache_(ids, location_type, location_id) {
  const cache = CacheService.getScriptCache();
  const keys = ids.map(id => ['fuz', FUZ_CACHE_VER, location_type, location_id, id].join(':'));
  const got = cache.getAll(keys) || {};
  const out = Object.create(null);
  for (let i = 0; i < ids.length; i++) {
    const s = got[keys[i]];
    if (s == null) { /* miss */ }
    else if (s === 'null') out[ids[i]] = null;
    else { try { out[ids[i]] = JSON.parse(s); } catch (_) { /* leave undefined */ } }
  }
  return out;
}

function _extractMetric_(aggRow, side, field) {
  if (!aggRow || typeof aggRow !== 'object') return "";
  const node = aggRow[side];
  if (!node || typeof node !== 'object') return "";
  const raw = node[field];

  // Treat missing volume/orderCount as 0
  if ((field === 'volume' || field === 'orderCount') &&
    (raw === null || raw === undefined || raw === "")) return 0;

  if (raw === null || raw === undefined || raw === "") return "";
  const n = Number(raw);
  return Number.isFinite(n) ? n : "";
}


function _alignOutput_(type_ids, idList, valueById) {
  const H = Array.isArray(type_ids) ? type_ids.length : 1;
  const out = new Array(H);
  for (let r = 0; r < H; r++) {
    const v = Array.isArray(type_ids) ? (Array.isArray(type_ids[r]) ? type_ids[r][0] : type_ids[r]) : type_ids;
    const id = Number(v);
    out[r] = [(Number.isFinite(id) && Object.prototype.hasOwnProperty.call(valueById, id)) ? valueById[id] : ""];
  }
  return out;
}

function _alignOutput2_(type_ids, idList, sellById, buyById) {
  const H = Array.isArray(type_ids) ? type_ids.length : 1;
  const out = new Array(H);
  for (let r = 0; r < H; r++) {
    const v = Array.isArray(type_ids) ? (Array.isArray(type_ids[r]) ? type_ids[r][0] : type_ids[r]) : type_ids;
    const id = Number(v);
    const s = (Number.isFinite(id) && Object.prototype.hasOwnProperty.call(sellById, id)) ? sellById[id] : "";
    const b = (Number.isFinite(id) && Object.prototype.hasOwnProperty.call(buyById, id)) ? buyById[id] : "";
    out[r] = [s, b];
  }
  return out;
}

/////////////////////////////
// Public wrappers         //
/////////////////////////////

function fuzzPriceDataByHub(type_ids, market_hub, order_type, order_level) {
  try {
    const ids = _flattenTypeIds_(type_ids);
    const stationId = _hubToStationId_(market_hub);
    const side = _canonSide_(order_type, 'sell');
    const field = _canonField_(order_level, 'avg');
    if (!ids.length) return _alignOutput_(type_ids, [], {});
    const aggs = postFetch(ids, stationId, "station");
    const values = {};
    for (let i = 0; i < ids.length; i++) {
      const id = ids[i];
      values[id] = _extractMetric_(aggs[id], side, field);
    }
    return _alignOutput_(type_ids, ids, values);
  } catch (e) {
    return _alignOutput_(type_ids, [], {});
  }
}

function fuzzApiPriceDataJitaSell(type_ids, market_hub, order_type, order_level) {
  try {
    const ids = _flattenTypeIds_(type_ids);
    const stationId = _hubToStationId_(market_hub == null ? 60003760 : market_hub);
    const side = _canonSide_(order_type, 'sell');
    const field = _canonField_(order_level, 'avg');
    if (!ids.length) return _alignOutput_(type_ids, [], {});
    const aggs = postFetch(ids, stationId, "station");
    const values = {};
    for (let i = 0; i < ids.length; i++) {
      const id = ids[i];
      values[id] = _extractMetric_(aggs[id], side, field);
    }
    return _alignOutput_(type_ids, ids, values);
  } catch (e) {
    return _alignOutput_(type_ids, [], {});
  }
}

function marketStatData(type_ids, location_type, location_id, order_type, order_level) {
  try {
    const ids = _flattenTypeIds_(type_ids);
    const lt = String(location_type || 'station').toLowerCase();
    const lid = Number(location_id);
    const side = _canonSide_(order_type, 'sell');
    const field = _canonField_(order_level, 'avg');
    if (!ids.length || !Number.isFinite(lid)) return _alignOutput_(type_ids, [], {});
    const aggs = postFetch(ids, lid, lt);
    const values = {};
    for (let i = 0; i < ids.length; i++) {
      const id = ids[i];
      values[id] = _extractMetric_(aggs[id], side, field);
    }
    return _alignOutput_(type_ids, ids, values);
  } catch (e) {
    return _alignOutput_(type_ids, [], {});
  }
}

function marketStatDataCache(type_ids, location_type, location_id, order_type, order_level) {
  try {
    const ids = _flattenTypeIds_(type_ids);
    const lt = String(location_type || 'station').toLowerCase();
    const lid = Number(location_id);
    const side = _canonSide_(order_type, 'sell');
    const field = _canonField_(order_level, 'avg');
    if (!ids.length || !Number.isFinite(lid)) return _alignOutput_(type_ids, [], {});
    const cacheMap = _readAggsFromCache_(ids, lt, lid);
    const values = {};
    for (let i = 0; i < ids.length; i++) {
      const id = ids[i];
      values[id] = _extractMetric_(cacheMap[id], side, field);
    }
    return _alignOutput_(type_ids, ids, values);
  } catch (e) {
    return _alignOutput_(type_ids, [], {});
  }
}

function marketStatDataBoth(type_ids, location_type, location_id, order_level) {
  try {
    const ids = _flattenTypeIds_(type_ids);
    const lt = String(location_type || 'station').toLowerCase();
    const lid = Number(location_id);
    const field = _canonField_(order_level, 'avg');

    if (!ids.length || !Number.isFinite(lid)) return _alignOutput2_(type_ids, [], {}, {});

    const aggs = postFetch(ids, lid, lt);
    const sMap = {}, bMap = {};

    for (let i = 0; i < ids.length; i++) {
      const id = ids[i];
      sMap[id] = _extractMetric_(aggs[id], 'sell', field);
      bMap[id] = _extractMetric_(aggs[id], 'buy', field);
    }
    return _alignOutput2_(type_ids, ids, sMap, bMap);
  } catch (e) {
    return _alignOutput2_(type_ids, [], {}, {});
  }
}

/**
 * Quick test against Jita station.
 * @customfunction
 */
function testfuzAPI() {
  const ids = [
    16239, 16243, 24030, 32881, 17366, 16273,
    34206, 34202, 34203, 34205, 34204, 34201,
    19761, 42695, 42830
  ];
  return fuzzApiPriceDataJitaSell(ids); // returns 2D aligned to input
}

function marketStatDataBothCache(type_ids, location_type, location_id, order_level) {
  try {
    const ids = _flattenTypeIds_(type_ids);
    const lt = String(location_type || 'station').toLowerCase();
    const lid = Number(location_id);
    const field = _canonField_(order_level, 'avg');

    if (!ids.length || !Number.isFinite(lid)) return _alignOutput2_(type_ids, [], {}, {});

    const cacheMap = _readAggsFromCache_(ids, lt, lid);
    const sMap = {}, bMap = {};

    for (let i = 0; i < ids.length; i++) {
      const id = ids[i];
      sMap[id] = _extractMetric_(cacheMap[id], 'sell', field);
      bMap[id] = _extractMetric_(cacheMap[id], 'buy', field);
    }
    return _alignOutput2_(type_ids, ids, sMap, bMap);
  } catch (e) {
    return _alignOutput2_(type_ids, [], {}, {});
  }
}
