/** ============================================================================
 * Fuzzworks Price Client + Cache Helpers (Apps Script / Sheets)
 * - Consistent DocumentCache usage
 * - Versioned cache keys
 * - Chunked POST requests + retries + tiny breath between chunks
 * - Safe with (optional) LoggerEx; falls back to console
 * - Functions:
 *     testfuzAPI()
 *     fuzzApiPriceDataJitaSell(type_ids, market_hub?, order_type?, order_level?)
 *     fuzzPriceDataByHub(type_ids, market_hub?, order_type?, order_level?)
 *     marketStatData(type_ids, location_type, location_id, order_type, order_level)
 * ----------------------------------------------------------------------------
 * Fuzzworks endpoint: https://market.fuzzwork.co.uk/aggregates/
 * Payload form: { station|system|region: <id>, types: "34,35,36,..."}
 * Returns: { <type_id>: { buy:{min,max,avg,median,volume}, sell:{...} }, ... }
 * ========================================================================== */

/* ------------------------------ Utilities -------------------------------- */

function _L_warn(tag, obj) {
  try {
    if (typeof LoggerEx !== 'undefined' && LoggerEx.warn) LoggerEx.warn(tag, obj);
    else console.warn(tag, obj);
  } catch (_) {}
}
function _L_info(tag, obj) {
  try {
    if (typeof LoggerEx !== 'undefined' && LoggerEx.log) LoggerEx.log(tag, obj);
    else console.log(tag, obj);
  } catch (_) {}
}

/** Run fn with simple backoff on common transient errors. */
function withRetries(fn, tries = 3, base = 300) {
  for (let i = 0; i < tries; i++) {
    try { return fn(); }
    catch (e) {
      const s = String(e && e.message || e);
      if (!/429|420|5\d\d|temporar|rate|timeout/i.test(s) || i === tries - 1) throw e;
      Utilities.sleep(base * Math.pow(2, i) + Math.floor(Math.random() * 200));
    }
  }
}

/** Hold a short document lock to avoid cache write thrash. */
function withDocLock(fn, ms = 30000) {
  const lock = LockService.getDocumentLock();
  lock.waitLock(ms);
  try { return fn(); }
  finally { lock.releaseLock(); }
}

/** Normalize order_type/order_level. Defaults: sell/min. */
function _normalizeOrder(order_type, order_level) {
  let type  = order_type ? String(order_type).toLowerCase() : null;
  let level = order_level ? String(order_level).toLowerCase() : null;

  if (type === "bid") type = "buy";
  if (type === "ask") type = "sell";
  const levelAliases = { mean: "avg", average: "avg", med: "median", vol: "volume", qty: "volume", quantity: "volume" };
  if (level && levelAliases[level]) level = levelAliases[level];

  if (!type && !level)        { type = "sell"; level = "min"; }
  else if (!type && level)    { type = (level === "max") ? "buy" : "sell"; }
  else if (type && !level)    { level = (type === "buy") ? "max" : "min"; }

  const validTypes  = ["buy","sell"];
  const validLevels = ["min","max","avg","median","volume"];
  if (!validTypes.includes(type))  throw new Error("order_type must be 'buy' or 'sell'");
  if (!validLevels.includes(level)) throw new Error("order_level must be one of 'min','max','avg','median','volume'");
  return { type, level };
}

/** 2D helpers to preserve the shape of input ranges */
function _as2D(input) {
  if (Array.isArray(input)) {
    return Array.isArray(input[0]) ? input : input.map(v => [v]);
  }
  return [[input]];
}
function _flatten2D(a2d) {
  const out = [];
  for (let r = 0; r < a2d.length; r++) for (let c = 0; c < a2d[0].length; c++) out.push(a2d[r][c]);
  return out;
}
function _reshape(flat, rows, cols) {
  const out = Array.from({ length: rows }, () => Array(cols).fill(""));
  let k = 0;
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) out[r][c] = flat[k++];
  return out;
}

/* ------------------------- Cache key / scope helpers ---------------------- */

const FUZ_CACHE_VER = 'v2';
function cacheScope() { return CacheService.getDocumentCache(); } // swap to getScriptCache() to share across bound scripts

function _fuzKey(location_type, location_id, type_id) {
  return `fuz:${FUZ_CACHE_VER}:${location_type}:${location_id}:${type_id}`;
}
function ttlForScope(lt) {
  lt = String(lt || '').toLowerCase();
  return lt === 'region' ? 30 * 60 : lt === 'system' ? 20 * 60 : 10 * 60; // seconds
}

/* ------------------------------ Core fetcher ------------------------------ */

/**
 * Pull price aggregates for type_ids from Fuzzworks.
 * - Caches per (location_type, location_id, type_id) in DocumentCache
 * - Chunked POSTs + retries on transient errors
 * - Returns: { <type_id>: fullAggregateObject }
 */
function postFetch(type_ids, location_id, location_type = "station") {
  if (!type_ids) throw new Error('type_ids is required');
  if (!Array.isArray(type_ids)) type_ids = [type_ids];

  const ids = type_ids.map(Number).filter(Number.isFinite);
  if (!ids.length) return {};

  const uniq = Array.from(new Set(ids));
  const lt = String(location_type).toLowerCase();
  if (!["region","system","station"].includes(lt)) {
    throw new Error("Invalid location_type; use 'region', 'system', or 'station'");
  }

  const cache = cacheScope();
  const ttlSec = ttlForScope(lt);

  // 1) cache-first
  const keys = uniq.map(id => _fuzKey(lt, location_id, id));
  const got = cache.getAll(keys) || {};
  const result = {};
  const missing = [];

  uniq.forEach((id, i) => {
    const raw = got[keys[i]];
    if (raw) {
      try { result[id] = JSON.parse(raw); }
      catch { missing.push(id); }
    } else {
      missing.push(id);
    }
  });

  // 2) POST only the missing ids (chunked)
  let fetched = {};
  if (missing.length) {
    const url = "https://market.fuzzwork.co.uk/aggregates/";
    const MAX_IDS_PER_POST = 700;

    for (let i = 0; i < missing.length; i += MAX_IDS_PER_POST) {
      const slice = missing.slice(i, i + MAX_IDS_PER_POST);
      const payload = { [lt]: location_id, types: slice.join(",") };
      const options = {
        method: "post",
        contentType: "application/json",
        payload: JSON.stringify(payload),
        muteHttpExceptions: true
      };

      try {
        const resp = withRetries(() => UrlFetchApp.fetch(url, options));
        const code = resp.getResponseCode();
        if (code === 200) {
          Object.assign(fetched, JSON.parse(resp.getContentText() || "{}"));
        } else {
          _L_warn('fuz.fetch.non200', { code, lt, location_id, count: slice.length });
        }
      } catch (e) {
        _L_warn('fuz.fetch.fail', { msg: String(e && e.message || e), lt, location_id, count: slice.length });
        // continue; we’ll return whatever we have
      }

      Utilities.sleep(100); // tiny breath
    }
  }

  // 3) short critical section: recheck + write cache + finalize
  if (missing.length) {
    withDocLock(() => {
      const missKeys = missing.map(id => _fuzKey(lt, location_id, id));
      const nowGot = cache.getAll(missKeys) || {};

      const toPut = {};
      missing.forEach((id, i) => {
        const k = missKeys[i];
        const pre = nowGot[k];

        if (pre) {
          try { result[id] = JSON.parse(pre); } catch {}
          return;
        }

        const row = fetched[id];
        if (row) {
          const s = JSON.stringify(row);
          if (s.length < 90000) {           // ← FIXED: numeric literal without underscore
            toPut[k] = s;
            result[id] = row;
          }
        }
      });

      if (Object.keys(toPut).length) {
        const entries = Object.entries(toPut);
        const CHUNK = 80;
        for (let i = 0; i < entries.length; i += CHUNK) {
          cache.putAll(Object.fromEntries(entries.slice(i, i + CHUNK)), ttlSec);
        }
      }
    });
  }

  return result;
}

/** Read a batch from cache; identify truly missing ids. */
function _getCachedFuz(type_ids, location_id, location_type) {
  const lt = String(location_type || '').toLowerCase();
  const cache = cacheScope();
  const keys = type_ids.map(id => _fuzKey(lt, location_id, id));
  const raw = cache.getAll(keys) || {};

  const have = {};
  const missing = [];
  for (let i = 0; i < type_ids.length; i++) {
    const id = type_ids[i];
    const s = raw[keys[i]];
    if (s) {
      try { have[id] = JSON.parse(s); }
      catch { missing.push(id); }
    } else {
      missing.push(id);
    }
  }
  return { have, missing };
}

/* -------------------------- Public custom functions ----------------------- */

/**
 * Quick test against Jita station.
 * @customfunction
 */
function testfuzAPI() {
  const ids = [
    16239,16243,24030,32881,17366,16273,
    34206,34202,34203,34205,34204,34201,
    19761,42695,42830
  ];
  return fuzzApiPriceDataJitaSell(ids); // returns 2D aligned to input
}

/**
 * Generic API to get prices for an array/range of type_ids at a station id (default Jita).
 * Defaults to sell/min if not specified.
 * Preserves the input shape (rows x cols).
 * @customfunction
 */
function fuzzApiPriceDataJitaSell(type_ids, market_hub = 60003760, order_type = null, order_level = null) {
  if (!type_ids) throw new Error('type_ids is required');

  const in2D = _as2D(type_ids);
  const rows = in2D.length, cols = in2D[0].length;

  // normalize order fields
  const norm = _normalizeOrder(order_type, order_level);

  // flatten, keep placeholders so we can re-align
  const flat = _flatten2D(in2D);
  const ids = flat.map(v => {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  });

  const valid = Array.from(new Set(ids.filter(n => n != null)));
  const fetched = valid.length ? postFetch(valid, Number(market_hub), "station") : {};

  const pick = (row) => {
    if (!row || !row[norm.type]) return null;
    const v = row[norm.type][norm.level];
    const num = Number(v);
    return Number.isFinite(num) ? num : null;
  };

  const outFlat = ids.map(id => (id == null ? "" : (pick(fetched[id]) ?? "")));
  return _reshape(outFlat, rows, cols);
}

/**
 * Hub-name helper (Jita/Amarr/Dodixie/Rens/Hek). Defaults sell/min.
 * Preserves input shape.
 * @customfunction
 */
function fuzzPriceDataByHub(type_ids, market_hub = "Jita", order_type = "sell", order_level = null) {
  if (!type_ids) throw new Error('type_ids is required');

  let hub = String(market_hub || '').toLowerCase();
  switch (hub) {
    case 'amarr':   hub = 60008494; break;
    case 'dodixie': hub = 60011866; break;
    case 'rens':    hub = 60004588; break;
    case 'hek':     hub = 60005686; break;
    case 'jita':
    default:        hub = 60003760;
  }
  return fuzzApiPriceDataJitaSell(type_ids, hub, order_type, order_level);
}

/**
 * marketStatData — cache-first accessor for Fuzzworks aggregates.
 * Supports: buy/sell × min|max|avg|median|volume
 * location_type ∈ {"region","system","station"}
 * Returns values aligned to the input shape.
 * @customfunction
 */
function marketStatData(type_ids, location_type, location_id, order_type, order_level) {
  if (!type_ids) throw new Error("type_ids is required");

  const in2D = _as2D(type_ids);
  const rows = in2D.length, cols = in2D[0].length;

  // normalize ids but keep placeholders so we can re-align later
  const flatIds = _flatten2D(in2D).map(v => {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  });

  // location guard
  const lt = String(location_type || "").toLowerCase();
  if (!["region","system","station"].includes(lt)) {
    throw new Error("Location Undefined (use 'region', 'system', or 'station')");
  }

  const { type: side, level: lvl } = _normalizeOrder(order_type, order_level);

  // unique valid ids
  const uniq = Array.from(new Set(flatIds.filter(n => n != null)));

  // 1) cache-first
  const { have, missing } = _getCachedFuz(uniq, Number(location_id), lt);

  // 2) fetch only truly missing ids, then merge
  if (missing.length) {
    const fetched = postFetch(missing, Number(location_id), lt) || {};
    for (const id of missing) {
      if (fetched[id] != null) have[id] = fetched[id];
    }
  }

  // 3) picker strictly for Fuzzworks fields
  function pick(row) {
    if (!row || !row[side]) return null;
    const node = row[side];
    const v = node[lvl];            // min|max|avg|median|volume
    const num = Number(v);
    return Number.isFinite(num) ? num : null;
  }

  // 4) map back to original shape
  const outFlat = flatIds.map(id => (id == null ? "" : (pick(have[id]) ?? "")));
  return _reshape(outFlat, rows, cols);
}
