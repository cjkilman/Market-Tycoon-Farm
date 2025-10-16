/** ============================================================================
 * Fuzzworks Price Client + Cache Helpers (Apps Script / Sheets) - V3
 * - Asynchronous, queue-based fetching to prevent spreadsheet timeouts.
 * - Shared ScriptCache for efficient data sharing between users.
 * - Chunked, concurrent POST requests with retries for reliability.
 * - Negative caching for invalid or unlisted type IDs to prevent re-queuing.
 * - De-duplication of queued tasks to minimize API calls.
 * ========================================================================== */

/* global CacheService, LoggerEx, UrlFetchApp, Utilities, LockService, ScriptApp */

/* ------------------------------ CONSTANTS --------------------------------- */

const FUZ_CACHE_VER = 'v2';
const MISSING_QUEUE_KEY = 'FUZ:MISSING_QUEUE';
const MAX_ID_PER_CHUNK = 700; 
const FETCHING_PLACEHOLDER = "Waiting on Queue";

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

function withScriptLock(fn, ms = 30000) {
  const lock = LockService.getScriptLock();
  lock.waitLock(ms);
  try { return fn(); }
  finally { lock.releaseLock(); }
}

function _normalizeOrder(order_type, order_level) {
  let type = (order_type != null) ? String(order_type).toLowerCase() : null;
  let level = (order_level != null) ? String(order_level).toLowerCase() : null;

  if (type === "bid") type = "buy";
  if (type === "ask") type = "sell";

  const levelAliases = { 
    mean: "avg", average: "avg", med: "median", vol: "volume", qty: "volume", 
    quantity: "volume", weightedavg: "weightedAverage", weightedavge: "weightedAverage", 
    stddev: "stddev", ordercount: "orderCount", percentile: "percentile"
  };

  if (level && levelAliases[level]) {
    level = levelAliases[level];
  } else if (level) {
    level = order_level; 
  }

  const validTypes = ["buy", "sell"];
  if (!type && !level) { type = "sell"; level = "min"; }
  else if (!type && level) { type = (level === "max") ? "buy" : "sell"; }
  else if (type && !level) { level = (type === "buy") ? "max" : "min"; }
  
  if (type && !validTypes.includes(type)) throw new Error("order_type must be 'buy' or 'sell'");
  
  return { type, level };
}

function _processInputIds(input) {
  if (!Array.isArray(input)) input = [[input]];
  if (input.length > 0 && !Array.isArray(input[0])) input = [input];
  if (input.length === 0) return { rows: 0, cols: 0, flatIds: [], validIds: [] };

  const rows = input.length;
  const cols = input[0] ? input[0].length : 0;
  
  const uniqueIds = new Set();
  const flatIds = [];

  for (let r = 0; r < rows; r++) {
    for (let c = 0; c < cols; c++) {
      const val = (input[r] && input[r][c] !== undefined) ? input[r][c] : '';
      const n = Number(val);
      const id = Number.isFinite(n) && n > 0 ? n : null;
      
      flatIds.push(id);
      if (id !== null) {
        uniqueIds.add(id);
      }
    }
  }
  
  return { rows, cols, flatIds, validIds: Array.from(uniqueIds) };
}

function _reshape(flat, rows, cols) {
  const out = Array.from({ length: rows }, () => Array(cols).fill(""));
  let k = 0;
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) out[r][c] = flat[k++];
  return out;
}

/* ------------------------- Cache & Queue Management ---------------------- */

function cacheScope() { return CacheService.getScriptCache(); }

function _fuzKey(location_type, location_id, type_id) {
  return `fuz:${FUZ_CACHE_VER}:${location_type}:${location_id}:${type_id}`;
}

function ttlForScope(lt) {
  return 30 * 60; // 30 minutes
}

function _queueMissingItems(missing_ids, location_id, location_type) {
    if (!missing_ids || missing_ids.length === 0) return;

    withScriptLock(function() {
        const scriptCache = cacheScope();
        const queueJson = scriptCache.get(MISSING_QUEUE_KEY);
        let queue = queueJson ? JSON.parse(queueJson) : [];

        const uniqueMissingIds = Array.from(new Set(missing_ids)).filter(Number.isFinite);
        if (uniqueMissingIds.length === 0) return;
        
        const lt = String(location_type).toLowerCase();

        const existingIds = new Set();
        queue.forEach(task => {
            if (task.location_id === location_id && task.location_type === lt) {
                task.ids.forEach(id => existingIds.add(id));
            }
        });

        const newIdsToAdd = uniqueMissingIds.filter(id => !existingIds.has(id));

        if (newIdsToAdd.length > 0) {
            for (let i = 0; i < newIdsToAdd.length; i += MAX_ID_PER_CHUNK) {
                const chunkIds = newIdsToAdd.slice(i, i + MAX_ID_PER_CHUNK);
                queue.push({ location_id: location_id, location_type: lt, ids: chunkIds });
            }
            _L_info('fuz.queue', { status: `Queued ${newIdsToAdd.length} new items for ${lt}:${location_id}` });
        }

        scriptCache.put(MISSING_QUEUE_KEY, JSON.stringify(queue), 3600);
    });
}

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

    if (s === 'null') {
      have[id] = null; // Found a negative cache entry
    } else if (s) {
      try { 
        have[id] = JSON.parse(s); 
      } catch { 
        missing.push(id); 
      }
    } else {
      missing.push(id); // Truly missing
    }
  }
  return { have, missing };
}

/* ------------------------------ Core Fetching Logic ------------------------------ */

function fuzzworkCacheRefresh() {
    const scriptCache = cacheScope();
    let tasksToProcess = [];

    withScriptLock(function() {
        const queueJson = scriptCache.get(MISSING_QUEUE_KEY);
        if (!queueJson) return;
        try {
            tasksToProcess = JSON.parse(queueJson);
        } catch (e) {
            _L_warn('fuz.refresh.error', { status: 'Queue cache corruption. Resetting.', error: e.message });
            scriptCache.remove(MISSING_QUEUE_KEY);
            return;
        }
        if (tasksToProcess.length > 0) {
            scriptCache.remove(MISSING_QUEUE_KEY);
        }
    });

    if (tasksToProcess.length === 0) {
        _L_info('fuz.refresh', { status: 'Queue is empty.' });
        return 0;
    }

    // --- De-duplication and Consolidation ---
    const consolidated = {};
    for (const task of tasksToProcess) {
        const key = `${task.location_type}:${task.location_id}`;
        if (!consolidated[key]) {
            consolidated[key] = {
                location_id: task.location_id,
                location_type: task.location_type,
                ids: new Set()
            };
        }
        task.ids.forEach(id => consolidated[key].ids.add(id));
    }

    const finalTasks = [];
    for (const key in consolidated) {
        const taskData = consolidated[key];
        const allIds = Array.from(taskData.ids);
        for (let i = 0; i < allIds.length; i += MAX_ID_PER_CHUNK) {
            finalTasks.push({
                location_id: taskData.location_id,
                location_type: taskData.location_type,
                ids: allIds.slice(i, i + MAX_ID_PER_CHUNK)
            });
        }
    }
    _L_info('fuz.refresh', { status: 'Consolidated queue.', tasks_before: tasksToProcess.length, tasks_after: finalTasks.length });
    
    // --- Execute Fetches ---
    try {
        fuzzyFetchAll(finalTasks);
    } catch (e) {
        _L_warn('fuz.network.fatal', { error: e.message, status: 'Re-queueing tasks.' });
        withScriptLock(function() {
            const currentQueueJson = scriptCache.get(MISSING_QUEUE_KEY);
            let currentQueue = currentQueueJson ? JSON.parse(currentQueueJson) : [];
            const requeue = currentQueue.concat(finalTasks);
            scriptCache.put(MISSING_QUEUE_KEY, JSON.stringify(requeue), 3600);
        });
    }
    
    return finalTasks.length;
}

function fuzzyFetchAll(tasks) {
    if (!tasks || tasks.length === 0) return { totalTasks: 0, successfulFetches: 0 };

    const NEGATIVE_CACHE_TTL_SEC = 6 * 3600; // 6 hours
    const URL = "https://market.fuzzwork.co.uk/aggregates/";
    const requests = [];

    for (const task of tasks) {
        const payload = { [task.location_type]: task.location_id, types: task.ids.join(",") };
        requests.push({
            url: URL,
            method: "post",
            contentType: "application/json",
            payload: JSON.stringify(payload),
            muteHttpExceptions: true
        });
    }

    const responses = withRetries(() => UrlFetchApp.fetchAll(requests));
    const cache = cacheScope();
    let successfulFetches = 0;

    for (let i = 0; i < responses.length; i++) {
        const resp = responses[i];
        const task = tasks[i];
        const toPut = {};
        const toPutNegative = {};

        try {
            const code = resp.getResponseCode();
            if (code === 200) {
                const fetchedData = JSON.parse(resp.getContentText() || "{}");
                const requestedIds = new Set(task.ids.map(id => String(id)));
                const receivedIds = new Set(Object.keys(fetchedData));
                
                _L_info('fuz.fetch.counts', { location: task.location_id, requested: requestedIds.size, received: receivedIds.size });

                receivedIds.forEach(idStr => {
                    const k = _fuzKey(task.location_type, task.location_id, idStr);
                    toPut[k] = JSON.stringify(fetchedData[idStr]);
                });

                requestedIds.forEach(idStr => {
                    if (!receivedIds.has(idStr)) {
                        const k = _fuzKey(task.location_type, task.location_id, idStr);
                        toPutNegative[k] = 'null';
                    }
                });
                
                successfulFetches++;

            } else {
                _L_warn('fuz.fetch.non200', { code, location: task.location_id, error: resp.getContentText() });
                task.ids.forEach(id => {
                    const k = _fuzKey(task.location_type, task.location_id, id);
                    toPutNegative[k] = 'null';
                });
            }
        } catch (e) {
            _L_warn('fuz.fetch.fail', { msg: String(e && e.message || e), location: task.location_id });
            task.ids.forEach(id => {
                const k = _fuzKey(task.location_type, task.location_id, id);
                toPutNegative[k] = 'null';
            });
        }

        if (Object.keys(toPut).length) {
            let ttlSec = ttlForScope(task.location_type);
            const JITTER_SECONDS = 300;
            const randomOffset = Math.floor(Math.random() * (JITTER_SECONDS * 2 + 1)) - JITTER_SECONDS;
            ttlSec = Math.max(600, ttlSec + randomOffset);

            const entries = Object.entries(toPut);
            const CHUNK = 80;
            for (let j = 0; j < entries.length; j += CHUNK) {
                cache.putAll(Object.fromEntries(entries.slice(j, j + CHUNK)), ttlSec);
            }
        }
        if (Object.keys(toPutNegative).length) {
            cache.putAll(toPutNegative, NEGATIVE_CACHE_TTL_SEC);
        }
    }

    return { totalTasks: tasks.length, successfulFetches };
}

/* -------------------------- Public Custom Functions ----------------------- */

/**
 * Generic API to get prices for an array/range of type_ids at a station id (default Jita).
 * Preserves the input shape (rows x cols).
 * @customfunction
 */
function fuzzApiPriceDataJitaSell(type_ids, market_hub = 60003760, order_type = null, order_level = null, location_type = "station", refresh_id = null) {
    if (!type_ids) throw new Error('type_ids is required');
    if (refresh_id != null) { /* no-op */ }

    const { rows, cols, flatIds, validIds } = _processInputIds(type_ids);
    const lt = String(location_type).toLowerCase();
    const norm = _normalizeOrder(order_type, order_level);
    const { have, missing } = _getCachedFuz(validIds, Number(market_hub), lt);
  
    if (missing.length) {
        _queueMissingItems(missing, Number(market_hub), lt);
    }

    const pick = (row) => {
        if (!row || !row[norm.type]) return null;
        const v = row[norm.type][norm.level];
        const num = Number(v);
        return Number.isFinite(num) ? num : null;
    };

    const outFlat = flatIds.map(id => {
        if (id == null) return "";
        if (!have.hasOwnProperty(id)) {
            return FETCHING_PLACEHOLDER;
        }
        const data = have[id];
        if (!data) {
            return ""; 
        }
        return (pick(data) ?? "");
    });
  
    return _reshape(outFlat, rows, cols);
}


/**
 * Hub-name helper (Jita/Amarr/Dodixie/Rens/Hek). Defaults sell/min.
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
  return fuzzApiPriceDataJitaSell(type_ids, hub, order_type, order_level, "station");
}

/**
 * The primary, cache-first accessor for Fuzzworks aggregates.
 * @customfunction
 */
function marketStatData(type_ids, location_type, location_id, order_type, order_level, refresh_id = null) {
    if (!type_ids) throw new Error("type_ids is required");
    if (refresh_id != null) { /* no-op */ }

    const { rows, cols, flatIds, validIds } = _processInputIds(type_ids);
    const lt = String(location_type || "").toLowerCase();
    if (!["region", "system", "station"].includes(lt)) {
        throw new Error("Location Undefined (use 'region', 'system', or 'station')");
    }

    const { type: side, level: lvl } = _normalizeOrder(order_type, order_level);
    const { have, missing } = _getCachedFuz(validIds, Number(location_id), lt);

    if (missing.length) {
        _queueMissingItems(missing, Number(location_id), lt);
    }

    function pick(row) {
        if (!row || !row[side]) return null;
        const v = row[side][lvl];
        const num = Number(v);
        return Number.isFinite(num) ? num : null;
    }

    const outFlat = flatIds.map(id => {
        if (id == null) return "";
        if (!have.hasOwnProperty(id)) {
            return FETCHING_PLACEHOLDER;
        }
        const data = have[id];
        return data ? (pick(data) ?? "") : "";
    });
    
    return _reshape(outFlat, rows, cols);
}

/**
 * Cache-first accessor for Fuzzworks aggregates, returning Buy and Sell stats side-by-side.
 * @customfunction
 */
function marketStatDataBoth(type_ids, location_type, location_id, order_level_sell = "min", order_level_buy = "max", refresh_id = null) {
    if (!type_ids) throw new Error("type_ids is required");
    if (refresh_id != null) { /* no-op */ }

    const { rows, cols, flatIds, validIds } = _processInputIds(type_ids);
    const lt = String(location_type || "").toLowerCase();
    if (!["region", "system", "station"].includes(lt)) {
        throw new Error("Location Undefined (use 'region', 'system', or 'station')");
    }

    const { have, missing } = _getCachedFuz(validIds, Number(location_id), lt);

    if (missing.length) {
        _queueMissingItems(missing, Number(location_id), lt);
    }
    
    function pickBoth(row) {
        if (!row) return [null, null];
        const buyLevel = _normalizeOrder("buy", order_level_buy).level;
        const sellLevel = _normalizeOrder("sell", order_level_sell).level;
        const buyValue = row.buy ? row.buy[buyLevel] : null;
        const sellValue = row.sell ? row.sell[sellLevel] : null;
        const finalBuy = Number.isFinite(Number(buyValue)) ? Number(buyValue) : null;
        const finalSell = Number.isFinite(Number(sellValue)) ? Number(sellValue) : null;
        return [finalSell, finalBuy];
    }

    const outFlat = flatIds.flatMap(id => {
        if (id == null) return ["", ""];
        if (!have.hasOwnProperty(id)) {
            return [FETCHING_PLACEHOLDER, FETCHING_PLACEHOLDER];
        }
        const data = have[id];
        if (!data) {
            return ["", ""];
        }
        const [sellVal, buyVal] = pickBoth(data);
        return [sellVal ?? "", buyVal ?? ""];
    });
    
    return _reshape(outFlat, rows, cols * 2); 
}