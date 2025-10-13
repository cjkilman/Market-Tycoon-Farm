/** ============================================================================
 * Fuzzworks Price Client + Cache Helpers (Apps Script / Sheets)
 * - Consistent ScriptCache usage for shared price data
 * - Versioned cache keys (v2 standard)
 * - Chunked POST requests + retries + self-managed asynchronous triggers
 * - Safe with (optional) LoggerEx; falls back to console
 * - Functions:
 * testfuzAPI()
 * fuzzApiPriceDataJitaSell(...)
 * fuzzPriceDataByHub(...)
 * marketStatData(...)
 * marketStatDataBoth(...)
 * ----------------------------------------------------------------------------
 * Fuzzworks endpoint: https://market.fuzzwork.co.uk/aggregates/
 * ========================================================================== */

/* global CacheService, LoggerEx, UrlFetchApp, Utilities, LockService, ScriptApp */

/* ------------------------------ Utilities -------------------------------- */

function _L_warn(tag, obj) {
  try {
    if (typeof LoggerEx !== 'undefined' && LoggerEx.warn) LoggerEx.warn(tag, obj);
    else console.warn(tag, obj);
  } catch (_) { }
}
function _L_info(tag, obj) {
  try {
    if (typeof LoggerEx !== 'undefined' && LoggerEx.log) LoggerEx.log(tag, obj);
    else console.log(tag, obj);
  } catch (_) { }
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

/** Hold a short script lock to avoid internal cache write thrash. */
function withScriptLock(fn, ms = 30000) {
  const lock = LockService.getScriptLock();
  lock.waitLock(ms);
  try { return fn(); }
  finally { lock.releaseLock(); }
}

/** Normalize order_type/order_level. Defaults: sell/min. */
function _normalizeOrder(order_type, order_level) {
  // FIX: Ensure type is null if not provided, preventing String(null).toLowerCase() crash.
  let type = (order_type != null) ? String(order_type).toLowerCase() : null;
  let level = (order_level != null) ? String(order_level).toLowerCase() : null;

  if (type === "bid") type = "buy";
  if (type === "ask") type = "sell";
  // FIX: Allowing more aliases and including Fuzzworks camelCase fields
  const levelAliases = {
    mean: "avg",
    average: "avg",
    med: "median",
    vol: "volume",
    qty: "volume",
    quantity: "volume",
    // Fuzzworks specific fields (user inputs might be lowercase, but we map to camelCase output)
    weightedavg: "weightedAverage",
    weightedavge: "weightedAverage",
    stddev: "stddev",
    ordercount: "orderCount",
    percentile: "percentile"
  };
  if (level && levelAliases[level]) {
    level = levelAliases[level];
  } else if (level) {
    // If the input is not a common alias, pass it through as is (e.g., 'max', 'min', or a custom Fuzzworks field)
    level = order_level;
  }


  // Final determination of type/level if they are still null
  const validTypes = ["buy", "sell"];

  if (!type && !level) { type = "sell"; level = "min"; } // Default case
  else if (!type && level) { type = (level === "max") ? "buy" : "sell"; } // Infer type from level
  else if (type && !level) { level = (type === "buy") ? "max" : "min"; } // Infer level from type

  // FIX: Only validate 'type' if it exists (i.e., if it wasn't null initially or was determined above)
  if (type && !validTypes.includes(type)) throw new Error("order_type must be 'buy' or 'sell'");

  return { type, level };
}

/** * Processes 1D or 2D input range into metadata needed for fetching.
 * Only iterates the input once.
 */
function _processInputIds(input) {
  // Handle single cell input
  if (!Array.isArray(input)) input = [[input]];

  // Handle jagged arrays or rows that aren't arrays
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
      const id = Number.isFinite(n) ? n : null;

      flatIds.push(id);
      if (id !== null) {
        uniqueIds.add(id);
      }
    }
  }

  return {
    rows,
    cols,
    flatIds,
    validIds: Array.from(uniqueIds)
  };
}

/** Reshapes a flat array back into a 2D array (utility remains the same) */
function _reshape(flat, rows, cols) {
  const out = Array.from({ length: rows }, () => Array(cols).fill(""));
  let k = 0;
  for (let r = 0; r < rows; r++) for (let c = 0; c < cols; c++) out[r][c] = flat[k++];
  return out;
}

/* ------------------------- Cache key / scope helpers ---------------------- */

const FUZ_CACHE_VER = 'v2';
const MISSING_QUEUE_KEY = 'FUZ:MISSING_QUEUE';

const MANUAL_REFRESH_MODE = false;

function cacheScope() { return CacheService.getScriptCache(); } // swap to getScriptCache() to share across bound scripts

function _fuzKey(location_type, location_id, type_id) {
  return `fuz:${FUZ_CACHE_VER}:${location_type}:${location_id}:${type_id}`;
}
function ttlForScope(lt) {
  // FIX: Align TTL with Fuzzworks refresh rate (30 minutes = 1800 seconds)
  return 30 * 60;
}

/* ------------------------------ Core fetcher (Now only for background use) ------------------------------ */

/**
 * Fetches data for MISSING type_ids from Fuzzworks and caches the results.
 * This function is designed to be run asynchronously via a timed trigger.
 * @param {number[]} type_ids 
 * @param {number} location_id 
 * @param {string} [location_type="station"] 
 */
function _performFetchAndCache(type_ids, location_id, location_type = "station") {
  if (!type_ids || !type_ids.length) return {};

  const ids = type_ids.map(Number).filter(Number.isFinite);
  if (!ids.length) return {};

  const uniq = Array.from(new Set(ids));
  const lt = String(location_type).toLowerCase();

  const cache = cacheScope();
  let ttlSec = ttlForScope(lt);

  // FIX: Apply cache jitter (+/- 5 minutes) to avoid massive simultaneous cache expiration
  // Max jitter window is 600 seconds (10 minutes total).
  const JITTER_SECONDS = 300;
  const randomOffset = Math.floor(Math.random() * (JITTER_SECONDS * 2 + 1)) - JITTER_SECONDS;
  ttlSec = Math.max(600, ttlSec + randomOffset); // Ensure TTL is at least 10 minutes (600s)

  let fetched = {};
  const url = "https://market.fuzzwork.co.uk/aggregates/";
  const MAX_IDS_PER_POST = 700;

  for (let i = 0; i < uniq.length; i += MAX_IDS_PER_POST) {
    const slice = uniq.slice(i, i + MAX_IDS_PER_POST);
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
    }

    Utilities.sleep(100); // tiny breath
  }

  // Write cache OUTSIDE of the Document Lock
  if (Object.keys(fetched).length) {
    const toPut = {};
    const allFetchedIds = Object.keys(fetched).map(Number);

    allFetchedIds.forEach(id => {
      const row = fetched[id];
      const s = JSON.stringify(row);
      if (s.length < 90000) {           // Ensure not too large for cache
        const k = _fuzKey(lt, location_id, id);
        toPut[k] = s;
      }
    });

    if (Object.keys(toPut).length) {
      const entries = Object.entries(toPut);
      const CHUNK = 80;

      // Chunk writes (putAll is limited to 1000 items, and max 100kb total)
      for (let i = 0; i < entries.length; i += CHUNK) {
        cache.putAll(Object.fromEntries(entries.slice(i, i + CHUNK)), ttlSec);
      }
    }
  }
}

/**
 * Helper to delete all existing triggers pointing to fuzzworkCacheRefresh.
 * NOTE: This is now unused as we rely on manual installation.
 */
function _deleteExistingTriggers() {
  const triggers = ScriptApp.getProjectTriggers();
  triggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === "fuzzworkCacheRefresh") {
      ScriptApp.deleteTrigger(trigger);
      return;
    }
  });
}


/** ----------------- ASYNCHRONOUS REFRESH FUNCTIONS ----------------------- */

function processQueue() {
  //Global Trigger to run
  fuzzworkCacheRefresh();
}

/**
 * Public function to queue or run the cache refresh task.
 * DESIGNED TO BE RUN BY A USER-INSTALLED TIME-DRIVEN TRIGGER.
 * @customfunction
 */
function fuzzworkCacheRefresh() {
  // Use a ScriptLock to ensure only one instance of the refresh task runs at a time
  return withScriptLock(function () {

    // NOTE: Triggers are now managed externally. We do NOT delete triggers here.

    const scriptCache = cacheScope();
    const queueJson = scriptCache.get(MISSING_QUEUE_KEY);

    if (!queueJson) {
      _L_info('fuz.refresh', { status: 'Queue empty.' });
      _deleteExistingTriggers();
      return 0;
    }

    const queue = JSON.parse(queueJson);
    const itemsProcessed = queue.length;

    if (itemsProcessed === 0) {
      scriptCache.remove(MISSING_QUEUE_KEY);
      return 0;
    }

    // Process all tasks in one execution slot (since the trigger is now time-driven)
    // NOTE: This assumes the total fetch time for all items won't exceed 6 minutes.
    // If it exceeds the limit, the script will crash and the trigger will run again
    // on its next scheduled interval.

    // We process only one task per trigger, and rely on the external trigger to restart.
    const task = queue.shift();

    try {
      _L_info('fuz.refresh', { status: `Processing ${task.ids.length} items for ${task.location_id}` });
      _performFetchAndCache(task.ids, task.location_id, task.location_type);
    } catch (e) {
      _L_warn('fuz.refresh.fail', { error: e.message, task });
      queue.push(task); // Re-queue task on failure
    }

    // Save remaining queue state
    scriptCache.put(MISSING_QUEUE_KEY, JSON.stringify(queue), 3600);

    // Clean up if finished
    if (queue.length === 0) {
      scriptCache.remove(MISSING_QUEUE_KEY);
    }
    ScriptApp.newTrigger("fuzzworkCacheRefresh")
      .timeBased()
      .after(5)
      .create();
    // NOTE: No internal trigger creation, rely on the external user-installed trigger.

    return itemsProcessed;
  });
}

/**
 * Adds missing items to the persistent queue for background refresh.
 * This is called by marketStatData.
 * @param {number[]} missing_ids 
 * @param {number} location_id 
 * @param {string} location_type 
 */
function _queueMissingItems(missing_ids, location_id, location_type) {
  if (!missing_ids || missing_ids.length === 0) return;

  const scriptCache = cacheScope();

  // Use ScriptLock to prevent two simultaneous custom function calls from corrupting the queue state
  return withScriptLock(function () {
    const queueJson = scriptCache.get(MISSING_QUEUE_KEY);
    let queue = queueJson ? JSON.parse(queueJson) : [];

    const uniqueMissingIds = Array.from(new Set(missing_ids)).filter(Number.isFinite);
    const lt = String(location_type).toLowerCase();

    // 1. Check if a task for this location already exists in the queue
    let existingTask = queue.find(task =>
      task.location_id === location_id && task.location_type === lt
    );

    if (existingTask) {
      // 2. Consolidate: Merge new missing IDs into the existing task's list
      const existingIds = new Set(existingTask.ids);
      let newIdsAdded = 0;

      uniqueMissingIds.forEach(id => {
        if (!existingIds.has(id)) {
          existingTask.ids.push(id);
          newIdsAdded++;
        }
      });

      if (newIdsAdded > 0) {
        _L_info('fuz.queue', { status: `Consolidated ${newIdsAdded} new IDs into existing task for ${lt}:${location_id}` });
      } else {
        return; // No new items to fetch for this location
      }

    } else {
      // 3. Create a new task
      queue.push({
        location_id: location_id,
        location_type: lt,
        ids: uniqueMissingIds
      });
      _L_info('fuz.queue', { status: `Created new task for ${lt}:${location_id} with ${uniqueMissingIds.length} IDs` });
    }

    // 4. Save updated queue state
    scriptCache.put(MISSING_QUEUE_KEY, JSON.stringify(queue), 3600);

    // 5. NOTE: No automatic trigger creation. The external trigger manages the schedule.
  });
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
    16239, 16243, 24030, 32881, 17366, 16273,
    34206, 34202, 34203, 34205, 34204, 34201,
    19761, 42695, 42830
  ];
  return fuzzApiPriceDataJitaSell(ids); // returns 2D aligned to input
}

/**
 * **CRITICAL TESTING/VERIFICATION FUNCTION**
 * Simulates a custom function call and reports cache hit/miss status and execution time.
 * @returns {string[][]} Test results summary.
 * @customfunction
 */
function testFuzzworksPerformance() {
  // Test Items (A mix of IDs likely to be cached and some potentially not)
  const TEST_IDS = [34, 35, 34, 16297, 16519, 99999999]; // 99999999 is fake to force a queue item
  const TEST_LOCATION = 60003760; // Jita IV - Moon 4 - Caldari Navy Assembly Plant

  const results = [];
  results.push(["Test Stage", "Status", "Time (ms)", "Cache Status"]);

  // --- TEST 1: Cache Miss Run (Queues fetch) ---
  const start1 = Date.now();
  const { flatIds: ids1, validIds: uniq1 } = _processInputIds(TEST_IDS);
  const { have: have1, missing: missing1 } = _getCachedFuz(uniq1, TEST_LOCATION, "station");

  if (missing1.length) {
    _queueMissingItems(missing1, TEST_LOCATION, "station");
  }
  const status1 = missing1.length === 0 ? "CACHE HIT" : "FETCH DELEGATED";
  results.push([
    "Initial Custom Function Run",
    status1,
    Date.now() - start1,
    `${missing1.length} Missing / ${Object.keys(have1).length} Found`
  ]);

  // --- TEST 2: Execute Background Refresh (Simulated Trigger Run) ---
  const start2 = Date.now();
  try {
    const itemsProcessed = fuzzworkCacheRefresh();
    results.push([
      "Background Fetch Execution",
      itemsProcessed > 0 ? "Processed Queue" : "Queue Empty",
      Date.now() - start2,
      `Items Processed: ${itemsProcessed}`
    ]);

    // Wait for the cache write (putAll is async but very fast)
    Utilities.sleep(1000);

  } catch (e) {
    results.push(["Background Fetch Execution", "ERROR", Date.now() - start2, e.message]);
  }

  // --- TEST 3: Cache Hit Run (Verifies data availability) ---
  const start3 = Date.now();
  const { have: have3, missing: missing3 } = _getCachedFuz(uniq1, TEST_LOCATION, "station");
  const status3 = missing3.length === 0 ? "SUCCESSFUL CACHE HIT" : "FAILURE: DATA STILL MISSING";
  results.push([
    "Final Cache Hit Check",
    status3,
    Date.now() - start3,
    `${missing3.length} Missing / ${Object.keys(have3).length} Found`
  ]);

  return results;
}


/**
 * Generic API to get prices for an array/range of type_ids at a station id (default Jita).
 * Defaults to sell/min if not specified.
 * Preserves the input shape (rows x cols).
 * @customfunction
 * @param {number[][]} type_ids The item IDs to fetch prices for.
 * @param {number} [market_hub=60003760] The location ID (station, system, or region).
 * @param {string} [order_type="sell"] The type of order side ('buy' or 'sell').
 * @param {string} [order_level="min"] The aggregate level ('min', 'max', 'avg', 'median', 'volume').
 * @param {string} [location_type="station"] The scope of the market ID ('station', 'system', or 'region').
 */
function fuzzApiPriceDataJitaSell(type_ids, market_hub = 60003760, order_type = null, order_level = null, location_type = "station") {
  if (!type_ids) throw new Error('type_ids is required');

  const { rows, cols, flatIds, validIds } = _processInputIds(type_ids);
  const lt = String(location_type).toLowerCase();

  // normalize order fields
  const norm = _normalizeOrder(order_type, order_level);

  // NOTE: This call is now non-blocking (uses _getCachedFuz only)
  const { have, missing } = _getCachedFuz(validIds, Number(market_hub), lt);

  // Trigger background fetch for missing items
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
    // Return cached value, or a placeholder if missing
    const data = have[id];
    if (id == null) return "";
    if (!data) return "FETCHING..."; // Placeholder

    return (pick(data) ?? "");
  });

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
    case 'amarr': hub = 60008494; break;
    case 'dodixie': hub = 60011866; break;
    case 'rens': hub = 60004588; break;
    case 'hek': hub = 60005686; break;
    case 'jita':
    default: hub = 60003760;
  }
  // All Hub lookups are explicitly station lookups, so we pass "station"
  return fuzzApiPriceDataJitaSell(type_ids, hub, order_type, order_level, "station");
}

/**
 * marketStatData — cache-first accessor for Fuzzworks aggregates.
 * Supports: buy/sell × min|max|avg|median|volume
 * location_type ∈ {"region","system","station"}
 * Returns values aligned to the input shape.
 * @customfunction
 * @param {number[][]} type_ids The item IDs to fetch prices for.
 * @param {string} location_type The scope of the market ID ('station', 'system', or 'region').
 * @param {number} location_id The location ID (station, system, or region).
 * @param {string} [order_type="sell"] The type of order side ('buy' or 'sell').
 * @param {string} [order_level="min"] The aggregate level ('min', 'max', 'avg', 'median', 'volume').
 */
function marketStatData(type_ids, location_type, location_id, order_type, order_level) {
  if (!type_ids) throw new Error("type_ids is required");

  const { rows, cols, flatIds, validIds } = _processInputIds(type_ids);

  // location guard
  const lt = String(location_type || "").toLowerCase();
  if (!["region", "system", "station"].includes(lt)) {
    throw new Error("Location Undefined (use 'region', 'system', or 'station')");
  }

  const { type: side, level: lvl } = _normalizeOrder(order_type, order_level);

  // unique valid ids
  const uniq = validIds;

  // 1) cache-first read
  const { have, missing } = _getCachedFuz(uniq, Number(location_id), lt);

  // 2) Trigger background fetch for missing items
  if (missing.length) {
    _queueMissingItems(missing, Number(location_id), lt);
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
  const outFlat = flatIds.map(id => {
    // Return cached value, or a placeholder if missing
    const data = have[id];
    if (id == null) return "";
    // FIX: Return blank string instead of "FETCHING..."
    if (!data) return "Fetching...";

    return (pick(data) ?? "");
  });

  return _reshape(outFlat, rows, cols);
}

/**
 * @customfunction
 * MarketStatDataBoth - Cache-first accessor for Fuzzworks aggregates, returning Buy and Sell stats side-by-side.
 * This is designed to be used with ARRAYFORMULA to output two columns of data (e.g., Average Buy Price | Average Sell Price).
 * * @param {number[][]} type_ids The item IDs to fetch prices for (can be a 1D or 2D range).
 * @param {string} location_type The scope of the market ID ('station', 'system', or 'region').
 * @param {number} location_id The location ID (station, system, or region).
 * @param {string} [order_level_sell="min"] The aggregate level for the SELL side ('min', 'max', 'avg', 'median', 'volume').
 * @param {string} [order_level_buy="max"] The aggregate level for the BUY side ('min', 'max', 'avg', 'median', 'volume').
 * @returns {Array<Array<any>>} A 2D array aligned to the input, but with twice the columns. Outputs Buy Price and Sell Price columns.
 */
function marketStatDataBoth(type_ids, location_type, location_id, order_level_sell = "min", order_level_buy = "max") {
  if (!type_ids) throw new Error("type_ids is required");

  const { rows, cols, flatIds, validIds } = _processInputIds(type_ids);

  // Location validation
  const lt = String(location_type || "").toLowerCase();
  if (!["region", "system", "station"].includes(lt)) {
    throw new Error("Location Undefined (use 'region', 'system', or 'station')");
  }

  // 1) Cache-first read (non-blocking)
  const { have, missing } = _getCachedFuz(validIds, Number(location_id), lt);

  // 2) Trigger background fetch for missing items
  if (missing.length) {
    _queueMissingItems(missing, Number(location_id), lt);
  }

  // 3) Picker function to extract both Buy and Sell metrics
  function pickBoth(row) {
    if (!row || !row.buy || !row.sell) {
      return [null, null];
    }

    // Normalize requested levels (using _normalizeOrder's level mapping)
    const buyLevel = _normalizeOrder("buy", order_level_buy).level;
    const sellLevel = _normalizeOrder("sell", order_level_sell).level;

    // Extract Buy value
    const buyValue = row.buy[buyLevel];
    const buyNum = Number(buyValue);
    const finalBuy = Number.isFinite(buyNum) ? buyNum : null;

    // Extract Sell value
    const sellValue = row.sell[sellLevel];
    const sellNum = Number(sellValue);
    const finalSell = Number.isFinite(sellNum) ? sellNum : null;

    // FIX: Return [Sell Price, Buy Price] to match standard sheet header order.
    return [finalSell, finalBuy];
  }

  // 4) Map back to original shape (outputting two columns per input)
  const outFlat = flatIds.flatMap(id => {
    const data = have[id];

    if (id == null) return ["", ""];

    if (!data) return ["Fetching...", "Fetching..."];

    const [sellVal, buyVal] = pickBoth(data);

    return [
      sellVal ?? "", // Column 1: Sell Price
      buyVal ?? ""  // Column 2: Buy Price
    ];
  });

  // Reshape needs to handle the fact that we doubled the column count (cols * 2)
  return _reshape(outFlat, rows, cols * 2);
}
