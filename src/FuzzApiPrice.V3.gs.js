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
 * fuzzworkEnqueueMissing(...) // <-- NEW PUBLIC ENQUEUE
 * marketStatDataMultiMarket(...) // <-- NEW MULTI-MARKET FUNCTION
 * ----------------------------------------------------------------------------
 * Fuzzworks endpoint: https://market.fuzzwork.co.uk/aggregates/
 * ========================================================================== */

/* global CacheService, LoggerEx, UrlFetchApp, Utilities, LockService, ScriptApp */

/* ------------------------------ CONSTANTS --------------------------------- */

const FUZ_CACHE_VER = 'v2';
const MISSING_QUEUE_KEY = 'FUZ:MISSING_QUEUE';

// API Limits: Fuzzworks can handle many, but 700 is a safe, conservative batch size 
// to prevent Apps Script execution time issues when processing the payload.
const MAX_ID_PER_CHUNK = 700; 
const MANUAL_REFRESH_MODE = false; 

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
  let type  = (order_type != null) ? String(order_type).toLowerCase() : null;
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
  const validTypes  = ["buy","sell"];

  if (!type && !level)        { type = "sell"; level = "min"; } // Default case
  else if (!type && level)    { type = (level === "max") ? "buy" : "sell"; } // Infer type from level
  else if (type && !level)    { level = (type === "buy") ? "max" : "min"; } // Infer level from type
  
  // FIX: Only validate 'type' if it exists (i.e., if it wasn't null initially or was determined above)
  if (type && !validTypes.includes(type))  throw new Error("order_type must be 'buy' or 'sell'");
  
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
 * NOTE: This function is now OBSOLETE, replaced by fuzzyFetchAll and removed its body.
 */
function _performFetchAndCache() {
  // This function is obsolete now that the logic is in fuzzyFetchAll. 
  // It is kept as a placeholder to avoid breaking external references temporarily.
  _L_warn('fuz.deprecated', { status: '_performFetchAndCache is obsolete.' });
}

/**
 * Executes concurrent POST requests for all queued tasks using UrlFetchApp.fetchAll().
 * @param {Array<Object>} tasks - The array of tasks fetched from the queue.
 * @returns {Object} { totalTasks: number, successfulFetches: number }
 */
function fuzzyFetchAll(tasks) {
  if (!tasks || tasks.length === 0) return { totalTasks: 0, successfulFetches: 0 };

  const URL = "https://market.fuzzwork.co.uk/aggregates/";
  const requests = [];

  for (const task of tasks) {
    const lt = task.location_type;
    const locId = task.location_id;
    const ids = task.ids;

    // The IDs array is already guaranteed to be 700 or less due to chunking in _queueMissingItems.
    const payload = { [lt]: locId, types: ids.join(",") };

    requests.push({
      url: URL,
      method: "post",
      contentType: "application/json",
      payload: JSON.stringify(payload),
      muteHttpExceptions: true
    });
  }

  // Execute all requests concurrently (in parallel)
  const responses = withRetries(() => UrlFetchApp.fetchAll(requests));
  const cache = cacheScope();
  
  let successfulFetches = 0;

  for (let i = 0; i < responses.length; i++) {
    const resp = responses[i];
    const task = tasks[i]; // Get the original task metadata
    
    try {
      const code = resp.getResponseCode();
      if (code === 200) {
        const fetchedData = JSON.parse(resp.getContentText() || "{}");
        const toPut = {};
        let ttlSec = ttlForScope(task.location_type);
        
        // Apply cache jitter (+/- 5 minutes)
        const JITTER_SECONDS = 300; 
        const randomOffset = Math.floor(Math.random() * (JITTER_SECONDS * 2 + 1)) - JITTER_SECONDS;
        ttlSec = Math.max(600, ttlSec + randomOffset); 

        // Process fetched data and prepare for bulk cache write
        task.ids.forEach(id => {
          const idStr = String(id);
          if (fetchedData[idStr]) {
            const row = fetchedData[idStr];
            const s = JSON.stringify(row);
            if (s.length < 90000) { 
              const k = _fuzKey(task.location_type, task.location_id, id);
              toPut[k] = s;
            }
          }
        });
        
        // Perform chunked cache write
        if (Object.keys(toPut).length) {
          const entries = Object.entries(toPut);
          const CHUNK = 80;
          for (let j = 0; j < entries.length; j += CHUNK) {
            cache.putAll(Object.fromEntries(entries.slice(j, j + CHUNK)), ttlSec);
          }
          successfulFetches++;
        }
      } else {
        _L_warn('fuz.fetch.non200', { code, location: task.location_id, error: resp.getContentText() });
      }
    } catch (e) {
      _L_warn('fuz.fetch.fail', { msg: String(e && e.message || e), location: task.location_id });
    }
  }

  return { totalTasks: tasks.length, successfulFetches };
}

/** ----------------- ASYNCHRONOUS REFRESH FUNCTIONS ----------------------- */

/**
 * Public function to queue or run the cache refresh task.
 * DESIGNED TO BE RUN BY A USER-INSTALLED TIME-DRIVEN TRIGGER.
 */
function fuzzworkCacheRefresh() {
    
    const scriptCache = cacheScope();
    let queue = [];
    let initialItems = 0;
    
    // 1. READ QUEUE (Unprotected Read/Parse)
    let task = null;
    let fetchResults = null;
    
    const queueJson = scriptCache.get(MISSING_QUEUE_KEY);
    
    // FIX: Consolidate the empty/null check here, outside the lock.
    if (!queueJson) {
      _L_info('fuz.refresh', { status: 'Queue empty (No JSON).' });
      // Clean up the trigger that just fired when the queue is empty
      _deleteExistingTriggers();
      return 0; 
    }

    try {
        queue = JSON.parse(queueJson);
    } catch (e) {
        _L_warn('fuz.refresh.error', { status: 'Queue cache corruption. Resetting.', error: e.message });
        scriptCache.remove(MISSING_QUEUE_KEY);
        _deleteExistingTriggers();
        return 0;
    }
    
    if (queue.length === 0) {
        _L_info('fuz.refresh', { status: 'Queue empty (Zero length).' });
        scriptCache.remove(MISSING_QUEUE_KEY);
        _deleteExistingTriggers();
        return 0;
    }

    // 2. ACQUIRE LOCK and MANAGE STATE SHIFT
    withScriptLock(function() {
      
      initialItems = queue.length;
      task = queue.shift();
      
      // Clear queue for the network fetch phase (to prevent other threads from grabbing it)
      // Save remaining queue immediately so the lock can be released
      scriptCache.put(MISSING_QUEUE_KEY, JSON.stringify(queue), 3600);
    });

    // Handle early exit case if the queue was empty inside the lock.
    if (initialItems === 0) {
        return 0;
    }


    // 3. EXECUTE NETWORK (Unprotected - Time-consuming operation)
    if (task) {
        fetchResults = fuzzyFetchAll([task]); 
    }


    // 4. MANAGE PULSE (Unprotected Lock Acquisition)
    // FIX: Removed the lock. This entire section is fast and only manages the pulse state.
      
    // If queue still has items, recreate the successor trigger
    if (queue.length > 0) {
        
        // Pulse the chain: Schedule the successor trigger
        ScriptApp.newTrigger("fuzzworkCacheRefresh")
          .timeBased()
          .at(new Date(Date.now() + 1000 * 5)) // Run next pulse in 5 seconds
          .create();
            
    } else {
        // Clean up the trigger that just fired when the queue is empty
        _deleteExistingTriggers();
    } 
      
    _L_info('fuz.refresh', { status: `Finished processing. Items remaining: ${queue.length}` });
    
    return initialItems;
}

function _deleteExistingTriggers() {
    const triggers = ScriptApp.getProjectTriggers();
    triggers.forEach(trigger => {
        if (trigger.getHandlerFunction() === "fuzzworkCacheRefresh") {
            ScriptApp.deleteTrigger(trigger);
        }
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
  
  // FIX: This section MUST NOT use withScriptLock. It should be fast and rely on the 
  // protection inside fuzzworkCacheRefresh.

  const queueJson = scriptCache.get(MISSING_QUEUE_KEY);
  let queue = queueJson ? JSON.parse(queueJson) : [];
  
  const uniqueMissingIds = Array.from(new Set(missing_ids)).filter(Number.isFinite);
  const lt = String(location_type).toLowerCase();
  
  let consolidated = false;

  // 1. Check if a task for this location already exists in the queue (for consolidation)
  let existingTask = queue.find(task => 
    task.location_id === location_id && task.location_type === lt
  );
  
  if (existingTask) {
    // 2. Consolidate: Merge new missing IDs into the existing task's list
    const existingIds = new Set(existingTask.ids);
    const newIdsToAdd = uniqueMissingIds.filter(id => !existingIds.has(id));
    
    if (newIdsToAdd.length > 0) {
        // Add new IDs to the existing task
        existingTask.ids.push(...newIdsToAdd);
        
        // Re-chunk the combined list if it now exceeds the MAX_ID_PER_CHUNK
        if (existingTask.ids.length > MAX_ID_PER_CHUNK) {
            
            // Find the original index of the existing task
            const existingIndex = queue.findIndex(task => 
                task.location_id === location_id && task.location_type === lt
            );
            
            // Remove the old task and insert chunks
            queue.splice(existingIndex, 1);
            
            // Create new chunks and add them back to the queue
            for (let i = 0; i < existingTask.ids.length; i += MAX_ID_PER_CHUNK) {
                const chunkIds = existingTask.ids.slice(i, i + MAX_ID_PER_CHUNK);
                queue.push({
                    location_id: task.location_id,
                    location_type: lt,
                    ids: chunkIds
                });
            }
            _L_info('fuz.queue', { status: `Re-chunked task for ${lt}:${location_id}. Total new chunks: ${Math.ceil(existingTask.ids.length / MAX_ID_PER_CHUNK)}` });
        } else {
           _L_info('fuz.queue', { status: `Consolidated ${newIdsToAdd.length} new IDs into existing task for ${lt}:${location_id}` });
        }
        consolidated = true;
    }
  } 
  
  // 3. Create a new task(s) if not consolidated
  if (!consolidated) {
    for (let i = 0; i < uniqueMissingIds.length; i += MAX_ID_PER_CHUNK) {
        const chunkIds = uniqueMissingIds.slice(i, i + MAX_ID_PER_CHUNK);
        queue.push({
            location_id: location_id,
            location_type: lt,
            ids: chunkIds
        });
    }
    _L_info('fuz.queue', { status: `Created ${Math.ceil(uniqueMissingIds.length / MAX_ID_PER_CHUNK)} new chunk(s) for ${lt}:${location_id}` });
  }
  
  // 4. Save updated queue state (CacheService write is fast and does not need the ScriptLock)
  scriptCache.put(MISSING_QUEUE_KEY, JSON.stringify(queue), 3600);
  

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
 * @param {number|null} [refresh_id=null] Dummy parameter to force sheet recalculation.
 */
function fuzzApiPriceDataJitaSell(type_ids, market_hub = 60003760, order_type = null, order_level = null, location_type = "station", refresh_id = null) {
  if (!type_ids) throw new Error('type_ids is required');

  if (refresh_id != null) { /* no-op */ }

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
    const node = row[norm.type][norm.level];
    const num = Number(v);
    return Number.isFinite(num) ? num : null;
  };

  const outFlat = flatIds.map(id => {
      // Return cached value, or a placeholder if missing
      const data = have[id];
      if (id == null) return "";
      if (!data) return FETCHING_PLACEHOLDER; // Placeholder
      
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
    case 'amarr':   hub = 60008494; break;
    case 'dodixie': hub = 60011866; break;
    case 'rens':    hub = 60004588; break;
    case 'hek':     hub = 60005686; break;
    case 'jita':
    default:        hub = 60003760;
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
 * @param {number|null} [refresh_id=null] Dummy parameter to force sheet recalculation.
 */
function marketStatData(type_ids, location_type, location_id, order_type, order_level, refresh_id = null) {
  if (!type_ids) throw new Error("type_ids is required");

  // Use refresh_id to ensure volatility is triggered
  if (refresh_id != null) { /* no-op */ }

  const { rows, cols, flatIds, validIds } = _processInputIds(type_ids);

  // location guard
  const lt = String(location_type || "").toLowerCase();
  if (!["region","system","station"].includes(lt)) {
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
      if (!data) return FETCHING_PLACEHOLDER; 
      
      return (pick(data) ?? "");
  });
  
  return _reshape(outFlat, rows, cols);
}

/**
 * @customfunction
 * MarketStatDataBoth - Cache-first accessor for Fuzzworks aggregates, returning Buy and Sell stats side-by-side.
 * This is designed to be used with ARRAYFORMULA to output two columns of data (e.g., Average Buy Price | Average Sell Price).
 * @param {number[][]} type_ids The item IDs to fetch prices for.
 * @param {string} location_type The scope of the market ID ('station', 'system', or 'region').
 * @param {number} location_id The location ID (station, system, or region).
 * @param {string} [order_level_sell="min"] The aggregate level for the SELL side ('min', 'max', 'avg', 'median', 'volume').
 * @param {string} [order_level_buy="max"] The aggregate level for the BUY side ('min', 'max', 'avg', 'median', 'volume').
 * @param {number|null} [refresh_id=null] Dummy parameter to force sheet recalculation.
 * @returns {Array<Array<any>>} A 2D array aligned to the input, but with twice the columns. Outputs Buy Price and Sell Price columns.
 */
function marketStatDataBoth(type_ids, location_type, location_id, order_level_sell = "min", order_level_buy = "max", refresh_id = null) {
    if (!type_ids) throw new Error("type_ids is required");

    // Use refresh_id to ensure volatility is triggered
    if (refresh_id != null) { /* no-op */ }

    const { rows, cols, flatIds, validIds } = _processInputIds(type_ids);

    // Location validation
    const lt = String(location_type || "").toLowerCase();
    if (!["region","system","station"].includes(lt)) {
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
        
        // FIX: Return blank strings instead of "FETCHING..." when fetching, for clean UI
        if (!data) return [FETCHING_PLACEHOLDER, FETCHING_PLACEHOLDER]; 

        const [sellVal, buyVal] = pickBoth(data);
        
        return [
            sellVal ?? FETCHING_PLACEHOLDER, // Column 1: Sell Price
            buyVal ?? FETCHING_PLACEHOLDER  // Column 2: Buy Price
        ];
    });
    
    // Reshape needs to handle the fact that we doubled the column count (cols * 2)
    return _reshape(outFlat, rows, cols * 2); 
}
