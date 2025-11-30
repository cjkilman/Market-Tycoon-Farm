/** ============================================================================
 * Fuzzworks Price Client + Cache Core (Apps Script)
 * - Complete module with Circuit Breaker, TTL Jitter, and Cache-first logic.
 * - Minimal external dependencies (only standard GAS APIs).
 * ========================================================================== */

// --- CORE DEPENDENCIES (Integrated) ---

/** Helper class to structure raw Fuzzworks API data for caching. */
class FuzDataObject {
    _normalizeNumber(value, defaultValue = 0) {
        const num = parseInt(value);
        return isNaN(num) ? defaultValue : num;
    }
    _normalizeFloat(value, defaultValue = "") {
        const num = parseFloat(value);
        return isNaN(num) ? defaultValue : num;
    }

    constructor(typeId, rawFuzData) {
        const buyData = rawFuzData?.buy || {};
        const sellData = rawFuzData?.sell || {};

        this.type_id = parseInt(typeId, 10);
        this.last_updated = new Date();

        this.buy = {
            avg: this._normalizeFloat(buyData.weightedAverage, ""),
            max: this._normalizeFloat(buyData.max, ""),
            min: this._normalizeFloat(buyData.min, ""),
            stddev: this._normalizeFloat(buyData.stddev, ""),
            median: this._normalizeFloat(buyData.median, ""),
            volume: this._normalizeNumber(buyData.volume, 0),
            orderCount: this._normalizeNumber(buyData.orderCount, 0)
        };

        this.sell = {
            avg: this._normalizeFloat(sellData.weightedAverage, ""),
            max: this._normalizeFloat(sellData.max, ""),
            min: this._normalizeFloat(sellData.min, ""),
            stddev: this._normalizeFloat(sellData.stddev, ""),
            median: this._normalizeFloat(sellData.median, ""),
            volume: this._normalizeNumber(sellData.volume, 0),
            orderCount: this._normalizeNumber(sellData.orderCount, 0)
        };
    }
}




// --- UTILITIES (for Custom Functions) ---

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
  const validLevels = ["min","max","avg","median","volume","ordercount"];
  if (level === 'orders' || level === 'ordercount' || level === 'numorders') level = 'orderCount';
  
  if (!validTypes.includes(type))  throw new Error("order_type must be 'buy' or 'sell'");
  if (!validLevels.includes(level)) throw new Error("order_level must be one of 'min','max','avg','median','volume','ordercount'");
  return { type, level };
}

function _hubToStationId_(hub) {
  if (hub == null || hub === '') return 60003760;
  const n = Number(hub);
  if (Number.isFinite(n)) return n;
  const s = String(hub).toLowerCase();
  if (s.indexOf('amarr') > -1) return 60008494;
  if (s.indexOf('dodixie') > -1) return 60011866;
  if (s.indexOf('rens') > -1) return 60004588;
  if (s.indexOf('hek') > -1) return 60005686;
  return 60003760;
}

/** Extracts a specific metric from a FuzDataObject. */
function _extractMetric_(fuzObject, side, field) {
  if (!fuzObject || typeof fuzObject !== 'object') return "";
  const node = fuzObject[side];
  if (!node || typeof node !== 'object') return "";
  const raw = node[field];
  
  if ((field === 'volume' || field === 'orderCount') &&
    (raw === null || raw === undefined || raw === "")) return 0;

  if (raw === null || raw === undefined || raw === "") return "";
  const n = Number(raw);
  return Number.isFinite(n) ? n : "";
}


// --- FuzAPI CORE MODULE ---

const fuzAPI = (() => {
  const _cache = CacheService.getScriptCache();
  const FUZ_CACHE_VER = 1;
  const CACHE_CHUNK_SIZE = 8000;

  const CIRCUIT_PROPS = {
    STATE: 'FuzCircuitState',
    FAIL_COUNT: 'FuzCircuitFailCount',
    OPEN_UNTIL: 'FuzCircuitOpenUntilMs'
  };
  const CIRCUIT_THRESHOLD = 3;
  const CIRCUIT_COOLDOWN_MS = 60 * 60 * 1000;
  const _props = PropertiesService.getScriptProperties();

function withRetries(fn, tries = 3, base = 300) {
      var retryPattern = /(?:\b(429|420|5\d\d)\b|dns|socket|ssl|handsh|timeout|temporar|rate|quota|Service invoked|empty-200|bad[-\s]?json)/i;
      var lastErr;
      for (var i = 0; i < tries; i++) {
          try {
              var res = fn();
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
              Utilities.sleep(base * Math.pow(2, i) + Math.floor(Math.random() * 200));
          }
      }
      throw lastErr;
  }

  function _isCircuitOpen() {
    const state = _props.getProperty(CIRCUIT_PROPS.STATE);
    if (state === 'OPEN') {
      const openUntil = parseInt(_props.getProperty(CIRCUIT_PROPS.OPEN_UNTIL) || '0', 10);
      if (Date.now() < openUntil) return true;
      _props.setProperty(CIRCUIT_PROPS.STATE, 'HALF_OPEN');
    }
    return false;
  }
  function _tripCircuit(error) {
    const failCount = parseInt(_props.getProperty(CIRCUIT_PROPS.FAIL_COUNT) || '0', 10) + 1;
    _props.setProperty(CIRCUIT_PROPS.FAIL_COUNT, String(failCount));

    if (failCount >= CIRCUIT_THRESHOLD) {
      const openUntil = Date.now() + CIRCUIT_COOLDOWN_MS;
      _props.setProperties({
        [CIRCUIT_PROPS.STATE]: 'OPEN',
        [CIRCUIT_PROPS.OPEN_UNTIL]: String(openUntil)
      });
      console.error(`Circuit Breaker TRIPPED: ${error}`);
    } else {
      console.warn(`Circuit Breaker failure count: ${failCount}/${CIRCUIT_THRESHOLD}.`);
    }
  }
  function _resetCircuit() {
    const state = _props.getProperty(CIRCUIT_PROPS.STATE);
    if (state === 'OPEN' || state === 'HALF_OPEN' || _props.getProperty(CIRCUIT_PROPS.FAIL_COUNT) !== null) {
      _props.deleteProperty(CIRCUIT_PROPS.FAIL_COUNT);
      _props.deleteProperty(CIRCUIT_PROPS.OPEN_UNTIL);
      _props.setProperty(CIRCUIT_PROPS.STATE, 'CLOSED');
    }
  }
  function _fuzKey(location_type, location_id, type_id) {
    return `fuz:${FUZ_CACHE_VER}:${location_type}:${location_id}:${type_id}`;
  }
  function _groupRequestsByLocation(missingRequests) {
    const grouped = {};
    missingRequests.forEach(req => {
      const type_id_num = Number(req.type_id);
      const market_id_num = Number(req.market_id);
      if (type_id_num > 0 && market_id_num > 0) {
        const groupKey = `${req.market_type}_${market_id_num}`;
        if (!grouped[groupKey]) grouped[groupKey] = { locationId: market_id_num, locationType: req.market_type, items: new Set() };
        grouped[groupKey].items.add(type_id_num);
      }
    });
    Object.values(grouped).forEach(group => { group.items = Array.from(group.items); });
    return grouped;
  }
  function _buildFetchAllRequests(groupedCalls) {
    const requests = [];
    for (const key in groupedCalls) {
      const call = groupedCalls[key];
      if (!call.items || call.items.length === 0) continue;

      const url = "https://market.fuzzwork.co.uk/aggregates/";
      const payload = { [call.locationType]: call.locationId, types: call.items.join(",") };

      requests.push({
        url: url,
        method: 'post',
        contentType: 'application/json',
        payload: JSON.stringify(payload),
        muteHttpExceptions: true,
        headers: { 'Accept': 'application/json' },
        fuz_context: { locationId: call.locationId, locationType: call.locationType, requestedIds: new Set(call.items) }
      });
    }
    return requests;
  }
  function _executeFetchAll(tasksToFetch) {
    if (!tasksToFetch || tasksToFetch.length === 0) return { newlyFetchedData: [], dataToCache: {} };

    const groupedCalls = _groupRequestsByLocation(tasksToFetch);
    const fetchRequests = _buildFetchAllRequests(groupedCalls);
    if (fetchRequests.length === 0) return { newlyFetchedData: [], dataToCache: {} };

    let responses;
    try {
      responses = withRetries(() => { return UrlFetchApp.fetchAll(fetchRequests); });
      _resetCircuit();
    } catch (e) {
      _tripCircuit(e.message);
      const dataToCache = {};
      tasksToFetch.forEach(req => {
        dataToCache[_fuzKey(req.market_type, req.market_id, req.type_id)] = "null";
      });
      return { newlyFetchedData: [], dataToCache };
    }

    const dataToCache = {};
    const processedDataByLocation = {};
    let apiErrorOccurred = false;

    responses.forEach((response, index) => {
      const originalRequestContext = fetchRequests[index].fuz_context;
      if (!originalRequestContext) return;
      const { locationId, locationType, requestedIds } = originalRequestContext;

      if (response.getResponseCode() === 200) {
        const parsed = JSON.parse(response.getContentText() || "{}");
        const locationKey = `${locationType}_${locationId}`;
        const receivedIds = new Set(Object.keys(parsed).map(Number));

        if (!processedDataByLocation[locationKey]) processedDataByLocation[locationKey] = { market_type: locationType, market_id: locationId, fuzObjects: [] };

        receivedIds.forEach(typeIdNum => {
          const rawItemData = parsed[String(typeIdNum)];
          const dataObject = new FuzDataObject(typeIdNum, rawItemData);
          processedDataByLocation[locationKey].fuzObjects.push(dataObject);
          dataToCache[_fuzKey(locationType, locationId, typeIdNum)] = JSON.stringify(dataObject);
        });

        requestedIds.forEach(requestedIdNum => {
          if (!receivedIds.has(requestedIdNum)) {
            dataToCache[_fuzKey(locationType, locationId, requestedIdNum)] = "null";
          }
        });
      } else {
        apiErrorOccurred = true;
        requestedIds.forEach(requestedIdNum => {
          dataToCache[_fuzKey(locationType, locationId, requestedIdNum)] = "null";
        });
      }
    });

    if (apiErrorOccurred) _tripCircuit(`API failed for one or more batches.`);
    return { newlyFetchedData: Object.values(processedDataByLocation), dataToCache };
  }
  function _cacheNewData(dataToCache) {
    const cacheKeys = Object.keys(dataToCache);
    if (cacheKeys.length === 0) return;

    const baseTtl = 1800;
    const JITTER_SECONDS = 300;
    const minTtl = 600;

    const randomOffset = Math.floor(Math.random() * (JITTER_SECONDS * 2 + 1)) - JITTER_SECONDS;
    const jitteredTtl = Math.max(minTtl, baseTtl + randomOffset);

    if (cacheKeys.length > CACHE_CHUNK_SIZE) {
      for (let i = 0; i < cacheKeys.length; i += CACHE_CHUNK_SIZE) {
        const chunkKeys = cacheKeys.slice(i, i + CACHE_CHUNK_SIZE);
        const chunkCacheObject = {};
        chunkKeys.forEach(key => chunkCacheObject[key] = dataToCache[key]);
        try { _cache.putAll(chunkCacheObject, jitteredTtl); } catch (e) { console.error(`Cache chunk failed: ${e.message}`) };
        Utilities.sleep(50);
      }
    } else {
      try { _cache.putAll(dataToCache, jitteredTtl); } catch (e) { console.error(`Cache putAll failed: ${e.message}`) };
    }
  }
  function _checkCacheForRequests(marketRequests) {
    const requiredKeys = marketRequests.map(req => _fuzKey(req.market_type, req.market_id, req.type_id));
    const cachedResults = _cache.getAll(requiredKeys) || {};
    let cachedData = [];
    const missingRequests = [];
    const tempGroupedCache = {};

    marketRequests.forEach((req, index) => {
      const key = requiredKeys[index];
      const cacheValue = cachedResults[key];
      const locationKey = `${req.market_type}_${req.market_id}`;

      if (!tempGroupedCache[locationKey]) tempGroupedCache[locationKey] = { market_type: req.market_type, market_id: req.market_id, fuzObjects: [] };

      if (cacheValue === "null") {
        const emptyMarketData = { avg: '', max: '', min: '', stddev: '', median: '', volume: 0, orderCount: 0 };
        tempGroupedCache[locationKey].fuzObjects.push(new FuzDataObject(req.type_id, {
            buy: emptyMarketData,
            sell: emptyMarketData
        }));
      } else if (cacheValue) {
        try {
          const itemData = JSON.parse(cacheValue);
          tempGroupedCache[locationKey].fuzObjects.push(itemData);
        } catch (e) {
          missingRequests.push(req);
        }
      } else {
        missingRequests.push(req);
      }
    });

    cachedData = Object.values(tempGroupedCache);
    return { cachedData, missingRequests };
  }

  function getDataForRequests(marketRequests) {
    if (!marketRequests || marketRequests.length === 0) return [];
    if (_isCircuitOpen()) return [];

    const { cachedData, missingRequests } = _checkCacheForRequests(marketRequests);

    let newlyFetchedData = [];
    if (missingRequests.length > 0) {
      const fetchResult = _executeFetchAll(missingRequests);
      newlyFetchedData = fetchResult.newlyFetchedData;
      _cacheNewData(fetchResult.dataToCache);
    }

    const finalDataMap = {};
    cachedData.forEach(crate => {
      const key = `${crate.market_type}_${crate.market_id}`;
      finalDataMap[key] = crate;
    });
    newlyFetchedData.forEach(newCrate => {
      const key = `${newCrate.market_type}_${newCrate.market_id}`;
      if (finalDataMap[key]) finalDataMap[key].fuzObjects.push(...newCrate.fuzObjects);
      else finalDataMap[key] = newCrate;
    });

    return Object.values(finalDataMap);
  }

  function requestItems(market_id, market_type, type_ids) {
    if (!Array.isArray(type_ids)) type_ids = [type_ids];
    const requests = type_ids
      .map(id => Number(id))
      .filter(id => !isNaN(id) && id > 0)
      .map(id => ({ type_id: id, market_id: Number(market_id), market_type: market_type }));

    if (requests.length === 0) return [];

    const marketDataCrates = getDataForRequests(requests);
    const targetCrate = marketDataCrates.find(crate =>
      crate.market_type === market_type && crate.market_id === Number(market_id)
    );

    return targetCrate ? targetCrate.fuzObjects : [];
  }

  function cacheRefresh() {
    // MODIFIED: Stubbed out to remove dependency on getMasterBatchFromControlTable
    console.log("fuzAPI: Initiating cache refresh. Call to sheet reader is intentionally bypassed.");
  }

  return {
    getDataForRequests: getDataForRequests,
    requestItems: requestItems,
    cacheRefresh: cacheRefresh
  };

})();


// --- Public Custom Functions (Wrappers for Google Sheets) ---

/**
 * Generic API to get prices for an array/range of type_ids at a location.
 * Preserves the input shape (rows x cols).
 * @customfunction
 */
function marketStatData(type_ids, location_type, location_id, order_type, order_level) {
  if (!type_ids) throw new Error("type_ids is required");

  const in2D = _as2D(type_ids);
  const rows = in2D.length, cols = in2D[0].length;

  const flatIds = _flatten2D(in2D).map(v => {
    const n = Number(v);
    return Number.isFinite(n) ? n : null;
  });

  const lt = String(location_type || "").toLowerCase();
  if (!["region","system","station"].includes(lt)) {
    throw new Error("Location Undefined (use 'region', 'system', or 'station')");
  }

  const { type: side, level: lvl } = _normalizeOrder(order_type, order_level);
  const validIds = flatIds.filter(n => n != null);

  const results = fuzAPI.requestItems(Number(location_id), lt, validIds);
  const resultsMap = new Map(results.map(o => [o.type_id, o]));
  
  const outFlat = flatIds.map(id => {
    const fuzObject = resultsMap.get(id);
    return (id == null || !fuzObject) ? "" : _extractMetric_(fuzObject, side, lvl);
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

  const hubId = _hubToStationId_(market_hub);
  
  return marketStatData(type_ids, "station", hubId, order_type, order_level);
}

/**
 * Generic API to get prices for an array/range of type_ids at a station id (default Jita).
 * Defaults to sell/min if not specified.
 * Preserves the input shape (rows x cols).
 * @customfunction
 */
function fuzzApiPriceDataJitaSell(type_ids, market_hub = 60003760, order_type = null, order_level = null) {
  const hubId = _hubToStationId_(market_hub); 
  return marketStatData(type_ids, "station", hubId, order_type, order_level);
}


// --- Stubbed Functions (Compatibility with MarketFetcherEsi.js and Sheets) ---

function marketStatDataCache(type_ids, location_type, location_id, order_type, order_level) { 
  console.warn("marketStatDataCache is deprecated. Falling back to marketStatData.");
  return marketStatData(type_ids, location_type, location_id, order_type, order_level);
}

function marketStatDataBoth(type_ids, location_type, location_id, order_level) {
  console.warn("marketStatDataBoth is deprecated. Use two separate calls to marketStatData.");
  const in2D = _as2D(type_ids);
  return in2D.map(row => ["", ""]);
}

function marketStatDataBothCache(type_ids, location_type, location_id, order_level) {
  console.warn("marketStatDataBothCache is deprecated. Use two separate calls to marketStatData.");
  const in2D = _as2D(type_ids);
  return in2D.map(row => ["", ""]);
}

/**
 * NEW: Wrapper function required by MarketFetcher.gs.js to fetch prices.
 * Returns a map where the value for each type_id is an object containing the
 * four required price points (minSell, maxBuy, medianSell, medianBuy).
 *
 * REFACTORED: Now returns the *entire* FuzDataObject (fuzObject) for
 * each type_id, allowing the caller to access all nested data (prices,
 * order counts, etc.).
 */
function getMarketPrices(type_ids, market_id, market_type) {
  // Call the core API logic
  const results = fuzAPI.requestItems(market_id, market_type, type_ids);
  const priceMap = {};

  results.forEach(fuzObject => {
    // Use the type_id from the object as the key
    const typeId = fuzObject.type_id;

    // Assign the *entire* fuzObject as the value
    priceMap[typeId] = fuzObject;
  });

  return priceMap;
}