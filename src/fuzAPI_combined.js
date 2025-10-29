/**
 * The master Facade and Engine for the EVE Market Data system.
 * This single module handles state, caching, API calls, and provides the public interface.
 * Includes negative caching and TTL jitter.
 */
const fuzAPI = (() => {

  // --- PRIVATE ENGINE COMPONENTS (Internal State & Logic) ---
  const _cache = CacheService.getScriptCache();

  const FUZ_CACHE_VER = 1; // Change this version to invalidate all old cache keys if logic changes
  const CACHE_CHUNK_SIZE = 8000; // Chunk size for putAll

  // In src/fuzAPI_combined.js (after CACHE_CHUNK_SIZE definition)

  const CIRCUIT_PROPS = {
    STATE: 'FuzCircuitState',
    FAIL_COUNT: 'FuzCircuitFailCount',
    OPEN_UNTIL: 'FuzCircuitOpenUntilMs'
  };
  const CIRCUIT_THRESHOLD = 3;             // Number of consecutive failures before opening
  const CIRCUIT_COOLDOWN_MS = 60 * 60 * 1000; // 60 minutes OPEN time

  // Use Script Properties to manage persistent state
  const _props = PropertiesService.getScriptProperties();

  /** Internal helper to check the current state of the circuit. */
  function _isCircuitOpen() {
    const state = _props.getProperty(CIRCUIT_PROPS.STATE);
    if (state === 'OPEN') {
      const openUntil = parseInt(_props.getProperty(CIRCUIT_PROPS.OPEN_UNTIL) || '0', 10);
      if (Date.now() < openUntil) {
        console.warn(`Circuit Breaker is OPEN. Blocking requests until ${new Date(openUntil).toLocaleString()}.`);
        return true;
      }
      // Cooldown period expired, transition to HALF-OPEN
      console.log("Circuit Breaker cooldown expired. Transitioning to HALF-OPEN.");
      _props.setProperty(CIRCUIT_PROPS.STATE, 'HALF_OPEN');
    }
    return false;
  }

  /** Internal helper to trip the circuit to the OPEN state. */
  function _tripCircuit(error) {
    const failCount = parseInt(_props.getProperty(CIRCUIT_PROPS.FAIL_COUNT) || '0', 10) + 1;
    _props.setProperty(CIRCUIT_PROPS.FAIL_COUNT, String(failCount));

    if (failCount >= CIRCUIT_THRESHOLD) {
      const openUntil = Date.now() + CIRCUIT_COOLDOWN_MS;
      _props.setProperties({
        [CIRCUIT_PROPS.STATE]: 'OPEN',
        [CIRCUIT_PROPS.OPEN_UNTIL]: String(openUntil)
      });
      console.error(`Circuit Breaker TRIPPED to OPEN state. Failures exceeded threshold (${CIRCUIT_THRESHOLD}). Blocking API calls for 60 minutes.`);
    } else {
      console.warn(`Circuit Breaker failure count: ${failCount}/${CIRCUIT_THRESHOLD}. Error: ${error}`);
    }
  }

  /** Internal helper to reset the circuit after success. */
  function _resetCircuit() {
    const state = _props.getProperty(CIRCUIT_PROPS.STATE);
    if (state === 'OPEN' || state === 'HALF_OPEN' || _props.getProperty(CIRCUIT_PROPS.FAIL_COUNT) !== null) {
      console.log("Circuit Breaker reset to CLOSED state.");
      _props.deleteProperty(CIRCUIT_PROPS.FAIL_COUNT);
      _props.deleteProperty(CIRCUIT_PROPS.OPEN_UNTIL);
      _props.setProperty(CIRCUIT_PROPS.STATE, 'CLOSED');
    }
  }
  // ... rest of the IIFE ...

  // --- Internal State Management Helpers ---
  function _getItemKey(item) { return `${item.type_id}-${item.market_id}-${item.market_type}`; }


  // --- Internal Task Processing Helpers ---
  function _fuzKey(location_type, location_id, type_id) {
    return `fuz:${FUZ_CACHE_VER}:${location_type}:${location_id}:${type_id}`;
  }

  /**
   * Groups requests by location to prepare for batch API calls.
   * @param {Array<Object>} missingRequests - Array of request objects {type_id, market_id, market_type}
   * @returns {Object} Grouped requests, e.g., {"station_60003760": {locationId, locationType, items: Set<number>}}
   */
  function _groupRequestsByLocation(missingRequests) {
    const grouped = {};
    missingRequests.forEach(req => {
      // Ensure IDs are numbers before grouping
      const type_id_num = Number(req.type_id);
      const market_id_num = Number(req.market_id);

      if (!isNaN(type_id_num) && !isNaN(market_id_num) && type_id_num > 0 && market_id_num > 0) {
        const groupKey = `${req.market_type}_${market_id_num}`;
        if (!grouped[groupKey]) {
          grouped[groupKey] = {
            locationId: market_id_num,
            locationType: req.market_type,
            items: new Set() // Use a Set for unique type_ids per location
          };
        }
        grouped[groupKey].items.add(type_id_num); // Add only the type_id to the Set
      } else {
        console.warn(`Skipping invalid request in _groupRequestsByLocation:`, req);
      }
    });

    // Convert Set of IDs back to Array for the next step
    Object.values(grouped).forEach(group => {
      group.items = Array.from(group.items); // Convert Set to Array for API call
    });

    return grouped;
  }

  /**
   * Builds the array of UrlFetchApp request objects.
   * @param {Object} groupedCalls - The output from _groupRequestsByLocation
   * @returns {Array<Object>} Array of request objects for UrlFetchApp.fetchAll
   */
  function _buildFetchAllRequests(groupedCalls) {
    const requests = [];

    // Iterate through each market destination
    for (const key in groupedCalls) {
      const call = groupedCalls[key]; // { locationId, locationType, items: Array<number> }

      if (!call.items || call.items.length === 0) {
        console.warn(`Skipping build request for ${key}: No valid items.`);
        continue;
      }

      const url = "https://market.fuzzwork.co.uk/aggregates/";
      const payload = {
        [call.locationType]: call.locationId, // Dynamic key (station, region, or system)
        types: call.items.join(",") // API expects comma-separated string of type_ids
      };

      requests.push({
        url: url,
        method: 'post',
        contentType: 'application/json',
        payload: JSON.stringify(payload),
        muteHttpExceptions: true,
        headers: { 'Accept': 'application/json' },
        // Include context for mapping response back AND for negative caching
        fuz_context: {
          locationId: call.locationId,
          locationType: call.locationType,
          requestedIds: new Set(call.items) // Store requested IDs as a Set for quick lookup
        }
      });
    }
    console.log(`Built ${requests.length} POST requests for ${Object.keys(groupedCalls).length} markets.`);
    return requests;
  }

  /**
   * Fetches data, prepares positive and negative cache entries.
   * @param {Array<Object>} tasksToFetch - Array of request objects {type_id, market_id, market_type}
   * @returns {Object} { newlyFetchedData: Array<Object>, dataToCache: Object }
   */
  function _executeFetchAll(tasksToFetch) {
    if (!tasksToFetch || tasksToFetch.length === 0) {
      console.log("_executeFetchAll: No tasks to fetch.");
      return { newlyFetchedData: [], dataToCache: {} };
    }

    const groupedCalls = _groupRequestsByLocation(tasksToFetch);
    const fetchRequests = _buildFetchAllRequests(groupedCalls);

    if (fetchRequests.length === 0) {
      console.log("_executeFetchAll: No valid fetch requests built.");
      return { newlyFetchedData: [], dataToCache: {} };
    }

    let responses;
    try {
      responses = withRetries(() => {
        console.log(`Attempting to fetch data via POST for ${fetchRequests.length} requests...`);
        return UrlFetchApp.fetchAll(fetchRequests);
      });
      // --- CIRCUIT BREAKER SUCCESS ---
      _resetCircuit();
      // -------------------------------
    } catch (e) {
      console.error(`_executeFetchAll failed after multiple retries: ${e.message}`);
      // On total failure, we can still negatively cache all items that were attempted
      const dataToCache = {};
      tasksToFetch.forEach(req => {
        const cacheKey = _fuzKey(req.market_type, req.market_id, req.type_id);
        dataToCache[cacheKey] = "null"; // Negative cache all on total fetch failure
      });
      console.warn(`Marking all ${tasksToFetch.length} requested items as negative cache due to fetchAll failure.`);
      return { newlyFetchedData: [], dataToCache };
    }

    const dataToCache = {}; // Will contain both positive (JSON string) and negative ("null") entries
    const processedDataByLocation = {};
    let positiveCacheCount = 0;
    let negativeCacheCount = 0;

    responses.forEach((response, index) => {
      const originalRequestContext = fetchRequests[index].fuz_context;
      if (!originalRequestContext) {
        console.error(`_executeFetchAll: Could not retrieve context for response index ${index}. Skipping.`);
        return; // Skip processing this response
      }
      const { locationId, locationType, requestedIds } = originalRequestContext; // requestedIds is a Set

      if (response.getResponseCode() === 200) {
        const parsed = JSON.parse(response.getContentText() || "{}"); // Ensure valid JSON
        const locationKey = `${locationType}_${locationId}`;
        // Get type_ids returned by the API, converting keys to numbers
        const receivedIds = new Set(Object.keys(parsed).map(Number));

        if (!processedDataByLocation[locationKey]) {
          processedDataByLocation[locationKey] = { market_type: locationType, market_id: locationId, fuzObjects: [] };
        }

        // --- Process Received Data (Positive Cache) ---
        receivedIds.forEach(typeIdNum => {
          const rawItemData = parsed[String(typeIdNum)]; // Access object using string key
          const dataObject = new FuzDataObject(typeIdNum, rawItemData); // Use Helper class
          processedDataByLocation[locationKey].fuzObjects.push(dataObject);

          const cacheKey = _fuzKey(locationType, locationId, typeIdNum);
          dataToCache[cacheKey] = JSON.stringify(dataObject); // Store the processed object
          positiveCacheCount++;
        });

        // --- Identify Missing Items (Negative Cache) ---
        // For each ID we requested...
        requestedIds.forEach(requestedIdNum => {
          // If the API did *not* include it in the response...
          if (!receivedIds.has(requestedIdNum)) {
            // Store "null" as the cache value
            const cacheKey = _fuzKey(locationType, locationId, requestedIdNum);
            dataToCache[cacheKey] = "null"; // Mark for negative caching
            negativeCacheCount++;
          }
        });

      } else {
        // --- Handle API Errors (Negative Cache All Requested for this batch) ---
        console.error(`API Error: Status ${response.getResponseCode()} for ${locationType}:${locationId}. Marking ${requestedIds.size} items as negative.`);
        requestedIds.forEach(requestedIdNum => {
          const cacheKey = _fuzKey(locationType, locationId, requestedIdNum);
          dataToCache[cacheKey] = "null"; // Negative cache on API error
          negativeCacheCount++;
        });
      }
    }); // End responses.forEach

    console.log(`Fetch complete. Positive items to cache: ${positiveCacheCount}. Negative items to cache: ${negativeCacheCount}.`);
    return { newlyFetchedData: Object.values(processedDataByLocation), dataToCache };
  }

  /**
   * Caches new data with added TTL jitter. Applies to both positive and negative entries.
   * @param {Object} dataToCache - Object mapping cache keys to stringified data (or "null").
   */
  function _cacheNewData(dataToCache) {
    const cacheKeys = Object.keys(dataToCache);
    if (cacheKeys.length > 0) {
      console.log(`Caching ${cacheKeys.length} new items (positive and negative)...`);

      // --- Jitter Logic ---
      const baseTtl = 1800; // Base TTL in seconds (30 minutes)
      const JITTER_SECONDS = 300; // +/- 5 minutes
      const minTtl = 600; // Minimum 10 minutes TTL

      // Calculate a single jittered TTL to apply to all chunks in this run
      const randomOffset = Math.floor(Math.random() * (JITTER_SECONDS * 2 + 1)) - JITTER_SECONDS;
      const jitteredTtl = Math.max(minTtl, baseTtl + randomOffset);

      // Note: We use the same jitteredTtl for both positive and negative entries.
      // If you wanted a *different* TTL for negative, you'd separate them here.
      // For example, negative entries could get `baseTtlNegative + randomOffset`.
      // For simplicity, we use one TTL for all new entries in this batch.
      console.log(`Applying jittered TTL: ${jitteredTtl} seconds to all entries (Base: ${baseTtl}s, Offset: ${randomOffset}s)`);
      // --- End Jitter Logic ---

      if (cacheKeys.length > CACHE_CHUNK_SIZE) {
        console.log(`Cache size exceeds threshold (${CACHE_CHUNK_SIZE}). Chunking putAll...`);
        for (let i = 0; i < cacheKeys.length; i += CACHE_CHUNK_SIZE) {
          const chunkKeys = cacheKeys.slice(i, i + CACHE_CHUNK_SIZE);
          const chunkCacheObject = {};
          chunkKeys.forEach(key => chunkCacheObject[key] = dataToCache[key]);
          try {
            _cache.putAll(chunkCacheObject, jitteredTtl); // Use jittered TTL
          } catch (e) {
            console.error(`Cache chunk failed: ${e.message}`)
          };
          console.log(`Cached chunk ${Math.floor(i / CACHE_CHUNK_SIZE) + 1}...`);
          Utilities.sleep(50);
        }
      } else {
        try {
          _cache.putAll(dataToCache, jitteredTtl); // Use jittered TTL
        } catch (e) {
          console.error(`Cache putAll failed: ${e.message}`)
        };
      }
      console.log("Caching complete.");
    }
  }

  /**
   * Checks cache, recognizing negative entries ("null").
   * @param {Array<Object>} marketRequests - Array of request objects {type_id, market_id, market_type}
   * @returns {Object} { cachedData: Array<Object>, missingRequests: Array<Object> }
   */
  function _checkCacheForRequests(marketRequests) {
    const requiredKeys = marketRequests.map(req => _fuzKey(req.market_type, req.market_id, req.type_id));
    const cachedResults = _cache.getAll(requiredKeys) || {}; // Ensure it's an object
    let cachedData = []; // Holds structured positive data
    const missingRequests = []; // Holds requests needing API fetch
    const tempGroupedCache = {}; // Groups positive results by location
    let negativeHitCount = 0;
    let positiveHitCount = 0;

    marketRequests.forEach((req, index) => {
      const key = requiredKeys[index]; // Use pre-calculated key
      const cacheValue = cachedResults[key];

      if (cacheValue === "null") {
        // --- Negative Cache Hit ---
        // Data is confirmed not to exist. Do not add to missingRequests.
        negativeHitCount++;

        // Define the structure of an empty price/volume object
        const emptyMarketData = {
          avg: '',
          max: '',
          min: '',
          stddev: '',
          median: '',
          volume: 0,
          orderCount: 0
        };

        // Group the placeholder data by location
        const locationKey = `${req.market_type}_${req.market_id}`;
        if (!tempGroupedCache[locationKey]) {
          tempGroupedCache[locationKey] = { market_type: req.market_type, market_id: req.market_id, fuzObjects: [] };
        }

        // Push the full placeholder object, mimicking the FuzDataObject structure
        tempGroupedCache[locationKey].fuzObjects.push({
          type_id: req.type_id,
          last_updated: new Date(), // Required property matching FuzDataObject
          buy: emptyMarketData,
          sell: emptyMarketData,
        });

      } else if (cacheValue) {
        // --- Positive Cache Hit ---
        try {
          const itemData = JSON.parse(cacheValue);
          // Group positive data for the final result structure
          const locationKey = `${req.market_type}_${req.market_id}`;
          if (!tempGroupedCache[locationKey]) {
            tempGroupedCache[locationKey] = { market_type: req.market_type, market_id: req.market_id, fuzObjects: [] };
          }
          tempGroupedCache[locationKey].fuzObjects.push(itemData);
          positiveHitCount++;
        } catch (e) {
          // Treat parse failure as missing, needs refetch
          console.warn(`Cache parse error for key ${key}. Marking as missing.`);
          missingRequests.push(req);
        }
      } else {
        // --- Cache Miss ---
        // cacheValue is null or undefined
        missingRequests.push(req);
      }
    });

    cachedData = Object.values(tempGroupedCache); // Final array of positive grouped data
    console.log(`[Cache Check] Requests: ${marketRequests.length}. Positive Hits: ${positiveHitCount}. Negative Hits: ${negativeHitCount}. Missing: ${missingRequests.length}`);
    return { cachedData, missingRequests };
  }


  // --- PUBLIC FACADE METHODS ---

  /**
   * Main public function to get data, uses cache-first approach with negative caching.
   * @param {Array<Object>} marketRequests - Array of request objects {type_id, market_id, market_type}
   * @returns {Array<Object>} Array of grouped market data ("crates")
   */
  function getDataForRequests(marketRequests) {
    if (!marketRequests || !Array.isArray(marketRequests) || marketRequests.length === 0) {
      console.warn("getDataForRequests called with invalid input.");
      return [];
    }
    console.log(`fuzAPI: Received ${marketRequests.length} market requests.`);

    // --- CIRCUIT BREAKER CHECK (Pre-fetch) ---
    if (_isCircuitOpen()) {
      console.warn("getDataForRequests skipped due to OPEN Circuit Breaker.");
      // Must return an empty set to prevent the worker from trying to write.
      return [];
    }

    // 1. Check cache (handles positive and negative hits)
    const { cachedData, missingRequests } = _checkCacheForRequests(marketRequests);

    // 2. Fetch missing data (if any)
    let newlyFetchedData = [];
    let dataToCache = {};
    if (missingRequests.length > 0) {
      const fetchResult = _executeFetchAll(missingRequests);
      newlyFetchedData = fetchResult.newlyFetchedData;
      dataToCache = fetchResult.dataToCache;
      // 3. Cache new data (positive and negative)
      _cacheNewData(dataToCache);
    } else {
      console.log("fuzAPI: All requests served from cache (positive or negative).");
    }

    // 4. Combine results (only positive data is returned)
    // Need to correctly merge newly fetched data with existing cached data structure
    const finalDataMap = {};
    cachedData.forEach(crate => {
      const key = `${crate.market_type}_${crate.market_id}`;
      finalDataMap[key] = crate;
    });
    newlyFetchedData.forEach(newCrate => {
      const key = `${newCrate.market_type}_${newCrate.market_id}`;
      if (finalDataMap[key]) {
        // Merge fuzObjects if crate already exists from cache check
        finalDataMap[key].fuzObjects.push(...newCrate.fuzObjects);
      } else {
        finalDataMap[key] = newCrate;
      }
    });

    const finalData = Object.values(finalDataMap);
    console.log(`fuzAPI: Returning data for ${finalData.length} markets.`);
    return finalData;
  }

  // requestItems remains largely the same, relying on getDataForRequests
  function requestItems(market_id, market_type, type_ids) {
    if (!Array.isArray(type_ids)) type_ids = [type_ids]; // Ensure array
    const requests = type_ids
      .map(id => Number(id)) // Ensure numbers
      .filter(id => !isNaN(id) && id > 0) // Filter invalid IDs
      .map(id => ({ type_id: id, market_id: Number(market_id), market_type: market_type }));

    if (requests.length === 0) {
      console.warn("requestItems: No valid type_ids provided.");
      return [];
    }

    const marketDataCrates = getDataForRequests(requests);
    // Find the specific crate for the requested market
    const targetCrate = marketDataCrates.find(crate =>
      crate.market_type === market_type && crate.market_id === Number(market_id)
    );

    return targetCrate ? targetCrate.fuzObjects : [];
  }

  // cacheRefresh remains the same, relying on getDataForRequests
  function cacheRefresh() {
    console.log("fuzAPI: Initiating FULL cache refresh (might time out)...");
    const allKnownRequests = getMasterBatchFromControlTable(); // Assumes exists
    if (allKnownRequests && allKnownRequests.length > 0) {
      getDataForRequests(allKnownRequests); // This now handles negative caching internally
    } else {
      console.log("fuzAPI Full Cache Refresh: Control Table is empty or failed to load.");
    }
  }

  return {
    getDataForRequests: getDataForRequests,
    requestItems: requestItems,
    cacheRefresh: cacheRefresh
  };

})(); // End of fuzAPI IIFE

