/**
 * The master Facade and Engine for the EVE Market Data system.
 * This single module handles state, caching, API calls, and provides the public interface.
 */
const fuzAPI = (() => {

  // --- PRIVATE ENGINE COMPONENTS (Internal State & Logic) ---
  const _cache = CacheService.getScriptCache();

  const FUZ_CACHE_VER = 1;
  const CACHE_CHUNK_SIZE = 8000; // Chunk size for putAll

  // --- Internal State Management Helpers ---
  function _getItemKey(item) { return `${item.type_id}-${item.market_id}-${item.market_type}`; }


  // --- Internal Task Processing Helpers ---
  function _fuzKey(location_type, location_id, type_id) {
    return `fuz:${FUZ_CACHE_VER}:${location_type}:${location_id}:${type_id}`;
  }

  function _groupRequestsByLocation(missingRequests) {
    const grouped = {};
    missingRequests.forEach(req => {
      const groupKey = `${req.market_type}_${req.market_id}`;
      if (!grouped[groupKey]) grouped[groupKey] = { locationId: req.market_id, locationType: req.market_type, items: new Set() };
      grouped[groupKey].items.add(req.type_id);
    });
    // Convert Set back to Array for the next step
    Object.values(grouped).forEach(group => group.items = Array.from(group.items));
    return grouped;
  }

function _buildFetchAllRequests(groupedCalls) {
  const requests = [];

  // Iterate through each market destination
  for (const key in groupedCalls) {
    const call = groupedCalls[key]; // { locationId, locationType, items: Array<number> }

    // --- Use POST for /aggregates/ ---
    const url = "https://market.fuzzwork.co.uk/aggregates/";

    // --- Build POST Payload ---
    // Use the locationType ('region' or 'station') as the key for the location ID
    const payload = {
      [call.locationType]: call.locationId, // Dynamic key based on location type
      types: call.items.join(",")                  // Pass the array of item IDs
    };

    requests.push({
      url: url,
      method: 'post', // Keep POST method
      contentType: 'application/json',
      payload: JSON.stringify(payload), // Send data in the body
      muteHttpExceptions: true,
      headers: { 'Accept': 'application/json' },
      // Include context for mapping response back
      fuz_context: {
        locationId: call.locationId,
        locationType: call.locationType
      }
    });
  }
  console.log(`Built ${requests.length} POST requests for ${Object.keys(groupedCalls).length} markets.`);
  return requests;
}

  function _executeFetchAll(tasksToFetch) {
    if (!tasksToFetch || tasksToFetch.length === 0) return { newlyFetchedData: [], dataToCache: {} };

    const groupedCalls = _groupRequestsByLocation(tasksToFetch);
    const fetchRequests = _buildFetchAllRequests(groupedCalls);
    let responses;

    try {
      responses = withRetries(() => {
        console.log(`Attempting to fetch data via POST for ${fetchRequests.length} requests...`);
        return UrlFetchApp.fetchAll(fetchRequests);
      });
    } catch (e) {
      console.error(`_executeFetchAll failed after multiple retries: ${e.message}`);
      return { newlyFetchedData: [], dataToCache: {} }; // Return empty on final failure
    }

    const dataToCache = {};
    const processedDataByLocation = {};

    responses.forEach((response, index) => {
      if (response.getResponseCode() === 200) {
        const parsed = JSON.parse(response.getContentText());
        // Find the original request details using the index
        const originalRequest = fetchRequests[index].fuz_context; // 💡 FIXED
        if (!originalRequest) {
          console.error(`_executeFetchAll: Could not retrieve context for response index ${index}. Skipping.`);
          return; // Skip processing this response
        }
        const { locationId, locationType } = originalRequest;
        const locationKey = `${locationType}_${locationId}`;
        

        if (!processedDataByLocation[locationKey]) {
          processedDataByLocation[locationKey] = { market_type: locationType, market_id: locationId, fuzObjects: [] };
        }

        for (const typeId in parsed) {
          const rawItemData = parsed[typeId];
          const dataObject = new FuzDataObject(typeId, rawItemData); // Use Helper class
          processedDataByLocation[locationKey].fuzObjects.push(dataObject);
          // Prepare to cache individual results (using FuzDataObject directly)
          const cacheKey = _fuzKey(locationType, locationId, typeId);
          dataToCache[cacheKey] = JSON.stringify(dataObject);
        }
      } else {
        console.error(`API Error: Status ${response.getResponseCode()} for request index ${index}`);
      }
    });

    return { newlyFetchedData: Object.values(processedDataByLocation), dataToCache };
  }

  function _cacheNewData(dataToCache) {
    const cacheKeys = Object.keys(dataToCache);
    if (cacheKeys.length > 0) {
      console.log(`Caching ${cacheKeys.length} new items...`);
      const ttl = 1800; // 30 minutes cache time

      if (cacheKeys.length > CACHE_CHUNK_SIZE) {
        console.log(`Cache size exceeds threshold (${CACHE_CHUNK_SIZE}). Chunking putAll...`);
        for (let i = 0; i < cacheKeys.length; i += CACHE_CHUNK_SIZE) {
          const chunkKeys = cacheKeys.slice(i, i + CACHE_CHUNK_SIZE);
          const chunkCacheObject = {};
          chunkKeys.forEach(key => chunkCacheObject[key] = dataToCache[key]);
          try { _cache.putAll(chunkCacheObject, ttl); } catch (e) { console.error(`Cache chunk failed: ${e.message}`) };
          console.log(`Cached chunk ${Math.floor(i / CACHE_CHUNK_SIZE) + 1}...`);
          Utilities.sleep(50);
        }
      } else {
        try { _cache.putAll(dataToCache, ttl); } catch (e) { console.error(`Cache putAll failed: ${e.message}`) };
      }
      console.log("Caching complete.");
    }
  }

  function _checkCacheForRequests(marketRequests) {
    const requiredKeys = marketRequests.map(req => _fuzKey(req.market_type, req.market_id, req.type_id));
    const cachedResults = _cache.getAll(requiredKeys);
    let cachedData = []; // Will hold structured "shipping crate" data from cache
    const missingRequests = [];
    const tempGroupedCache = {}; // Group cached items by location temporarily

    marketRequests.forEach(req => {
      const key = _fuzKey(req.market_type, req.market_id, req.type_id);
      if (cachedResults[key]) {
        try {
          const itemData = JSON.parse(cachedResults[key]);
          const locationKey = `${req.market_type}_${req.market_id}`;
          if (!tempGroupedCache[locationKey]) {
            tempGroupedCache[locationKey] = { market_type: req.market_type, market_id: req.market_id, fuzObjects: [] };
          }
          tempGroupedCache[locationKey].fuzObjects.push(itemData);
        } catch (e) {
          missingRequests.push(req); // Treat parse failure as missing
        }
      } else {
        missingRequests.push(req);
      }
    });
    cachedData = Object.values(tempGroupedCache); // Convert grouped cache back to array
    console.log(`[Cache Check] Found ${cachedData.reduce((sum, crate) => sum + crate.fuzObjects.length, 0)} items in cache for ${marketRequests.length} requests. Missing: ${missingRequests.length}`);
    return { cachedData, missingRequests };
  }


  // --- PUBLIC FACADE METHODS ---
  function getDataForRequests(marketRequests) {
    if (!marketRequests || marketRequests.length === 0) return [];
    console.log(`fuzAPI: Received ${marketRequests.length} market requests.`);

    // 2. Check cache
    const { cachedData, missingRequests } = _checkCacheForRequests(marketRequests);

    // 3. Fetch missing data
    const { newlyFetchedData, dataToCache } = _executeFetchAll(missingRequests);

    // 4. Cache new data
    _cacheNewData(dataToCache);

    // 5. Combine results
    const finalData = cachedData.concat(newlyFetchedData);


    return finalData;
  }

  function requestItems(market_id, market_type, type_ids) {
    const requests = type_ids.map(id => ({ type_id: id, market_id: market_id, market_type: market_type }));
    const marketDataCrates = getDataForRequests(requests);
    if (marketDataCrates && marketDataCrates.length > 0) return marketDataCrates[0].fuzObjects;
    return [];
  }

/**
   * (REVISED/OPTIONAL) Refreshes cache for the entire Control Table list.
   * NOTE: This attempts the whole list at once and might time out.
   * The batched orchestrator is generally preferred.
   */
  function cacheRefresh() {
    console.log("fuzAPI: Initiating FULL cache refresh (might time out)...");
    const allKnownRequests = getMasterBatchFromControlTable(); // Use helper
    if (allKnownRequests.length > 0) {
        getDataForRequests(allKnownRequests); // Process the full list
    } else {
        console.log("fuzAPI Full Cache Refresh: Control Table is empty.");
    }
  }

  return {
    getDataForRequests: getDataForRequests,
    requestItems: requestItems,
    // (Expose a "get list" method for debugging if needed)
    cacheRefresh: cacheRefresh
  };

})(); // End of fuzAPI IIFE