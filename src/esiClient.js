var ESI = (function () {
  const ESI_CONFIG = {
    PROP_QUOTA_FLAG: 'DAILY_QUOTA_EXHAUSTED',
    ESI_CLIENT_FLAG: 'ESI_CLIENT_LOCKED'
  };

  const _props = PropertiesService.getScriptProperties();
  const _log = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ESI') : console;

  // Execution cache to prevent redundant Properties Service network calls
  let _cachedLockState = null;

  function _isLocked() {
    if (_cachedLockState !== null) return _cachedLockState;

    try {
      const quota = _props.getProperty(ESI_CONFIG.PROP_QUOTA_FLAG) === 'true';
      const client = _props.getProperty(ESI_CONFIG.ESI_CLIENT_FLAG) === 'LOCKED';
      _cachedLockState = (quota || client);
      return _cachedLockState;
    } catch (e) {
      _log.error("ESI_MODULE: Failed to read lock properties. Defaulting to locked state.");
      return true;
    }
  }

  function _reset() {
    _props.deleteProperty(ESI_CONFIG.PROP_QUOTA_FLAG);
    _props.deleteProperty(ESI_CONFIG.ESI_CLIENT_FLAG);
    _cachedLockState = false;
    _log.info("ESI_MODULE: Master Circuit reset complete.");
  }

  /**
   * Fetches requests concurrently while preserving input array indices.
   * Returns an array matching the input length. Failed items are returned as null.
   */
  function _robustFetchAll(requests, chunkSize = 50, maxRetries = 3) {
    const finalResponses = new Array(requests.length).fill(null);

    if (_isLocked()) {
      _log.error("ABORT: Fetch blocked. ESI Master Circuit is currently locked.");
      return finalResponses;
    }

    // Map tracking the original index of every request object
    let indexedRequests = requests.map((req, idx) => ({ req, originalIndex: idx }));

    for (let i = 0; i < indexedRequests.length; i += chunkSize) {
      const chunk = indexedRequests.slice(i, i + chunkSize);
      let attempts = 0;
      let currentChunk = [...chunk];

      while (attempts < maxRetries && currentChunk.length > 0) {
        attempts++;
        let fetchPayload = currentChunk.map(item => item.req);
        let responses;

        try {
          responses = UrlFetchApp.fetchAll(fetchPayload);
        } catch (e) {
          if (e.message.includes('too many times') || e.message.includes('quota')) {
            _log.error("FATAL: Google UrlFetchApp Daily Quota Exhausted.");
            _props.setProperty(ESI_CONFIG.ESI_CLIENT_FLAG, 'LOCKED');
            _cachedLockState = true;
            return finalResponses;
          }
          throw e;
        }

        const nextChunk = [];
        let lowestErrorLimit = 100;
        let sleepForReset = 0;

        responses.forEach((res, index) => {
          const code = res.getResponseCode();
          const headers = res.getHeaders();
          const originalIndex = currentChunk[index].originalIndex;

          const errRemain = Number(headers['X-Esi-Error-Limit-Remain'] || headers['x-esi-error-limit-remain'] || 100);
          const errReset = Number(headers['X-Esi-Error-Limit-Reset'] || headers['x-esi-error-limit-reset'] || 0);

          if (errRemain < lowestErrorLimit) {
            lowestErrorLimit = errRemain;
            sleepForReset = errReset;
          }

          if (code === 200 || code === 404) {
            finalResponses[originalIndex] = res;
          } else if (code === 429 || code === 420 || code >= 500) {
            nextChunk.push(currentChunk[index]);
          } else {
            finalResponses[originalIndex] = res; // Pass through other non-retryable codes (e.g. 400, 403)
          }
        });

        if (lowestErrorLimit < 15) {
          const waitMs = (sleepForReset * 1000) + 2000;
          _log.warn(`[ESI LIMIT] Error limit dropped to ${lowestErrorLimit}. Sleeping ${waitMs}ms to prevent ban.`);
          Utilities.sleep(waitMs);
        } else if (nextChunk.length > 0) {
          Utilities.sleep(2000 * attempts);
        }

        currentChunk = nextChunk;
      }
    }
    return finalResponses;
  }

  function forEndpoint(authClient, endpoint) {
    
    function _request(method, options) {
      if (_isLocked()) {
        return {
          error: "Blocked: ESI Master Circuit is locked.",
          data: null,
          headers: {}
        };
      }

      const params = options.params || {};
      const payload = options.payload;
      const client = authClient.setFunction(endpoint);

      const req1 = client.buildRequest({ ...params, page: 1 });
      const fetchObj = {
        url: req1.url,
        method: method,
        headers: req1.headers,
        muteHttpExceptions: true
      };

      if (payload && method.toLowerCase() === 'post') {
        fetchObj.payload = JSON.stringify(payload);
        fetchObj.contentType = 'application/json';
      }

      const responses = _robustFetchAll([fetchObj]);
      const initialResp = responses[0];

      if (!initialResp || initialResp.getResponseCode() !== 200) {
        return {
          error: `Initial fetch failed: ${initialResp ? initialResp.getResponseCode() : 'No Response'}`,
          data: null,
          headers: initialResp ? initialResp.getHeaders() : {}
        };
      }

      let allData = JSON.parse(initialResp.getContentText());

      if (method.toLowerCase() === 'get') {
        const headers = initialResp.getHeaders();
        const maxPages = Number(headers['X-Pages'] || headers['x-pages'] || 1);

        if (maxPages > 1) {
          const requests = [];
          for (let p = 2; p <= maxPages; p++) {
            const req = client.buildRequest({ ...params, page: p });
            requests.push({
              url: req.url,
              method: 'get',
              headers: req.headers,
              muteHttpExceptions: true
            });
          }

          const paginatedResponses = _robustFetchAll(requests);
          let paginationFailed = false;

          paginatedResponses.forEach((res, idx) => {
            if (res && res.getResponseCode() === 200) {
              allData = allData.concat(JSON.parse(res.getContentText()));
            } else {
              paginationFailed = true;
              _log.error(`PAGINATION ERROR: Page ${idx + 2} of ${maxPages} dropped completely after maximum retries.`);
            }
          });

          if (paginationFailed) {
            return {
              error: "Fetch aborted: One or more data pages failed to load entirely.",
              data: null,
              headers: initialResp ? initialResp.getHeaders() : {}
            };
          }
        }
      }

      return { error: null, data: allData, headers: initialResp.getHeaders() };
    }

    return {
      get: (params) => _request('get', { params: params }),
      post: (payload) => _request('post', { payload: payload })
    };
  }

  return {
    forEndpoint: forEndpoint,
    isLocked: _isLocked,
    reset: _reset
  };
})();