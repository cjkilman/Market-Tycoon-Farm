/**
 * ESI_MODULE: Final Form
 * Purpose: Robust, "Good Citizen" API Requester with ETag and Auth-Awareness.
 * Dependencies: GESI, LoggerEx, PropertiesService, CacheService.
 */
var ESI = (function () {
  // --- ESI CONFIGURATION (Aligned to FuzAPI Dialect) ---
  const ESI_CONFIG = {
    PROP_QUOTA_FLAG: 'DAILY_QUOTA_EXHAUSTED',
    VAL_QUOTA_TRIPPED: 'true' // Aligned exactly with FuzAPI
  };
  
  const _props = PropertiesService.getScriptProperties();
  const _log = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('ESI') : console;

  /**
   * Checks the global quota lock before running any ESI network calls.
   */
  function _isLocked() { 
    return _props.getProperty(ESI_CONFIG.PROP_QUOTA_FLAG) === ESI_CONFIG.VAL_QUOTA_TRIPPED; 
  }

  /**
   * Trips the global lock using the FuzAPI dialect.
   */
  function _tripLock(msg) { 
    _log.error(`FATAL: Google Quota Hit via ESI_MODULE: ${msg}`); 
    _props.setProperty(ESI_CONFIG.PROP_QUOTA_FLAG, ESI_CONFIG.VAL_QUOTA_TRIPPED); 
  }

  /**
   * Closes local ESI circuits and clears the shared global quota lock.
   */
  function resetEsiModule() {
    _props.deleteProperty(ESI_CONFIG.PROP_QUOTA_FLAG);
    _props.deleteProperty('EsiCircuitState');
    _props.deleteProperty('EsiCircuitFailCount');
    console.log("ESI_MODULE: Master Circuit reset complete.");
  }

  function forEndpoint(authClient, endpoint, hooks = {}, cacheStrategy = null) {
    
    function _fire(hookName, arg) {
      if (hooks[hookName] && typeof hooks[hookName] === 'function') {
        try { hooks[hookName](arg); } catch (e) { _log.error(`Hook ${hookName} failed: ${e.message}`); }
      }
    }

    function _request(method, options) {
      const params = options.params || {};
      let cached = null;

      // 1. Cache Check (ETag Handshake)
      if (method === 'get' && cacheStrategy) {
        cached = cacheStrategy.get(endpoint, params);
      }

      // 2. Global Quota Gate
      if (_isLocked()) {
        const err = { error: "LOCKED", data: null };
        _fire('onLock', err);
        return err;
      }

      // 3. Auth Boundary (GESIClient Error Catch)
      let client;
      try {
        client = authClient.setFunction(endpoint);
      } catch (e) {
        _fire('onAuthError', { error: e.message, endpoint });
        return { error: "AUTH_FAILURE", data: null };
      }

      const req = (method === 'get') ? client.buildRequest(params) : client.buildRequest({});

      // 4. Retry Loop (Good Citizen Protocols)
      const MAX_RETRIES = 3;
      for (let attempt = 0; attempt < MAX_RETRIES; attempt++) {
        try {
          const fetchOpts = { method: method, headers: req.headers, muteHttpExceptions: true };
          if (cached && cached.etag) fetchOpts.headers['If-None-Match'] = cached.etag;
          if (method === 'post') { fetchOpts.contentType = 'application/json'; fetchOpts.payload = JSON.stringify(options.payload); }

          const res = UrlFetchApp.fetch(req.url, fetchOpts);
          const code = res.getResponseCode();
          const headers = res.getAllHeaders();

          // 304: Not Modified (Use Cache)
          if (code === 304) return { error: null, data: cached.data, cached: true };

          // 200: Success (Parse & Update Cache)
          if (code === 200) {
            const data = JSON.parse(res.getContentText());
            if (method === 'get' && cacheStrategy) {
              const etag = headers['ETag'] || headers['etag'];
              const expires = headers['Expires'] || headers['expires'];

              let ttl = 3600; // Default 1 hour
              if (expires && !isNaN(Date.parse(expires))) {
                const diff = Math.floor((new Date(expires).getTime() - Date.now()) / 1000);
                ttl = diff > 0 ? diff : 3600;
              }
              cacheStrategy.put(endpoint, params, data, ttl, etag);
            }
            return { error: null, data: data };
          }

          // 404: Stop
          if (code === 404) { _fire('on404', { endpoint, params: params }); return { error: "404 Not Found", data: null }; }

          // Throttling/Retries
          if (code === 429 || code >= 500) {
            const wait = (code === 429) ? (Number(headers['Retry-After'] || 30) * 1000) : (Math.pow(2, attempt) * 1000);
            _log.warn(`Attempt ${attempt + 1} failed (${code}). Sleeping ${wait}ms.`);
            Utilities.sleep(wait);
            continue;
          }
          return { error: "HTTP " + code, data: null };
          
        } catch (e) {
          const msg = String(e.message).toLowerCase();
          // Trip lock if Google throws a hard quota or rate limit error
          if (msg.includes('quota') || msg.includes('too many times') || msg.includes('limit exceeded')) { 
            _tripLock(e.message); 
            return { error: "LOCKED", data: null }; 
          }
        }
      }
      return { error: "Max retries exceeded", data: null };
    }

    return {
      get: (params) => _request('get', { params: params }),
      post: (payload) => _request('post', { payload: payload })
    };
  }

  // Outer return correctly references the globally scoped functions
  return { forEndpoint: forEndpoint, isLocked: _isLocked, reset: resetEsiModule };
})();