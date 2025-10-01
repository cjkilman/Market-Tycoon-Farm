/** ── DEBUG TOGGLES ─────────────────────────────────────────────────── */
function _dbgOn(){ PropertiesService.getScriptProperties().setProperty('FUZ_DEBUG','1'); }
function _dbgOff(){ PropertiesService.getScriptProperties().deleteProperty('FUZ_DEBUG'); }
function _isDbg(){ return PropertiesService.getScriptProperties().getProperty('FUZ_DEBUG') === '1'; }

/** Light wrapper so logs only print when debug is on */
function _DBG(tag, obj){ if (_isDbg()) try{ console.log('[DBG]', tag, JSON.stringify(obj)); }catch(_){} }

/** ── VISIBILITY HELPERS ────────────────────────────────────────────── */
/** Quick peek at cache disposition for ids (hit/miss/null/bytes) */
function fuzDebugCacheStatus(ids, lt, lid){
  ids = Array.isArray(ids) ? ids.flat().map(Number).filter(Number.isFinite) : [Number(ids)];
  lt  = String(lt||'station').toLowerCase();
  const cache = CacheService.getScriptCache();
  const keys  = ids.map(id => ['fuz', FUZ_CACHE_VER, lt, lid, id].join(':'));
  const raw   = cache.getAll(keys)||{};
  const out   = {};
  for (let i=0;i<ids.length;i++){
    const k=keys[i], s=raw[k];
    if (s==null) out[ids[i]]={state:'MISS'};
    else if (s==='null') out[ids[i]]={state:'NEG'};
    else {
      const bytes = Utilities.newBlob(s).getBytes().length;
      out[ids[i]]={state:'HIT', bytes};
    }
  }
  _DBG('cacheStatus',{lt,lid,out});
  return out;
}

/** Nuke cache for ids (surgical) */
function fuzDebugCacheClear(ids, lt, lid){
  ids = Array.isArray(ids) ? ids.flat().map(Number).filter(Number.isFinite) : [Number(ids)];
  lt  = String(lt||'station').toLowerCase();
  const cache = CacheService.getScriptCache();
  // CacheService has no deleteAll; overwrite with 1s TTL
  const doomed = {};
  for (const id of ids) doomed[['fuz',FUZ_CACHE_VER,lt,lid,id].join(':')] = '';
  cache.putAll(doomed, 1);
  _DBG('cacheClear',{lt,lid,ids});
  return 'ok';
}

// Which fields should never be zero in practice (volume/orderCount can be 0)
const ZERO_SUSPECT_FIELDS = ['avg','weightedAverage','median','min','max','fivePercent'];

function fuzDebugFindZeroPriceFields(ids, lt, lid){
  ids = Array.isArray(ids) ? ids.flat().map(Number).filter(Number.isFinite) : [Number(ids)];
  lt  = String(lt||'station').toLowerCase();
  const cache = CacheService.getScriptCache();
  const keys  = ids.map(id => ['fuz',FUZ_CACHE_VER,lt,lid,id].join(':'));
  const got   = cache.getAll(keys) || {};
  const out   = {};
  for (let i=0;i<ids.length;i++){
    const id = ids[i], s = got[keys[i]];
    if (!s || s === 'null') continue;
    try {
      const row = JSON.parse(s);
      const hits = [];
      ['sell','buy'].forEach(side=>{
        const node = row && row[side];
        if (!node) return;
        ZERO_SUSPECT_FIELDS.forEach(f=>{
          if (node[f] === 0) hits.push(`${side}.${f}`);
        });
      });
      if (hits.length) out[id] = hits;
    } catch(_) {}
  }
  return out; // { id: ["sell.avg","buy.median", ...], ... }
}



function fuzDebugPurgeZeros(ids, lt, lid){
  const suspects = fuzDebugFindZeroPriceFields(ids, lt, lid);
  const cache = CacheService.getScriptCache();
  const doomed = {};
  Object.keys(suspects).forEach(id=>{
    const k = ['fuz',FUZ_CACHE_VER,String(lt).toLowerCase(),lid,Number(id)].join(':');
    doomed[k] = '';  // overwrite with 1s TTL to clear
  });
  if (Object.keys(doomed).length){
    cache.putAll(doomed, 1);
  }
  return {purged:Object.keys(suspects).map(Number)};
}

/** Dry run: tell me which ids would fetch vs hit (no writes) */
function fuzDebugPlan(ids, lt, lid){
  ids = Array.isArray(ids) ? ids.flat().map(Number).filter(Number.isFinite) : [Number(ids)];
  lt  = String(lt||'station').toLowerCase();
  const cache = CacheService.getScriptCache();
  const keys  = ids.map(id => ['fuz',FUZ_CACHE_VER,lt,lid,id].join(':'));
  const got   = cache.getAll(keys)||{};
  const hits=[], negs=[], miss=[];
  for (let i=0;i<ids.length;i++){
    const s=got[keys[i]];
    if (s==null) miss.push(ids[i]);
    else if (s==='null') negs.push(ids[i]);
    else hits.push(ids[i]);
  }
  _DBG('plan',{lt,lid,hits,negs,miss});
  return {hits,negs,miss};
}

/** Tiny harness: run postFetch and report counts/timings (returns metadata only) */
function fuzDebugFetch(ids, lt, lid){
  const t0 = Date.now();
  const before = fuzDebugPlan(ids, lt, lid);
  const res = postFetch(ids, lid, lt);
  const t1 = Date.now();
  const after = fuzDebugPlan(ids, lt, lid);
  const fetchedCount = Object.values(res).filter(v => v && typeof v==='object').length;
  const meta = { ms:(t1-t0), before, after, fetchedCount };
  _DBG('fetchRun', meta);
  return meta;
}

function breakShit(){
const IDS = [16297,16519,8105,33440,16527,16303,16515,33441,16523,16513,33442,16521,16299];

// Amarr system = 30002187
fuzDebugCacheClear(IDS, "system", 30002187);
Utilities.sleep(200);
fuzDebugFetch(IDS, "system", 30002187);
fuzDebugPlan(IDS, "system", 30002187); // expect all HIT now
}

function meep2(){
  postFetchRefresh([16297,16519], "station", 60003760);
}

function postFetchRefresh(ids, lt, lid){
  ids = Array.isArray(ids) ? ids.flat().map(Number).filter(Number.isFinite) : [Number(ids)];
  lt  = String(lt||'station').toLowerCase();
  const cache = CacheService.getScriptCache();
  const doomed = {};
  for (const id of ids) doomed[['fuz',FUZ_CACHE_VER,lt,lid,id].join(':')] = '';
  cache.putAll(doomed, 1);                 // nuke (1s TTL)
  Utilities.sleep(150);                    // tiny gap
  return postFetch(ids, lid, lt);          // fetch again
}


/**
 * Two columns: [SELL.avg, BUY.avg] @ Jita
 * @customfunction
 */
function testFuzAPIBoth() {
  const ids = [16239,16243,24030,32881,17366,16273,34206,34202,34203,34205,34204,34201,19761,42695,42830];
  return marketStatDataBoth(ids, "station", 60003760, "avg");
}

/**
 * Cache-only (no network). Blanks if not cached or dead.
 * @customfunction
 */
function testFuzAPICache() {
  const ids = [16239,16243,24030,32881,17366,16273,34206,34202,34203,34205,34204,34201,19761,42695,42830];
  return marketStatDataCache(ids, "station", 60003760, "sell", "avg");
}

/**
 * Pick another field (e.g. "volume", "median", "fivePercent", "orderCount")
 * @customfunction
 */
function testFuzAPIVolume() {
  const ids = [16239,16243,24030,32881,17366,16273,34206,34202,34203,34205,34204,34201,19761,42695,42830];
  return marketStatData(ids, "station", 60003760, "sell", "volume");
}

/***** ===== TODAY’S TEST KIT (Fuz API only) ===== *****/

// — Publish IDs (from your table)
const PUB_IDS = [16297,16519,8105,33440,16527,16303,16515,33441,16523,16513,33442,16521,16299];

// — Scopes (Feed = system)
const PENIRGMAN_SYS_ID = 30001666;   // Penirgman system
const DOMAIN_REGION_ID = 10000043;   // Domain region

// Store Amarr system id once (Script Properties)
function setAmarrSystemId(id){ PropertiesService.getScriptProperties().setProperty('AMARR_SYSTEM_ID', String(id)); }
function getAmarrSystemId(){ return Number(PropertiesService.getScriptProperties().getProperty('AMARR_SYSTEM_ID')||NaN); }

// — Q1: Endpoint “healthy” enough? (fetch, then confirm cached prices exist)
function fuzTest_Q1_health(amarrSystemId){
  const AMARR_SYS = Number.isFinite(amarrSystemId) ? amarrSystemId : getAmarrSystemId();
  if (!Number.isFinite(AMARR_SYS)) throw new Error('Set Amarr system id: setAmarrSystemId(<id>) or pass it to fuzTest_Q1_health(<id>).');

  // Trigger fetches
  fuzDebugFetch(PUB_IDS, "system", PENIRGMAN_SYS_ID);
  fuzDebugFetch(PUB_IDS, "system", AMARR_SYS);
  fuzDebugFetch(PUB_IDS, "region", DOMAIN_REGION_ID);

  // Read what sheets would read (cache) and count non-blank price fields
  function countNonBlank(ids, lt, lid, field){
    field = String(field||'avg').toLowerCase();
    const map = _readAggsFromCache_(ids, lt, lid);
    let ok = 0, sample = null;
    for (let i=0;i<ids.length;i++){
      const row = map[ids[i]];
      const v = row && (row.sell && (row.sell[field] ?? row.sell.weightedAverage) ||
                        row.buy  && (row.buy[field]  ?? row.buy.weightedAverage));
      const has = (v != null && v !== "");
      if (has) ok++;
      if (!sample && row) sample = row;
    }
    return { ok, sample };
  }

  return {
    penirgman: countNonBlank(PUB_IDS, "system", PENIRGMAN_SYS_ID, "avg"),
    amarr:     countNonBlank(PUB_IDS, "system", AMARR_SYS,         "avg"),
    domain:    countNonBlank(PUB_IDS, "region", DOMAIN_REGION_ID,  "avg")
  };
}

// — Q2: Cache fills? (MISS → HIT + runtime) — runs once per scope
function fuzTest_Q2_cache(amarrSystemId){
  const AMARR_SYS = Number.isFinite(amarrSystemId) ? amarrSystemId : getAmarrSystemId();
  if (!Number.isFinite(AMARR_SYS)) throw new Error('Set Amarr system id first.');

  function run(ids, lt, lid){
    const before = fuzDebugPlan(ids, lt, lid);
    const meta   = fuzDebugFetch(ids, lt, lid); // does the fetch
    const after  = fuzDebugPlan(ids, lt, lid);
    return { ms: meta.ms, before, after, newHits: Math.max(0, after.hits.length - before.hits.length) };
  }

  return {
    penirgman: run(PUB_IDS, "system", PENIRGMAN_SYS_ID),
    amarr:     run(PUB_IDS, "system", AMARR_SYS),
    domain:    run(PUB_IDS, "region", DOMAIN_REGION_ID)
  };
}

// — Q3: Negatives behave? (bogus id should become NEG after fetch)
function fuzTest_Q3_negatives(){
  const bogus = [99999999];
  function run(lt, lid){
    const before = fuzDebugPlan(bogus, lt, lid);
    fuzDebugFetch(bogus, lt, lid);
    const after  = fuzDebugPlan(bogus, lt, lid);
    return { before, after };
  }
  return {
    penirgman: run("system", PENIRGMAN_SYS_ID),
    domain:    run("region", DOMAIN_REGION_ID)
  };
}


