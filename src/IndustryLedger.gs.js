/* global GESI, SpreadsheetApp, Logger, UrlFetchApp, Utilities, LockService, PropertiesService, ML, getOrCreateSheet, getCorpAuthChar, _chunkAndPut, _getAndDechunk, _deleteShardedData */

// ======================================================================
// INDUSTRY LEDGER MODULE (Gemini V3 Engine)
// ======================================================================

const LOG_INDUSTRY = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('IndustryLedger') : console);

// --- STATE KEYS ---
const INDUSTRY_JOB_KEY = 'processedIndustryJobIds';
const BPC_JOB_KEY = 'processedBpcJobIds';
const BPC_WAC_KEY = 'BpcWeightedAverageCost';
const INDUSTRY_JOB_PHASE = 'IndustryJobPhase'; 
const CORP_JOBS_CACHE_KEY = 'CORP_JOBS_RAW_V1';

// --- CONFIG ---
const SOFT_TIME_LIMIT_MS = 280000; 
const CORP_JOBS_TTL = 3600;        

const INDUSTRY_ACTIVITY_MANUFACTURING = 1;
const INDUSTRY_ACTIVITY_COPYING = 5;
const INDUSTRY_ACTIVITY_INVENTION = 8;

/**
 * MASTER RUNNER: Executes the full Industry Ledger process.
 */
function runIndustryLedgerPhase(ss) {
  const log = LOG_INDUSTRY;
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const START_TIME = new Date().getTime();

  log.info('--- Starting Industry Ledger Phase (Gemini V3) ---');

  let phase = parseInt(SCRIPT_PROP.getProperty(INDUSTRY_JOB_PHASE) || '0', 10);
  
  // PHASE 0: FETCH
  if (phase === 0) {
    try {
      log.info('Phase 0: Fetching Corp Jobs (Parallel)...');
      const jobs = _getCorporateJobsRaw(true); // Force Fresh Fetch
      
      if (jobs && jobs.length > 0) {
          SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '1'); 
          phase = 1;
      } else {
          log.warn('Phase 0: No jobs fetched. Aborting.');
          return;
      }
    } catch (e) {
      log.error('Phase 0 FAILED. Check Auth/ESI!', e);
      return; 
    }
  }
  
  // PHASE 1: BPC COSTING
  if (phase === 1) {
    if (Date.now() - START_TIME > SOFT_TIME_LIMIT_MS) return;
    try {
      log.info('Phase 1: BPC Costing...');
      runBpcCreationLedger(ss);
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '2'); 
      phase = 2;
    } catch (e) {
      log.error('Phase 1 FAILED:', e);
    }
  }

  // PHASE 2: MANUFACTURING COGS
  if (phase === 2) {
    if (Date.now() - START_TIME > SOFT_TIME_LIMIT_MS) return;
    try {
      log.info('Phase 2: Manufacturing COGS...');
      runIndustryLedgerUpdate(ss);
      SCRIPT_PROP.setProperty(INDUSTRY_JOB_PHASE, '3'); 
      phase = 3;
    } catch (e) {
      log.error('Phase 2 FAILED:', e);
    }
  }

  // PHASE 3: CLEANUP
  if (phase === 3) {
    SCRIPT_PROP.deleteProperty(INDUSTRY_JOB_PHASE);
    _deleteShardedData(CORP_JOBS_CACHE_KEY + ':' + getCorpAuthChar());
    log.info('Phase 3: Cleanup complete.');
  }
}

/**
 * THE GEMINI V3 ENGINE: Parallel Fetch for Industry Jobs.
 */
function _getCorporateJobsRaw(forceRefresh) {
  const authToon = getCorpAuthChar(); 
  if (!authToon) {
      LOG_INDUSTRY.error("Auth character not found.");
      return null;
  }

  const cacheKey = CORP_JOBS_CACHE_KEY + ':' + authToon;

  if (!forceRefresh) {
    const cachedJson = _getAndDechunk(cacheKey);
    if (cachedJson) return JSON.parse(cachedJson);
  }

  LOG_INDUSTRY.info(`Fetching Industry Jobs for ${authToon}...`);
  
  const allJobs = [];
  const client = GESI.getClient();
  if (client.setFunction) client.setFunction('corporations_corporation_industry_jobs');

  try {
      let corpId = 0;
      try {
         const charObj = GESI.getCharacterData ? GESI.getCharacterData(authToon) : null;
         if (charObj) corpId = charObj.corporation_id;
      } catch(e) {}
      
      if (!corpId && GESI.name === authToon) {
         const charData = GESI.getCharacterData ? GESI.getCharacterData() : null;
         if (charData) corpId = charData.corporation_id;
      }

      if (!corpId) {
         try {
            const search = GESI.search(['character'], authToon);
            if (search && search.character && search.character.length > 0) {
                const charId = search.character[0];
                const pubChar = GESI.characters_character(charId);
                corpId = pubChar.corporation_id;
            }
         } catch (e) {}
      }

      if (!corpId) throw new Error(`Could not resolve Corp ID for ${authToon}`);

      const req1 = client.buildRequest({ 
          corporation_id: corpId, 
          include_completed: true, 
          page: 1 
      });
      
      const resp1 = UrlFetchApp.fetch(req1.url, {
          method: req1.method || 'get',
          headers: req1.headers,
          muteHttpExceptions: true
      });

      if (resp1.getResponseCode() !== 200) {
          throw new Error(`Page 1 failed: ${resp1.getResponseCode()} - ${resp1.getContentText()}`);
      }

      const json1 = JSON.parse(resp1.getContentText());
      allJobs.push(...json1);

      const headers = resp1.getAllHeaders();
      const totalPages = Number(headers['x-pages'] || headers['X-Pages'] || 1);
      LOG_INDUSTRY.info(`Page 1 fetched. Total Pages: ${totalPages}`);

      if (totalPages > 1) {
          const requests = [];
          for (let p = 2; p <= totalPages; p++) {
              const reqP = client.buildRequest({ 
                  corporation_id: corpId, 
                  include_completed: true, 
                  page: p 
              });
              requests.push({
                  url: reqP.url,
                  method: reqP.method || 'get',
                  headers: reqP.headers,
                  muteHttpExceptions: true
              });
          }

          const responses = UrlFetchApp.fetchAll(requests);
          responses.forEach((r, i) => {
              if (r.getResponseCode() === 200) {
                  allJobs.push(...JSON.parse(r.getContentText()));
              } else {
                  LOG_INDUSTRY.warn(`Page ${i + 2} failed. Code: ${r.getResponseCode()}`);
              }
          });
      }

      LOG_INDUSTRY.info(`Fetched ${allJobs.length} total jobs.`);
      _chunkAndPut(cacheKey, JSON.stringify(allJobs), CORP_JOBS_TTL);
      
      return allJobs;

  } catch (e) {
      LOG_INDUSTRY.error(`Critical Fetch Error: ${e.message}`);
      return null;
  }
}

function _getNewCompletedJobs(ss, processedJobIds, activityIds) {
  const rawJobs = _getCorporateJobsRaw(false); // Read from cache
  if (!rawJobs) return [];

  const newJobs = [];
  const activitySet = new Set(activityIds);

  for (const job of rawJobs) {
      if (job.status === 'delivered' && 
          activitySet.has(job.activity_id) && 
          !processedJobIds.has(job.job_id)) {
          
          job.end_date = new Date(job.end_date);
          newJobs.push(job);
      }
  }
  return newJobs;
}

// ... (Rest of the Ledger Processing logic remains same) ...
// Assuming the rest of the file logic from previous turn is retained here.
// The critical part was removing _chunkAndPut / _getAndDechunk definitions.