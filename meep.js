/* eslint-disable no-console */
/* eslint-disable no-unused-vars */

// -----------------------------------------------------------------------------
// --- SDE ENGINE FACTORY (SDE_API) ---
// -----------------------------------------------------------------------------

const SDE_API = (function () {

  // --- INTERNAL SPREADSHEET ACCESS (Centralized Access Point) ---
  let SS_INTERNAL = null;
  function _getSS_Internal() {
    if (!SS_INTERNAL) {
      SS_INTERNAL = SpreadsheetApp.getActiveSpreadsheet();
    }
    return SS_INTERNAL;
  }
  // --- END INTERNAL SPREADSHEET ACCESS ---
  
  // 1. RENAME AND INITIALIZE THE PROPERTY STORE
  const SDE_PROP_STORE = PropertiesService.getScriptProperties();

  // --- ENCAPSULATED CONSTANTS ---
  const KEY_JOB_RUNNING = 'SDE_JOB_RUNNING';
  const KEY_JOB_LIST = 'SDE_JOB_LIST';
  const KEY_JOB_INDEX = 'SDE_JOB_INDEX';
  const KEY_BACKUP_SETTINGS = 'SDE_BACKUP_SETTINGS';
  const GLOBAL_STATE_KEY = 'GLOBAL_SYSTEM_STATE';
  const KEY_JOB_CHUNK_INDEX = 'SDE_JOB_CHUNK_INDEX';
  const KEY_LAST_WRITE_MS = 'SDE_LAST_WRITE_MS';
  const CACHE_HEADERS_KEY_BASE = 'SDE_HEADERS_';
  const CACHE_DATA_KEY_BASE = 'SDE_DATA_';
  
  const PROCESS_FUNCTION_NAME = 'sde_job_PROCESS';
  const RETRY_DELAY_MS = 5000;
  const CONFLICT_RETRY_DELAY_MS = 30000;
  
  const THROTTLE_BASE_SLEEP_MS = 250;
  const THROTTLE_LATENCY_FACTOR = 1.2;
  const THROTTLE_MAX_SLEEP_MS = 5000;
  // --- END ENCAPSULATED CONSTANTS ---

  // -----------------------------------------------------------------------------
  // --- ENCAPSULATED HELPER FUNCTIONS (Moved from Global Scope) ---
  // -----------------------------------------------------------------------------

  

  function _deleteTriggersFor(functionName) {
    const allTriggers = ScriptApp.getProjectTriggers();
    let deletedCount = 0;
    allTriggers.forEach(trigger => {
      if (trigger.getHandlerFunction() === functionName) {
        ScriptApp.deleteTrigger(trigger);
        deletedCount++;
      }
    });
    if (deletedCount > 0) {
      console.log(`Deleted ${deletedCount} trigger(s) for ${functionName}.`);
    }
    return deletedCount;
  }

  function _scheduleSdeProcessRetry(delay = RETRY_DELAY_MS) {
    _deleteTriggersFor(PROCESS_FUNCTION_NAME);
    ScriptApp.newTrigger(PROCESS_FUNCTION_NAME).timeBased().after(delay).create();
  }


  
  // --- SdePage Class ---
  class SdePage {
    constructor(sheet, csvFile, headers = null, backupRanges = null, publishedOnly = true) {
      this.sheet = sheet;
      this.backupRanges = backupRanges; 
      this.csvFile = csvFile;
      this.headers = headers; 
      this.publishedOnly = publishedOnly;
    }
  }


  // -----------------------------------------------------------------------------
  // --- FACTORY METHOD: forPage(sdePage) ---
  // -----------------------------------------------------------------------------
  
  function forPage(sdePage) {
    const DOC_LOCK_TIMEOUT = 30000;
    const docLock = LockService.getDocumentLock();
    const sheetName = sdePage.sheet;
    
    const ss = _getSS_Internal();
    let pageSheet = ss.getSheetByName(sheetName);

    // --- PRIVATE/LOCKED METHODS ---

      // --- Core Utility Functions (Assumed defined/available) ---
  const downloadTextData = (csvFile) => { /* ... */ };
  const CSVToArray = (strData, strDelimiter = ",", headers = null, publishedOnly = true) => { /* ... */ };
    
function createOrClearSdeSheet(sheetName) {
    const ss = _getSS_Internal(); 
    let workSheet = ss.getSheetByName(sheetName); 
    
    if (workSheet) {
      workSheet.clearContents();
    } else {
      workSheet = ss.insertSheet();
      workSheet.setName(sheetName);
    }
    return workSheet;
  }

  function deleteBlankColumnsAndColumns(workSheet) {
    if (!workSheet) return;
    const maxColumns = workSheet.getMaxColumns();
    const lastColumn = workSheet.getLastColumn();
    const maxRows = workSheet.getMaxRows();
    const lastRow = workSheet.getLastRow();

    const columnsToRemove = maxColumns - lastColumn;
    const rowsToRemove = maxRows - lastRow;

    if (columnsToRemove > 0) {
      workSheet.deleteColumns(lastColumn + 1, columnsToRemove);
    }
    if (rowsToRemove > 0) {
      workSheet.deleteRows(lastRow + 1, rowsToRemove);
    }
  }

  function _writeChunk(dataChunk, startRow, numCols, sheetName) {
      let writeDurationMs = 0;
      const docLock = LockService.getDocumentLock();
      const DOC_LOCK_TIMEOUT = 5000; // TryLock 5s
      
      if (!docLock.tryLock(DOC_LOCK_TIMEOUT)) {
          return { success: false, duration: 0 }; 
      }
      
      try {
          const ss = _getSS_Internal();
          const workSheet = ss.getSheetByName(sheetName);
          if (!workSheet) throw new Error(`Sheet ${sheetName} missing during write.`);

          const chunkStartTime = new Date().getTime();
          workSheet.getRange(startRow, 1, dataChunk.length, numCols).setValues(dataChunk);
          writeDurationMs = new Date().getTime() - chunkStartTime;
          SpreadsheetApp.flush();
      } catch (e) {
          console.error(`_writeChunk: Write failed while locked: ${e.message}`);
          throw e;
      } finally {
          docLock.releaseLock();
      }
      return { success: true, duration: writeDurationMs }; 
  }

    function clearAndWriteHeaders(headers) {
      const headerCount = headers.length;
      let duration = 0;
      
      docLock.waitLock(DOC_LOCK_TIMEOUT);
      try {
        const startTime = new Date().getTime();
        pageSheet = createOrClearSdeSheet(sheetName);

        const maxCols = pageSheet.getMaxColumns();
        if (maxCols > headerCount) {
          pageSheet.deleteColumns(headerCount + 1, maxCols - headerCount);
        } else if (maxCols < headerCount) {
          pageSheet.insertColumnsAfter(maxCols, headerCount - maxCols);
        }
        duration = new Date().getTime() - startTime;
      } finally {
        docLock.releaseLock();
      }
      return duration;
    }

    function finalizeSheet() {
      docLock.waitLock(DOC_LOCK_TIMEOUT);
      try {
        pageSheet = ss.getSheetByName(sheetName);
        if (pageSheet) {
           deleteBlankColumnsAndColumns(pageSheet);
           pageSheet.autoResizeColumns(1, pageSheet.getLastColumn());
        }
      } finally {
        docLock.releaseLock();
      }
    }

    // --- CORE LOGIC: writeChunks (The Document Locked Chunk Loader) ---
    
    function writeChunks(scriptStartTime) {
      const SDE_PROP = SDE_PROP_STORE; 
      let currentRow = parseInt(SDE_PROP.getProperty(KEY_JOB_CHUNK_INDEX) || '0', 10);
      const CACHE_HEADERS_KEY = CACHE_HEADERS_KEY_BASE + sheetName;
      const CACHE_DATA_KEY = CACHE_DATA_KEY_BASE + sheetName;
      
      // --- 1. Download & Parse (Resume logic) ---
      if (currentRow === 0) {
          let csvData;
          try {
              const csvContent = downloadTextData(sdePage.csvFile); 
              csvData = CSVToArray(csvContent, ",", sdePage.headers, sdePage.publishedOnly);
          } catch (e) {
              console.error(`SDE.writeChunks: Download/Parse failed for ${sdePage.sheet}.`, e);
              return true; // Finished
          }
          
          if (!csvData || csvData.length < 2 || csvData[0].length === 0) {
              console.warn(`SDE.writeChunks: Parsed data for ${sdePage.sheet} is empty or invalid.`);
              finalizeSheet(); 
              return true; 
          }
          SDE_PROP.setProperty(CACHE_HEADERS_KEY, JSON.stringify(csvData[0]));
          SDE_PROP.setProperty(CACHE_DATA_KEY, JSON.stringify(csvData.slice(1)));
      }

      // --- 2. Initialize/Resume from Script Properties ---
      const CHUNK_SIZE = 1000;
      const SCRIPT_TIME_LIMIT = 240000;
      
      let lastWriteDurationMs = parseInt(SDE_PROP.getProperty(KEY_LAST_WRITE_MS) || '500', 10);
      
      const headers = JSON.parse(SDE_PROP.getProperty(CACHE_HEADERS_KEY) || '[]');
      const dataRows = JSON.parse(SDE_PROP.getProperty(CACHE_DATA_KEY) || '[]');

      if (dataRows.length === 0) return true;
      
      const totalRows = dataRows.length;
      const numCols = headers.length;

      try {
          // --- 3. Setup Sheet (Locked) ---
          if (currentRow === 0) {
              clearAndWriteHeaders(headers); // LOCK 1: Clear sheet and resize
          }
          
          console.log(`SDE.writeChunks: Total rows: ${totalRows}. Resuming at index: ${currentRow}.`);
          
          // --- 4. ThrottledWriteChunkBatches (The main loop) ---
          while (currentRow < totalRows) {
              const elapsedTime = new Date().getTime() - scriptStartTime;
              if (elapsedTime > SCRIPT_TIME_LIMIT) {
                  // --- PAUSE AND RESUME ---
                  SDE_PROP.setProperty(KEY_JOB_CHUNK_INDEX, currentRow.toString());
                  SDE_PROP.setProperty(KEY_LAST_WRITE_MS, String(lastWriteDurationMs));
                  return false; // Paused
              }
              
              // --- Adaptive Throttle Logic ---
              let sleepMs = Math.max(THROTTLE_BASE_SLEEP_MS, lastWriteDurationMs * THROTTLE_LATENCY_FACTOR);
              sleepMs = Math.min(THROTTLE_MAX_SLEEP_MS, sleepMs);
              Utilities.sleep(sleepMs);
              
              const chunkEnd = Math.min(currentRow + CHUNK_SIZE, totalRows);
              const chunk = dataRows.slice(currentRow, chunkEnd);
              const writeRow = currentRow + 1; 

              // CRITICAL BATCH WRITE (DOCUMENT LOCKED by helper)
              const writeResult = _writeChunk(chunk, writeRow, numCols, sdePage.sheet);
              
              if (writeResult.success) {
                  // SUCCESS PATH: Update state
                  lastWriteDurationMs = writeResult.duration;
                  
                  currentRow = chunkEnd;
                  SDE_PROP.setProperty(KEY_JOB_CHUNK_INDEX, currentRow.toString());
                  SDE_PROP.setProperty(KEY_LAST_WRITE_MS, String(lastWriteDurationMs));
              } else {
                  // FAILURE PATH: Document Lock Conflict (Return false to pause job)
                  console.warn(`SDE.writeChunks: Document Lock conflict encountered (5s timeout). Pausing to resume at row ${currentRow}.`);
                  return false; 
              }
          }
          
          // --- 5. Finalize (Locked) ---
          finalizeSheet();
          
          // --- Final Cleanup ---
          SDE_PROP.deleteProperty(KEY_JOB_CHUNK_INDEX);
          SDE_PROP.deleteProperty(KEY_LAST_WRITE_MS);
          SDE_PROP.deleteProperty(CACHE_HEADERS_KEY);
          SDE_PROP.deleteProperty(CACHE_DATA_KEY);
          return true; // Completed

      } catch (e) {
          // Final safety state save on unexpected error
          SDE_PROP.setProperty(KEY_JOB_CHUNK_INDEX, currentRow.toString());
          SDE_PROP.setProperty(KEY_LAST_WRITE_MS, String(lastWriteDurationMs));
          throw e;
      }
    }

    return { writeChunks, clearAndWriteHeaders };
  }


  // -----------------------------------------------------------------------------
  // --- ENCAPSULATED CONTROLLER LOGIC (Final Public Interface) ---
  // -----------------------------------------------------------------------------
  
  function _resetSdeJobState() {
    SDE_PROP_STORE.deleteProperty(KEY_JOB_RUNNING);
    SDE_PROP_STORE.deleteProperty(KEY_JOB_LIST);
    SDE_PROP_STORE.deleteProperty(KEY_JOB_INDEX);
    SDE_PROP_STORE.deleteProperty(KEY_JOB_CHUNK_INDEX);
    SDE_PROP_STORE.deleteProperty(KEY_LAST_WRITE_MS);
    
    // Clear page-specific caches
    // NOTE: This relies on getConfig() which loads KEY_JOB_LIST to get page names.
    const pages = SDE_API.getConfig() || [];
    pages.forEach(page => {
        SDE_PROP_STORE.deleteProperty(CACHE_HEADERS_KEY_BASE + page.sheet);
        SDE_PROP_STORE.deleteProperty(CACHE_DATA_KEY_BASE + page.sheet);
    });
    console.log("SDE Job state completely reset.");
  }
  
  // NOTE: Full logic for START, PROCESS, FINALIZE, setConfig, getConfig omitted for brevity
  // but relies on the structure below.

  // ... (Full controller implementation remains the same) ...

  function _sde_job_START_Internal() { /* ... */ }
  function _sde_job_PROCESS_Internal() { /* ... */ }
  function _sde_job_FINALIZE_Internal() { /* ... */ }
  
  function setConfig(pagesConfig) { /* ... */ }
  function getConfig() { /* ... */ }


  // --- RETURN THE PUBLIC SDE API & EXPOSE CONTROLLERS ---
  return {
    SdePage: SdePage, 
    forPage: forPage, 
    downloadTextData: downloadTextData, 
    CSVToArray: CSVToArray,
    
    setConfig: setConfig,
    getConfig: getConfig, 

    sde_job_START: _sde_job_START_Internal,
    sde_job_PROCESS: _sde_job_PROCESS_Internal,
    sde_job_FINALIZE: _sde_job_FINALIZE_Internal,

    KEY_JOB_RUNNING, KEY_JOB_LIST, KEY_JOB_INDEX, KEY_BACKUP_SETTINGS,
    GLOBAL_STATE_KEY, KEY_JOB_CHUNK_INDEX,
  };
})();

var sdeLib = SDE_API;

// -----------------------------------------------------------------------------
// --- PUBLIC GLOBAL WRAPPER FUNCTIONS (GAS Entry Points) ---
// -----------------------------------------------------------------------------

function sde_job_START() {
  // NOTE: This assumes the calling function (in Main.js) handles setConfig first.
  sdeLib.sde_job_START();
}

function sde_job_PROCESS() {
  const lock = LockService.getScriptLock();
  try {
    if (!lock.tryLock(5000)) {
       sdeLib._scheduleSdeProcessRetry(30000); 
       return;
    }
    sdeLib.sde_job_PROCESS();
  } catch(e) {
    Logger.log("FATAL ERROR in sde_job_PROCESS wrapper:", e);
  } finally {
    try { lock.releaseLock(); } catch(e) {}
  }
}

function sde_job_FINALIZE() {
  sdeLib.sde_job_FINALIZE();
}