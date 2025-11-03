/* eslint-disable no-console */
/* eslint-disable no-unused-vars */

/**
 * SDE_Job_Controller.gs
 * This file is a self-contained module for running a stateful,
 * multi-step SDE import job that is resilient to the 6-minute execution limit.
 *
 * NOTE: This file assumes the constant SCRIPT_PROPS is declared once in Main.js.
 *
 * --- FIX v4.2 (User Design: Resumable Chunking) ---
 * - Implements the same resumable 'while' loop pattern as Orchestrator.js.
 * - buildSDEs now writes in chunks and watches the clock.
 * - If time limit is hit, it saves its row index to ScriptProperties and
 * returns 'false' (not finished).
 * - sde_job_PROCESS checks this return value and re-triggers itself to
 * resume the job, rather than advancing.
 * - This is the correct, fast, and robust solution.
 */

// --- FIX: Safely define global constants with 'var' ---
if (typeof SCRIPT_PROPS === 'undefined') {
  var SCRIPT_PROPS = PropertiesService.getScriptProperties();
}
if (typeof KEY_JOB_RUNNING === 'undefined') {
  var KEY_JOB_RUNNING = 'SDE_JOB_RUNNING';
}
if (typeof KEY_JOB_LIST === 'undefined') {
  var KEY_JOB_LIST = 'SDE_JOB_LIST';
}
if (typeof KEY_JOB_INDEX === 'undefined') {
  var KEY_JOB_INDEX = 'SDE_JOB_INDEX';
}
if (typeof KEY_BACKUP_SETTINGS === 'undefined') {
  var KEY_BACKUP_SETTINGS = 'SDE_BACKUP_SETTINGS';
}
if (typeof GLOBAL_STATE_KEY === 'undefined') {
  var GLOBAL_STATE_KEY = 'GLOBAL_SYSTEM_STATE'; // Maintenance Flag
}
// --- NEW: Resumable chunk index ---
if (typeof KEY_JOB_CHUNK_INDEX === 'undefined') {
  var KEY_JOB_CHUNK_INDEX = 'SDE_JOB_CHUNK_INDEX'; // Stores the next row to write
}
// --- END FIX ---


// --- Global Spreadsheet Object (Optimization: call getActiveSpreadsheet() once) ---
var SS;

/**
 * Lazy-loads the active spreadsheet object.
 * @returns {Spreadsheet} The active spreadsheet object.
 */
function getSS() {
  if (!SS) {
    SS = SpreadsheetApp.getActiveSpreadsheet();
  }
  return SS;
}

// -----------------------------------------------------------------------------
// --- SDE ENGINE LIBRARY (sdeLib) ---
// -----------------------------------------------------------------------------

const sdeLib = () => {

  // --- "Private" Helper Functions ---
  
  const downloadTextData = (csvFile) => {
    console.time("downloadTextData( csvFile:" + csvFile + " )");
    const baseURL = 'https://www.fuzzwork.co.uk/dump/latest/' + csvFile;
    const csvContent = UrlFetchApp.fetch(baseURL).getContentText();
    console.timeEnd("downloadTextData( csvFile:" + csvFile + " )");
    return csvContent.trim().replace(/\n$/, "");
  };

  const createOrClearSdeSheet = (activeSpreadsheet, sheetName) => {
    console.time("createOrClearSdeSheet({sheetName:" + sheetName + "})");
    if (!sheetName) throw "sheet name is required;";
    
    let workSheet = activeSpreadsheet.getSheetByName(sheetName); 
    
    if (workSheet) {
      workSheet.clearContents();
    } else {
      workSheet = activeSpreadsheet.insertSheet();
      workSheet.setName(sheetName);
    }
    console.timeEnd("createOrClearSdeSheet({sheetName:" + sheetName + "})");
    return workSheet;
  };

  const CSVToArray = (strData, strDelimiter = ",", headers = null, publishedOnly = true) => {
    // ... (This function is correct, no changes) ...
    console.time("CSVToArray(strData)");

    if (!strData || strData.trim().length === 0) {
        console.warn("CSVToArray: Input data string is empty. Returning empty array.");
        return [];
    }
    
    const allLines = Utilities.parseCsv(strData, strDelimiter.charCodeAt(0));
    
    if (allLines.length === 0) return [];
    
    const rawHeaders = allLines[0].map(h => h.trim());
    let arrData = [];
    let headersIndex = []; 
    
    const skipHeaders = !headers || !headers.length || !headers[0];
    
    if (!skipHeaders) {
        const outputHeaders = [];
        for (const requestedHeader of headers) {
            const index = rawHeaders.indexOf(requestedHeader);
            if (index !== -1) {
                headersIndex.push(index);
                outputHeaders.push(requestedHeader);
            } else {
              throw new Error(`CSVToArray: Requested header "${requestedHeader}" not found in CSV file.`);
            }
        }
        arrData.push(outputHeaders); 
    } else {
        headersIndex = rawHeaders.map((_, i) => i);
        arrData.push(rawHeaders); 
    }

    const expectedLength = arrData[0].length;
    if (expectedLength === 0) {
        console.warn("CSVToArray: No valid headers found or requested. Returning empty array.");
        return [];
    }

    const publishIdx = rawHeaders.indexOf("published");
    const startIndex = 1; 
    
    for (let i = startIndex; i < allLines.length; i++) {
        const cols = allLines[i]; 
        
        if (cols.length < rawHeaders.length) {
            console.warn(`Skipping row ${i}: Expected ${rawHeaders.length} columns, found ${cols.length}`);
            continue;
        }

        let skipRow = false;
        
        if (publishedOnly && publishIdx !== -1) {
            if (parseInt(cols[publishIdx]) !== 1) {
                skipRow = true;
            }
        }
        if (skipRow) continue;
        
        let row = [];
        
        for (const indexToKeep of headersIndex) {
            let cleanValue = (cols[indexToKeep] || "").trim(); 
            cleanValue = cleanValue.replace(/^'+(.*)$/, "''$1"); 
            
            if (!isNaN(cleanValue) && cleanValue !== '') {
                if (cleanValue.includes('.')) {
                    cleanValue = parseFloat(cleanValue);
                } else {
                    cleanValue = parseInt(cleanValue);
                }
            }
            row.push(cleanValue);
        }
        
        if (row.length === expectedLength) {
            arrData.push(row);
        }
    }

    console.timeEnd("CSVToArray(strData)");
    return arrData;
  };

  const autoResizeColumns = (workSheet) => {
    if (!workSheet) return;
    const lastColumn = workSheet.getLastColumn();
    if (lastColumn > 0) {
      workSheet.autoResizeColumns(1, lastColumn);
    }
  };

  // --- "Public" Class (Exposed via 'return') ---
  class SdePage {
    constructor(sheet, csvFile, headers = null, backupRanges = null, publishedOnly = true) {
      this.sheet = sheet;
      this.backupRanges = null;
      this.csvFile = csvFile;
      this.headers = null; 
      this.publishedOnly = false;
      if (headers != null) { 
        this.headers = headers;
        if (!Array.isArray(headers)) this.headers = [headers];
      }
      if (backupRanges != null) {
        this.backupRanges = backupRanges;
        if (!Array.isArray(backupRanges)) this.backupRanges = [backupRanges];
      }
      if (publishedOnly == null) {
        this.publishedOnly = true;
      } else {
        this.publishedOnly = publishedOnly;
      }
    }
  }

  // --- "Public" Engine Function (buildSDEs) ---
  // ---
  // --- THIS FUNCTION IS NOW v4.2 - Implements your resumable chunking design ---
  // ---
  const buildSDEs = (sdePage, scriptStartTime) => {
    if (sdePage == null) throw "sdePage is required";
    console.time("buildSDEs( sheetName:" + sdePage.sheet + ")");
    
    // --- Your Design Parameters ---
    const CHUNK_SIZE = 4000; // Write 5000 rows at a time
    const DOC_LOCK_TIMEOUT = 30000; // 30 second wait for DocumentLock
    const SCRIPT_TIME_LIMIT = 240000; // 4 minutes (240,000 ms)
    
    // --- Your Adaptive Throttle Parameters ---
    const THROTTLE_BASE_SLEEP_MS = 250;      // Min 0.25s sleep between writes
    const THROTTLE_LATENCY_FACTOR = 1.2;     // Sleep for 1.2x the last write duration
    const THROTTLE_MAX_SLEEP_MS = 5000;      // Max 5s sleep
    let lastWriteDurationMs = 500;           // Default for first loop
    // --- End Parameters ---

    const activeSpreadsheet = getSS(); 

    // STAGE 1: Fetch & Parse (Done once)
    const csvContent = downloadTextData(sdePage.csvFile); 
    const csvData = CSVToArray(csvContent, ",", sdePage.headers, sdePage.publishedOnly); 

    // --- CRASH-PROOF CHECK ---
    if (!csvData || csvData.length < 2 || csvData[0].length === 0) {
        console.warn(`FATAL_DATA_WARNING: Parsed data for ${sdePage.sheet} is empty or invalid. Skipping sheet update.`);
        return true; // Return true (finished) to skip this job
    }
    // --- END CRASH-PROOF CHECK ---

    let workSheet = null; 
    const docLock = LockService.getDocumentLock();

    // --- NEW: Read the saved chunk index ---
    let currentRow = parseInt(SCRIPT_PROPS.getProperty(KEY_JOB_CHUNK_INDEX) || '0', 10);
    // --- END NEW ---

    try {
      // STAGE 2: Prepare
      
      const headers = csvData.slice(0, 1);
      const dataRows = csvData.slice(1);
      const numCols = headers[0].length;
      workSheet = activeSpreadsheet.getSheetByName(sdePage.sheet);

      // --- NEW: Only clear/write headers if we are on the first chunk ---
      if (currentRow === 0) {
        console.log(`buildSDEs: First run for ${sdePage.sheet}. Clearing sheet and writing headers.`);
        docLock.waitLock(DOC_LOCK_TIMEOUT);
        try {
          workSheet = createOrClearSdeSheet(activeSpreadsheet, sdePage.sheet); 
          workSheet.getRange(1, 1, 1, numCols).setValues(headers);
        } finally {
          docLock.releaseLock();
        }
      } else {
        if (!workSheet) {
          throw new Error(`Sheet ${sdePage.sheet} not found on resume.`);
        }
        console.log(`buildSDEs: Resuming job for ${sdePage.sheet} from row ${currentRow}.`);
      }
      // --- END NEW ---
      
      // STAGE 3: Write & Finalize (in Chunks)
      console.info(`buildSDEs: Total rows to write: ${dataRows.length}. Starting at: ${currentRow}`);

      while (currentRow < dataRows.length) {
        // --- Your Timeout Check Logic ---
        const elapsedTime = new Date().getTime() - scriptStartTime; // Check against the PROCESS job start time
        if (elapsedTime > SCRIPT_TIME_LIMIT) {
          // --- PAUSE AND RESUME ---
          SCRIPT_PROPS.setProperty(KEY_JOB_CHUNK_INDEX, currentRow.toString());
          console.warn(`buildSDEs: Predictive timeout hit (${elapsedTime}ms). Saving state to resume at row ${currentRow}.`);
          console.timeEnd("buildSDEs( sheetName:" + sdePage.sheet + ")");
          return false; // --- Signal to sde_job_PROCESS to re-run this job ---
          // --- END PAUSE ---
        }
        // --- End Timeout Check ---

        // --- Your Adaptive Throttle Logic ---
        let sleepMs = Math.max(THROTTLE_BASE_SLEEP_MS, lastWriteDurationMs * THROTTLE_LATENCY_FACTOR);
        sleepMs = Math.min(THROTTLE_MAX_SLEEP_MS, sleepMs); // Cap at max
        console.log(`buildSDEs: Throttling for ${sleepMs.toFixed(0)}ms (based on last write of ${lastWriteDurationMs}ms)`);
        Utilities.sleep(sleepMs);
        // --- End Throttle ---

        const chunkEnd = Math.min(currentRow + CHUNK_SIZE, dataRows.length);
        const chunk = dataRows.slice(currentRow, chunkEnd);
        const writeRow = currentRow + 2; // +1 for 1-based index, +1 for header row
        
        if (chunk.length > 0) {
          // --- Your DocumentLock Logic ---
          docLock.waitLock(DOC_LOCK_TIMEOUT);
          try {
            console.log(`buildSDEs: Writing chunk to ${sdePage.sheet}. Rows ${writeRow} to ${writeRow + chunk.length - 1}`);
            
            const chunkStartTime = new Date().getTime(); // Time the write
            workSheet.getRange(writeRow, 1, chunk.length, numCols).setValues(chunk);
            lastWriteDurationMs = new Date().getTime() - chunkStartTime; // Update for next loop

          } finally {
            docLock.releaseLock();
          }
          // --- End DocumentLock Logic ---
        }
        currentRow = chunkEnd;
      }
      
      console.log(`buildSDEs: Finished writing all ${dataRows.length} data rows.`);
      
      autoResizeColumns(workSheet);

    } catch (e) {
      // Ensure lock is released on error if it was held
      if (docLock.hasLock()) {
        docLock.releaseLock();
      }
      // --- NEW: Save chunk index on failure ---
      SCRIPT_PROPS.setProperty(KEY_JOB_CHUNK_INDEX, currentRow.toString());
      console.error(`buildSDEs: Error during write. State saved to resume at row ${currentRow}.`);
      // --- END NEW ---
      throw e; // Re-throw the error to be caught by sde_job_PROCESS
    }
    
    // --- JOB IS FINISHED ---
    console.timeEnd("buildSDEs( sheetName:" + sdePage.sheet + ")");
    SCRIPT_PROPS.setProperty(KEY_JOB_CHUNK_INDEX, '0'); // Reset chunk index for next job
    return true; // --- Signal to sde_job_PROCESS that this job is done ---
  };

  // --- Return the Public Interface ---
  return {
    SdePage: SdePage, // SdePage class is now defined inside sdeLib
    buildSDEs: buildSDEs,
    // --- Expose private functions for the new processor ---
    downloadTextData: downloadTextData,
    CSVToArray: CSVToArray,
    createOrClearSdeSheet: createOrClearSdeSheet,
    autoResizeColumns: autoResizeColumns
  };
};


// -----------------------------------------------------------------------------
// --- STATEFUL JOB CONTROLLER FUNCTIONS ---
// -----------------------------------------------------------------------------

/**
 * Helper function to delete triggers.
 */
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
}

/**
 * Helper: Checks if the SDE update is running.
 */
function isSdeJobRunning() {
  return SCRIPT_PROPS.getProperty(KEY_JOB_RUNNING) === 'true';
}

/**
 * STAGE 1: START (Called by user)
 * Runs silently without UI interruptions.
 */
function sde_job_START() {
  console.log('--- SDE JOB START INITIATED (Silent Mode) ---');
  
  if (isSdeJobRunning()) {
    Logger.log('START: Job already running. Aborting new request.');
    return;
  }
  
  // --- FIX: Use ScriptLock and wait ---
  const lock = LockService.getScriptLock();
  try {
    console.log('START: Attempting to acquire ScriptLock (max wait 7 min)...');
    lock.waitLock(420000); // Wait up to 7 minutes
    console.log('START: ScriptLock acquired.');
  } catch (e) {
    console.error(`START: Failed to acquire ScriptLock. Another process is running. Aborting. ${e.message}`);
    Logger.log('START: Failed to acquire ScriptLock. Aborting.');
    return;
  }
  // --- END FIX ---
  
  try {
    SCRIPT_PROPS.setProperty(KEY_JOB_RUNNING, 'true');
    
    // Halt Formulas
    const ss = getSS();
    const loadingHelper = ss.getRangeByName("'Utility'!B3:C3");
    const backupSettings = loadingHelper.getValues();
    loadingHelper.setValues([[0, 0]]);
    SCRIPT_PROPS.setProperty(KEY_BACKUP_SETTINGS, JSON.stringify(backupSettings));
    
    // --- START: MODIFICATION TO SET MAINTENANCE FLAG ---
    console.log('START: Setting system to MAINTENANCE mode.');
    SCRIPT_PROPS.setProperty(GLOBAL_STATE_KEY, 'MAINTENANCE');
    // We still delete the master orchestrator to be safe
    _deleteTriggersFor('masterOrchestrator');
    console.log('START: Orchestrator trigger deleted. System is halted.');
    // --- END: MODIFICATION ---

    // Define the Job List
    const { SdePage } = sdeLib(); // Get SdePage class from the library
    const sdePages = [
      new SdePage("SDE_invTypes", "invTypes.csv",  [ "typeID","groupID","typeName","volume"]), 
      new SdePage("SDE_staStations", "staStations.csv", ["stationID", "stationName", "solarSystemID", "regionID"]),
      new SdePage("SDE_industryActivityProducts", "industryActivityProducts.csv", ["typeID", "activityID", "productTypeID", "quantity"]),
      new SdePage("SDE_industryActivityMaterials", "industryActivityMaterials.csv", ["typeID", "activityID", "materialTypeID", "quantity"]),
      new SdePage("SDE_invGroups", "invGroups.csv", ["groupID", "categoryID", "groupName"]),
      new SdePage("SDE_invCategories", "invCategories.csv", ["categoryID", "categoryName"]),
    ];

    // Save State & Start First Trigger
    SCRIPT_PROPS.setProperty(KEY_JOB_LIST, JSON.stringify(sdePages));
    SCRIPT_PROPS.setProperty(KEY_JOB_INDEX, '0');
    SCRIPT_PROPS.setProperty(KEY_JOB_CHUNK_INDEX, '0'); // <-- NEW: Initialize chunk index
    _deleteTriggersFor('sde_job_PROCESS');
    Logger.log(`START: Saved ${sdePages.length} pages. Creating trigger for sde_job_PROCESS.`);
    ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(5000).create();

  } catch (e) {
    Logger.log(`ERROR in sde_job_START: ${e.message} at line ${e.lineNumber}. SYSTEM HALTED.`);
    // --- FIX: DO NOT CALL FINALIZE ON ERROR ---
  } finally {
    lock.releaseLock();
    console.log('START: Lock released.');
  }
}

/**
 * STAGE 2: PROCESS (Run by a trigger)
 * --- NEW v4.2: Checks return value of buildSDEs ---
 */
function sde_job_PROCESS() {
  const SCRIPT_START_TIME = new Date().getTime(); // Pass this to buildSDEs for timeout check
  
  if (SCRIPT_PROPS.getProperty(KEY_JOB_RUNNING) !== 'true') {
    Logger.log('PROCESS: Job flag cleared (cancelled). Aborting trigger.');
    return;
  }
  
  // --- FIX: Use ScriptLock ---
  const lock = LockService.getScriptLock();
  try {
    console.log('PROCESS: Attempting to acquire ScriptLock (max wait 5s)...');
    if (!lock.tryLock(5000)) {
       Logger.log('PROCESS: Lock contention. Re-triggering for later attempt.');
       _deleteTriggersFor('sde_job_PROCESS');
       ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(30000).create();
       Logger.log('PROCESS: Created new trigger to retry in 30 seconds.');
       return;
    }
    console.log('PROCESS: ScriptLock acquired.');
  } catch (e) {
     Logger.log('PROCESS: Lock error. Re-triggering for later attempt.');
     _deleteTriggersFor('sde_job_PROCESS');
     ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(30000).create();
     return;
  }
  // --- END FIX ---

  let jobIndex = -1; 
  try {
    const jobListJSON = SCRIPT_PROPS.getProperty(KEY_JOB_LIST);
    const jobIndexStr = SCRIPT_PROPS.getProperty(KEY_JOB_INDEX);
    const jobList = JSON.parse(jobListJSON);
    jobIndex = parseInt(jobIndexStr, 10);

    if (jobIndex >= jobList.length) {
      Logger.log('PROCESS: Index reached end of list. Calling FINALIZE.');
      sde_job_FINALIZE(); 
      return;
    }

    const SDE = sdeLib();
    const currentJob = jobList[jobIndex];
    // Reconstruct the SdePage object from the plain JSON
    const sdePage = new SDE.SdePage(currentJob.sheet, currentJob.csvFile, currentJob.headers, currentJob.backupRanges, currentJob.publishedOnly);

    Logger.log(`PROCESS: Running Job ${jobIndex + 1} of ${jobList.length}: ${currentJob.sheet}`);
    
    // RUN THE ACTUAL FILE TRANSFER
    // This call now handles its own chunking and will return TRUE (finished) or FALSE (paused)
    const jobFinished = SDE.buildSDEs(sdePage, SCRIPT_START_TIME);
    
    // --- NEW: Check return value ---
    if (jobFinished === true) {
      // Job is done, advance to next job
      Logger.log(`PROCESS: Finished job ${currentJob.sheet}. Scheduling next job.`);
      SCRIPT_PROPS.setProperty(KEY_JOB_INDEX, (jobIndex + 1).toString());
      // Note: buildSDEs already reset the chunk index to 0
    } else {
      // Job is NOT done (hit time limit), re-run this same job
      Logger.log(`PROCESS: Pausing job ${currentJob.sheet}. Re-scheduling to resume.`);
      // Do not change jobIndex. buildSDEs already saved the chunkIndex.
    }
    // --- END NEW ---
    
    // Re-trigger for the next step (either resume or new job)
    _deleteTriggersFor('sde_job_PROCESS'); 
    ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(2000).create();

  } catch (e) {
    Logger.log(`FATAL ERROR in sde_job_PROCESS (Job ${jobIndex}): ${e.message} at line ${e.lineNumber}. SYSTEM HALTED.`);
    // --- FIX: DO NOT CALL FINALIZE ON ERROR ---
  } finally {
    lock.releaseLock();
    console.log('PROCESS: Lock released.');
  }
}

/**
 * STAGE 3: FINALIZE (Called by user or by process)
 * Runs silently without UI interruptions.
 */
function sde_job_FINALIZE() {
  // --- FIX: Use ScriptLock and wait ---
  const lock = LockService.getScriptLock();
  try {
    console.log('FINALIZE: Attempting to acquire ScriptLock (max wait 7 min)...');
    lock.waitLock(420000); // Wait up to 7 minutes
    console.log('FINALIZE: ScriptLock acquired.');
  } catch (e) {
    Logger.log('FINALIZE: Lock unavailable. Aborting.');
    return;
  }
  // --- END FIX ---
  
  console.log('--- SDE JOB FINALIZE STARTED (Silent Mode) ---');

  try {
    // 1. Release formula lock
    const backupSettingsJSON = SCRIPT_PROPS.getProperty(KEY_BACKUP_SETTINGS);
    if (backupSettingsJSON) {
      const backupSettings = JSON.parse(backupSettingsJSON);
      const ss = getSS(); // Optimization
      const loadingHelper = ss.getRangeByName("'Utility'!B3:C3");
      loadingHelper.setValues(backupSettings);
      Logger.log('FINALIZE: Restored formula settings.');
    }

    // 2. Restart Orchestrator & Clear Maintenance Flag
    // --- FIX: Set state back to RUNNING ---
    SCRIPT_PROPS.setProperty(GLOBAL_STATE_KEY, 'RUNNING');
    Logger.log('FINALIZE: System state set to RUNNING.');
    // --- END FIX ---
    _deleteTriggersFor('masterOrchestrator'); 
    ScriptApp.newTrigger('masterOrchestrator').timeBased().everyMinutes(15).create();
    Logger.log('FINALIZE: Orchestrator trigger recreated.');

    // 3. Clear all state properties and triggers
    SCRIPT_PROPS.deleteProperty(KEY_JOB_RUNNING);
    SCRIPT_PROPS.deleteProperty(KEY_JOB_LIST);
    SCRIPT_PROPS.deleteProperty(KEY_JOB_INDEX);
    SCRIPT_PROPS.deleteProperty(KEY_JOB_CHUNK_INDEX); // <-- NEW: Clear chunk index
    SCRIPT_PROPS.deleteProperty(KEY_BACKUP_SETTINGS);
    SCRIPT_PROPS.deleteProperty('finalizationStep');
    _deleteTriggersFor('sde_job_PROCESS');
    Logger.log('FINALIZE: All state properties and job triggers cleared. Cleanup complete.');

  } catch (e) {
    Logger.log(`ERROR in sde_job_FINALIZE: ${e.message} at line ${e.lineNumber}`);
  } finally {
    lock.releaseLock();
    console.log('--- SDE JOB FINALIZE COMPLETE ---');
  }
}