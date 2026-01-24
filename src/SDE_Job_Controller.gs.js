/* eslint-disable no-console */
/* eslint-disable no-unused-vars */

/**
 * SDE_Job_Controller.gs
 * Stateful, multi-step SDE import job resilient to execution limits.
 */

// --- Safely define global constants ---
if (typeof KEY_JOB_RUNNING === 'undefined') { var KEY_JOB_RUNNING = 'SDE_JOB_RUNNING'; }
if (typeof KEY_JOB_LIST === 'undefined') { var KEY_JOB_LIST = 'SDE_JOB_LIST'; }
if (typeof KEY_JOB_INDEX === 'undefined') { var KEY_JOB_INDEX = 'SDE_JOB_INDEX'; }
if (typeof KEY_BACKUP_SETTINGS === 'undefined') { var KEY_BACKUP_SETTINGS = 'SDE_BACKUP_SETTINGS'; }
if (typeof GLOBAL_STATE_KEY === 'undefined') { var GLOBAL_STATE_KEY = 'GLOBAL_SYSTEM_STATE'; }
if (typeof KEY_JOB_CHUNK_INDEX === 'undefined') { var KEY_JOB_CHUNK_INDEX = 'SDE_JOB_CHUNK_INDEX'; }

var SS;
function getSS() {
  if (!SS) { SS = SpreadsheetApp.getActiveSpreadsheet(); }
  return SS;
}

// -----------------------------------------------------------------------------
// --- SDE ENGINE LIBRARY (sdeLib) ---
// -----------------------------------------------------------------------------

const sdeLib = () => {
  let _sheetCache = {};

 const downloadTextData = (csvFile) => {
    console.time("downloadTextData( csvFile:" + csvFile + " )");
    const baseURL = 'https://raw.githubusercontent.com/cjkilman/eve-sde-dump/main/' + csvFile;
    
    try {
      const csvContent = UrlFetchApp.fetch(baseURL).getContentText();
      console.timeEnd("downloadTextData( csvFile:" + csvFile + " )");
      return csvContent.trim().replace(/\n$/, "");
    } catch (e) {
      // --- ABORT ON ERROR: Quota/Limit Detection ---
      if (e.message.includes('too many times') || e.message.includes('limit exceeded')) {
        console.error("CRITICAL: SDE Download hit Google Quota. Shutting down SDE Job.");
        sde_job_KILL_ALL_TRIGGERS(); // Hard stop
      }
      throw e; 
    }
  };

  /**
   * Internal helper: Ensures a sheet exists without destructive clearing.
   */
  const getOrCreateSheet = (activeSpreadsheet, sheetName) => {
    let sheet = activeSpreadsheet.getSheetByName(sheetName);
    if (!sheet) {
      sheet = activeSpreadsheet.insertSheet(sheetName, activeSpreadsheet.getNumSheets());
      sheet.setName(sheetName);
    }
    return sheet;
  };



  /**
   * Prepares the sheet. Row 1 is cleared to be overwritten by sanitized headers.
   */
  const createOrClearSdeSheet = (activeSpreadsheet, sheetName) => {
    let sheet = getOrCreateSheet(activeSpreadsheet, sheetName);
    sheet.clearContents();
    _sheetCache[sheetName] = sheet;
    return sheet;
  };

  const CSVToArray = (strData, strDelimiter = ",", headers = null, publishedOnly = true) => {
    const allLines = Utilities.parseCsv(strData, strDelimiter.charCodeAt(0));
    if (allLines.length === 0) return [];

    const rawHeaders = allLines[0].map(h => h.trim());

    let colIndices = (headers && headers.length > 0)
      ? headers.map(h => rawHeaders.indexOf(h)).filter(idx => idx !== -1)
      : rawHeaders.map((_, i) => i);

    const publishIdx = rawHeaders.indexOf("published");
    const marketGroupIdx = rawHeaders.indexOf("marketGroupID");

    let arrData = [];

    for (let i = 1; i < allLines.length; i++) {
      const cols = allLines[i];

      // Gates
      if (publishedOnly === true && publishIdx !== -1) {
        const pubValue = String(cols[publishIdx]).trim();
        if (pubValue !== '1' && pubValue.toLowerCase() !== 'true') continue;
      }
      if (marketGroupIdx !== -1) {
        const mgValue = String(cols[marketGroupIdx]).trim().toLowerCase();
        if (mgValue === "" || mgValue === "null" || mgValue === "0") continue;
      }

      // --- ESCAPE LOGIC ---
      let sanitizedRow = colIndices.map(idx => {
        let val = String(cols[idx] || "").trim();

        // 1. If it's a Number -> Add ' to prevent scientific notation (e.g. '34)
        if (val !== "" && !isNaN(val)) {
          return "'" + val;
        }

        // 2. If it starts with ' -> Add ANOTHER ' so it displays correctly (e.g. ''Arbalest')
        if (val.startsWith("'")) {
          return "'" + val;
        }

        // 3. Else -> Leave it alone
        return val;
      });
      arrData.push(sanitizedRow);
    }

    // Headers: Always tick them to be safe
    const finalHeaders = (headers && headers.length > 0) ? headers : rawHeaders;
    arrData.unshift(finalHeaders.map(h => "'" + h));

    return arrData;
  };

  function _writeChunkInternal(dataChunk, startRow, numCols, sheetName) {
    const chunkStartTime = new Date().getTime();
    const docLock = LockService.getDocumentLock();
    if (!docLock.tryLock(5000)) return { success: false, duration: 0 };

    try {
      let workSheet = _sheetCache[sheetName];
      if (!workSheet) throw new Error(`Sheet '${sheetName}' not in cache.`);
      workSheet.getRange(startRow, 1, dataChunk.length, numCols).setValues(dataChunk);
    } finally {
      docLock.releaseLock();
    }
    return { success: true, duration: new Date().getTime() - chunkStartTime };
  }

  class SdePage {
    constructor(sheet, csvFile, headers = null, backupRanges = null, publishedOnly = true) {
      this.sheet = sheet;
      this.csvFile = csvFile;
      this.headers = (headers && !Array.isArray(headers)) ? [headers] : headers;
      this.backupRanges = (backupRanges && !Array.isArray(backupRanges)) ? [backupRanges] : backupRanges;
      this.publishedOnly = (publishedOnly == null) ? true : publishedOnly;
    }
  }

  const buildSDEs = (sdePage, scriptStartTime) => {
    if (sdePage == null) throw "sdePage is required";
    const activeSpreadsheet = getSS();

    const csvContent = downloadTextData(sdePage.csvFile);
    const csvData = CSVToArray(csvContent, ",", sdePage.headers, sdePage.publishedOnly);

    if (!csvData || csvData.length < 1) return true;

    const numCols = csvData[0].length;
    let currentRow = parseInt(SCRIPT_PROPS.getProperty(KEY_JOB_CHUNK_INDEX) || '0', 10);
    let finalSheetReference;

    if (currentRow === 0) {
      finalSheetReference = createOrClearSdeSheet(activeSpreadsheet, sdePage.sheet);
    } else {
      finalSheetReference = activeSpreadsheet.getSheetByName(sdePage.sheet);
      _sheetCache[sdePage.sheet] = finalSheetReference;
    }

    while (currentRow < csvData.length) {
      // 285000ms = 4.75 minutes (Safety margin for 6-minute limit)
      if ((new Date().getTime() - scriptStartTime) > 285000) {
        SCRIPT_PROPS.setProperty(KEY_JOB_CHUNK_INDEX, currentRow.toString());
        return false;
      }

      const chunkSize = 2000;
      const chunkEnd = Math.min(currentRow + chunkSize, csvData.length);
      const chunk = csvData.slice(currentRow, chunkEnd);

      let result = _writeChunkInternal(chunk, currentRow + 1, numCols, sdePage.sheet);
      if (result.success) {
        currentRow = chunkEnd;
      } else {
        Utilities.sleep(1000);
      }
    }

    // --- Final Trimming ---
    // This runs only when the loop completes successfully
    SCRIPT_PROPS.setProperty(KEY_JOB_CHUNK_INDEX, '0');

    const maxCols = finalSheetReference.getMaxColumns();
    const maxRows = finalSheetReference.getMaxRows();
    const dataRows = csvData.length;

    if (maxCols > numCols) {
      finalSheetReference.deleteColumns(numCols + 1, maxCols - numCols);
    }
    if (maxRows > dataRows) {
      finalSheetReference.deleteRows(dataRows + 1, maxRows - dataRows);
    }

    return true;
  };

  // This closes the sdeLib arrow function
  return { SdePage, buildSDEs };
};

// -----------------------------------------------------------------------------
// --- CONTROLLER FUNCTIONS ---
// -----------------------------------------------------------------------------

/**
 * Helper: Checks if the SDE update is running.
 */
function isSdeJobRunning() {
  return SCRIPT_PROPS.getProperty(KEY_JOB_RUNNING) === 'true';
}

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
 * STAGE 1: START (Called by user)
 * Updated: Orchestrator trigger logic removed.
 */
function sde_job_START() {
  console.log('--- SDE JOB START INITIATED (Silent Mode) ---');

  if (isSdeJobRunning()) {
    Logger.log('START: Job already running. Aborting new request.');
    return;
  }
  // 1. RUN THE HOOK FIRST (Before locking anything)
  const shouldContinue = tryCallHook('ON_SDE_START');

  if (shouldContinue === false) {
    console.log('START: Process cancelled by User (ON_SDE_START returned false).');
    SpreadsheetApp.getActiveSpreadsheet().toast("Update Cancelled.", "System", 3);
    return; // STOP EVERYTHING
  }

  // --- Robust Lock Handling ---
  const lock = LockService.getScriptLock();
  let lockAcquired = false;
  try {
    console.log('START: Attempting to acquire ScriptLock (max wait 7 min)...');
    lock.waitLock(420000);
    lockAcquired = true;
    console.log('START: ScriptLock acquired.');

    SCRIPT_PROPS.setProperty(KEY_JOB_RUNNING, 'true');

    // --- DYNAMIC UTILITY CONFIG ---
    let utilConf = { sheetName: "Utility", range: "B3:C3" }; // Default Fallback
    if (typeof GET_UTILITY_CONFIG === 'function') {
      utilConf = GET_UTILITY_CONFIG();
    }

    // Halt Formulas
    const ss = getSS();
    const loadingHelper = ss.getRange(`'${utilConf.sheetName}'!${utilConf.range}`);

    // Capture current values to restore later
    const backupSettings = loadingHelper.getValues();
    SCRIPT_PROPS.setProperty(KEY_BACKUP_SETTINGS, JSON.stringify(backupSettings));

    // Create a zero-filled array matching the range size (Dynamic "Off" Switch)
    const zeroValues = backupSettings.map(r => r.map(() => 0));
    loadingHelper.setValues(zeroValues);
    SpreadsheetApp.flush();

    // --- Setting MAINTENANCE FLAG (Optional: Keep if other tools use it, otherwise safe to remove) ---
    console.log('START: Setting system to MAINTENANCE mode.');
    SCRIPT_PROPS.setProperty(GLOBAL_STATE_KEY, 'MAINTENANCE');



    // --- DYNAMIC CONFIGURATION LOADER ---
    const { SdePage } = sdeLib();
    let configRaw = [];

    // 1. Check if the active project has a specific config function
    if (typeof GET_SDE_CONFIG === 'function') {
      console.log('START: Loading project-specific SDE configuration.');
      configRaw = GET_SDE_CONFIG();
    } else {
      // 2. Fallback Default (If you forget to add the config to Main.js)
      console.warn('START: No GET_SDE_CONFIG found. Using DEFAULT fallback list.');
      configRaw = [
        { name: "SDE_invTypes", file: "invTypes.csv", cols: ["typeID", "groupID", "typeName", "volume", "marketGroupID", "basePrice"] },
        { name: "SDE_invGroups", file: "invGroups.csv", cols: null },
      ];
    }

    // 3. Convert JSON Config to SdePage Objects
    const sdePages = configRaw.map(item => new SdePage(item.name, item.file, item.cols));

    if (sdePages.length === 0) {
      throw new Error("SDE Config is empty! Check GET_SDE_CONFIG in Main.js");
    }

    // Save State & Start First Trigger
    SCRIPT_PROPS.setProperty(KEY_JOB_LIST, JSON.stringify(sdePages));
    SCRIPT_PROPS.setProperty(KEY_JOB_INDEX, '0');
    SCRIPT_PROPS.setProperty(KEY_JOB_CHUNK_INDEX, '0');
    _deleteTriggersFor('sde_job_PROCESS');
    Logger.log(`START: Saved ${sdePages.length} pages. Creating trigger for sde_job_PROCESS.`);
    ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(5000).create();

  } catch (e) {
    if (!lockAcquired) {
      console.error(`START: Failed to acquire ScriptLock. Another process is running. Aborting. ${e.message}`);
    } else {
      Logger.log(`ERROR in sde_job_START: ${e.message} at line ${e.lineNumber}. SYSTEM HALTED.`);
    }
  } finally {
    if (lockAcquired) {
      lock.releaseLock();
      console.log('START: Lock released.');
    }
  }
}

/**
 * Safe Hook Caller
 * Returns the result of the function (or true if function doesn't exist)
 */
function tryCallHook(functionName) {
  if (typeof this[functionName] === 'function') {
    console.log(`HOOK: Found '${functionName}'. Executing...`);
    try {
      const result = this[functionName]();
      // If the function returns nothing (undefined), assume it meant "True/Continue"
      return result === undefined ? true : result;
    } catch (e) {
      console.warn(`HOOK: Error running '${functionName}': ${e.message}`);
      return true; // Default to continue if hook fails
    }
  }
  return true; // Default to continue if hook is missing
}

/**
 * Hard abort for emergency quota saving.
 * Must be a top-level function to be visible to the PROCESS catch block.
 */
function sde_job_KILL_ALL_TRIGGERS() {
  const SCRIPT_PROPS = PropertiesService.getScriptProperties();
  
  // 1. Delete the repeating SDE trigger to stop the loop
  ScriptApp.getProjectTriggers().forEach(t => {
    if (t.getHandlerFunction() === 'sde_job_PROCESS') {
      ScriptApp.deleteTrigger(t);
    }
  });
  
  // 2. Clear the running flag so other workers know the job is dead
  SCRIPT_PROPS.deleteProperty('SDE_JOB_RUNNING');
  
  console.error("SYSTEM: Emergency Shutdown. SDE triggers purged due to Quota exhaustion.");
}

/**
 * STAGE 2: PROCESS (Run by a trigger)
 * Checks return value of buildSDEs for resume/advance logic.
 * Includes resilience fix for API timeouts.
 */
function sde_job_PROCESS() {
  const SCRIPT_START_TIME = new Date().getTime(); // Pass this to buildSDEs for timeout check

  if (SCRIPT_PROPS.getProperty(KEY_JOB_RUNNING) !== 'true') {
    Logger.log('PROCESS: Job flag cleared (cancelled). Aborting trigger.');
    return;
  }

  // --- ScriptLock acquisition (prevents concurrent PROCESS executions) ---
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
    // This should only happen if tryLock itself fails unexpectedly (not contention)
    Logger.log('PROCESS: Lock acquisition error. Re-triggering for later attempt.');
    _deleteTriggersFor('sde_job_PROCESS');
    ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(30000).create();
    return;
  }
  // --- END ScriptLock ---

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

    // RUN THE ACTUAL FILE TRANSFER (handles chunking/pausing internally)
    const jobFinished = SDE.buildSDEs(sdePage, SCRIPT_START_TIME);

    // Check return value
    if (jobFinished === true) {
      // Job is done, advance to next job
      Logger.log(`PROCESS: Finished job ${currentJob.sheet}. Scheduling next job.`);
      SCRIPT_PROPS.setProperty(KEY_JOB_INDEX, (jobIndex + 1).toString());
    } else {
      // Job is NOT done (hit time limit or lock contention on initial clear), re-run this same job
      Logger.log(`PROCESS: Pausing job ${currentJob.sheet}. Re-scheduling to resume.`);
      // Do not change jobIndex. buildSDEs already saved the chunkIndex.
    }

    // Re-trigger for the next step (either resume or new job)
    _deleteTriggersFor('sde_job_PROCESS');
    ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(2000).create();

} catch (e) {
    const errorMessage = e.message.toLowerCase();

    // --- NEW: QUOTA CHECK ---
    // If we hit the quota, we MUST kill all triggers immediately to stop the loop.
    if (errorMessage.includes("too many times") || errorMessage.includes("limit exceeded")) {
      console.error("ABORTING: Quota reached. Shutting down SDE Job.");
      sde_job_KILL_ALL_TRIGGERS(); 
      return; // Do not schedule any further triggers
    }

    // Check for fatal errors that should NOT resume
    if (errorMessage.includes("csvtoarray") || errorMessage.includes("not found") || errorMessage.includes("critical")) {
      Logger.log(`FATAL ERROR in sde_job_PROCESS (Job ${jobIndex}): ${e.message}. Calling FINALIZE to abort.`);
      sde_job_FINALIZE(); 

    } else {
      // Assume it's a temporary timeout (like a network hiccup), re-trigger to attempt resume.
      Logger.log(`RESUMABLE ERROR in sde_job_PROCESS (Job ${jobIndex}): ${e.message}. Re-triggering to attempt resume.`);
      _deleteTriggersFor('sde_job_PROCESS');
      ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(10000).create();
    }
  } finally {
    lock.releaseLock();
    console.log('PROCESS: Lock released.');
  }
}


/**
 * STAGE 3: FINALIZE (Called by user or by process)
 */
function sde_job_FINALIZE() {
  // --- Robust Lock Handling ---
  const lock = LockService.getScriptLock();
  let lockAcquired = false;
  try {
    console.log('FINALIZE: Attempting to acquire ScriptLock (max wait 7 min)...');
    lock.waitLock(420000);
    lockAcquired = true;
    console.log('FINALIZE: ScriptLock acquired.');

    console.log('--- SDE JOB FINALIZE STARTED (Silent Mode) ---');

    // 1. Release formula lock
    const backupSettingsJSON = SCRIPT_PROPS.getProperty(KEY_BACKUP_SETTINGS);
    if (backupSettingsJSON) {
      // Re-fetch config in case it changed (or just to get location)
      let utilConf = { sheetName: "Utility", range: "B3:C3" };
      if (typeof GET_UTILITY_CONFIG === 'function') {
        utilConf = GET_UTILITY_CONFIG();
      }

      const backupSettings = JSON.parse(backupSettingsJSON);
      const ss = getSS();
      const loadingHelper = ss.getRange(`'${utilConf.sheetName}'!${utilConf.range}`);
      loadingHelper.setValues(backupSettings);
      Logger.log(`FINALIZE: Restored formula settings to ${utilConf.sheetName}!${utilConf.range}.`);
    }

    // 2. Clear Maintenance Flag (WITHOUT restarting Orchestrator)
    SCRIPT_PROPS.setProperty(GLOBAL_STATE_KEY, 'RUNNING');
    Logger.log('FINALIZE: System state set to RUNNING.');

    tryCallHook('ON_SDE_COMPLETE');

    // 3. Clear all state properties and triggers
    SCRIPT_PROPS.deleteProperty(KEY_JOB_RUNNING);
    SCRIPT_PROPS.deleteProperty(KEY_JOB_LIST);
    SCRIPT_PROPS.deleteProperty(KEY_JOB_INDEX);
    SCRIPT_PROPS.deleteProperty(KEY_JOB_CHUNK_INDEX);
    SCRIPT_PROPS.deleteProperty(KEY_BACKUP_SETTINGS);
    SCRIPT_PROPS.deleteProperty('finalizationStep');
    _deleteTriggersFor('sde_job_PROCESS');
    Logger.log('FINALIZE: All state properties and job triggers cleared. Cleanup complete.');

  } catch (e) {
    if (!lockAcquired) {
      Logger.log('FINALIZE: Lock unavailable. Aborting.');
    } else {
      Logger.log(`ERROR in sde_job_FINALIZE: ${e.message} at line ${e.lineNumber}`);
    }
  } finally {
    if (lockAcquired) {
      lock.releaseLock();
      console.log('--- SDE JOB FINALIZE COMPLETE ---');
    }
  }
}