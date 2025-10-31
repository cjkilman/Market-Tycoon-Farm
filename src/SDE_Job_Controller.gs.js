/**
 * SDE_Job_Controller.gs
 * This file is a self-contained module for running a stateful,
 * multi-step SDE import job that is resilient to the 6-minute execution limit.
 *
 * NOTE: This file assumes the constant SCRIPT_PROPS is declared once in Main.js.
 */

// --- Global Constants for Stateful Job Model ---
const KEY_JOB_RUNNING = 'SDE_JOB_RUNNING';
const KEY_JOB_LIST = 'SDE_JOB_LIST';
const KEY_JOB_INDEX = 'SDE_JOB_INDEX';
const KEY_BACKUP_SETTINGS = 'SDE_BACKUP_SETTINGS';

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

  /**
   * Retrieves or creates the worksheet object, clearing its contents if it exists.
   */
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

  const deleteBlankColumnsAndColumns = (workSheet) => {
    if (!workSheet) throw "workSheet not defined";
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
  };

  /**
   * Robust CSV parser that handles column filtering and type conversion.
   * (This is the final, resilient version that prevents column mismatch errors.)
   */
  /**
   * Robust CSV parser that handles column filtering and type conversion.
   * (This is the final, resilient version that prevents column mismatch errors.)
   */
  const CSVToArray = (strData, strDelimiter = ",", headers = null, publishedOnly = true) => {
    console.time("CSVToArray(strData)");

    if (!strData || strData.trim().length === 0) {
        console.warn("CSVToArray: Input data string is empty. Returning empty array.");
        return [];
    }
    
    // Use Google's robust parser. This returns a 2D array.
    const allLines = Utilities.parseCsv(strData, strDelimiter.charCodeAt(0));
    
    if (allLines.length === 0) return [];
    
    // --- 1. Determine Column Indices and Filter Headers ---
    const rawHeaders = allLines[0].map(h => h.trim());
    let arrData = [];
    let headersIndex = []; // The indices of columns we want to keep
    
    const skipHeaders = !headers || !headers.length || !headers[0];
    
    if (!skipHeaders) {
        // We only keep the headers that were requested, in the order requested.
        const outputHeaders = [];
        for (const requestedHeader of headers) {
            const index = rawHeaders.indexOf(requestedHeader);
            if (index !== -1) {
                headersIndex.push(index);
                outputHeaders.push(requestedHeader);
            }
        }
        arrData.push(outputHeaders); // Add the filtered header row
    } else {
        // If 'headers' is null (all columns), keep ALL indices.
        headersIndex = rawHeaders.map((_, i) => i);
        arrData.push(rawHeaders); // Add the original header row
    }

    const expectedLength = arrData[0].length;
    if (expectedLength === 0) {
        console.warn("CSVToArray: No valid headers found or requested. Returning empty array.");
        return [];
    }

    // --- 2. Process Data Rows ---
    const publishIdx = rawHeaders.indexOf("published");
    const startIndex = 1; // Start after headers
    
    for (let i = startIndex; i < allLines.length; i++) {
        const cols = allLines[i]; // This is now a pre-parsed array
        
        // CRITICAL BOUNDS CHECK: Ensure row is not malformed
        if (cols.length < rawHeaders.length) {
            console.warn(`Skipping row ${i}: Expected ${rawHeaders.length} columns, found ${cols.length}`);
            continue;
        }

        let skipRow = false;
        
        // Check publishedOnly filter
        if (publishedOnly && publishIdx !== -1) {
            // Check the *original* column array 'cols'
            if (parseInt(cols[publishIdx]) !== 1) {
                skipRow = true;
            }
        }
        if (skipRow) continue;
        
        let row = [];
        
        // Filter and process columns based only on the indices we gathered in Step 1
        for (const indexToKeep of headersIndex) {
            // We no longer need the bounds check (if (indexToKeep >= cols.length)) 
            // because we checked cols.length against rawHeaders.length already.
            
            let cleanValue = cols[indexToKeep].trim();
            
            // Your original single-quote fix (may not be needed with parseCsv, but safe)
            cleanValue = cleanValue.replace(/^'+(.*)$/, "''$1"); 
            
            // Type conversion
            if (!isNaN(cleanValue) && cleanValue !== '') {
                if (cleanValue.includes('.')) {
                    cleanValue = parseFloat(cleanValue);
                } else {
                    cleanValue = parseInt(cleanValue);
                }
            }
            row.push(cleanValue);
        }
        
        // Only push if the row matches the expected column count
        if (row.length === expectedLength) {
            arrData.push(row);
        }
    }

    console.timeEnd("CSVToArray(strData)");
    return arrData;
  };

  // Add this function inside the sdeLib = () => { ... } block

  const autoResizeColumns = (workSheet) => {
    if (!workSheet) return;
    const lastColumn = workSheet.getLastColumn();
    if (lastColumn > 0) {
      // This resizes all columns that have data (from A to the last column)
      workSheet.autoResizeColumns(1, lastColumn);
    }
  };

 // --- "Public" Class (Exposed via 'return') ---
  class SdePage {
    constructor(sheet, csvFile, headers = null, backupRanges = null, publishedOnly = true) {
      this.sheet = sheet;
      this.backupRanges = null;
      this.csvFile = csvFile;
      this.headers = null; // <-- Explicitly default to null
      this.publishedOnly = false;

      if (headers != null) { // If headers ARE provided, override the null default
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
  // --- SDE Default Header Constants ---
// Defines the default minimal headers to import for each file.
const SDE_DEFAULTS = {
  invTypes: ["typeID", "groupID", "typeName", "volume", "published"],
  staStations: ["stationID", "stationName", "solarSystemID", "regionID"],
  industryActivityProducts: ["typeID", "activityID", "productTypeID", "quantity"],
  industryActivityMaterials: ["typeID", "activityID", "materialTypeID", "quantity"],
  invGroups: ["groupID", "categoryID", "groupName"],
  invCategories: ["categoryID", "categoryName"],
  //Diabled Crashy PIG of a Tabl
 dgmTypeAttributes: ["typeID", "attributeID", "valueFloat", "valueInt"]
};

  // --- "Public" Engine Function (buildSDEs) ---
  const buildSDEs = (sdePage) => {
    if (sdePage == null) throw "sdePage is required";
    console.time("buildSDEs( sheetName:" + sdePage.sheet + ")");

    const activeSpreadsheet = getSS(); 

    // STAGE 1: Fetch & Parse
    const csvContent = downloadTextData(sdePage.csvFile); 
    const csvData = CSVToArray(csvContent, ",", sdePage.headers, sdePage.publishedOnly); 

    // --- CRASH-PROOF CHECK ---
    if (!csvData || csvData.length < 2 || csvData[0].length === 0) {
        console.warn(`FATAL_DATA_WARNING: Parsed data for ${sdePage.sheet} is empty or invalid. Skipping sheet update.`);
        return; 
    }
    // --- END CRASH-PROOF CHECK ---

    let workSheet = null; 

    try {
      // STAGE 2: Prepare & Backup
      workSheet = createOrClearSdeSheet(activeSpreadsheet, sdePage.sheet); 
      
      var backedupValues = [];
      // (Your backup logic here...)
      
      // STAGE 3: Write & Finalize
      const destinationRange = workSheet.getRange(1, 1, csvData.length, csvData[0].length);
      destinationRange.setValues(csvData);

      // (Your restore logic here...)
      
      // --- CHANGED ---
      // deleteBlankColumnsAndColumns(workSheet); // <-- REMOVED
      autoResizeColumns(workSheet); // <-- ADDED
      // --- END CHANGED ---

    } catch (e) {
      throw e;
    }
    console.timeEnd("buildSDEs( sheetName:" + sdePage.sheet + ")");
  };

  // --- Return the Public Interface ---
  return {
    SdePage: SdePage,
    buildSDEs: buildSDEs
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
  allTriggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === functionName) {
      ScriptApp.deleteTrigger(trigger);
    }
  });
}

/**
 * Helper: Checks if the SDE update is running. (Relies on SCRIPT_PROPS global)
 */
function isSdeJobRunning() {
  // SCRIPT_PROPS must be defined in Main.js
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
  
  const lock = LockService.getDocumentLock();
  if (!lock.tryLock(5000)) {
    Logger.log('START: Failed to acquire DocumentLock. Aborting.');
    return;
  }
  
  try {
    SCRIPT_PROPS.setProperty(KEY_JOB_RUNNING, 'true');
    
    // Halt Formulas & Pause Orchestrator
    const ss = getSS();
    const loadingHelper = ss.getRangeByName("'Utility'!B3:C3");
    const backupSettings = loadingHelper.getValues();
    loadingHelper.setValues([[0, 0]]);
    SCRIPT_PROPS.setProperty(KEY_BACKUP_SETTINGS, JSON.stringify(backupSettings));
    _deleteTriggersFor('masterOrchestrator'); 
    Logger.log('START: Orchestrator trigger deleted. Formulas halted.');

    // Define the Job List using sdeLib
    const SDE = sdeLib(); 
    const sdePages = [
      // PASSING NULL HEADERS IS THE FIX TO BYPASS THE BUGGY COLUMN SELECTION FOR THIS FILE
      new SDE.SdePage("SDE_invTypes", "invTypes.csv",  [ "typeID","groupID","typeName","volume"]), 
      
      new SDE.SdePage("SDE_staStations", "staStations.csv", ["stationID", "stationName", "solarSystemID", "regionID"]),
      new SDE.SdePage("SDE_industryActivityProducts", "industryActivityProducts.csv", ["typeID", "activityID", "productTypeID", "quantity"]),
      new SDE.SdePage("SDE_industryActivityMaterials", "industryActivityMaterials.csv", ["typeID", "activityID", "materialTypeID", "quantity"]),
      new SDE.SdePage("SDE_invGroups", "invGroups.csv", ["groupID", "categoryID", "groupName"]),
      new SDE.SdePage("SDE_invCategories", "invCategories.csv", ["categoryID", "categoryName"]),
     // new SDE.SdePage("SDE_dgmTypeAttributes", "dgmTypeAttributes.csv", ["typeID", "attributeID", "valueFloat", "valueInt"])
    ];

    // Save State & Start First Trigger
    SCRIPT_PROPS.setProperty(KEY_JOB_LIST, JSON.stringify(sdePages));
    SCRIPT_PROPS.setProperty(KEY_JOB_INDEX, '0');
    _deleteTriggersFor('sde_job_PROCESS');
    Logger.log(`START: Saved ${sdePages.length} pages. Creating trigger for sde_job_PROCESS.`);
    ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(5000).create();

  } catch (e) {
    Logger.log(`ERROR in sde_job_START: ${e.message} at line ${e.lineNumber}`);
    sde_job_FINALIZE(); 
  } finally {
    lock.releaseLock();
    console.log('START: Lock released.');
  }
}

/**
 * STAGE 2: PROCESS (Run by a trigger)
 */
function sde_job_PROCESS() {
  if (isSdeJobRunning() === false) { // Check property using helper
    Logger.log('PROCESS: Job flag cleared (cancelled). Aborting trigger.');
    return;
  }
  const lock = LockService.getDocumentLock();
  if (!lock.tryLock(30000)) {
    Logger.log('PROCESS: Lock contention. Re-triggering for later attempt.');
    _deleteTriggersFor('sde_job_PROCESS');
    ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(30000).create();
    Logger.log('PROCESS: Created new trigger to retry in 30 seconds.');
    return;
  }
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
    const sdePage = new SDE.SdePage(currentJob.sheet, currentJob.csvFile, currentJob.headers, currentJob.backupRanges, currentJob.publishedOnly);

    Logger.log(`PROCESS: Running Job ${jobIndex + 1} of ${jobList.length}: ${currentJob.sheet}`);
    
    // RUN THE ACTUAL FILE TRANSFER
    SDE.buildSDEs(sdePage);
    
    // Update state and re-trigger
    SCRIPT_PROPS.setProperty(KEY_JOB_INDEX, (jobIndex + 1).toString());
    _deleteTriggersFor('sde_job_PROCESS'); 
    Logger.log('PROCESS: Successfully imported, creating trigger for next job.');
    ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(2000).create();

  } catch (e) {
    Logger.log(`FATAL ERROR in sde_job_PROCESS (Job ${jobIndex}): ${e.message} at line ${e.lineNumber}. Calling FINALIZE.`);
    sde_job_FINALIZE(); 
  } finally {
    lock.releaseLock();
  }
}

/**
 * STAGE 3: FINALIZE (Called by user or by process)
 * Runs silently without UI interruptions.
 */
function sde_job_FINALIZE() {
  const lock = LockService.getDocumentLock();
  if (!lock.tryLock(30000)) {
    Logger.log('FINALIZE: Lock unavailable. Assuming another finalization is running.');
    return;
  }
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

    // 2. Restart Orchestrator 
    _deleteTriggersFor('masterOrchestrator'); 
    ScriptApp.newTrigger('masterOrchestrator').timeBased().everyMinutes(15).create();
    Logger.log('FINALIZE: Orchestrator trigger recreated.');

    // 3. Clear all state properties and triggers
    SCRIPT_PROPS.deleteProperty(KEY_JOB_RUNNING);
    SCRIPT_PROPS.deleteProperty(KEY_JOB_LIST);
    SCRIPT_PROPS.deleteProperty(KEY_JOB_INDEX);
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