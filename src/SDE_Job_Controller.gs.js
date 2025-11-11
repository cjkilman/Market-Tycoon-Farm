/* eslint-disable no-console */
/* eslint-disable no-unused-vars */

/**
 * SDE_Job_Controller.gs
 * This file is a self-contained module for running a stateful,
 * multi-step SDE import job that is resilient to the 6-minute execution limit.
 *
 * NOTE: This file assumes the constant SCRIPT_PROPS is declared once in Main.js.
 *
 * --- FIX v4.6 (Dynamic Chunking) ---
 * - Replaced fixed CHUNK_SIZE with dynamic calculation based on TARGET_WRITE_TIME_MS (3s).
 * - Aggressively reduces chunk size on Document Lock contention or slow write times.
 */

// --- Safely define global constants with 'var' ---
// NOTE: Google Apps Script requires 'var' for true global scope across files
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

// --- Global Spreadsheet Object (Optimization: call getActiveSpreadsheet() once) ---
var SS;

/**
 * Lazy-loads the active spreadsheet object.
 * @returns {GoogleAppsScript.Spreadsheet.Spreadsheet} The active spreadsheet object.
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

  // --- CRITICAL FIX: _sheetCache is now an object map { sheetName: sheetObject } ---
  let _sheetCache = {};
  // --- END CRITICAL FIX ---

  // --- "Private" Helper Functions ---

  const downloadTextData = (csvFile) => {
    console.time("downloadTextData( csvFile:" + csvFile + " )");
    const baseURL = 'https://www.fuzzwork.co.uk/dump/latest/' + csvFile;
    const csvContent = UrlFetchApp.fetch(baseURL).getContentText();
    console.timeEnd("downloadTextData( csvFile:" + csvFile + " )");
    return csvContent.trim().replace(/\n$/, "");
  };

  /**
   * Refactored: Creates or clears the sheet and writes headers directly.
   */
  const createOrClearSdeSheet = (activeSpreadsheet, sheetName, headers) => {
    console.time("createOrClearSdeSheet({sheetName:" + sheetName + "})");
    if (!sheetName) throw "sheet name is required;";
    if (!headers || !headers.length) throw "headers are required to set up the sheet;";

    let sheet = activeSpreadsheet.getSheetByName(sheetName);
    let isNewSheet = false;

    if (sheet) {
      sheet.clearContents();
    } else {
      isNewSheet = true;
      sheet = activeSpreadsheet.insertSheet();
      sheet.setName(sheetName);
    }

    // Write the headers (Always row 1)
    sheet.getRange(1, 1, 1, headers.length).setValues([headers]);

    // --- CRITICAL FIX: Populate the cache map ---
    _sheetCache[sheetName] = sheet;
    // --- END CRITICAL FIX ---

    console.timeEnd("createOrClearSdeSheet({sheetName:" + sheetName + "})");
    return sheet;
  };



/**
 * Downloads a specific CSV file from Fuzzwork and parses it using the robust internal parser.
 * This is the public interface for the Orchestrator to fetch SDE files like mapDenormalize.
 * @param {string} fileName - The name of the CSV file to download (e.g., 'mapDenormalize.csv').
 * @param {Array<string>} [headers=null] - Array of specific columns to keep.
 * @param {boolean} [publishedOnly=false] - Whether to filter for published items.
 * @returns {Array<Array>} The 2D array of parsed data (including header row), or [] on failure.
 */
const fetchSDEFile = (fileName, headers = null, publishedOnly = false) => {
    const SCRIPT_NAME = 'fetchSDEFile';
    
    // NOTE: Assumes downloadTextData and CSVToArray are available in the sdeLib scope.
    
    try {
        if (!fileName || typeof fileName !== 'string') {
            throw new Error("File name is required.");
        }
        
        // 1. Download the file content (reusing existing logic)
        // This relies on the downloadTextData helper within the sdeLib closure.
        const csvContent = downloadTextData(fileName); 
        
        // 2. Parse the data (reusing existing robust parser)
        // Uses the built-in CSVToArray logic pattern for robust parsing and column filtering.
        const parsedData = CSVToArray(csvContent, ",", headers, publishedOnly); 
        
        if (!parsedData || parsedData.length < 2) {
            throw new Error("Parsed data is empty or invalid after download.");
        }
        
        console.log(`${SCRIPT_NAME}: Successfully downloaded and parsed ${parsedData.length} rows from ${fileName}.`);
        
        return parsedData;

    } catch (e) {
        console.error(`${SCRIPT_NAME}: FATAL ERROR during SDE fetch: ${e.message}`);
        // Returns empty array on failure, forcing the worker to handle the error state.
        return []; 
    }
};

  /**
   * ==================================================================
   * --- BUG FIX: REPLACED CSVToArray ---
   * This new function uses Google's built-in, robust CSV parser.
   * It correctly filters headers *once* and handles the publishedOnly
   * flag without errors.
   * ==================================================================
   */
  /**
* ==================================================================
* --- ROBUST CSVToArray (Replaces the old regex parser) ---
*
* This version uses Google's built-in, robust CSV parser.
* It correctly fixes the "off-by-one" bug and parses the entire file
* without failing silently.
* ==================================================================
*/
  const CSVToArray = (strData, strDelimiter = ",", headers = null, publishedOnly = true) => {
    console.time("CSVToArray(strData)");

    if (!strData || strData.trim().length === 0) {
      console.warn("CSVToArray: Input data string is empty. Returning empty array.");
      return [];
    }

    // 1. Use the robust, built-in parser
    // This fixes the silent failure and ensures the *entire* file is read.
    const allLines = Utilities.parseCsv(strData, strDelimiter.charCodeAt(0));

    if (allLines.length === 0) return [];

    // 2. Process Headers
    // This logic runs *once* on the header row, fixing the "off-by-one" bug.
    const rawHeaders = allLines[0].map(h => h.trim());
    let arrData = []; // This will be the final array [ [headers], [row1], [row2] ]
    let headersIndex = []; // Array of *indices* to keep

    const skipHeaders = !headers || !headers.length || !headers[0];

    if (!skipHeaders) {
      // User provided specific headers
      const outputHeaders = [];
      for (const requestedHeader of headers) {
        const index = rawHeaders.indexOf(requestedHeader);
        if (index !== -1) {
          headersIndex.push(index);
          outputHeaders.push(requestedHeader);
        } else {
          // This is a critical error. The requested header doesn't exist.
          throw new Error(`CSVToArray: Requested header "${requestedHeader}" not found in CSV file.`);
        }
      }
      arrData.push(outputHeaders); // Add the filtered header row
    } else {
      // User wants all headers
      headersIndex = rawHeaders.map((_, i) => i); // Keep all indices
      arrData.push(rawHeaders); // Add the full header row
    }

    const expectedLength = arrData[0].length; // The number of columns we expect in the output
    if (expectedLength === 0) {
      console.warn("CSVToArray: No valid headers found or requested. Returning empty array.");
      return [];
    }

    // 3. Find the 'published' column *once*
    const publishIdx = rawHeaders.indexOf("published");
    const startIndex = 1; // Start from the first data row

    // 4. Process Data Rows
    for (let i = startIndex; i < allLines.length; i++) {
      const cols = allLines[i];

      // Safety check: malformed row
      if (cols.length < rawHeaders.length) {
        console.warn(`Skipping row ${i}: Expected ${rawHeaders.length} columns, found ${cols.length}`);
        continue;
      }

      // --- PublishedOnly Filter Logic ---
      let skipRow = false;
      if (publishedOnly && publishIdx !== -1) {
        // Check the value *only* if filtering is on and the column exists
        // Use String().trim() to robustly check '1' vs '0', '', or null
        if (String(cols[publishIdx]).trim() !== '1') {
          skipRow = true;
        }
      }
      if (skipRow) continue;
      // --- End Filter Logic ---

      let row = [];

      // 5. Build the filtered row
      for (const indexToKeep of headersIndex) {
        let cleanValue = (cols[indexToKeep] || "").trim(); // Get value and trim

        // Clean up ' quotes
        cleanValue = cleanValue.replace(/^'+(.*)$/, "''$1");

        // Convert numbers
        if (!isNaN(cleanValue) && cleanValue !== '') {
          if (cleanValue.includes('.')) {
            cleanValue = parseFloat(cleanValue);
          } else {
            cleanValue = parseInt(cleanValue);
          }
        }
        row.push(cleanValue);
      }

      // Final check
      if (row.length === expectedLength) {
        arrData.push(row);
      }
    }

    console.timeEnd("CSVToArray(strData)");
    return arrData;
  };

  // ==================================================================
  // --- END OF REPLACEMENT ---
  // ==================================================================

  const autoResizeColumns = (sheet) => {
    // This function is present but commented out in buildSDEs for stability
    if (!sheet) return;
    const lastColumn = sheet.getLastColumn();
    if (lastColumn > 0) {
      sheet.autoResizeColumns(1, lastColumn);
    }
  };

  /**
   * NEW: Non-blocking Document Lock helper function.
   */
  function _writeChunkInternal(dataChunk, startRow, numCols, sheetName) {
    const chunkStartTime = new Date().getTime();
    let writeDurationMs = 0;
    const DOC_LOCK_TIMEOUT = 5000; // TryLock 5s 

    const docLock = LockService.getDocumentLock();

    // Attempt non-blocking lock
    if (!docLock.tryLock(DOC_LOCK_TIMEOUT)) {
      return { success: false, duration: 0 };
    }

    try {
      // Now retrieving the Sheet object from the module-level map
      let workSheet = _sheetCache[sheetName];
      if (!workSheet) {
        // CRITICAL FAILURE: Cache must be hot by this point.
        throw new Error(`CRITICAL: Sheet object for '${sheetName}' not found in memory cache. Job state compromised.`);
      }

      workSheet.getRange(startRow, 1, dataChunk.length, numCols).setValues(dataChunk);


    } catch (e) {
      console.error(`_writeChunkInternal: Write failed while locked: ${e.message}`);
      throw e;
    } finally {
      docLock.releaseLock();
      writeDurationMs = new Date().getTime() - chunkStartTime;
    }

    return { success: true, duration: writeDurationMs };
  }

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
  const buildSDEs = (sdePage, scriptStartTime) => {
    if (sdePage == null) throw "sdePage is required";
    console.time("buildSDEs( sheetName:" + sdePage.sheet + ")");

    // --- Your Design Parameters ---
    const MAX_CHUNK_SIZE = 5000;      // Max rows to write at once
    const MIN_CHUNK_SIZE = 500;       // Min rows to write at once
    const TARGET_WRITE_TIME_MS = 3000; // Target time for setValues() to take (3 seconds)
    const DOC_LOCK_TIMEOUT = 30000;    // 30 second wait for DocumentLock (used only for initial clear)
    const SCRIPT_TIME_LIMIT = 285000; // 4m 45s - Predictive reschedule

    // --- DYNAMIC STATE ---
    // Initialize currentChunkSize to the maximum safe size
    let currentChunkSize = MAX_CHUNK_SIZE;

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

    const docLock = LockService.getDocumentLock();

    // --- NEW: Read the saved chunk index ---
    let currentRow = parseInt(SCRIPT_PROPS.getProperty(KEY_JOB_CHUNK_INDEX) || '0', 10);
    // --- END NEW ---

    // Hold the sheet object reference for final resize
    let finalSheetReference;

    try {
      // STAGE 2: Prepare
      // Headers are still needed here for column count and for createOrClearSdeSheet
      const headers = csvData.slice(0, 1)[0];
      const dataRows = csvData.slice(1);
      const numCols = headers.length;

      // --- NEW: Only clear/write headers if we are on the first chunk ---
      if (currentRow === 0) {
        console.log(`buildSDEs: First run for ${sdePage.sheet}. Clearing sheet and writing headers.`);

        // Use non-blocking lock to perform the initial clear, waiting up to DOC_LOCK_TIMEOUT
        if (!docLock.tryLock(DOC_LOCK_TIMEOUT)) {
          // If initial lock fails, we can't start the job. Treat as a temporary pause state.
          SCRIPT_PROPS.setProperty(KEY_JOB_CHUNK_INDEX, currentRow.toString());
          console.warn(`buildSDEs: Initial Document Lock failed. Pausing to retry start.`);
          return false;
        }

        try {
          // createOrClearSdeSheet now handles clearing AND writing headers
          finalSheetReference = createOrClearSdeSheet(activeSpreadsheet, sdePage.sheet, headers);
        } finally {
          docLock.releaseLock();
        }
      } else {
        finalSheetReference = activeSpreadsheet.getSheetByName(sdePage.sheet);
        if (!finalSheetReference) {
          throw new Error(`Sheet ${sdePage.sheet} not found on resume.`);
        }
        // Also ensure the sheet is in the cache for _writeChunkInternal to find it
        _sheetCache[sdePage.sheet] = finalSheetReference;
        console.log(`buildSDEs: Resuming job for ${sdePage.sheet} from row ${currentRow}.`);
      }
      // --- END NEW ---

      // STAGE 3: Write & Finalize (in Chunks)
      console.info(`buildSDEs: Total rows to write: ${dataRows.length}. Starting at: ${currentRow}`);

      while (currentRow < dataRows.length) {

        // --- Adaptive Throttle Logic (Sleep only if last write was successful) ---
        if (lastWriteDurationMs > 0) {
          let sleepMs = Math.max(THROTTLE_BASE_SLEEP_MS, lastWriteDurationMs * THROTTLE_LATENCY_FACTOR);
          sleepMs = Math.min(THROTTLE_MAX_SLEEP_MS, sleepMs); // Cap at max
          console.log(`buildSDEs: Throttling for ${sleepMs.toFixed(0)}ms (based on last write of ${lastWriteDurationMs}ms)`);
          Utilities.sleep(sleepMs);
        }

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

        const chunkEnd = Math.min(currentRow + currentChunkSize, dataRows.length); // Use DYNAMIC SIZE
        const chunk = dataRows.slice(currentRow, chunkEnd);
        const writeRow = currentRow + 2; // +1 for 1-based index, +1 for header row (which is now written by createOrClearSdeSheet)

        if (chunk.length > 0) {

          let result;
          try {
            // --- NEW: Call the non-blocking writer ---
            result = _writeChunkInternal(chunk, writeRow, numCols, sdePage.sheet);
            // ----------------------------------------
          } catch (e) {
            // Catches CRITICAL cache miss error or severe API error
            throw e;
          }

          if (result.success === true) {
            // Success: Update duration and advance row
            lastWriteDurationMs = result.duration;

            // --- DYNAMIC CHUNK SIZE ADJUSTMENT ---
            // Calculate the ratio: If it took 10s (slow), ratio is 3000/10000 = 0.3
            const adjustmentFactor = TARGET_WRITE_TIME_MS / lastWriteDurationMs;

            // Adjust the size for the NEXT chunk
            currentChunkSize = Math.round(currentChunkSize * adjustmentFactor);

            // Enforce Min/Max bounds
            currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.min(MAX_CHUNK_SIZE, currentChunkSize));

            console.log(`buildSDEs: Chunk write successful. Rows written: ${chunk.length}. Duration: ${lastWriteDurationMs}ms. Next chunk size adjusted to ${currentChunkSize}.`);
            // --- END DYNAMIC ADJUSTMENT ---

            currentRow = chunkEnd;

          } else {
            // Failure: Lock contention (tryLock failed). Aggressive reduction.
            lastWriteDurationMs = 0;

            // --- AGGRESSIVE HALVING ---
            currentChunkSize = Math.max(MIN_CHUNK_SIZE, Math.round(currentChunkSize / 2));
            console.warn(`buildSDEs: Document Lock contention hit. Halving chunk size aggressively to ${currentChunkSize}. Retrying chunk immediately.`);
            // --- END AGGRESSIVE HALVING ---

            continue; // Immediately start next iteration of while loop
          }
        } else {
          break;
        }
      }

      console.log(`buildSDEs: Finished writing all ${dataRows.length} data rows.`);

      // Use the final reference acquired during Stage 2
      // autoResizeColumns(finalSheetReference); // Commented out for stability

    } catch (e) {
      // Ensure lock is released on error if it was held (only needed for initial clear, but safe to check)
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
 */
function sde_job_START() {
  console.log('--- SDE JOB START INITIATED (Silent Mode) ---');

  if (isSdeJobRunning()) {
    Logger.log('START: Job already running. Aborting new request.');
    return;
  }

  // --- Robust Lock Handling ---
  const lock = LockService.getScriptLock();
  let lockAcquired = false; // Flag to track lock state
  try {
    console.log('START: Attempting to acquire ScriptLock (max wait 7 min)...');
    lock.waitLock(420000); // Wait up to 7 minutes
    lockAcquired = true; // Set flag ONLY if waitLock() succeeds
    console.log('START: ScriptLock acquired.');
    // --- END Robust Lock Handling ---

    SCRIPT_PROPS.setProperty(KEY_JOB_RUNNING, 'true');

    // Halt Formulas
    const ss = getSS();
    const loadingHelper = ss.getRange("'Utility'!B3:D3");
    const backupSettings = loadingHelper.getValues();
    loadingHelper.setValues([[0, 0, 0]]);
    // --- YOUR SUGGESTED FLUSH ---
    SpreadsheetApp.flush(); // Forces the sheet to update NOW
    // --- END FLUSH ---
    SCRIPT_PROPS.setProperty(KEY_BACKUP_SETTINGS, JSON.stringify(backupSettings));

    // --- Setting MAINTENANCE FLAG ---
    console.log('START: Setting system to MAINTENANCE mode.');
    SCRIPT_PROPS.setProperty(GLOBAL_STATE_KEY, 'MAINTENANCE');
    // We still delete the master orchestrator to be safe
    _deleteTriggersFor('masterOrchestrator');
    console.log('START: Orchestrator trigger deleted. System is halted.');
    // --- END: MAINTENANCE FLAG ---

    // Define the Job List
    const { SdePage } = sdeLib(); // Get SdePage class from the library
    const sdePages = [
      new SdePage("SDE_invTypes", "invTypes.csv", ["typeID", "groupID", "typeName", "volume"]),
      new SdePage("SDE_staStations", "staStations.csv", ["stationID", "stationName", "solarSystemID", "regionID"]),
      new SdePage("SDE_industryActivityProducts", "industryActivityProducts.csv", ["typeID", "activityID", "productTypeID", "quantity"]),
      new SdePage("SDE_industryActivityMaterials", "industryActivityMaterials.csv", ["typeID", "activityID", "materialTypeID", "quantity"]),
      new SdePage("SDE_invGroups", "invGroups.csv", ["groupID", "categoryID", "groupName"]),
      new SdePage("SDE_invCategories", "invCategories.csv", ["categoryID", "categoryName"])
    ];

    // Save State & Start First Trigger
    SCRIPT_PROPS.setProperty(KEY_JOB_LIST, JSON.stringify(sdePages));
    SCRIPT_PROPS.setProperty(KEY_JOB_INDEX, '0');
    SCRIPT_PROPS.setProperty(KEY_JOB_CHUNK_INDEX, '0'); // Initialize chunk index
    _deleteTriggersFor('sde_job_PROCESS');
    Logger.log(`START: Saved ${sdePages.length} pages. Creating trigger for sde_job_PROCESS.`);
    ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(5000).create();

  } catch (e) {
    if (!lockAcquired) {
      console.error(`START: Failed to acquire ScriptLock. Another process is running. Aborting. ${e.message}`);
      Logger.log('START: Failed to acquire ScriptLock. Aborting.');
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
    // --- FIX: Resilience for API timeouts (Service Spreadsheets timed out) ---
    // If buildSDEs threw an error (but saved state), we re-trigger to resume.
    Logger.log(`RESUMABLE ERROR in sde_job_PROCESS (Job ${jobIndex}): ${e.message} at line ${e.lineNumber}. Re-triggering to attempt resume.`);
    _deleteTriggersFor('sde_job_PROCESS');
    // Wait 10s to give the Google API service time to recover from the timeout.
    ScriptApp.newTrigger('sde_job_PROCESS').timeBased().after(10000).create();
    // --- END FIX ---
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
  let lockAcquired = false; // Flag to track lock state
  try {
    console.log('FINALIZE: Attempting to acquire ScriptLock (max wait 7 min)...');
    lock.waitLock(420000); // Wait up to 7 minutes
    lockAcquired = true; // Set flag ONLY if waitLock() succeeds
    console.log('FINALIZE: ScriptLock acquired.');
    // --- END Robust Lock Handling ---

    console.log('--- SDE JOB FINALIZE STARTED (Silent Mode) ---');

    // 1. Release formula lock
    const backupSettingsJSON = SCRIPT_PROPS.getProperty(KEY_BACKUP_SETTINGS);
    if (backupSettingsJSON) {
      const backupSettings = JSON.parse(backupSettingsJSON);
      const ss = getSS(); // Optimization
      // FIX: Changed range from B3:C3 (2 cols) to B3:D3 (3 cols)
      const loadingHelper = ss.getRange("'Utility'!B3:D3");
      loadingHelper.setValues(backupSettings);
      Logger.log('FINALIZE: Restored formula settings.');
    }

    // 2. Restart Orchestrator & Clear Maintenance Flag
    SCRIPT_PROPS.setProperty(GLOBAL_STATE_KEY, 'RUNNING');
    Logger.log('FINALIZE: System state set to RUNNING.');
    _deleteTriggersFor('masterOrchestrator');
    ScriptApp.newTrigger('masterOrchestrator').timeBased().everyMinutes(15).create();
    Logger.log('FINALIZE: Orchestrator trigger recreated.');

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
      // The fatal error from the mismatch is caught here
      Logger.log(`ERROR in sde_job_FINALIZE: ${e.message} at line ${e.lineNumber}`);
    }
  } finally {
    if (lockAcquired) {
      lock.releaseLock();
      console.log('--- SDE JOB FINALIZE COMPLETE ---');
    }
  }
}