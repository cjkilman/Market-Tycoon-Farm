/* global SpreadsheetApp, LoggerEx, PT, writeDataToSheet */

/**
 * Utility function to clean and normalize all data in the Material Ledger sheet.
 * FIXES:
 * 1. Maps Array-rows to Objects so normalizeRow_ can read them.
 * 2. Uses 'writeDataToSheet' to handle large data volumes (fixes Cell Limit crashes).
 * 3. Corrects function naming typos.
 */
function cleanupLedgerSheet(sheetName) {
  // Use 'Material_Ledger' as the default sheet name
  sheetName = sheetName || 'Material_Ledger';
  
  // A helper to get the spreadsheet object
  const getSS_ = () => SpreadsheetApp.getActiveSpreadsheet();

  // Header definition - MUST match the column order in your sheet
  const HEAD = ['date', 'type_id', 'item_name', 'qty', 'unit_value', 'source', 'contract_id', 'char', 'unit_value_filled'];

  // Logger instance
  const LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ML_CLEANUP') : console;

  // Function to get PT API
  function getPT_() {
    try {
        if (typeof PT !== 'undefined' && PT.yyyymmdd) return PT;
    } catch (e) {
        LOG.warn('PT Dependency Check Failed - Using standard Date utilities.');
    }
    return null;
  }
  
  // Normalize one logical row → the HEAD order
  function normalizeRow_(r) {
    var out = {};
    const PT_API = getPT_();
    
    // --- Date Parsing ---
    let dateVal = r.date;
    if (!(dateVal instanceof Date)) {
        // Simple fallback parsing if PT is missing
        dateVal = PT_API ? PT_API.parseDateSafe(dateVal) : new Date(dateVal);
    }
    let validDate = (dateVal instanceof Date) && !isNaN(dateVal.getTime());
    let dateToFormat = validDate ? dateVal : new Date();

    out.date = PT_API
               ? PT_API.yyyymmdd(dateToFormat)
               : Utilities.formatDate(dateToFormat, Session.getScriptTimeZone(), "yyyy-MM-dd");
    
    // --- Other Fields ---
    out.type_id = r.type_id;
    out.item_name = r.item_name || '';

    // Numeric fields: Remove commas if present in string representation
    out.qty = Number(String(r.qty).replace(/,/g, '')) || 0;

    var u0 = Number(String(r.unit_value).replace(/[^0-9.]/g, '')) || 0; 
    var u1 = Number(String(r.unit_value_filled).replace(/[^0-9.]/g, '')) || 0;

    out.unit_value = u0 > 0 ? u0 : ''; 
    out.source = r.source || '';
    out.contract_id = r.contract_id || '';
    out.char = r.char || '';

    var finalUnitValue = u0 > 0 ? u0 : (u1 > 0 ? u1 : 0);
    out.unit_value_filled = finalUnitValue > 0 ? finalUnitValue : '';

    // --- Final Mapping to Array ---
    return HEAD.map(function (k) { return (out[k] == null ? '' : out[k]); });
  }

  // --- MAIN EXECUTION ---
  try {
    const ss = getSS_();
    const sh = ss.getSheetByName(sheetName);
    if (!sh) {
        LOG.error(`Sheet not found: ${sheetName}`);
        SpreadsheetApp.getUi().alert(`Error: Sheet not found: ${sheetName}`);
        return;
    }

    const lastRow = sh.getLastRow();
    
    // If empty, stop
    if (lastRow < 2) {
      LOG.info(`Ledger is empty (only header row). Nothing to clean.`);
      return 0;
    }

    // 1. Read all data including header
    // We strictly read the width of HEAD to avoid grabbing empty columns
    const rawValues = sh.getRange(1, 1, lastRow, HEAD.length).getValues();

    const header = rawValues[0];
    const dataRows = rawValues.slice(1);

    // 2. Process Data
    const cleanedData = dataRows
      .filter(row => row.join('').trim() !== '') // Remove totally empty rows
      .map(rowArray => {
          // [CRITICAL FIX]: Map Array -> Object
          let rowObj = {};
          HEAD.forEach((colName, index) => {
              rowObj[colName] = rowArray[index];
          });
          // Now pass the Object to the normalizer
          return normalizeRow_(rowObj);
      });

    // Reconstruct final array
    const finalData = [header].concat(cleanedData);

    // 3. SAFE WRITE BACK (Chunked)
    // Clear the sheet first to remove old/bad data
    sh.clearContents();

    // Use writeDataToSheet from Utility.js to handle limits
    if (typeof writeDataToSheet !== 'function') {
        throw new Error("Utility.js not found or writeDataToSheet missing.");
    }

    const writeState = {
        logInfo: LOG.info,
        logError: LOG.error,
        logWarn: LOG.warn,
        config: {
            MAX_CELLS_PER_CHUNK: 50000, // Safe chunk size
            TARGET_WRITE_TIME_MS: 2000
        }
    };

    const result = writeDataToSheet(sheetName, finalData, 1, 1, writeState);

    if (result.success) {
        LOG.info(`Successfully cleaned and rewrote ${cleanedData.length} rows.`);
        SpreadsheetApp.getUi().alert(`Cleanup successful! Processed ${cleanedData.length} rows.`);
    } else {
        throw new Error(result.error || "Write failed during chunking.");
    }

    return cleanedData.length;

  } catch (e) {
    LOG.error(`Cleanup failed for ${sheetName}: ${e.message}`);
    SpreadsheetApp.getUi().alert(`Error during cleanup: ${e.message}`);
    throw e;
  }
}

function fixMaterialLedgerHeaders() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // CORRECT: Only pass the sheet name here
  var sheet = ss.getSheetByName("Material_Ledger");
  
  // Headers defined locally
  var HEAD = ['date', 'type_id', 'item_name', 'qty', 'unit_value', 'source', 'contract_id', 'char', 'unit_value_filled'];
  
  if (sheet) {
    // 1. Shift existing data down
    sheet.insertRowBefore(1);
    
    // 2. Write headers into the new empty row
    sheet.getRange(1, 1, 1, HEAD.length).setValues([HEAD]);
    
    Logger.log("Success: Material Ledger headers have been restored.");
  } else {
    Logger.log("Error: Sheet 'Material_Ledger' not found.");
  }
}