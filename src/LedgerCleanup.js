/**
 * Utility function to clean and normalize all data in the Material Ledger sheet.
 * This is primarily intended to fix corrupted data types (like quoted numbers
 * and Excel date serial numbers with commas/quotes) imported from CSV/API sources.
 *
 * NOTE: Assumes PT (Project Time) library and LoggerEx are available globally.
 */
function cleanupLedgerSheet(sheetName) {
  // Use 'Material_Ledger' as the default sheet name
  sheetName = sheetName || 'Material_Ledger';
  
  // A helper to get the spreadsheet object (assuming global access from Utility.js)
  const getSS_ = () => SpreadsheetApp.getActiveSpreadsheet();

  // Header definition copied from MaterialLedger.js
  const HEAD = ['date', 'type_id', 'item_name', 'qty', 'unit_value', 'source', 'contract_id', 'char', 'unit_value_filled'];

  // Logger instance (assuming global access from Logger.js)
  const LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ML_CLEANUP') : console;

  // Function to get PT API (copied from MaterialLedger.js for date handling)
  function getPT_() {
    try {
        if (typeof PT !== 'undefined' && PT.yyyymmdd) return PT;
    } catch (e) {
        LOG.warn('PT Dependency Check Failed - Using standard Date utilities.');
    }
    return null;
  }
  const PT_API = getPT_();
  
  // Helper to safely parse and clean a string value for conversion to number.
  function cleanValue(v) {
    if (typeof v === 'string') {
        // Remove quotes, commas, and any non-numeric suffixes (like 'ISK')
        v = v.replace(/"/g, '').replace(/,/g, '').replace(/ISK/g, '').trim();
    }
    return v;
  }

 // Normalize one logical row → the HEAD order
  function normalizeRow_(r) {
    var out = {};
    const PT_API = getPT_();
    
    // --- Date Parsing (FIXED) ---
    let dateVal = r.date;

    // 1. Attempt to parse date only if it's NOT already a valid Date object.
    if (!(dateVal instanceof Date)) {
        // Use PT_API safe parsing if available, otherwise rely on new Date()
        dateVal = PT_API ? PT_API.parseDateSafe(dateVal) : new Date(dateVal);
    }
    
    // 2. Validate the resulting date object. If it's not a valid date, 
    // default to today's date.
    let validDate = (dateVal instanceof Date) && !isNaN(dateVal);
    let dateToFormat = validDate ? dateVal : (PT_API ? PT_API.now() : new Date());

    // 3. Format the valid date object.
    out.date = PT_API
               ? PT_API.yyyymmdd(dateToFormat)
               : Utilities.formatDate(dateToFormat, Session.getScriptTimeZone(), "yyyy-MM-dd");
    
    // --- Other Fields (Retaining previous fixes for numeric types) ---
    out.type_id = r.type_id;
    out.item_name = r.item_name || '';

    // Numeric fields
   out.qty = Number(String(r.qty).replace(/,/g, '')) || 0;

    var u0 = +r.unit_value || 0; // Manual override (Number: 0 or >0)
    var u1 = +r.unit_value_filled || 0; // Calculated value (Number: 0 or >0)

    out.unit_value = u0 > 0 ? u0 : ''; 

    out.source = r.source || '';
    out.contract_id = r.contract_id || '';
    out.char = r.char || '';

    var finalUnitValue = u0 > 0 ? u0 : (u1 > 0 ? u1 : 0);
    out.unit_value_filled = finalUnitValue > 0 ? finalUnitValue : '';

    // --- Final Mapping ---
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
    const lastCol = HEAD.length; // Use fixed column count to prevent reading extra columns

    if (lastRow < 2) {
      LOG.info(`Ledger is empty (only header row). Nothing to clean.`);
      return 0;
    }

    // Read all data including the header, only up to the expected number of columns
    const rawValues = sh.getRange(1, 1, lastRow, lastCol).getValues();

    const header = rawValues[0];
    const dataRows = rawValues.slice(1);

    // Filter out blank rows and clean the remaining data
    const cleanedData = dataRows
      // Filter out any row where joining the values results in an empty string
      .filter(row => row.map(v => String(v)).join('').trim() !== '') 
      .map(row => normalizeRow(row));

    // Reconstruct the final array with header
    const finalData = [header].concat(cleanedData);

    // Clear everything and overwrite the sheet content
    sh.clearContents();
    sh.getRange(1, 1, finalData.length, HEAD.length).setValues(finalData);

    LOG.info(`Successfully cleaned and rewrote ${cleanedData.length} rows in '${sheetName}'.`);
    SpreadsheetApp.getUi().alert(`Cleanup successful! Rewrote ${cleanedData.length} rows in '${sheetName}'.`);

    return cleanedData.length;

  } catch (e) {
    LOG.error(`Cleanup failed for ${sheetName}: ${e.message}`);
    SpreadsheetApp.getUi().alert(`Error during cleanup: ${e.message}`);
    throw e;
  }
}