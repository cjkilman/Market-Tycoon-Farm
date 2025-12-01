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

  // Reimplementation of the core logic from ML.normalizeRow_ to clean a row array
  function normalizeRow(r) {
    // Map the raw array row (r) into a temporary object structure
    const inputObj = {
        date: r[0], type_id: r[1], item_name: r[2], qty: r[3],
        unit_value: r[4], source: r[5], contract_id: r[6],
        char: r[7], unit_value_filled: r[8]
    };

    let dateVal = cleanValue(inputObj.date);
    let out = {};

    // 1. Clean and format Date
    if (dateVal) {
        let d = null;
        let numVal = Number(dateVal);
        
        if (!isNaN(numVal) && numVal >= 40000 && numVal <= 50000) {
            // Excel serial date detection (typical range 40000-50000)
            try {
                // Base date is 1899-12-30. GAS dates typically read Excel dates shifted by +1 day.
                d = new Date(Date.UTC(1899, 11, 30));
                // Add the serial number of days, compensating for Excel's 1900 leap year bug by subtracting 1 day
                d.setDate(d.getDate() + numVal - 1); 
            } catch (e) {
                // Fallback attempt
                d = new Date(dateVal);
            }
        } else {
            // Standard date string parsing (e.g., "2025-11-30")
            d = new Date(dateVal);
        }

        // Format the date using the existing PT mechanism or standard utilities
        out.date = (d && !isNaN(d.getTime()))
            ? (PT_API ? PT_API.yyyymmdd(d) : Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd"))
            : ''; 
    } else {
        out.date = '';
    }

    // 2. Clean other fields
    out.type_id = cleanValue(inputObj.type_id) || '';
    out.item_name = inputObj.item_name || ''; 
    
    // Qty and unit values are converted to Numbers, falling back to 0 or ''
    out.qty = Number(cleanValue(inputObj.qty)) || 0;

    const u0_raw = cleanValue(inputObj.unit_value);
    const u1_raw = cleanValue(inputObj.unit_value_filled);

    const u0 = Number(u0_raw) || 0; // Manual override
    const u1 = Number(u1_raw) || 0; // Calculated value

    // 3. Apply the original ML logic for unit_value and unit_value_filled
    out.unit_value        = u0 > 0 ? u0 : '';
    out.source            = inputObj.source || ''; 
    out.contract_id       = inputObj.contract_id || ''; 
    out.char              = inputObj.char || ''; 

    // If manual override exists (u0>0), use it, otherwise use calculated (u1>0), else blank.
    out.unit_value_filled = u0 > 0 ? u0 : (u1 > 0 ? u1 : '');

    // Convert back to Array in HEAD order
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