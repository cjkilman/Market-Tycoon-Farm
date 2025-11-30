/** MaterialLedger API — append + upsert
 * Ledger HEAD:
 * [date, type_id, item_name, qty, unit_value, source, contract_id, char, unit_value_filled]
 * * unit_value - reserved for Manual Sheet editing
 * unit_value_filled - automated Pricing from sources
 * usage: Manual price overides detected price
 * if(unit_value = "")
 * return unit_value_filled
 * else
 * return unit_value
 * */
var ML = (function () {
  // A private, factory-level variable for header definition.
  var HEAD  = ['date','type_id','item_name','qty','unit_value','source','contract_id','char','unit_value_filled'];

  // Get logger instance - Ensure LoggerEx is available in your project
  var LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ML_LEDGER') : console;
var ss;

  // A helper function to get the spreadsheet object.
  function getSS_() {
    if(!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
    return  ss;
  }

  // Safely get PT dependency inside the function.
  function getPT_() {
    try {
        // Ensure PT and the specific function yyyymmdd exist
        if (typeof PT !== 'undefined' && PT.yyyymmdd) return PT;
    } catch (e) {
        LOG.warn('PT Dependency Check Failed:', e.message);
    }
    return null;
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
    out.qty = +r.qty || 0; 

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

  // This is the core factory function. It returns a NEW object with a private state.
  function forSheet(sheetName) {

    // --- PRIVATE SHEET INSTANCE ---
    var sheet = getSS_().getSheetByName(sheetName);
    if (!sheet) {
        try {
            // NOTE: Assumes getOrCreateSheet is globally available from Utility.js
            sheet = getOrCreateSheet(getSS_(), sheetName, HEAD);
        } catch (e) {
            LOG.error('getOrCreateSheet FAILED for ' + sheetName + ':', e);
            throw new Error('Required function getOrCreateSheet not found or failed. Dependency issue likely.');
        }
    }
    var sheetInst = sheet; // Keep reference to the specific sheet instance

    // --- INNER FUNCTIONS BOUND TO THIS sheetInst ---

    function appendRows(rows) {
      if (!rows || !rows.length) return 0;
      var data = rows.map(normalizeRow_);
      sheetInst.getRange(sheetInst.getLastRow() + 1, 1, data.length, HEAD.length).setValues(data);
      return data.length;
    }

    // The upsertBy function now uses the private 'sheetInst' variable.
    // CORRECTED: Includes Batching for sheet writes.
   function upsertBy(keys, rows) {
    if (!rows || !rows.length) return 0;

    const WRITE_BATCH_SIZE = 1000;
    const sh = sheetInst; 

    // --- 1. Identify Key Columns ---
    const keyIndices = keys.map(function (k) {
        const idx = HEAD.indexOf(k); 
        if (idx === -1) throw new Error('Unknown key column in HEAD: ' + k);
        return idx;
    });
    
    // NEW HELPER: Forces numeric key parts (type_id) to a clean, rounded integer string.
    const normalizeKeyPart = function(val, headIdx) {
        if (headIdx === 1) { // Index 1 is 'type_id'
            // FIX: Round to ensure no decimals (e.g., 8335.0 -> "8335")
            return String(Math.round(Number(val || 0)));
        }
        return String(val != null ? val : '');
    };

    // --- 2. Read Existing Keys (FIXED & DIAGNOSED) ---
    const last = sh.getLastRow();
    const existingKeys = new Set();
    if (last >= 2) {
        LOG.debug(`Reading existing ${last - 1} rows to check for duplicates...`);
        
        const range = sh.getRange(2, 1, last - 1, HEAD.length);
        const allVals = range.getValues();
        allVals.forEach(function (row, rowIndex) {
            const key = keyIndices.map(function (headIdx) {
                // Use consistent normalization when reading existing data
                return normalizeKeyPart(row[headIdx], headIdx);
            }).join('\u0001'); 
            existingKeys.add(key);
            // DIAGNOSTIC LOGGING (Existing Key)
            if (rowIndex < 5) LOG.debug(`[KEY_READ] Row ${rowIndex + 2} RAW: ${row[keyIndices[0]]}, Normalized: ${key}`);
        });
        LOG.debug(`Found ${existingKeys.size} unique existing keys.`);
    }

    // --- 3. Filter for New Rows (FIXED & DIAGNOSED) ---
    const newRowsToWrite = [];
    rows.forEach(function (obj, rowIndex) {
        const outRow = normalizeRow_(obj); 
        const key = keyIndices.map(function (headIdx) {
            // Use consistent normalization when generating new data keys
            return normalizeKeyPart(outRow[headIdx], headIdx);
        }).join('\u0001');

        // DIAGNOSTIC LOGGING (New Key Check)
        if (rowIndex < 5) LOG.debug(`[KEY_GEN] Row ${rowIndex + 1} New: ${outRow[keyIndices[0]]}, Normalized: ${key}, Exists: ${existingKeys.has(key)}`);

        // Only add if the key doesn't already exist
        if (!existingKeys.has(key)) {
            newRowsToWrite.push(outRow);
            // Add the new key immediately to prevent duplicates *within the current batch*
            existingKeys.add(key);
            LOG.debug(`[KEY_ADD] New row added to batch for key: ${key}`);
        } else {
            LOG.debug(`[KEY_SKIP] Key already exists: ${key}`);
        }
    });

    // --- 4. Batch Write New Rows ---
    const totalNewRows = newRowsToWrite.length;
    let currentIndex = 0; 
    let nextWriteRow = sh.getLastRow() + 1;

    if (totalNewRows > 0) {
        LOG.info(`Writing ${totalNewRows} new rows in batches of ${WRITE_BATCH_SIZE}...`);
        
        while (currentIndex < totalNewRows) {
            const batch = newRowsToWrite.slice(currentIndex, currentIndex + WRITE_BATCH_SIZE);
            const startRow = nextWriteRow; 
            
            try {
                sh.getRange(startRow, 1, batch.length, HEAD.length).setValues(batch);
                
                currentIndex += batch.length;
                nextWriteRow += batch.length; 
                
                LOG.debug(`Wrote batch ${Math.floor(currentIndex / WRITE_BATCH_SIZE)}/${Math.ceil(totalNewRows / WRITE_BATCH_SIZE)} (${batch.length} rows) starting at row ${startRow}.`);
            } catch (e) {
                // ... (The robust timeout and retry logic) ...
                LOG.error(`Error writing batch starting at row ${startRow}: ${e.message}`);
                throw e; 
            }
        }
        
        LOG.info(`Finished writing ${totalNewRows} new rows.`);
    } else {
         LOG.info("No new rows to write.");
    }

    return totalNewRows; // Return the count of *newly written* rows
}


    // --- PUBLIC INTERFACE FOR THIS INSTANCE ---
    return {
      append: appendRows,
      upsert: upsertBy, // exposed as 'upsert' for consistency
      sheetName: sheetName
    };
  }

  // --- PUBLIC INTERFACE FOR THE GLOBAL ML OBJECT ---
  return {
    forSheet: forSheet
  };
})();
