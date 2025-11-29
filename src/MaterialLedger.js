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


  // A helper function to get the spreadsheet object.
  function getSS_() {
    return SpreadsheetApp.getActiveSpreadsheet();
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

    // Use safe access to PT for date formatting
    // FIX: Ensure r.date is parsed correctly before formatting
    let dateVal = r.date;
    if (!(dateVal instanceof Date)) {
        dateVal = PT_API ? PT_API.parseDateSafe(dateVal) : new Date(dateVal);
    }
    // Format only if valid date, otherwise use today's date string
    out.date = (dateVal && !isNaN(dateVal))
                 ? (PT_API ? PT_API.yyyymmdd(dateVal) : Utilities.formatDate(dateVal, Session.getScriptTimeZone(), "yyyy-MM-dd"))
                 : (PT_API ? PT_API.yyyymmdd(PT_API.now()) : Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd"));

    out.type_id = r.type_id;
    out.item_name = r.item_name || '';
    out.qty   = +r.qty || 0;

    var u0 = +r.unit_value || 0; // Manual override
    var u1 = +r.unit_value_filled || 0; // Calculated value

    out.unit_value        = u0 > 0 ? u0 : ''; // Store manual override only if > 0
    out.source            = r.source || '';
    out.contract_id       = r.contract_id || '';
    out.char              = r.char || '';

    // If manual override exists (u0>0), use it; otherwise use calculated (u1>0), else blank.
    out.unit_value_filled = u0 > 0 ? u0 : (u1 > 0 ? u1 : '');

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
      if (!rows || !rows.length) return 0; // Exit if no rows to process

      const WRITE_BATCH_SIZE = 1000;
      const sh = sheetInst; 

      // --- 1. Identify Key Columns ---
      const keyIndices = keys.map(function (k) {
        const idx = HEAD.indexOf(k); 
        if (idx === -1) throw new Error('Unknown key column in HEAD: ' + k);
        return idx;
      });

      // --- 2. Read Existing Keys (CRITICAL DEDUPLICATION READ) ---
      const last = sh.getLastRow();
      const existingKeys = new Set();
      if (last >= 2) {
        LOG.debug(`Reading existing ${last - 1} rows to check for duplicates...`);
        
        const range = sh.getRange(2, 1, last - 1, HEAD.length);
        const allVals = range.getValues();
        allVals.forEach(function (row) {
          const key = keyIndices.map(function (headIdx) {
            return String(row[headIdx] != null ? row[headIdx] : '');
          }).join('\u0001'); 
          existingKeys.add(key);
        });
        LOG.debug(`Found ${existingKeys.size} unique existing keys.`);
      }

      // --- 3. Filter for New Rows ---
      const newRowsToWrite = [];
      rows.forEach(function (obj) {
        const outRow = normalizeRow_(obj); 
        const key = keyIndices.map(function (headIdx) {
          return String(outRow[headIdx] != null ? outRow[headIdx] : '');
        }).join('\u0001');

        // Only add if the key doesn't already exist
        if (!existingKeys.has(key)) {
          newRowsToWrite.push(outRow);
          // Add the new key immediately to prevent duplicates *within the current batch*
          existingKeys.add(key);
        }
      });

      // --- 4. Batch Write New Rows (FAST COUNTER IMPLEMENTATION) ---
      const totalNewRows = newRowsToWrite.length;
      let currentIndex = 0; 
      
      // CRITICAL FIX: Calculate starting row ONCE before the loop.
      let nextWriteRow = sh.getLastRow() + 1;

      if (totalNewRows > 0) {
        LOG.info(`Writing ${totalNewRows} new rows in batches of ${WRITE_BATCH_SIZE}...`);
        
        while (currentIndex < totalNewRows) {
          const batch = newRowsToWrite.slice(currentIndex, currentIndex + WRITE_BATCH_SIZE);
          // FIX: Use the fast, pre-calculated counter instead of sh.getLastRow()
          const startRow = nextWriteRow; 
          
          try {
              sh.getRange(startRow, 1, batch.length, HEAD.length).setValues(batch);
              
              // ADVANCE THE COUNTERS: Move to the start of the next batch
              currentIndex += batch.length;
              nextWriteRow += batch.length; 
              
              LOG.debug(`Wrote batch ${Math.floor(currentIndex / WRITE_BATCH_SIZE)}/${Math.ceil(totalNewRows / WRITE_BATCH_SIZE)} (${batch.length} rows) starting at row ${startRow}.`);
          } catch (e) {
              LOG.error(`Error writing batch starting at row ${startRow}: ${e.message}`);
              
              if (e.message.includes("Service timed out")) {
                 LOG.warn("Retrying batch write after timeout...");
                 Utilities.sleep(1000); 
                 try {
                     // Retry once (nextWriteRow has not advanced for this batch yet)
                     sh.getRange(startRow, 1, batch.length, HEAD.length).setValues(batch);
                     LOG.info(`Retry successful for batch starting at row ${startRow}.`);
                     
                     // Advance counters only AFTER successful retry
                     currentIndex += batch.length;
                     nextWriteRow += batch.length;
                 } catch (e2) {
                     LOG.error(`Retry FAILED for batch starting at row ${startRow}: ${e2.message}`);
                     throw e2; 
                 }
              } else {
                throw e; 
              }
          }
        } // End while loop
        
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
