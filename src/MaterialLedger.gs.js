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
  
  // Get logger instance
  var LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ML_LEDGER') : console;


  // A helper function to get the spreadsheet object.
  function getSS_() {
    return SpreadsheetApp.getActiveSpreadsheet();
  }
  
  // Safely get PT dependency inside the function.
  function getPT_() {
    try {
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
    out.date  = r.date || (PT_API ? PT_API.yyyymmdd(PT_API.now()) : new Date().toISOString().slice(0, 10)); 
    out.type_id = r.type_id;
    out.item_name = r.item_name || '';
    out.qty   = +r.qty || 0;

    var u0 = +r.unit_value || 0;
    var u1 = +r.unit_value_filled || 0;

    out.unit_value        = u0 || '';
    out.source            = r.source || '';
    out.contract_id       = r.contract_id || '';
    out.char              = r.char || '';

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
    var sheetInst = sheet; //

    // --- INNER FUNCTIONS BOUND TO THIS sheetInst ---

    function appendRows(rows) {
      if (!rows || !rows.length) return 0;
      var data = rows.map(normalizeRow_);
      sheetInst.getRange(sheetInst.getLastRow() + 1, 1, data.length, HEAD.length).setValues(data);
      return data.length;
    }

    // The upsertBy function now uses the private 'sheetInst' variable.
    function upsertBy(keys, rows) {
      if (!rows || !rows.length) return 0;
      
      var sh = sheetInst;
      var keyIndices = keys.map(function (k) {
        var idx = HEAD.indexOf(k);
        if (idx === -1) throw new Error('Unknown key column in HEAD: ' + k);
        return idx;
      });

      var last = sh.getLastRow();
      var existingKeys = new Set();
      if (last >= 2) {
        var range = sh.getRange(2, 1, last - 1, HEAD.length);
        var allVals = range.getValues();
        allVals.forEach(function (row) {
          var key = keyIndices.map(function (headIdx) {
            return String(row[headIdx] || '');
          }).join('\u0001');
          existingKeys.add(key);
        });
      }

      var newRowsToWrite = [];
      rows.forEach(function (obj) {
        var outRow = normalizeRow_(obj);
        var key = keyIndices.map(function (headIdx) {
          return String(outRow[headIdx] || '');
        }).join('\u0001');
        
        if (!existingKeys.has(key)) {
          newRowsToWrite.push(outRow);
        }
      });

      if (newRowsToWrite.length > 0) {
        sh.getRange(sh.getLastRow() + 1, 1, newRowsToWrite.length, HEAD.length).setValues(newRowsToWrite);
      }
      
      return newRowsToWrite.length;
    }

    // --- PUBLIC INTERFACE FOR THIS INSTANCE ---
    return {
      append: appendRows,
      upsert: upsertBy, // exposed as 'upsert' for GESI Extentions.js
      sheetName: sheetName
    };
  }

  // --- PUBLIC INTERFACE FOR THE GLOBAL ML OBJECT ---
  return {
    forSheet: forSheet
  };
})();
