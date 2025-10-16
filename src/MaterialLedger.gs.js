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
  // keep your defaults
  var SHEET = 'Material_Ledger';
  var HEAD  = ['date','type_id','item_name','qty','unit_value','source','contract_id','char','unit_value_filled'];

  // Module-level cache for the active spreadsheet instance
  var _SS = null; 

  // NEW: allow swapping the active sheet at runtime
  function setSheet(name) {
    var s = String(name || '').trim();
    if (s) SHEET = s;
    return SHEET;
  }

  function getSS_() {
    if (!_SS) {
      _SS = SpreadsheetApp.getActiveSpreadsheet();
    }
    return _SS;
  }
  
  function getSheet_() {
    var ss = getSS_();
    // NOTE: Requires getOrCreateSheet() from Utility.js to be globally accessible
    return getOrCreateSheet(ss, SHEET, HEAD); 
  }

  // Returns 1-based index map { headerName: index }
  function headerIndex_(sh) {
    // Read only the header row up to the maximum expected length
    var row = sh.getRange(1, 1, 1, HEAD.length).getValues()[0];
    var m = {};
    for (var i = 0; i < row.length; i++) {
      var h = String(row[i] || '').trim();
      if (h) m[h] = i + 1; // 1-based
    }
    return m;
  }

  // Normalize one logical row → the HEAD order, reuse your time helper
  function normalizeRow_(r) {
    // NOTE: Assumes PT (Project Time) is globally available
    var out = {};
    out.date  = r.date || PT.yyyymmdd(PT.now()); 
    out.type_id = r.type_id;
    out.item_name = r.item_name || '';
    out.qty   = +r.qty || 0;

    // keep original values, but ensure the "filled" fallback is carried
    var u0 = +r.unit_value || 0;
    var u1 = +r.unit_value_filled || 0;

    out.unit_value        = u0 || ''; // leave blank if zero/empty
    out.source            = r.source || '';
    out.contract_id       = r.contract_id || '';
    out.char              = r.char || '';

    // always persist a usable filled value (fallback when u0 <= 0)
    out.unit_value_filled = u0 > 0 ? u0 : (u1 > 0 ? u1 : '');

    return HEAD.map(function (k) { return (out[k] == null ? '' : out[k]); });
  }

  function appendRows(rows) {
    if (!rows || !rows.length) return 0;
    var sh = getSheet_();
    var data = rows.map(normalizeRow_);
    
    // Single bulk write for append is already efficient
    sh.getRange(sh.getLastRow() + 1, 1, data.length, HEAD.length).setValues(data);
    return data.length;
  }

  // **OPTIMIZED VERSION**
  // Upsert by one or more logical key columns (e.g. ['contract_id','source'])
  // Reads all existing data, updates the array in memory, and writes back in one call.
  function upsertBy(keys, rows) {
    if (!rows || !rows.length) return 0;
    
    var sh = getSheet_();
    var H = headerIndex_(sh);
    var keyColNames = keys || [];
    var keyIndices = keyColNames.map(function (k) {
      var idx = HEAD.indexOf(k);
      if (idx === -1) throw new Error('Unknown key column in HEAD: ' + k);
      return idx; // 0-based index in the HEAD array (and eventual data array)
    });

    var last = sh.getLastRow();
    var allVals = []; // This will hold all existing data

    // 1. READ EXISTING DATA (Single bulk read)
    if (last >= 2) {
      var range = sh.getRange(2, 1, last - 1, HEAD.length);
      allVals = range.getValues();
    }
    
    // 2. CREATE MAP: Composite Key (string) → 0-based index in allVals array
    var map = new Map();
    for (var r = 0; r < allVals.length; r++) {
      var row = allVals[r];
      // Build the composite key string from the values at the key indices
      var key = keyIndices.map(function (headIdx) {
        return String(row[headIdx] || '');
      }).join('\u0001'); // Use a unique separator
      map.set(key, r); // Store the 0-based index in the allVals array
    }

    var appends = [];
    var updatedCount = 0;
    
    // 3. PROCESS INCOMING ROWS (In memory)
    rows.forEach(function (obj) {
      var outRow = normalizeRow_(obj);
      
      // Generate key for the incoming row
      var key = keyIndices.map(function (headIdx) {
        return String(outRow[headIdx] || '');
      }).join('\u0001');

      var rowIndex = map.get(key);
      
      if (rowIndex != null) {
        // UPDATE: Overwrite the existing row array in allVals
        allVals[rowIndex] = outRow;
        updatedCount++;
      } else {
        // APPEND: Queue as a new row
        appends.push(outRow);
      }
    });
    
    // 4. PERFORM BULK WRITE (One final bulk API call)
    var finalData = allVals.concat(appends);
    
    if (finalData.length > 0) {
      // Get range starting at row 2, covering the size of the final merged array
      var targetRange = sh.getRange(2, 1, finalData.length, HEAD.length);
      targetRange.setValues(finalData);
    }

    // 5. CLEAR EXCESS ROWS (if necessary, only if we deleted/updated rows resulting in a shorter final list)
    var newLastRow = finalData.length + 1;
    var maxRows = sh.getMaxRows();
    
    if (newLastRow < maxRows) {
        sh.deleteRows(newLastRow, maxRows - newLastRow + 1);
    }
    
    // NOTE: Clearing columns is typically handled by getOrCreateSheet.
    
    return rows.length; // Returns the total number of processed rows (updated + appended)
  }

  // Optional ergonomic wrapper: bind a sheet and use the same methods
  function forSheet(name) {
    setSheet(name);
    return { append: appendRows, upsert: upsertBy, setSheet: setSheet };
  }

  return {
    // existing surface (unchanged) + tiny additions
    appendRows: appendRows,
    upsertBy:   upsertBy,
    setSheet:   setSheet,
    forSheet:   forSheet,
    HEAD:       HEAD,
    sheetName:  function(){ return SHEET; }
  };
})();
