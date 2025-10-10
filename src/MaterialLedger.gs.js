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
    return getOrCreateSheet(ss, SHEET, HEAD); // uses your Utility helper
  }

  function headerIndex_(sh) {
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
    var out = {};
    out.date  = r.date || PT.yyyymmdd(PT.now()); // reuse project Time
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
    sh.getRange(sh.getLastRow() + 1, 1, data.length, HEAD.length).setValues(data);
    return data.length;
  }

  // Upsert by one or more logical key columns (e.g. ['contract_id','source'])
  function upsertBy(keys, rows) {
    if (!rows || !rows.length) return 0;
    var sh = getSheet_();
    var H  = headerIndex_(sh);

    var keyColNames = keys || [];
    var keyColIndices = []; // 1-based column indices for all requested key names
    var keyColHdrMap = {}; // Maps key name -> 0-based column index in the HEAD array

    // 1. Determine 1-based indices of all key columns
    keyColNames.forEach(function (k) {
      if (!H[k]) throw new Error('Unknown key column: ' + k);
      keyColIndices.push(H[k]); // 1-based sheet column index
    });
    
    // Determine the minimum range needed to read existing key data
    var firstKeyCol = Math.min.apply(null, keyColIndices);
    var lastKeyCol = Math.max.apply(null, keyColIndices);
    
    // The number of columns we actually need to read from the sheet
    var readWidth = lastKeyCol - firstKeyCol + 1;

    // The shift factor when mapping sheet index to read values array index
    var readOffset = firstKeyCol;
    
    // Map of the column index in the 'read values array' to the original HEAD index
    var sheetColToReadIndex = {};
    keyColIndices.forEach(function(sheetCol) {
      sheetColToReadIndex[sheetCol] = sheetCol - readOffset;
    });


    var last  = sh.getLastRow();
    var vals  = [];
    
    // 2. Read ONLY the necessary key columns from the sheet
    if (last >= 2) {
      vals = sh.getRange(2, firstKeyCol, last - 1, readWidth).getValues();
    }
    
    // 3. Map composite key → absolute row number (optimized read)
    var map = new Map();
    for (var r = 0; r < vals.length; r++) {
      var row = vals[r];
      var key = keyColIndices.map(function (sheetCol) {
        // Get the value using the optimized read index
        var val = row[sheetColToReadIndex[sheetCol]] || '';
        return String(val); 
      }).join('\u0001');
      map.set(key, r + 2); // 1-based rows, data starts at row 2
    }

    var appends = [];
    rows.forEach(function (obj) {
      var outRow = normalizeRow_(obj);
      
      // 4. Generate key for the incoming row
      var key    = keyColNames.map(function (k) {
        var idx = HEAD.indexOf(k); // 0-based index in the HEAD array
        return String(outRow[idx] || '');
      }).join('\u0001');

      var at = map.get(key);
      if (at) {
        // Update existing row
        sh.getRange(at, 1, 1, HEAD.length).setValues([outRow]);
      } else {
        // Append new row
        appends.push(outRow);
      }
    });

    if (appends.length) {
      sh.getRange(sh.getLastRow() + 1, 1, appends.length, HEAD.length).setValues(appends);
    }
    return rows.length;
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
