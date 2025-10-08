/** MaterialLedger API — append + upsert
 * Ledger HEAD:
 * [date, type_id, item_name, qty, unit_value, source, contract_id, char, unit_value_filled]
 */
var ML = (function () {
  var SHEET = 'Material_Ledger';
  var HEAD  = ['date','type_id','item_name','qty','unit_value','source','contract_id','char','unit_value_filled'];

  function getSheet_() {
    var ss = SpreadsheetApp.getActiveSpreadsheet();
    return getOrCreateSheet(ss, SHEET, HEAD); // your helper
  }
  function headerIndex_(sh) {
    var row = sh.getRange(1, 1, 1, HEAD.length).getValues()[0];
    var m = {}; for (var i=0;i<row.length;i++) m[String(row[i]).trim()] = i;
    return m;
  }
  function typeNameMap_(rangeName) {
    var ss = SpreadsheetApp.getActive();
    var rng = ss.getRangeByName(rangeName || 'sde_typeid_name');
    if (!rng) return null;
    var vals = rng.getValues(), map = new Map();
    for (var i=0;i<vals.length;i++) {
      var id = Number(String(vals[i][0]).replace(/[^\d+Ee\.-]/g,'')); // col A
      var name = String(vals[i][2] || '').trim();                      // col C
      if (Number.isFinite(id) && name) map.set(Math.floor(id), name);
    }
    return map;
  }
  function key_(source, contract_id) {
    var s = String(source || '').toUpperCase();
    var k = String(contract_id || '').trim();
    return (s && k) ? (s + '|' + k) : '';
  }
  function existingKeys_(sh, sourcesFilter) {
    var set = new Set(), last = sh.getLastRow();
    if (last < 2) return set;
    var H = headerIndex_(sh), iSrc = H.source, iKey = H.contract_id;
    var srcCol = sh.getRange(2, iSrc+1, last-1, 1).getValues();
    var keyCol = sh.getRange(2, iKey+1, last-1, 1).getValues();
    var want = (Array.isArray(sourcesFilter) && sourcesFilter.length)
      ? new Set(sourcesFilter.map(function(x){ return String(x).toUpperCase(); }))
      : null;
    for (var r=0;r<srcCol.length;r++){
      var s = String(srcCol[r][0] || '').toUpperCase();
      if (want && !want.has(s)) continue;
      var k = String(keyCol[r][0] || '').trim();
      if (k) set.add(key_(s, k));
    }
    return set;
  }
  function existingRowMap_(sh, sourcesFilter) {
    // key -> rowNumber (1-based)
    var map = new Map(), last = sh.getLastRow();
    if (last < 2) return map;
    var H = headerIndex_(sh), iSrc = H.source, iKey = H.contract_id;
    var width = Math.max(HEAD.length, sh.getLastColumn());
    var vals = sh.getRange(2, 1, last-1, width).getValues(); // includes src, key
    var want = (Array.isArray(sourcesFilter) && sourcesFilter.length)
      ? new Set(sourcesFilter.map(function(x){ return String(x).toUpperCase(); }))
      : null;
    for (var r=0;r<vals.length;r++){
      var s = String(vals[r][iSrc] || '').toUpperCase();
      if (want && !want.has(s)) continue;
      var cid = String(vals[r][iKey] || '').trim();
      if (!cid) continue;
      var k = key_(s, cid);
      if (k && !map.has(k)) map.set(k, r + 2); // +2 for header offset
    }
    return map;
  }
  function normalizeRows_(rows, opts) {
    opts = opts || {};
    var map = opts.fillNamesFromSDE ? (typeNameMap_(opts.sdeRangeName) || null) : null;
    var out = [], batchSet = new Set(), bad = 0;

    for (var i=0;i<rows.length;i++) {
      var r = rows[i], a;
      if (Array.isArray(r)) {
        a = r.slice(0, HEAD.length);
      } else if (r && typeof r === 'object') {
        a = [r.date, r.type_id, r.item_name, r.qty, r.unit_value, r.source, r.contract_id, r.char, r.unit_value_filled];
      } else { bad++; continue; }

      var d = (a[0] instanceof Date) ? a[0] : new Date(a[0]);
      if (!(d instanceof Date) || isNaN(d)) { bad++; continue; }
      var tid = Number(a[1]); if (!Number.isFinite(tid)) { bad++; continue; }
      var name = String(a[2] || '').trim();
      var qty  = Number(a[3]); if (!Number.isFinite(qty) || qty === 0) { bad++; continue; }
      var uval = Number(a[4]); if (!Number.isFinite(uval) || uval === 0) { bad++; continue; }
      var src  = a[5] == null ? '' : String(a[5]);
      var cid  = a[6] == null ? '' : String(a[6]);
      var who  = a[7] == null ? '' : String(a[7]);
      var filled = a[8]; if (!Number.isFinite(filled)) filled = uval;

      if (!name && map) name = map.get(Math.floor(tid)) || '';
      var k = key_(src, cid);
      if (!k) { bad++; continue; }

      if (opts.dedupeWithinBatch !== false) {
        if (batchSet.has(k)) continue;
        batchSet.add(k);
      }
      out.push([d, tid, name, qty, uval, src, cid, who, filled]);
    }
    return { rows: out, bad: bad };
  }

  /** Append only (skip if key already exists) */
  function append(rows, opts) {
    opts = opts || {};
    if (!rows || !rows.length) return { appended: 0, skippedExisting: 0, bad: 0 };

    var sh = getSheet_();
    var norm = normalizeRows_(rows, {
      fillNamesFromSDE: !!opts.fillNamesFromSDE,
      sdeRangeName: opts.sdeRangeName || 'sde_typeid_name',
      dedupeWithinBatch: opts.dedupeWithinBatch !== false
    });
    var prepared = norm.rows;
    if (!prepared.length) return { appended: 0, skippedExisting: 0, bad: norm.bad };

    var srcs = Array.from(new Set(prepared.map(function(r){ return String(r[5]).toUpperCase(); })));
    var exist = existingKeys_(sh, srcs);

    var fresh = [], skipped = 0;
    for (var i=0;i<prepared.length;i++) {
      var r = prepared[i], k = key_(r[5], r[6]);
      if (exist.has(k)) { skipped++; continue; }
      exist.add(k); fresh.push(r);
    }
    if (!fresh.length) return { appended: 0, skippedExisting: skipped, bad: norm.bad };

    var COLS = HEAD.length;
    var rowsPerBatch = Math.max(50, Math.floor((opts.cellsPerChunk || 7000) / COLS));
    var start = sh.getLastRow() + 1;

    for (var off = 0; off < fresh.length; off += rowsPerBatch) {
      var seg = fresh.slice(off, off + rowsPerBatch);
      sh.getRange(start + off, 1, seg.length, COLS).setValues(seg);
    }
    try {
      var n = fresh.length;
      sh.getRange(start, 1, n, 1).setNumberFormat('yyyy-mm-dd');
      sh.getRange(start, 2, n, 1).setNumberFormat('0');
      sh.getRange(start, 4, n, 1).setNumberFormat('#,##0');
      sh.getRange(start, 5, n, 1).setNumberFormat('#,##0.00');
      sh.getRange(start, 9, n, 1).setNumberFormat('#,##0.00');
    } catch (_) {}

    return { appended: fresh.length, skippedExisting: skipped, bad: norm.bad };
  }

  /** Upsert: update rows where key exists; append the rest.
   * opts:
   *  - mode: 'replace' (default) or 'merge'
   *      replace → overwrite all 9 fields
   *      merge   → only overwrite fields that are non-empty / non-null in input
   *  - cellsPerChunk: batching for appends
   *  - fillNamesFromSDE, sdeRangeName, dedupeWithinBatch: same as append
   */
  function upsert(rows, opts) {
    opts = opts || {};
    if (!rows || !rows.length) return { updated: 0, appended: 0, bad: 0 };

    var sh = getSheet_();
    var norm = normalizeRows_(rows, {
      fillNamesFromSDE: !!opts.fillNamesFromSDE,
      sdeRangeName: opts.sdeRangeName || 'sde_typeid_name',
      dedupeWithinBatch: opts.dedupeWithinBatch !== false
    });
    var prepared = norm.rows;
    if (!prepared.length) return { updated: 0, appended: 0, bad: norm.bad };

    var srcs = Array.from(new Set(prepared.map(function(r){ return String(r[5]).toUpperCase(); })));
    var keyToRow = existingRowMap_(sh, srcs);
    var COLS = HEAD.length;

    // Partition into updates vs appends
    var updates = []; // { rn, row }
    var appends = [];
    for (var i=0;i<prepared.length;i++) {
      var r = prepared[i];
      var k = key_(r[5], r[6]);
      var rn = keyToRow.get(k);
      if (rn) updates.push({ rn: rn, row: r }); else appends.push(r);
    }

    // Write updates grouped by contiguous row numbers
    var updated = 0;
    if (updates.length) {
      updates.sort(function(a,b){ return a.rn - b.rn; });

      if (String(opts.mode || 'replace').toLowerCase() === 'merge') {
        // read existing values block-by-block, merge, then write back
        for (var u=0; u<updates.length;) {
          var start = updates[u].rn;
          var block = [updates[u]];
          var v = u + 1;
          while (v < updates.length && updates[v].rn === updates[v-1].rn + 1) {
            block.push(updates[v]); v++;
          }
          var existVals = sh.getRange(start, 1, block.length, COLS).getValues();
          var merged = [];
          for (var j=0;j<block.length;j++) {
            var incoming = block[j].row;
            var existing = existVals[j];
            var out = existing.slice();
            for (var c=0;c<COLS;c++) {
              var val = incoming[c];
              var isEmpty = (val === '' || val == null);
              // numbers: allow 0; strings: '' means keep existing
              if (!isEmpty) out[c] = val;
            }
            merged.push(out);
          }
          sh.getRange(start, 1, merged.length, COLS).setValues(merged);
          updated += merged.length;
          u = v;
        }
      } else {
        // replace
        for (var u2=0; u2<updates.length;) {
          var start2 = updates[u2].rn;
          var block2 = [updates[u2].row];
          var w = u2 + 1;
          while (w < updates.length && updates[w].rn === updates[w-1].rn + 1) {
            block2.push(updates[w].row); w++;
          }
          sh.getRange(start2, 1, block2.length, COLS).setValues(block2);
          updated += block2.length;
          u2 = w;
        }
      }
    }

    // Append remainder using same batching as append()
    var appended = 0;
    if (appends.length) {
      var rowsPerBatch = Math.max(50, Math.floor((opts.cellsPerChunk || 7000) / COLS));
      var startAppend = sh.getLastRow() + 1;
      for (var off = 0; off < appends.length; off += rowsPerBatch) {
        var seg = appends.slice(off, off + rowsPerBatch);
        sh.getRange(startAppend + off, 1, seg.length, COLS).setValues(seg);
        appended += seg.length;
      }
      try {
        var n = appended;
        sh.getRange(startAppend, 1, n, 1).setNumberFormat('yyyy-mm-dd');
        sh.getRange(startAppend, 2, n, 1).setNumberFormat('0');
        sh.getRange(startAppend, 4, n, 1).setNumberFormat('#,##0');
        sh.getRange(startAppend, 5, n, 1).setNumberFormat('#,##0.00');
        sh.getRange(startAppend, 9, n, 1).setNumberFormat('#,##0.00');
      } catch (_) {}
    }

    return { updated: updated, appended: appended, bad: norm.bad };
  }

  return { append: append, upsert: upsert, HEAD: HEAD, SHEET: SHEET };
})();
