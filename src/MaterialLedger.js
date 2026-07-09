// ==========================================
// DAILY MAINTENANCE & EMERGENCY TOOLS
// ==========================================

function runDowntimeMaintenance() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('MAINTENANCE') : console;

  LOG.info("Starting Daily Ledger Maintenance...");
  if (typeof pauseSheet === 'function') pauseSheet(ss);

  try {
    ML.forSheet("Material_Ledger").condenseHistory(30, ['type_id', 'source', 'char'], true);
    LOG.info("Material Ledger Condensed.");

    ML.forSheet("Sales_Ledger").condenseHistory(30, ['type_id', 'source', 'char'], true);
    LOG.info("Sales Ledger Condensed.");
  } catch (e) {
    LOG.error("Maintenance Error: " + e.message);
  } finally {
    if (typeof wakeUpSheet === 'function') wakeUpSheet(ss);
    LOG.info("Maintenance Complete. Sheet Awake.");
  }
}

function purgeMaterialLedger() {
  var LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('DEDUPE') : console;
  LOG.info("Starting Material Ledger Purge...");
  try {
    var res = ML.forSheet("Material_Ledger").dedupeExisting(['date', 'type_id', 'source', 'char']);
    LOG.info("Material_Ledger: Purged " + res.removed + " duplicates.");
  } catch (e) {
    LOG.error("Purge Failed: " + e.message);
  }
}

function purgeSalesLedger() {
  var LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('DEDUPE') : console;
  LOG.info("Starting Sales Ledger Purge...");
  try {
    var res = ML.forSheet("Sales_Ledger").dedupeExisting(['date', 'type_id', 'source', 'char']);
    LOG.info("Sales_Ledger: Purged " + res.removed + " duplicates.");
  } catch (e) {
    LOG.error("Purge Failed: " + e.message);
  }
}

// ==========================================
// GLOBALS & CACHE
// ==========================================

var GLOBALS = { dataCache: new Map(), rangeCache: new Map() };

function getCachedData(ss, rangeName) {
  if (!GLOBALS.dataCache.has(rangeName)) {
    const range = ss.getRangeByName(rangeName);
    if (!range) return null;
    GLOBALS.rangeCache.set(rangeName, range);
    GLOBALS.dataCache.set(rangeName, range.getValues());
  }
  return GLOBALS.dataCache.get(rangeName);
}

// ==========================================
// CORE ML MODULE
// ==========================================

var ML = (function () {
  var HEAD = ['date', 'type_id', 'item_name', 'qty', 'unit_value', 'source', 'contract_id', 'char', 'unit_value_filled'];

  function getSS_(providedSs) {
    if (providedSs && typeof providedSs.getParent === 'function') return providedSs.getParent();
    return providedSs || SpreadsheetApp.getActiveSpreadsheet();
  }

  function forSheet(sheetName, providedSs) {
    const ss = getSS_(providedSs);
    var sh = ss.getSheetByName(sheetName);
    if (!sh) sh = getOrCreateSheet(ss, sheetName, HEAD);

    const rawHead = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
    const HEAD_CURRENT = rawHead.map(h => String(h).trim());

    const HEAD_LOWER = HEAD_CURRENT.map(h => h.toLowerCase());
    const idxDate = HEAD_LOWER.indexOf('date');
    const idxQty = HEAD_LOWER.indexOf('qty');
    const idxUnitValueFilled = HEAD_LOWER.indexOf('unit_value_filled');

    // --- TRANSLATOR: Now strictly enforces yyyy-MM-dd:HHmmss ---
    function normalizeRow_(r) {
      var out = {};
      let d = r.date;

      let dt;
      if (d instanceof Date) {
        dt = d;
      } else {
        dt = new Date(d);
      }
      if (isNaN(dt.getTime())) dt = new Date();

      out.date = Utilities.formatDate(dt, Session.getScriptTimeZone(), "yyyy-MM-dd:HHmmss");

      out.type_id = r.type_id || 0;
      out.item_name = r.item_name || '';
      out.qty = Number(String(r.qty || 0).replace(/,/g, '')) || 0;
      var u0 = Number(String(r.unit_value || 0).replace(/,/g, ''));
      var u1 = Number(String(r.unit_value_filled || 0).replace(/,/g, ''));
      out.unit_value = u0 > 0 ? u0 : '';
      out.source = r.source || '';
      out.contract_id = r.contract_id || '';
      out.char = r.char || '';
      var finalVal = u0 > 0 ? u0 : (u1 > 0 ? u1 : 0);
      out.unit_value_filled = finalVal > 0 ? finalVal : '';
      out.metadata = r.metadata || '';
      return HEAD_CURRENT.map(k => (out[k] === undefined ? '' : out[k]));
    }

    function upsertBy(keys, rows, holdAnesthesia) {
      if (!rows || !rows.length) return { appended: 0, upserted: 0, totalRows: 0, status: "SUCCESS" };

      const keyIndices = keys.map(k => {
        const idx = HEAD_CURRENT.indexOf(k.trim());
        if (idx === -1) throw new Error(`CRITICAL: Key "${k}" not found.`);
        return idx;
      });

      // --- BARCODE SCANNER ---
      const normalizeK = (v, idx) => {
        const colName = HEAD_CURRENT[idx].toLowerCase();

        if (colName === 'date') {
          if (typeof v === 'string' && /^\d{4}-\d{2}-\d{2}/.test(v)) {
            return v.trim();
          }

          let dt = (v instanceof Date) ? v : new Date(v);
          if (!isNaN(dt.getTime())) {
            return Utilities.formatDate(dt, Session.getScriptTimeZone(), "yyyy-MM-dd:HHmmss");
          }
          return String(v).trim();
        }

        if (colName === 'type_id') {
          return String(Math.round(Number(v || 0)));
        }

        let str = String(v || '').trim().toLowerCase();
        if (str !== '' && !isNaN(Number(str))) return String(Number(str));
        return str;
      };

      const incomingMap = new Map();
      const validQtyIdx = HEAD_CURRENT.indexOf('qty');
      const validTypeIdIdx = HEAD_CURRENT.indexOf('type_id');

      rows.forEach(obj => {
        const out = normalizeRow_(obj);

        const checkQty = Number(out[validQtyIdx]) || 0;
        const checkTypeId = Number(out[validTypeIdIdx]) || 0;
        if (checkQty === 0 || checkTypeId === 0) return;

        const k = keyIndices.map(idx => normalizeK(out[idx], idx)).join('|');
        incomingMap.set(k, out);
      });

      const existingMap = new Map();
      const last = sh.getLastRow();

      if (last >= 2) {
        const data = sh.getRange(2, 1, last - 1, HEAD_CURRENT.length).getValues();
        data.forEach((row) => {
          const k = keyIndices.map(idx => normalizeK(row[idx], idx)).join('|');
          existingMap.set(k, row);
        });
      }

      let appendedCount = 0;
      let upsertedCount = 0;

      incomingMap.forEach((val, key) => {
        if (existingMap.has(key)) {
          upsertedCount++;
        } else {
          appendedCount++;
        }
        existingMap.set(key, val);
      });

      const allValues = Array.from(existingMap.values());

      let needsWakeUp = false;
      try {
        if (!holdAnesthesia && typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

        if (last > 1) sh.getRange(2, 1, last - 1, HEAD_CURRENT.length).clearContent();
        sh.getRange(2, 1, allValues.length, HEAD_CURRENT.length).setValues(allValues);

        const rangeName = (sheetName === "Material_Ledger") ? "NR_MATERIAL_LEDGER" : "NR_SALES_LEDGER";
        ss.setNamedRange(rangeName, sh.getRange(1, 1, allValues.length + 1, HEAD_CURRENT.length));

        if (typeof GLOBALS !== 'undefined' && GLOBALS.dataCache) {
          GLOBALS.dataCache.delete(rangeName);
        }

        if (allValues.length > 0) {
          updateBlendedSummary();
        }

        return { appended: appendedCount, upserted: upsertedCount, totalRows: allValues.length, status: "SUCCESS" };

      } finally {
        if (!holdAnesthesia && needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss);
      }
    }

    function query(criteria) {
      const rangeName = (sheetName === "Material_Ledger") ? "NR_MATERIAL_LEDGER" : "NR_SALES_LEDGER";
      const data = getCachedData(ss, rangeName);
      if (!data || data.length < 2) return [];
      return data.slice(1).map(row => {
        const obj = {};
        HEAD_CURRENT.forEach((h, i) => obj[h] = row[i]);
        if (typeof criteria === 'object') {
          for (let key in criteria) {
            if (Array.isArray(criteria[key]) && !criteria[key].includes(obj[key])) return null;
            else if (!Array.isArray(criteria[key]) && obj[key] != criteria[key]) return null;
          }
        }
        return obj;
      }).filter(item => item !== null);
    }

    function condenseHistory(cutoffDays, keys, holdAnesthesia) {
      const lastRow = sh.getLastRow();
      if (lastRow <= 2) return { rows: 0, status: "SUCCESS" };

      const rangeName = (sheetName === "Material_Ledger") ? "NR_MATERIAL_LEDGER" : "NR_SALES_LEDGER";
      const data = getCachedData(ss, rangeName);
      if (!data || data.length < 2) return { rows: 0, status: "SUCCESS" };

      const rows = data.slice(1);
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - cutoffDays);

      const keptRows = [];
      const condensedGroups = new Map();

      rows.forEach(row => {
        const rawDate = row[idxDate];
        // Safely extract just the YYYY-MM-DD part so JS can do the 7-day math
        const dateStr = String(rawDate || "").substring(0, 10);
        const rowDate = (dateStr !== "") ? new Date(dateStr) : new Date(0);

        if (rowDate < cutoffDate) {
          const k = keys.map(key => {
            const idx = HEAD_LOWER.indexOf(String(key).toLowerCase());
            return row[idx];
          }).join('|');

          if (!condensedGroups.has(k)) {
            let newRow = [...row];
            newRow[idxDate] = Utilities.formatDate(cutoffDate, Session.getScriptTimeZone(), "yyyy-MM-dd:000000");
            newRow.total_cost = Number(row[idxQty]) * Number(row[idxUnitValueFilled]);

            // --- THE FIX: Nuke the static unit_value ---
            const idxUnitValue = HEAD_LOWER.indexOf('unit_value');
            if (idxUnitValue !== -1) newRow[idxUnitValue] = '';

            condensedGroups.set(k, newRow);
          } else {
            let group = condensedGroups.get(k);
            const newQty = Number(row[idxQty]) || 0;
            const newCost = newQty * (Number(row[idxUnitValueFilled]) || 0);

            group[idxQty] = Number(group[idxQty]) + newQty;
            group.total_cost += newCost;

            const totalQty = Number(group[idxQty]);
            group[idxUnitValueFilled] = totalQty > 0 ? (group.total_cost / totalQty) : 0;
            condensedGroups.set(k, group);
          }
        } else {
          keptRows.push(row);
        }
      });

      const condensedRows = Array.from(condensedGroups.values()).map(r => {
        delete r.total_cost;
        return r;
      });

      const finalRows = keptRows.concat(condensedRows);

      let needsWakeUp = false;
      try {
        if (!holdAnesthesia && typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

        if (lastRow > 1) sh.getRange(2, 1, lastRow - 1, HEAD.length).clearContent();

        if (finalRows.length > 0) {
          sh.getRange(2, 1, finalRows.length, HEAD.length).setValues(finalRows);
          GLOBALS.dataCache.delete(rangeName);

          // --- THE FIX: Force the Summary to update and clear its own cache ---
          updateBlendedSummary();
        }

        return { rows: finalRows.length, status: "SUCCESS" };
      } finally {
        if (!holdAnesthesia && needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss);
      }
    }

    function dedupeExisting(keys) {
      const keyIndices = keys.map(k => {
        const idx = HEAD_CURRENT.indexOf(k.trim());
        if (idx === -1) throw new Error(`CRITICAL: Key "${k}" not found.`);
        return idx;
      });

      const normalizeK = (v, idx) => {
        const colName = HEAD_CURRENT[idx].toLowerCase();

        if (colName === 'date') {
          if (typeof v === 'string' && /^\d{4}-\d{2}-\d{2}/.test(v)) {
            return v.trim();
          }
          let dt = (v instanceof Date) ? v : new Date(v);
          if (!isNaN(dt.getTime())) {
            return Utilities.formatDate(dt, Session.getScriptTimeZone(), "yyyy-MM-dd:HHmmss");
          }
          return String(v).trim();
        }

        if (colName === 'type_id') {
          return String(Math.round(Number(v || 0)));
        }

        let str = String(v || '').trim().toLowerCase();
        if (str !== '' && !isNaN(Number(str))) return String(Number(str));
        return str;
      };

      const existingMap = new Map();
      const last = sh.getLastRow();

      if (last < 2) return { removed: 0, status: "NO_DATA" };

      const data = sh.getRange(2, 1, last - 1, HEAD_CURRENT.length).getValues();
      const originalCount = data.length;

      data.forEach((row) => {
        const k = keyIndices.map(idx => normalizeK(row[idx], idx)).join('|');
        existingMap.set(k, row);
      });

      const allValues = Array.from(existingMap.values());
      const newCount = allValues.length;
      const removedCount = originalCount - newCount;

      if (removedCount === 0) {
        return { removed: 0, status: "NO_DUPLICATES" };
      }

      let needsWakeUp = false;
      try {
        if (typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

        if (last > 1) sh.getRange(2, 1, last - 1, HEAD_CURRENT.length).clearContent();
        sh.getRange(2, 1, allValues.length, HEAD_CURRENT.length).setValues(allValues);

        const rangeName = (sheetName === "Material_Ledger") ? "NR_MATERIAL_LEDGER" : "NR_SALES_LEDGER";
        ss.setNamedRange(rangeName, sh.getRange(1, 1, allValues.length + 1, HEAD_CURRENT.length));

        if (typeof GLOBALS !== 'undefined' && GLOBALS.dataCache) {
          GLOBALS.dataCache.delete(rangeName);
        }

        return { removed: removedCount, status: "SUCCESS" };
      } finally {
        if (needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss);
      }
    }

    function updateBlendedSummary() {
      const isMaterial = (sheetName === "Material_Ledger");
      const targetSheetName = isMaterial ? "Blended_Cost" : "Blended_Sales";
      const targetRangeName = isMaterial ? "NR_BLENDED_COST" : "NR_BLENDED_SALES";

      const summarySheet = ss.getSheetByName(targetSheetName);
      if (!summarySheet) {
        console.error(targetSheetName + ' sheet not found.');
        return;
      }

      const ledgerData = query();
      if (!ledgerData || ledgerData.length === 0) return;

      const summaryMap = new Map();

      ledgerData.forEach(row => {
        const id = row.type_id;
        const qty = Number(row.qty) || 0;
        const price = Number(row.unit_value) || Number(row.unit_value_filled) || 0;

        if (!id || qty <= 0 || price <= 0) return;

        if (!summaryMap.has(id)) {
          summaryMap.set(id, { type_id: id, total_qty: 0, total_sum: 0 });
        }

        const current = summaryMap.get(id);
        current.total_qty += qty;
        current.total_sum += (qty * price);
      });

      const columnsToSave = ['type_id', 'total_sum', 'unit_weighted_average'];
      const outputData = [];

      summaryMap.forEach(obj => {
        obj.unit_weighted_average = Number((obj.total_sum / obj.total_qty).toFixed(6));
        const rowArray = columnsToSave.map(col => obj[col] !== undefined ? obj[col] : '');
        outputData.push(rowArray);
      });

      if (outputData.length === 0) return;

      const sumLast = summarySheet.getLastRow();
      if (sumLast > 1) {
        summarySheet.getRange(2, 1, sumLast - 1, columnsToSave.length).clearContent();
      }
      summarySheet.getRange(2, 1, outputData.length, columnsToSave.length).setValues(outputData);

      ss.setNamedRange(targetRangeName, summarySheet.getRange(2, 1, outputData.length, columnsToSave.length));

      if (typeof GLOBALS !== 'undefined' && GLOBALS.dataCache) {
        GLOBALS.dataCache.delete(targetRangeName);
      }
    }

    return {
      upsert: upsertBy,
      query: query,
      condenseHistory: condenseHistory,
      dedupeExisting: dedupeExisting
    };
  }

  return { forSheet: forSheet };
})();