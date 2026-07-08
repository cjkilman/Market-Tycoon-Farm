function runDowntimeMaintenance() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('MAINTENANCE') : console;

  LOG.info("Starting Daily Ledger Maintenance...");

  if (typeof pauseSheet === 'function') pauseSheet(ss);

  try {
    // Material Ledger: Split by Item, Source (Loot vs Journal), and Character
    ML.forSheet("Material_Ledger").condenseHistory(7, ['type_id', 'source', 'char'], true);
    LOG.info("Material Ledger Condensed.");

    // Sales Ledger: Split by Item, Source (Market vs Contract), and Character
    ML.forSheet("Sales_Ledger").condenseHistory(7, ['type_id', 'source', 'char'], true);
    LOG.info("Sales Ledger Condensed.");

  } catch (e) {
    LOG.error("Maintenance Error: " + e.message);
  } finally {
    if (typeof wakeUpSheet === 'function') wakeUpSheet(ss);
    LOG.info("Maintenance Complete. Sheet Awake.");
  }
}
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


var ML = (function () {
  // --- MODULE SCOPE ---
  var HEAD = ['date', 'type_id', 'item_name', 'qty', 'unit_value', 'source', 'contract_id', 'char', 'unit_value_filled'];


  function getSS_(providedSs) {
    if (providedSs && typeof providedSs.getParent === 'function') return providedSs.getParent();
    return providedSs || SpreadsheetApp.getActiveSpreadsheet();
  }



  function forSheet(sheetName) {
    const ss = getSS_();
    var sh = ss.getSheetByName(sheetName);
    if (!sh) sh = getOrCreateSheet(ss, sheetName, HEAD);

    const rawHead = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
    const HEAD_CURRENT = rawHead.map(h => String(h).trim());

    function normalizeRow_(r) {
      var out = {};
      const PT_API = (typeof PT !== 'undefined' && PT.yyyymmdd) ? PT : null;
      let d = r.date;
      let dateStr = "";
      if (typeof d === 'string' && /^\d{4}-\d{2}-\d{2}/.test(d)) {
        dateStr = d.substring(0, 10);
      } else {
        let dt = (d instanceof Date) ? d : new Date(d);
        if (isNaN(dt.getTime())) dt = new Date();
        dateStr = PT_API ? PT_API.yyyymmdd(dt) : Utilities.formatDate(dt, Session.getScriptTimeZone(), "yyyy-MM-dd");
      }
      out.date = dateStr;
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
      if (!rows || !rows.length) return { rows: 0, status: "SUCCESS" };
      const keyIndices = keys.map(k => {
        const idx = HEAD_CURRENT.indexOf(k.trim());
        if (idx === -1) throw new Error(`CRITICAL: Key "${k}" not found.`);
        return idx;
      });
      const normalizeK = (v, i) => (i === 1) ? String(Math.round(Number(v || 0))) : String(v || '');
      const incomingMap = new Map();
      rows.forEach(obj => {
        const out = normalizeRow_(obj);
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
      incomingMap.forEach((val, key) => existingMap.set(key, val));
      const allValues = Array.from(existingMap.values());
      let needsWakeUp = false;
      try {
        if (!holdAnesthesia && typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);
        sh.getRange(2, 1, sh.getMaxRows() - 1, HEAD_CURRENT.length).clearContent();
        sh.getRange(2, 1, allValues.length, HEAD_CURRENT.length).setValues(allValues);
        const rangeName = (sheetName === "Material_Ledger") ? "NR_MATERIAL_LEDGER" : "NR_SALES_LEDGER";
        ss.setNamedRange(rangeName, sh.getRange(1, 1, allValues.length + 1, HEAD_CURRENT.length));
        return { rows: allValues.length, status: "SUCCESS" };
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

    return { upsert: upsertBy, query: query };
  }
  return { forSheet: forSheet };
})();