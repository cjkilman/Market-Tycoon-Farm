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
  }
}


function RUN_MATERIAL_AUDIT() { AUDIT_LEDGER_INTEGRITY("Material_Ledger"); }

function AUDIT_LEDGER_INTEGRITY(sheetName) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sh = ss.getSheetByName(sheetName);

  if (!sh) {
    console.error(`ERROR: Could not find a sheet named '${sheetName}'. Please check that the name matches your tab exactly.`);
    return;
  }

  const data = sh.getDataRange().getValues();
  const headers = data[0];
  const dateIdx = headers.indexOf('date');
  const typeIdx = headers.indexOf('type_id');

  if (dateIdx === -1 || typeIdx === -1) {
    console.error("ERROR: Sheet missing required headers 'date' or 'type_id'.");
    return;
  }

  const registry = new Map();
  const nearDuplicates = [];

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const dateVal = row[dateIdx];

    // Safety check for empty or invalid dates
    if (!dateVal) continue;

    const date = new Date(dateVal);
    const tid = row[typeIdx];

    // Create a key that is just YYYY-MM-DD + TID
    const key = `${date.toISOString().split('T')[0]}|${tid}`;

    if (registry.has(key)) {
      nearDuplicates.push({ key, rowId: i + 1 });
    } else {
      registry.set(key, i + 1);
    }
  }

  if (nearDuplicates.length > 0) {
    console.warn(`FOUND ${nearDuplicates.length} POTENTIAL DEDUPE CONFLICTS in ${sheetName}:`);
    // Log first 10 for clarity
    nearDuplicates.slice(0, 10).forEach(d => console.log(`Conflict at Row ${d.rowId} for key: ${d.key}`));
  } else {
    console.log(`Integrity Check PASSED for ${sheetName}. No conflicting date-TID pairs found.`);
  }
}

function purgeMaterialLedger() { purgeLedger("Material_Ledger"); }
function purgeSalesLedger() { purgeLedger("Sales_Ledger"); }

function purgeLedger(sheetName) {
  var LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('DEDUPE') : console;
  LOG.info(`Starting ${sheetName} Purge...`);
  try {
    var res = ML.forSheet(sheetName).dedupeExisting(['date', 'type_id', 'source', 'char','contract_id']);
    LOG.info(`${sheetName}: Purged ${res.removed} duplicates.`);
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

  /**
   * Headers Row 1
   * Named Ranges reflect this and Used by the Sheet
   * Upsert has Dedupe and Filters Blank Line inputs
   * Dates save to Sheet are Sheet objects Input Dates can Be Sheet Objects or ESI Strings incase they'ey're not handled correctly on input
   * Blank Rows Contain no Valid Date objects... We Don't try to Create a phony Date from and invalid object
   */

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

      // --- STRICT DATE PARSING (ESI ISO Strings & Native Dates) ---
      if (d instanceof Date) {
        dt = d;
      } else if (d) {
        dt = new Date(d);
      } else {
        throw new Error(`CRITICAL: Expected a Date or ESI string, but received type '${typeof d}' (Value: ${JSON.stringify(d)}).`);
      }

      // Catch malformed strings that result in an "Invalid Date"
      if (isNaN(dt.getTime())) {
        throw new Error(`CRITICAL: Could not parse ESI timestamp into a valid Date object (Value: ${d}).`);
      }

      out.date = dt;

      // --- STRICT NUMBER PARSING (IDs & Financials) ---
      out.type_id = Number(String(r.type_id || 0).replace(/,/g, '')) || 0;
      out.item_name = r.item_name || '';
      out.qty = Number(String(r.qty || 0).replace(/,/g, '')) || 0;

      var u0 = Number(String(r.unit_value || 0).replace(/,/g, ''));
      var u1 = Number(String(r.unit_value_filled || 0).replace(/,/g, ''));
      out.unit_value = u0 > 0 ? u0 : '';

      // --- STRICT STRING PARSING ---
      out.source = String(r.source || '').trim();
      out.contract_id = String(r.contract_id || '').trim(); // Absolute String Lock
      out.char = String(r.char || '').trim();

      var finalVal = u0 > 0 ? u0 : (u1 > 0 ? u1 : 0);
      out.unit_value_filled = finalVal > 0 ? finalVal : '';
      out.metadata = (typeof r.metadata === 'object') ? JSON.stringify(r.metadata) : (r.metadata || '');

      // Map back using LOWERCASE headers to guarantee immunity from capitalized sheet columns
      return HEAD_LOWER.map(k => (out[k] === undefined ? '' : out[k]));
    }

    // --- STANDALONE KEY NORMALIZER (DRY & KEY-STABLE) ---
    function _normalizeKeySegment(v, colName) {
      if (colName === 'date') {
        let dt = (v instanceof Date) ? v : new Date(v);
        if (!isNaN(dt.getTime())) {
          return dt.toISOString().split('T')[0];
        }
        return String(v).trim();
      }

      if (colName === 'type_id') {
        const cleanV = String(v || 0).replace(/,/g, '');
        return String(Math.round(Number(cleanV) || 0));
      }

      // THE SHIELD: Never let JS run math or lowercase on the Contract ID
      if (colName === 'contract_id') {
        return String(v).trim();
      }

      // For everything else: strings, names, or generic numbers
      let str = String(v || '').trim();
      const cleanStr = str.replace(/,/g, '');

      // Generic numbers
      if (cleanStr !== '' && !isNaN(Number(cleanStr))) {
        return String(Number(cleanStr));
      }

      // Generic strings (lowercased for safe matching on things like 'source' or 'char')
      return str.toLowerCase();
    }

    function upsertBy(keys, rows, holdAnesthesia, skipSummary) {
      if (!rows || !rows.length) return { appended: 0, upserted: 0, totalRows: 0, status: "SUCCESS" };

      // 1. SCOPE LOCK: Define everything immediately so nothing goes missing
      const last = sh.getLastRow();
      const keyIndices = keys.map(k => {
        const idx = HEAD_LOWER.indexOf(k.toLowerCase().trim());
        if (idx === -1) throw new Error(`CRITICAL: Key "${k}" not found.`);
        return idx;
      });

      const incomingRows = [];
      const validQtyIdx = HEAD_LOWER.indexOf('qty');
      const validTypeIdIdx = HEAD_LOWER.indexOf('type_id');
      const validDateIdx = HEAD_LOWER.indexOf('date');

      // 2. NORMALIZE
      rows.forEach(obj => {
        const out = normalizeRow_(obj);
        const checkQty = Number(out[validQtyIdx]) || 0;
        const checkTypeId = Number(out[validTypeIdIdx]) || 0;
        const checkDate = out[validDateIdx];

        if (checkTypeId <= 0 || Math.abs(checkQty) === 0 || !(checkDate instanceof Date) || isNaN(checkDate.getTime())) {
          return;
        }

        const k = keyIndices.map(idx => _normalizeKeySegment(out[idx], HEAD_LOWER[idx])).join('|');
        incomingRows.push({ key: k, data: out });
      });

      if (incomingRows.length === 0) return { appended: 0, upserted: 0, totalRows: 0, status: "SUCCESS" };

      const existingKeys = new Set();
      let fullExistingData = [];

      // 3. LOAD EXISTING
      if (last >= 2) {
        fullExistingData = sh.getRange(2, 1, last - 1, HEAD_CURRENT.length).getValues();
        fullExistingData.forEach(row => {
          const k = keyIndices.map(idx => _normalizeKeySegment(row[idx], HEAD_LOWER[idx])).join('|');
          existingKeys.add(k);
        });
      }

      // 4. SQUASH INCOMING AND SEPARATE UPDATES VS APPENDS (The Twin Trap Fix)
      const squashedIncoming = new Map();
      incomingRows.forEach(item => squashedIncoming.set(item.key, item.data));

      const pureAppends = [];
      const updates = new Map();

      squashedIncoming.forEach((data, key) => {
        if (existingKeys.has(key)) {
          updates.set(key, data);
        } else {
          pureAppends.push(data);
        }
      });

      let needsWakeUp = false;
      let finalRowsCount = last - 1;
      const initialUpdatesCount = updates.size; 

      // 5. WRITE
      try {
        if (!holdAnesthesia && typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

        if (initialUpdatesCount === 0 && pureAppends.length > 0) {
          sh.getRange(last + 1, 1, pureAppends.length, HEAD_CURRENT.length).setValues(pureAppends);
          finalRowsCount = (last - 1) + pureAppends.length;
        }
        else if (initialUpdatesCount > 0) {
          const finalData = [];
          fullExistingData.forEach(row => {
            const k = keyIndices.map(idx => _normalizeKeySegment(row[idx], HEAD_LOWER[idx])).join('|');
            if (updates.has(k)) {
              finalData.push(updates.get(k));
              updates.delete(k);
            } else {
              finalData.push(row);
            }
          });

          finalData.push(...pureAppends);
          sh.getRange(2, 1, finalData.length, HEAD_CURRENT.length).setValues(finalData);
          finalRowsCount = finalData.length;
        }

        const rangeName = (sheetName === "Material_Ledger") ? "NR_MATERIAL_LEDGER" : "NR_SALES_LEDGER";
        ss.setNamedRange(rangeName, sh.getRange(1, 1, finalRowsCount + 1, HEAD_CURRENT.length));

        if (typeof GLOBALS !== 'undefined' && GLOBALS.dataCache) GLOBALS.dataCache.delete(rangeName);

        if (pureAppends.length > 0 || initialUpdatesCount > 0) {
          if (!skipSummary) updateBlendedSummary();
        }

        return { appended: pureAppends.length, upserted: initialUpdatesCount, totalRows: finalRowsCount, status: "SUCCESS" };

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
        const dateStr = String(rawDate || "").substring(0, 10);
        const rowDate = (rawDate instanceof Date) ? rawDate : new Date(rawDate || 0);

        if (rowDate < cutoffDate) {
          const k = keys.map(key => {
            const idx = HEAD_LOWER.indexOf(String(key).toLowerCase());
            return _normalizeKeySegment(row[idx], HEAD_LOWER[idx]);
          }).join('|');

          const idxUnitValue = HEAD_LOWER.indexOf('unit_value');
          const actualPrice = Number(row[idxUnitValue]) || Number(row[idxUnitValueFilled]) || 0;

          if (!condensedGroups.has(k)) {
            let newRow = [...row];
            cutoffDate.setHours(0, 0, 0, 0);
            newRow[idxDate] = new Date(cutoffDate.getTime());
            newRow.total_cost = Number(row[idxQty]) * actualPrice;

            if (idxUnitValue !== -1) newRow[idxUnitValue] = '';
            condensedGroups.set(k, newRow);
          } else {
            let group = condensedGroups.get(k);
            const newQty = Number(row[idxQty]) || 0;
            const newCost = newQty * actualPrice;

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

        if (lastRow > 1) sh.getRange(2, 1, lastRow - 1, HEAD_CURRENT.length).clearContent();

        if (finalRows.length > 0) {
          sh.getRange(2, 1, finalRows.length, HEAD_CURRENT.length).setValues(finalRows);
          
          // --- THE BLOAT GENERATOR FIX ---
          const rowsDiff = (lastRow - 1) - finalRows.length;
          if (rowsDiff > 0) {
            sh.deleteRows(finalRows.length + 2, rowsDiff);
          }
          
          GLOBALS.dataCache.delete(rangeName);
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

      const contractIdx = HEAD_LOWER.indexOf('contract_id');
      const existingMap = new Map();
      const last = sh.getLastRow();

      if (last < 2) return { removed: 0, status: "NO_DATA" };

      const data = sh.getRange(2, 1, last - 1, HEAD_CURRENT.length).getValues();
      const originalCount = data.length;

      data.forEach((row) => {
        let k;
        // THE FIX: If a contract_id exists and is valid, use it as the absolute unique key
        if (contractIdx !== -1 && row[contractIdx] && String(row[contractIdx]).trim() !== '') {
          k = `CID|${String(row[contractIdx]).trim()}`;
        } else {
          // Fallback to standard compound key for non-contract entries
          k = keyIndices.map(idx => _normalizeKeySegment(row[idx], HEAD_LOWER[idx])).join('|');
        }
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
        
        const rowsDiff = (last - 1) - allValues.length;
        if (rowsDiff > 0) {
          sh.deleteRows(allValues.length + 2, rowsDiff);
        }

        const rangeName = (sheetName === "Material_Ledger") ? "NR_MATERIAL_LEDGER" : "NR_SALES_LEDGER";
        ss.setNamedRange(rangeName, sh.getRange(1, 1, allValues.length + 1, HEAD_CURRENT.length));

        if (typeof GLOBALS !== 'undefined' && GLOBALS.dataCache) {
          GLOBALS.dataCache.delete(rangeName);
        }

        updateBlendedSummary();

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
        const id = Number(row.type_id);
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
        obj.unit_weighted_average = obj.total_qty !== 0 ? Number((obj.total_sum / obj.total_qty).toFixed(6)) : 0;
        const rowArray = columnsToSave.map(col => obj[col] !== undefined ? obj[col] : '');
        outputData.push(rowArray);
      });

      if (outputData.length === 0) return;

      // 3. Clear previous content starting at Row 2 (protecting your static sheet headers)
      const sumLast = summarySheet.getLastRow();
      if (sumLast > 1) {
        summarySheet.getRange(2, 1, sumLast - 1, columnsToSave.length).clearContent();
      }

      // 4. Write pure data starting at Row 2
      summarySheet.getRange(2, 1, outputData.length, columnsToSave.length).setValues(outputData);

      // --- THE BLOAT GENERATOR FIX ---
      const rowsDiff = (sumLast - 1) - outputData.length;
      if (rowsDiff > 0) {
        summarySheet.deleteRows(outputData.length + 2, rowsDiff);
      }

      // 5. Update the named range to span the entire block INCLUDING the static headers on Row 1
      ss.setNamedRange(targetRangeName, summarySheet.getRange(1, 1, outputData.length + 1, columnsToSave.length));

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

function retroactivelyNormalizeLedgers() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheetsToFix = ["Material_Ledger", "Sales_Ledger"];

  sheetsToFix.forEach(sheetName => {
    const sh = ss.getSheetByName(sheetName);

    if (!sh) {
      console.error(`CRITICAL: Could not find a sheet named "${sheetName}". Skipping.`);
      return;
    }

    const lastRow = sh.getLastRow();
    const lastCol = sh.getLastColumn();

    if (lastRow < 2) {
      console.warn(`No data found in ${sheetName}.`);
      return;
    }

    console.log(`Starting normalization for ${sheetName}... processing ${lastRow - 1} rows.`);

    const headers = sh.getRange(1, 1, 1, lastCol).getValues()[0].map(h => String(h).toLowerCase().trim());
    const data = sh.getRange(2, 1, lastRow - 1, lastCol).getValues();

    const dateIdx = headers.indexOf('date');

    const numericIndices = headers.reduce((acc, header, idx) => {
      if (['type_id', 'qty', 'unit_value', 'price', 'cost', 'tax', 'vol'].some(kw => header.includes(kw))) {
        acc.push(idx);
      }
      return acc;
    }, []);

    const fixedData = data.map(row => {
      if (dateIdx !== -1 && typeof row[dateIdx] === 'string') {
        const dStr = row[dateIdx].trim();
        const mangledMatch = dStr.match(/^(\d{4}-\d{2}-\d{2}):(\d{2})(\d{2})(\d{2})$/);

        if (mangledMatch) {
          const cleanIso = `${mangledMatch[1]}T${mangledMatch[2]}:${mangledMatch[3]}:${mangledMatch[4]}`;
          const parsedDt = new Date(cleanIso);
          if (!isNaN(parsedDt.getTime())) row[dateIdx] = parsedDt;
        } else {
          const parsedDt = new Date(dStr);
          if (!isNaN(parsedDt.getTime())) row[dateIdx] = parsedDt;
        }
      }

      numericIndices.forEach(idx => {
        let val = row[idx];
        if (val !== "" && val !== null) {
          let cleanNum = Number(String(val).replace(/,/g, '').trim());
          if (!isNaN(cleanNum)) {
            row[idx] = cleanNum;
          }
        }
      });

      return row;
    });

    sh.getRange(2, 1, lastRow - 1, lastCol).setValues(fixedData);
    console.log(`Success! Normalized ${lastRow - 1} rows in ${sheetName}.`);
  });
}