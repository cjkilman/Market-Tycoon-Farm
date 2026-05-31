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

/**
 * Market Ledger (ML) Manager.
 * Handles the ingestion, merging, and historical compression of high-volume market data.
 * Prevents Apps Script timeouts by utilizing bulk array operations and selective pruning.
 * 
 * 
 * * @namespace ML
 */
var ML = (function () {
    var HEAD = ['date', 'type_id', 'item_name', 'qty', 'unit_value', 'source', 'contract_id', 'char', 'unit_value_filled'];
    var LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ML_LEDGER') : console;

    function getSS_() { return SpreadsheetApp.getActiveSpreadsheet(); }

    function normalizeRow_(r) {
        var out = {};
        const PT_API = (typeof PT !== 'undefined' && PT.yyyymmdd) ? PT : null;
        let d = r.date;
        if (!(d instanceof Date)) d = PT_API ? PT_API.parseDateSafe(d) : new Date(d);
        let valid = (d instanceof Date) && !isNaN(d);
        let dt = valid ? d : new Date();
        out.date = PT_API ? PT_API.yyyymmdd(dt) : Utilities.formatDate(dt, Session.getScriptTimeZone(), "yyyy-MM-dd");
        out.type_id = r.type_id;
        out.item_name = r.item_name || '';
        out.qty = Number(String(r.qty).replace(/,/g, '')) || 0;
        var u0 = +r.unit_value || 0;
        var u1 = +r.unit_value_filled || 0;
        out.unit_value = u0 > 0 ? u0 : '';
        out.source = r.source || '';
        out.contract_id = r.contract_id || '';
        out.char = r.char || '';
        var finalVal = u0 > 0 ? u0 : (u1 > 0 ? u1 : 0);
        out.unit_value_filled = finalVal > 0 ? finalVal : '';
        return HEAD.map(k => (out[k] == null ? '' : out[k]));
    }



    function forSheet(sheetName) {
        var sh = getSS_().getSheetByName(sheetName);
        if (!sh) sh = getOrCreateSheet(getSS_(), sheetName, HEAD);

        function upsertBy(keys, rows, holdAnesthesia) {
            if (!rows || !rows.length) return { rows: 0, status: "SUCCESS" };
            let updateCount = 0, totalWritten = 0, needsWakeUp = false;
            const ss = getSS_(), existingKeys = new Map();
            // Validate headers and halt if any key is missing
            const keyIndices = keys.map(k => {
                const cleanHead = HEAD.map(h => String(h).trim());
                const idx = cleanHead.indexOf(k.trim());

                if (idx === -1) {
                    throw new Error(`CRITICAL: Key "${k}" not found in sheet "${sheetName}".`);
                }
                return idx;
            });
            const normalizeK = (v, i) => (i === 1) ? String(Math.round(Number(v || 0))) : String(v || '');

            const last = sh.getLastRow();
            if (last >= 2) {
                const data = sh.getRange(2, 1, last - 1, HEAD.length).getValues();
                data.forEach((row, i) => existingKeys.set(keyIndices.map(idx => normalizeK(row[idx], idx)).join('|'), i));
                var allValues = data;
            } else { var allValues = []; }

            rows.forEach(obj => {
                const out = normalizeRow_(obj);
                const k = keyIndices.map(idx => normalizeK(out[idx], idx)).join('|');
                if (existingKeys.has(k)) { allValues[existingKeys.get(k)] = out; updateCount++; }
                else { allValues.push(out); totalWritten++; }
            });

            try {
                // HOLD ANESTHESIA FLAG RESPECTED
                if (!holdAnesthesia && typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);
                
                // Full Write/Overwrite for atomicity and cleaning
                sh.getRange(2, 1, allValues.length, HEAD.length).setValues(allValues);

                // 1. SNAP THE LEDGER RANGE
                const rangeName = (sheetName === "Material_Ledger") ? "NR_MATERIAL_LEDGER" : "NR_SALES_LEDGER";
                ss.setNamedRange(rangeName, sh.getRange(1, 1, allValues.length + 1, HEAD.length));

                // 2. CRUNCH BLENDED SUMMARIES
                const totals = {};
                allValues.forEach(row => {
                    const qty = Math.abs(Number(row[3])), tid = row[1];
                    const price = (+row[4] > 0) ? +row[4] : (+row[8] || 0);
                    if (tid && qty > 0 && price > 0) {
                        if (!totals[tid]) totals[tid] = { i: 0, q: 0 };
                        totals[tid].i += (qty * price); totals[tid].q += qty;
                    }
                });

                const blendedPriceSummary = [["type_id", "total_sum", "unit_weighted_average"]];
                Object.keys(totals).forEach(id => blendedPriceSummary.push([id, totals[id].i, Math.round((totals[id].i / totals[id].q) * 100) / 100]));

                const tName = (sheetName === "Material_Ledger") ? "Blended_Cost" : "Blended_Sales";
                const tSh = ss.getSheetByName(tName);
                if (tSh) {
                    tSh.clearContents();
                    tSh.getRange(1, 1, blendedPriceSummary.length, 3).setValues(blendedPriceSummary);
                    // 3. SNAP THE BLENDED RANGE
                    const bName = (tName === "Blended_Cost") ? "NR_BLENDED_COST" : "NR_BLENDED_SALES";
                    ss.setNamedRange(bName, tSh.getRange(1, 1, blendedPriceSummary.length, 3));
                    if (tSh.getMaxRows() > blendedPriceSummary.length) tSh.deleteRows(blendedPriceSummary.length + 1, tSh.getMaxRows() - blendedPriceSummary.length);
                }

                return { rows: updateCount + totalWritten, status: "SUCCESS" };

            } finally { 
                // HOLD ANESTHESIA FLAG RESPECTED
                if (!holdAnesthesia && needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss); 
            }
        }

        function condenseHistory(cutoffDays, keys, holdAnesthesia) {
            cutoffDays = cutoffDays || 7;
            keys = keys || ['type_id', 'source', 'char'];

            const lastRow = sh.getLastRow();
            if (lastRow < 2) return { rows: 0, status: "SUCCESS" };

            let needsWakeUp = false;
            const ss = getSS_();

            const cutoffDate = new Date();
            cutoffDate.setDate(cutoffDate.getDate() - cutoffDays);
            const cutoffStr = Utilities.formatDate(cutoffDate, Session.getScriptTimeZone(), "yyyy-MM-dd");

            const cleanHead = HEAD.map(h => String(h).trim());
            const keyIndices = keys.map(k => {
                const idx = cleanHead.indexOf(k.trim());
                if (idx === -1) throw new Error(`CRITICAL: Key "${k}" not found in sheet "${sheetName}".`);
                return idx;
            });

            const data = sh.getRange(2, 1, lastRow - 1, HEAD.length).getValues();
            const recentRows = [];
            const historicalMap = {};

            data.forEach(row => {
                if (!row[0]) return;

                const rDateStr = (row[0] instanceof Date)
                    ? Utilities.formatDate(row[0], Session.getScriptTimeZone(), "yyyy-MM-dd")
                    : String(row[0]);

                if (rDateStr >= cutoffStr) {
                    recentRows.push(row);
                } else {
                    let k = rDateStr;
                    keyIndices.forEach(idx => {
                        k += "|" + String(row[idx] || '');
                    });

                    const qty = Number(row[3]) || 0;
                    const price = Number(row[4]) > 0 ? Number(row[4]) : (Number(row[8]) || 0);

                    if (!historicalMap[k]) {
                        historicalMap[k] = {
                            rowTemplate: row.slice(),
                            totalQty: 0,
                            totalValue: 0
                        };
                        historicalMap[k].rowTemplate[0] = rDateStr;
                    }

                    historicalMap[k].totalQty += qty;
                    historicalMap[k].totalValue += (qty * price);
                }
            });

            const compressedRows = [];
            for (let key in historicalMap) {
                const group = historicalMap[key];
                if (group.totalQty !== 0) {
                    const wAvgPrice = group.totalValue / group.totalQty;
                    const cRow = group.rowTemplate;

                    cRow[3] = group.totalQty;
                    cRow[4] = Math.round(wAvgPrice * 100) / 100;
                    cRow[8] = "";

                    compressedRows.push(cRow);
                }
            }

            const finalValues = compressedRows.concat(recentRows);
            finalValues.sort((a, b) => {
                const dateA = (a[0] instanceof Date) ? Utilities.formatDate(a[0], Session.getScriptTimeZone(), "yyyy-MM-dd") : String(a[0]);
                const dateB = (b[0] instanceof Date) ? Utilities.formatDate(b[0], Session.getScriptTimeZone(), "yyyy-MM-dd") : String(b[0]);
                return dateA > dateB ? 1 : (dateA < dateB ? -1 : 0);
            });

            try {
                if (!holdAnesthesia && typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

                // 1. Clear the entire data range first to remove any "hidden" ghost rows
                sh.getRange(2, 1, sh.getMaxRows() - 1, HEAD.length).clearContent();

                // 2. Write the condensed ledger
                sh.getRange(2, 1, finalValues.length, HEAD.length).setValues(finalValues);

                // 3. Force-trim everything below the new data size
                var lastRowUsed = finalValues.length + 1;
                var totalRows = sh.getMaxRows();
                if (totalRows > lastRowUsed) {
                    sh.deleteRows(lastRowUsed + 1, totalRows - lastRowUsed);
                }
                const rangeName = (sheetName === "Material_Ledger") ? "NR_MATERIAL_LEDGER" : "NR_SALES_LEDGER";
                ss.setNamedRange(rangeName, sh.getRange(1, 1, finalValues.length + 1, HEAD.length));

                // --- CRUNCH BLENDED SUMMARIES (UPDATED) ---
                const totals = {};
                finalValues.forEach(row => {
                    const qty = Math.abs(Number(row[3])), tid = row[1];
                    const price = (+row[4] > 0) ? +row[4] : (+row[8] || 0);
                    if (tid && qty > 0 && price > 0) {
                        if (!totals[tid]) totals[tid] = { i: 0, q: 0 };
                        totals[tid].i += (qty * price); totals[tid].q += qty;
                    }
                });

                const blendedPriceSummary = [["type_id", "total_sum", "unit_weighted_average"]];
                Object.keys(totals).forEach(id => blendedPriceSummary.push([id, totals[id].i, Math.round((totals[id].i / totals[id].q) * 100) / 100]));

                const tName = (sheetName === "Material_Ledger") ? "Blended_Cost" : "Blended_Sales";
                const tSh = ss.getSheetByName(tName);
                if (tSh) {
                    tSh.clearContents();
                    tSh.getRange(1, 1, blendedPriceSummary.length, 3).setValues(blendedPriceSummary);
                    const bName = (tName === "Blended_Cost") ? "NR_BLENDED_COST" : "NR_BLENDED_SALES";
                    ss.setNamedRange(bName, tSh.getRange(1, 1, blendedPriceSummary.length, 3));
                    if (tSh.getMaxRows() > blendedPriceSummary.length) tSh.deleteRows(blendedPriceSummary.length + 1, tSh.getMaxRows() - blendedPriceSummary.length);
                }

                return { rows: finalValues.length, status: "SUCCESS" };
            } finally {
                if (!holdAnesthesia && needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss);
            }
        }

        return { upsert: upsertBy, condenseHistory: condenseHistory, sheetName: sheetName };
    }
    return { forSheet: forSheet };
})();