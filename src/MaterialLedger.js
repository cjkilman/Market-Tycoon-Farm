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

        function upsertBy(keys, rows) {
            if (!rows || !rows.length) return { rows: 0, status: "SUCCESS" };
            let updateCount = 0, totalWritten = 0, needsWakeUp = false;
            const ss = getSS_(), existingKeys = new Map();
            const keyIndices = keys.map(k => HEAD.indexOf(k));
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
                if (typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);
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

                const summary = [["type_id", "total_sum", "unit_weighted_average"]];
                Object.keys(totals).forEach(id => summary.push([id, totals[id].i, Math.round((totals[id].i / totals[id].q) * 100) / 100]));

                const tName = (sheetName === "Material_Ledger") ? "Blended_Cost" : "Blended_Sales";
                const tSh = ss.getSheetByName(tName);
                if (tSh) {
                    tSh.clearContents();
                    tSh.getRange(1, 1, summary.length, 3).setValues(summary);
                    // 3. SNAP THE BLENDED RANGE
                    const bName = (tName === "Blended_Cost") ? "NR_BLENDED_COST" : "NR_BLENDED_SALES";
                    ss.setNamedRange(bName, tSh.getRange(1, 1, summary.length, 3));
                    if (tSh.getMaxRows() > summary.length) tSh.deleteRows(summary.length + 1, tSh.getMaxRows() - summary.length);
                }


                return { rows: updateCount + totalWritten, status: "SUCCESS" };
            } finally { if (needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss); }
        }

        return { upsert: upsertBy, sheetName: sheetName };
    }
    return { forSheet: forSheet };
})();