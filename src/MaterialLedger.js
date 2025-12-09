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
    var HEAD = ['date', 'type_id', 'item_name', 'qty', 'unit_value', 'source', 'contract_id', 'char', 'unit_value_filled'];

    // Get logger instance - Ensure LoggerEx is available in your project
    var LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('ML_LEDGER') : console;
    var ss;

    // A helper function to get the spreadsheet object.
    function getSS_() {
        if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
        return ss;
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

        // --- Date Parsing (FIXED) ---
        let dateVal = r.date;

        // 1. Attempt to parse date only if it's NOT already a valid Date object.
        if (!(dateVal instanceof Date)) {
            // Use PT_API safe parsing if available, otherwise rely on new Date()
            dateVal = PT_API ? PT_API.parseDateSafe(dateVal) : new Date(dateVal);
        }

        // 2. Validate the resulting date object. If it's not a valid date, 
        // default to today's date.
        let validDate = (dateVal instanceof Date) && !isNaN(dateVal);
        let dateToFormat = validDate ? dateVal : (PT_API ? PT_API.now() : new Date());

        // 3. Format the valid date object.
        out.date = PT_API
            ? PT_API.yyyymmdd(dateToFormat)
            : Utilities.formatDate(dateToFormat, Session.getScriptTimeZone(), "yyyy-MM-dd");

        // --- Other Fields (Retaining previous fixes for numeric types) ---
        out.type_id = r.type_id;
        out.item_name = r.item_name || '';

        // Numeric fields
        out.qty = Number(String(r.qty).replace(/,/g, '')) || 0;

        var u0 = +r.unit_value || 0; // Manual override (Number: 0 or >0)
        var u1 = +r.unit_value_filled || 0; // Calculated value (Number: 0 or >0)

        out.unit_value = u0 > 0 ? u0 : '';

        out.source = r.source || '';
        out.contract_id = r.contract_id || '';
        out.char = r.char || '';

        var finalUnitValue = u0 > 0 ? u0 : (u1 > 0 ? u1 : 0);
        out.unit_value_filled = finalUnitValue > 0 ? finalUnitValue : '';

        // --- Final Mapping ---
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

        function upsertBy(keys, rows) {
            if (!rows || !rows.length) {
                return { rows: 0, status: "SUCCESS", errorMerssage: "" };
            }

            const WRITE_BATCH_SIZE = 1000;
            const sh = sheetInst;
            const ss = sh.getParent();

            // --- INITIALIZE SCOPE VARIABLES ---
            let updateCount = 0;
            let totalWritten = 0;
            let needsWakeUp = false;

            const existingKeyToArrayIndexMap = new Map();
            let allExistingValues = [];
            const newRowsToAppend = [];

            // --- 1. Identify Key Columns ---
            const keyIndices = keys.map(function (k) {
                const idx = HEAD.indexOf(k);
                if (idx === -1) throw new Error('Unknown key column in HEAD: ' + k);
                return idx;
            });

            const normalizeKeyPart = function (val, headIdx) {
                if (headIdx === 1) { // Index 1 is 'type_id'
                    return String(Math.round(Number(val || 0)));
                }
                return String(val != null ? val : '');
            };

            // --- 2. Read Existing Keys ---
            const last = sh.getLastRow();
            if (last >= 2) {
                const range = sh.getRange(2, 1, last - 1, HEAD.length);
                allExistingValues = range.getValues();

                allExistingValues.forEach(function (row, rowIndex) {
                    const key = keyIndices.map(function (headIdx) {
                        return normalizeKeyPart(row[headIdx], headIdx);
                    }).join('\u0001');
                    existingKeyToArrayIndexMap.set(key, rowIndex);
                });
            }

            // --- 3. Separate Rows ---
            rows.forEach(function (obj) {
                const outRow = normalizeRow_(obj);
                const key = keyIndices.map(function (headIdx) {
                    return normalizeKeyPart(outRow[headIdx], headIdx);
                }).join('\u0001');

                if (existingKeyToArrayIndexMap.has(key)) {
                    const rowIndex = existingKeyToArrayIndexMap.get(key);
                    allExistingValues[rowIndex] = outRow;
                    updateCount++;
                } else {
                    newRowsToAppend.push(outRow);
                }
            });

            // --- SHEET CONTROL & WRITE OPERATIONS ---
            try {
                if (typeof pauseSheet === 'function') {
                    needsWakeUp = pauseSheet(ss);
                }

                // --- 4. Write Updates ---
                if (updateCount > 0) {
                    sh.getRange(2, 1, allExistingValues.length, HEAD.length).setValues(allExistingValues);
                }

                // --- 5. Batch Write New Rows (Appends) ---
                const totalNewRows = newRowsToAppend.length;
                let currentIndex = 0;
                let nextWriteRow = sh.getLastRow() + 1;

                if (totalNewRows > 0) {
                    // *** CRITICAL FIX: CHECK & INSERT ROWS ***
                    const maxRows = sh.getMaxRows();
                    const requiredRows = nextWriteRow + totalNewRows - 1;
                    if (requiredRows > maxRows) {
                        // Add exactly what we need plus a buffer of 50
                        sh.insertRowsAfter(maxRows, (requiredRows - maxRows) + 50);
                    }
                    // *****************************************

                    const docLock = LockService.getDocumentLock();
                    let lockAcquired = false;

                    try {
                        if (!docLock.tryLock(0)) {
                            return { rows: updateCount, status: "PREDICTIVE BAILOUT", errorMerssage: "Document Lock held by priority process." };
                        }
                        lockAcquired = true;

                        while (currentIndex < totalNewRows) {
                            const batch = newRowsToAppend.slice(currentIndex, currentIndex + WRITE_BATCH_SIZE);
                            sh.getRange(nextWriteRow, 1, batch.length, HEAD.length).setValues(batch);
                            currentIndex += batch.length;
                            nextWriteRow += batch.length;
                            totalWritten += batch.length;
                        }
                    } catch (e) {
                        return { rows: updateCount + totalWritten, status: "WRITE ERROR", errorMerssage: e.message };
                    } finally {
                        if (lockAcquired) docLock.releaseLock();
                    }
                }

                return { rows: updateCount + totalWritten, status: "SUCCESS", errorMerssage: "" };

            } catch (outerError) {
                if (typeof LoggerEx !== 'undefined') LoggerEx.withTag('ML_UPSERT').error('Fatal error during upsert process:', outerError);
                return { rows: updateCount + totalWritten, status: "FATAL_ERROR", errorMerssage: outerError.message };
            } finally {
                if (needsWakeUp && typeof wakeUpSheet === 'function') {
                    wakeUpSheet(ss);
                }
            }
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
