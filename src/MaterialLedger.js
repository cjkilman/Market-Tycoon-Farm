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

        /**
      * Updates existing rows by key (e.g., [type_id, contract_id]) or appends new rows.
      * @param {Array<string>} keys - Array of column names to use as the unique key (e.g., ['type_id', 'contract_id']).
      * @param {Array<Object>} rows - Array of row objects containing new data.
      * @returns {number} The total count of rows updated or appended.
      */
        function upsertBy(keys, rows) {
            if (!rows || !rows.length) {
                return { rows: 0, status: "SUCCESS", errorMerssage: "" };
            }

            const WRITE_BATCH_SIZE = 1000;
            const sh = sheetInst;
            let updateCount = 0;

            // --- Access Globals (Required for Pause/Resume) ---
            const ss = sh.getParent();

            // --- 1. Identify Key Columns & Read Existing Data (omitted for brevity) ---
            // ... (Reading, mapping, and updateCount logic for Steps 1-3 here) ...

            // --- NEW: SHEET CONTROL WRAPPER ---
            let needsWakeUp = false;

            try {
                // 1. CRITICAL: Pause sheet calculations right before writing
                if (typeof pauseSheet === 'function') {
                    // Only pause if there is actual data writing work to be done.
                    needsWakeUp = pauseSheet(ss);
                }

                // --- 4. Write Updates (Overwrite existing range with modified values) ---
                if (updateCount > 0) {
                    // Write the updates to the existing rows range
                    sh.getRange(2, 1, allExistingValues.length, HEAD.length).setValues(allExistingValues);
                }

                // --- 5. Batch Write New Rows (Appends) ---
                const totalNewRows = newRowsToAppend.length;
                let currentIndex = 0;
                let nextWriteRow = sh.getLastRow() + 1;
                let totalWritten = 0;

                if (totalNewRows > 0) {
                    const docLock = LockService.getDocumentLock();
                    let lockAcquired = false;

                    try {
                        // CRITICAL FIX: CHECK FOR PRIORITY INTERRUPT ONCE BEFORE THE LOOP
                        if (!docLock.tryLock(0)) {
                            // Lock not acquired (Signal is SET): Predictive Bailout
                            return {
                                rows: updateCount, // Only counting the updates done so far
                                status: "PREDICTIVE BAILOUT",
                                errorMerssage: "Document Lock held by priority process."
                            };
                        }

                        // Lock acquired (Signal is CLEAR): Flag it for release and proceed
                        lockAcquired = true;

                        // Start the actual writing loop (NO MORE LOCK CHECKS INSIDE)
                        while (currentIndex < totalNewRows) {

                            const batch = newRowsToAppend.slice(currentIndex, currentIndex + WRITE_BATCH_SIZE);
                            const startRow = nextWriteRow;

                            // Perform the batch write
                            sh.getRange(startRow, 1, batch.length, HEAD.length).setValues(batch);

                            currentIndex += batch.length;
                            nextWriteRow += batch.length;
                            totalWritten += batch.length;
                        }

                    } catch (e) {
                        // Return an error object if a write fails
                        return {
                            rows: totalWritten + updateCount,
                            status: "WRITE ERROR",
                            errorMerssage: e.message
                        };
                    } finally {
                        // CRITICAL: Release the Document Lock ONLY if it was acquired.
                        if (lockAcquired) {
                            docLock.releaseLock();
                        }
                    }
                }

                // --- FINAL SUCCESS RETURN ---
                return {
                    rows: updateCount + totalWritten,
                    status: "SUCCESS",
                    errorMerssage: ""
                };

            } catch (outerError) {
                // Catches errors during the update/append/pause phase
                LoggerEx.withTag('ML_UPSERT').error('Fatal error during upsert process:', outerError);
                return {
                    rows: updateCount + totalWritten,
                    status: "FATAL_ERROR",
                    errorMerssage: outerError.message
                };
            } finally {
                // 2. CRITICAL: Resume sheet calculations, GUARANTEED
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
