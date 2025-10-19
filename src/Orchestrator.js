/**
 * Pushes cached data to the Market_Data_Raw sheet using a dynamic loop and batched writes.
 * FINAL DYNAMIC BATCHED-WRITE VERSION: This script processes the maximum number of chunks
 * in a single run and writes the collected data in safe batches for ultimate speed and reliability.
 */
function updateMarketDataSheet() {
    const log = LoggerEx.withTag('DATA_PUSHER_DYNAMIC_BATCH');
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const props = PropertiesService.getScriptProperties();
    const startTime = new Date();

    const CONTROL_SHEET_NAME = 'Market_Control';
    const LIVE_SHEET_NAME = 'Market_Data_Raw';
    const TEMP_SHEET_NAME = 'Market_Data_Temp';
    const OLD_SHEET_NAME = 'Market_Data_Old';
    const RESUME_KEY = 'marketDataPush_lastRowProcessed';
    const TIMESTAMP_KEY = 'marketDataPush_cycleTimestamp';
    const CHUNK_SIZE = 2000; // The number of rows to process in each loop iteration.
    const WRITE_BATCH_SIZE = 5000; // The number of rows to write to the sheet at a time.
    const CALC_CONTROL_RANGE = 'Utility!B3:C3';
    const MAX_EXECUTION_TIME_SECONDS = 270; // 5-minute safety limit.

    const calcControl = ss.getRange(CALC_CONTROL_RANGE);
    const originalCalcState = calcControl.getValues();
    calcControl.setValues([[0, 0]]);

    try {
        log.info('Calculation pause signal sent. Starting data push cycle...');

        const controlSheet = ss.getSheetByName(CONTROL_SHEET_NAME);
        if (!controlSheet) throw new Error(`Missing sheet: '${CONTROL_SHEET_NAME}'`);

        let startRow = parseInt(props.getProperty(RESUME_KEY) || '2');
        if (startRow <= 1) startRow = 2;
        const lastControlRow = controlSheet.getLastRow();

        if (startRow > lastControlRow) {
            log.info('All rows already processed. Resetting.');
            props.deleteProperty(RESUME_KEY);
            props.deleteProperty(TIMESTAMP_KEY);
            return;
        }

        let tempSheet = ss.getSheetByName(TEMP_SHEET_NAME);
        let updateTimestamp;

        if (startRow === 2) {
            log.info('First run of cycle. Preparing temp sheet and saving timestamp.');
            if (tempSheet) tempSheet.clear();
            else tempSheet = ss.insertSheet(TEMP_SHEET_NAME);
            
            tempSheet.hideSheet();
            const headers = [['cacheKey', 'type_id', 'location_type', 'location_id', 'sell_min', 'buy_max', 'sell_volume', 'buy_volume', 'last_updated']];
            tempSheet.getRange(1, 1, 1, headers[0].length).setValues(headers);

            updateTimestamp = new Date();
            props.setProperty(TIMESTAMP_KEY, updateTimestamp.getTime().toString());
        } else {
            tempSheet = ss.getSheetByName(TEMP_SHEET_NAME);
            if (!tempSheet) {
                log.warn('Temporary sheet was missing. Resetting.');
                props.deleteProperty(RESUME_KEY);
                props.deleteProperty(TIMESTAMP_KEY);
                return;
            }
            const savedTimestamp = props.getProperty(TIMESTAMP_KEY);
            updateTimestamp = savedTimestamp ? new Date(parseInt(savedTimestamp)) : new Date();
        }

        const allOutputRowsForThisRun = [];

        // --- DYNAMIC PROCESSING LOOP ---
        while ((new Date() - startTime) / 1000 < MAX_EXECUTION_TIME_SECONDS) {
            if (startRow > lastControlRow) break;

            const numRowsToProcess = Math.min(CHUNK_SIZE, lastControlRow - startRow + 1);
            const controlData = controlSheet.getRange(startRow, 1, numRowsToProcess, 3).getValues();

            const allCacheKeysInChunk = controlData.map(row => _fuzKey(String(row[1]).trim().toLowerCase(), Number(row[2]), Number(row[0]))).filter(Boolean);
            
            // Re-integrate the queuing logic for missing items
            const cacheCheckResults = cacheScope().getAll(allCacheKeysInChunk);
            const requestsToQueue = {};
            allCacheKeysInChunk.forEach(key => {
                if (!cacheCheckResults.hasOwnProperty(key)) {
                    const parts = key.split(':');
                    const locKey = `${parts[2]}:${parts[3]}`;
                    if (!requestsToQueue[locKey]) {
                        requestsToQueue[locKey] = { location_type: parts[2], location_id: Number(parts[3]), ids: new Set() };
                    }
                    requestsToQueue[locKey].ids.add(Number(parts[4]));
                }
            });
            for (const key in requestsToQueue) {
                _queueMissingItems(Array.from(requestsToQueue[key].ids), requestsToQueue[key].location_id, requestsToQueue[key].location_type);
            }

            const allCachedData = cacheScope().getAll(allCacheKeysInChunk);
            
            for (const cacheKey of allCacheKeysInChunk) {
                const parts = cacheKey.split(':');
                const req = { type_id: Number(parts[4]), location_type: parts[2], location_id: Number(parts[3]) };
                const rawValue = allCachedData[cacheKey];
                let data = null;
                if (rawValue && rawValue !== 'null') try { data = JSON.parse(rawValue); } catch (e) {}

                allOutputRowsForThisRun.push([
                    cacheKey, req.type_id, req.location_type, req.location_id,
                    (data?.sell?.min > 0) ? data.sell.min : '', (data?.buy?.max > 0) ? data.buy.max : '',
                    (data?.sell?.volume != null) ? data.sell.volume : '', (data?.buy?.volume != null) ? data.buy.volume : '',
                    updateTimestamp
                ]);
            }
            startRow += numRowsToProcess;
        }

        // --- BATCHED WRITE OPERATION ---
        if (allOutputRowsForThisRun.length > 0) {
            log.info(`Processing complete for this run. Writing ${allOutputRowsForThisRun.length} rows in batches...`);
            for (let i = 0; i < allOutputRowsForThisRun.length; i += WRITE_BATCH_SIZE) {
                const batch = allOutputRowsForThisRun.slice(i, i + WRITE_BATCH_SIZE);
                tempSheet.getRange(tempSheet.getLastRow() + 1, 1, batch.length, batch[0].length).setValues(batch);
                log.info(`Appended batch of ${batch.length} rows to temporary sheet.`);
            }
        }

        if (startRow > lastControlRow) {
            log.info('Final chunk processed. Swapping sheets now.');
            withSheetLock(function() {
                const oldSheet = ss.getSheetByName(LIVE_SHEET_NAME);
                if (oldSheet) oldSheet.setName(OLD_SHEET_NAME);
                tempSheet.setName(LIVE_SHEET_NAME);
                tempSheet.showSheet();
                const backupSheet = ss.getSheetByName(OLD_SHEET_NAME);
                if (backupSheet) ss.deleteSheet(backupSheet);
            });
            props.deleteProperty(RESUME_KEY);
            props.deleteProperty(TIMESTAMP_KEY);
            log.info('Full data push cycle complete.');
        } else {
            props.setProperty(RESUME_KEY, startRow.toString());
            log.info(`Execution time limit reached. Next run will start at row ${startRow}.`);
        }

    } catch (e) {
        log.error("An error occurred during the update cycle:", e.stack);
        props.deleteProperty(RESUME_KEY);
        props.deleteProperty(TIMESTAMP_KEY);
    } finally {
        calcControl.setValues(originalCalcState);

        log.info('Spreadsheet calculations have been resumed.');
    }
}