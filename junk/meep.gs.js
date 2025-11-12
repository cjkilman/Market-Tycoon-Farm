/**
 * Performs an Atomic Swap (delete old, rename new) using a non-blocking TryLock.
 * If the lock is busy, the operation is skipped immediately.
 * * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss The Spreadsheet object.
 * @param {string} oldSheetName The name of the sheet to be DELETED and REPLACED (e.g., 'LocationManager').
 * @param {string} newSheetName The name of the sheet containing the NEW data (e.g., 'LocationManager_Temp').
 * @returns {Object} Status Object: {success: boolean, duration: number, errorMessage: string}
 */
function atomicRename(ss, oldSheetName, newSheetName) {
    const log = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('AtomicRename') : console);
    const startTime = new Date().getTime(); 
    if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();

    let swapSuccess = false;
    let errorMessage = "";
    const docLock = LockService.getDocumentLock();

    // 1. Attempt to acquire TryLock (wait up to 5 seconds to acquire, then give up)
    if (!docLock.tryLock(5000)) { 
        errorMessage = `Lock conflict: Could not acquire Document Lock within 5000ms. Atomic rename skipped.`;
        log.warn(errorMessage);
        return {
            success: false, 
            duration: new Date().getTime() - startTime,
            errorMessage: errorMessage
        };
    }

    try {
        // 2. Lock Acquired: Execute Critical Swap
        const oldSheet = ss.getSheetByName(oldSheetName);
        const newSheet = ss.getSheetByName(newSheetName);

        if (!newSheet) {
            errorMessage = `CRITICAL SWAP FAILED: New sheet '${newSheetName}' not found.`;
            log.error(errorMessage);
            throw new Error("New sheet for swap is missing.");
        }

        // 2a. Delete the old sheet (The long I/O operation)
        if (oldSheet) {
            ss.deleteSheet(oldSheet);
            log.info(`Deleted old sheet: ${oldSheetName}`);
        }

        // 2b. Rename the new sheet
        newSheet.setName(oldSheetName);
        log.info(`SUCCESS: Sheet '${newSheetName}' renamed to '${oldSheetName}'.`);
        swapSuccess = true;

    } catch (e) {
        // Catch errors during the rename/delete process
        if (!errorMessage) { 
            errorMessage = `CRITICAL SWAP FAILED. Error: ${e.message}`;
            log.error(errorMessage);
        }
    } finally {
        // 3. Release Lock (Guaranteed to be held if execution reached this block)
        docLock.releaseLock();
    }
    
    // 4. Flush (outside lock and only on success)
    if (swapSuccess) {
        SpreadsheetApp.flush(); 
    }
    
    const duration = new Date().getTime() - startTime;

    // 5. Final standardized return structure
    return {
        success: swapSuccess, 
        duration: duration, 
        errorMessage: errorMessage
    };
}