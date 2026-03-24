// Parked Sinpiiets

/* Inside ML.forSheet(sheetName) scope of MaterialLedger.js */

// ... (functions like normalizeRow_ remain the same) ...

function upsertBy(keys, rows) {
    if (!rows || !rows.length) {
        return { rows: 0, status: "SUCCESS", errorMerssage: "" };
    }

    const WRITE_BATCH_SIZE = 1000; 
    const sh = sheetInst; 
    let updateCount = 0;
    
    // --- 1. Identify Key Columns & Read Existing Data (remains the same) ---
    // ... (Reading, mapping, and updateCount logic for Steps 1-3) ...

    // --- 4. Write Updates (remains the same) ---
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
            // CRITICAL: Release the lock ONLY if it was acquired at the start.
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
}

//TODO: Prune 90 Days
function pruneLedger(days)
{
const sh = sheetInst; 
}