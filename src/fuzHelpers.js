/**
 * Reads the Control Table and returns a clean, structured array of market requests.
 * This is the single source of truth for what to process.
 * @returns {Array<Object>} An array of objects, e.g., [{type_id: 34, market_id: 60003760, market_type: 'station'}]
 */
function getMasterBatchFromControlTable(ss = null) {
  try {
    ss = ss || SpreadsheetApp.getActiveSpreadsheet();
    const controlSheet = ss.getSheetByName('Market_Control');
    if (!controlSheet) throw new Error("Sheet 'Market_Control' not found.");

    const lastRow = controlSheet.getLastRow();
    
    // OPTIMIZATION 1: Fail fast if sheet is empty to prevent range errors
    if (lastRow < 2) {
      console.warn("MasterReader: Control table is empty.");
      return [];
    }

    // OPTIMIZATION 2: Get Values. 
    // row 2, col 1 (A), down to last row, 3 columns wide (A,B,C)
    const values = controlSheet.getRange(2, 1, lastRow - 1, 3).getValues();

    // OPTIMIZATION 3: Single-Pass Pre-allocated Loop
    // Combining filter and map into one loop saves iterating through the list twice.
    const marketRequests = [];
    
    for (let i = 0; i < values.length; i++) {
      const row = values[i];
      
      // Fast check: Ensure TypeID (Col 0) and LocationID (Col 2) exist
      if (row[0] && row[2]) {
        marketRequests.push({
          type_id: Number(row[0]),      // Number() is generally faster than parseInt in V8
          market_type: String(row[1]),  // stored as-is (e.g. "Station")
          market_id: Number(row[2])
        });
      }
    }

    console.log(`MasterReader: Loaded ${marketRequests.length} requests from Control Table.`);
    return marketRequests;

  } catch (e) {
    console.error(`Failed to read from Control Table: ${e.message}`);
    throw e; 
  }
}