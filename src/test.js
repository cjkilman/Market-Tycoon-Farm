/**
 * Test function to isolate and debug the invTypes.csv parsing process.
 * Runs the buildSDEs engine for a single, known problem file.
 * * NOTE: This test uses the specific 5 headers that were crashing the process 
 * to verify the robust CSVToArray function is now working correctly.
 */
function test_SDE_invTypes() {
  // Access the library interface
  const SDE = sdeLib();
  
  // Define the SDE Page configuration for invTypes.csv
  const testPage = new SDE.SdePage(
    "SDE_invTypes_TEST", // Use a unique sheet name for testing
    "invTypes.csv",      // The file that was crashing
    ["typeID", "groupID", "typeName", "volume", "published"] // The 5 columns that were causing the column count mismatch
  );
  
  try {
    console.log(`--- STARTING CRASH-PROOF TEST: ${testPage.csvFile} ---`);
    
    // The buildSDEs function runs the parser and has the crash-proof check.
    SDE.buildSDEs(testPage);
    
    console.log(`--- TEST COMPLETE: ${testPage.csvFile} processed. ---`);
    console.log(`Please check the new sheet 'SDE_invTypes_TEST' and the logs for successful completion.`);
    
  } catch (e) {
    // If it crashes here, the error is outside the parser (e.g., sheet interaction).
    console.error(`FATAL TEST ERROR DURING EXECUTION for ${testPage.csvFile}:`);
    console.error(`Error: ${e.message}`);
    // If it crashes, immediately run Finalize to unlock the system.
    sde_job_FINALIZE(); 
  }
}