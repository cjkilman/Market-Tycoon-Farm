/**
 * CRITICAL DEBUGGER: Reveals why the Calculator thinks Cost is 199k
 * Run this function directly.
 */
function debugSmallShieldBoosterCost() {
  const TYPE_ID = 399; // Small Shield Booster I
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  Logger.log("--- DEBUGGING COST FOR TYPE " + TYPE_ID + " ---");

  // 1. Fetch the Blended Costs directly from the map
  // (We use your existing helper function to see what the script sees)
  var requiredIds = [34, 35, 36]; // Trit, Pye, Mex
  var costMap = _getBlendedCostMap(ss, requiredIds);
  
  var recipe = [
    {id: 34, name: "Tritanium", qty: 1327},
    {id: 35, name: "Pyerite", qty: 481},
    {id: 36, name: "Mexallon", qty: 113}
  ];
  
  var totalCalc = 0;
  
  recipe.forEach(mat => {
    var price = costMap.get(mat.id) || 0;
    var lineTotal = price * mat.qty;
    totalCalc += lineTotal;
    
    Logger.log(`Material: ${mat.name} (ID ${mat.id})`);
    Logger.log(`   > Quantity: ${mat.qty}`);
    Logger.log(`   > Script Price: ${price.toFixed(2)} ISK`); // <--- LOOK HERE
    Logger.log(`   > Line Total: ${lineTotal.toFixed(2)} ISK`);
    
    if (mat.id === 34 && price > 10) {
      Logger.log("   *** CRITICAL ALERT: Tritanium Price is suspiciously high! (>10 ISK) ***");
    }
  });
  
  Logger.log("--------------------------------");
  Logger.log(`TOTAL CALCULATED BUILD COST: ${totalCalc.toFixed(2)} ISK`);
  Logger.log(`VS YOUR "EFFECTIVE COST": 199,300.00 ISK`);
}


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

/**
 * TEST FUNCTION: Attempts a direct, uncached GESI call to corporate industry jobs.
 * This function bypasses sheet lookups and caching to test ESI authorization directly.
 * * Instructions: 
 * 1. REPLACE "YOUR_AUTHORIZED_CHARACTER_NAME" with the exact name you used for GESI authorization.
 * 2. Run this function (TEST_ESI_AUTH_STATUS) from the Apps Script editor.
 * 3. Check the Logger (View -> Logs) for the result.
 */
function TEST_ESI_AUTH_STATUS() {

  // ⚠️ MANDATORY: REPLACE THIS PLACEHOLDER WITH YOUR CHARACTER NAME
  const authToon = "YOUR_AUTHORIZED_CHARACTER_NAME";

  const ENDPOINT = 'corporations_corporation_industry_jobs';
  const LOG = Logger;

  if (authToon === "YOUR_AUTHORIZED_CHARACTER_NAME") {
    LOG.log("ERROR: Please replace the placeholder character name in the function.");
    return;
  }

  LOG.log(`--- Starting ESI Auth Test for: ${authToon} ---`);

  try {
    // Attempt the direct, raw ESI call
    const rawObjects = GESI.invokeRaw(
      ENDPOINT,
      {
        include_completed: true,
        name: authToon,
        show_column_headings: false,
        version: null
      }
    );

    if (Array.isArray(rawObjects) && rawObjects.length > 0) {
      LOG.log(`✅ SUCCESS! Found ${rawObjects.length} jobs.`);
      LOG.log("First job ID: " + rawObjects[0].job_id);
    } else if (Array.isArray(rawObjects) && rawObjects.length === 0) {
      LOG.log("✅ SUCCESS! The ESI call worked, but zero industry jobs were returned.");
    } else {
      LOG.log("❌ FAILURE: GESI returned data that was not an array (Check Logs for the actual error).");
    }

  } catch (e) {
    LOG.log(`❌ ESI CALL FAILED: ${e.message}`);

    if (e.message.includes("403")) {
      LOG.log("ACTION: This is an Authorization (403 Forbidden) error. Token is invalid or missing ESI scopes.");
      LOG.log("-> Go to GESI -> Authorize Character and re-authorize with ALL corporate scopes checked.");
    } else if (e.message.includes("420")) {
      LOG.log("ACTION: This is a Rate Limit (420) error. Wait 5 minutes before trying again.");
    } else {
      LOG.log("ACTION: The error is unknown. Check external network status.");
    }
  }
}