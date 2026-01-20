// Global Property Service
const SCRIPT_PROPS = PropertiesService.getScriptProperties();

// --- CONFIGURATION SECTION ---

/**
 * CONFIG 1: SDE Tables (Tycoon Version)
 * Defines the massive datasets needed for Market & Industry.
 */
function GET_SDE_CONFIG() {
  return [
    // The Basics
    { name: "SDE_invTypes", file: "invTypes.csv", cols: ["typeID", "groupID", "typeName", "volume", "marketGroupID", "basePrice"] },
    { name: "SDE_invGroups", file: "invGroups.csv", cols: null },
    { name: "SDE_staStations", file: "staStations.csv", cols: null },
    
    // Map Data (For Route Planning)
    { name: "SDE_mapSolarSystems", file: "mapSolarSystems.csv", cols: ["regionID", "solarSystemID", "solarSystemName", "security"] },
    
    // Industry Data (For Manufacturing)
    { name: "SDE_industryActivityMaterials", file: "industryActivityMaterials.csv", cols: null },
    { name: "SDE_industryActivityProducts", file: "industryActivityProducts.csv", cols: null }
  ];
}

/**
 * CONFIG 2: Utility Sheet Settings
 * Single source of truth for the 'Utility' sheet name and range.
 */
function GET_UTILITY_CONFIG() {
  return {
    sheetName: "Utility", 
    range: "B3:C3" // The cells that control the formulas
  };
}

// --- MENU & UI ---

function onOpen() {
  SpreadsheetApp.getUi()
    .createMenu('Market Sheet Tools')
    .addItem('Refresh All Data', 'refreshData')
    .addItem('Update SDE Data', 'sde_job_START') // <--- Pointing to the NEW Engine
    .addToUi();
}

/**
 * HOOK: Called BEFORE SDE Start
 * Returns TRUE to continue, FALSE to cancel.
 */
function ON_SDE_START() {
  const ui = SpreadsheetApp.getUi();
  const response = ui.alert(
    '⚠️ Update SDE Database?',
    'This will download fresh data from GitHub.\n\n' +
    '• Formulas will be paused.\n' +
    '• The sheet will be locked for ~3 minutes.\n' +
    '• ORCHESTRATOR will be PAUSED.\n\n' +
    'Do you want to proceed?',
    ui.ButtonSet.YES_NO
  );

  if (response == ui.Button.NO || response == ui.Button.CLOSE) {
    return false; // Tells Controller to ABORT
  }

  SpreadsheetApp.getActiveSpreadsheet().toast("Pausing Orchestrator & Initializing Update...", "System Status", 10);
  
  // TYCOON SPECIFIC: Pause the business logic
  _manageOrchestrator(false);
  
  return true; // Tells Controller to PROCEED
}

/**
 * HOOK: Called when the job is 100% done
 */
function ON_SDE_COMPLETE() {
  // TYCOON SPECIFIC: Restart the business logic
  _manageOrchestrator(true);

  SpreadsheetApp.getActiveSpreadsheet().toast("SDE Update Complete. Orchestrator Resumed.", "System Status", -1);
}

// --- HELPER: ORCHESTRATOR MANAGER ---

/**
 * Pauses or Restarts the main Market Orchestrator trigger.
 * @param {boolean} turnOn - True to create trigger, False to delete it.
 */
function _manageOrchestrator(turnOn) {
  const FUNCTION_NAME = 'masterOrchestrator'; // <--- Ensure this matches your actual function name
  
  // 1. Always delete existing triggers first (to avoid duplicates or to pause)
  const allTriggers = ScriptApp.getProjectTriggers();
  allTriggers.forEach(trigger => {
    if (trigger.getHandlerFunction() === FUNCTION_NAME) {
      ScriptApp.deleteTrigger(trigger);
    }
  });

  // 2. If turning ON, create a new one
  if (turnOn) {
    ScriptApp.newTrigger(FUNCTION_NAME)
      .timeBased()
      .everyMinutes(10) // <--- Adjust frequency as needed
      .create();
    console.log("Orchestrator Trigger RESTARTED.");
  } else {
    console.log("Orchestrator Trigger PAUSED.");
  }
}

// --- REFRESH TOOLS (Unified) ---

const TIME_DELAY = 2000;

function refreshData() {
  SpreadsheetApp.flush();
  refreshAllData();
  refreshDynamicData();
  refreshStaticData();
}

function refreshAllData() {
  const conf = GET_UTILITY_CONFIG();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(conf.sheetName);
  if (sheet) {
    sheet.getRange(conf.sheetName + '!' + conf.range).setValues([[0, 0]]);
  }
}

function refreshDynamicData() {
  Utilities.sleep(TIME_DELAY);
  const conf = GET_UTILITY_CONFIG();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(conf.sheetName);
  if (sheet) {
    sheet.getRange(conf.sheetName + '!B3').setValue(1);
  }
}

function refreshStaticData() {
  Utilities.sleep(TIME_DELAY);
  const conf = GET_UTILITY_CONFIG();
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const sheet = ss.getSheetByName(conf.sheetName);
  if (sheet) {
    sheet.getRange(conf.sheetName + '!C3').setValue(1);
  }
}

/**
 * Helper: Query Enhancer
 */
function sqlFromHeaderNames(rangeName, queryString, useColNums) {
  let ss = SpreadsheetApp.getActiveSpreadsheet();
  let range;
  try {
    range = ss.getRange(rangeName);
  } catch (e) {
    range = ss.getRangeByName(rangeName);
  }
  let headers = range.getValues()[0];
  for (var i = 0; i < headers.length; i++) {
    if (headers[i].length < 1) continue;
    var re = new RegExp("\\b" + headers[i] + "\\b", "gm");
    if (useColNums) {
      var columnName = "Col" + Math.floor(i + 1);
      queryString = queryString.replace(re, columnName);
    } else {
      var columnLetter = range.getCell(1, i + 1).getA1Notation().split(/[0-9]/)[0];
      queryString = queryString.replace(re, columnLetter);
    }
  }
  return queryString;
}