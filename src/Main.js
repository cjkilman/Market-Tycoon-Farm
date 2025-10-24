function onOpen() {
  var ui = SpreadsheetApp.getUi();
  // Or DocumentApp or FormApp.
  ui.createMenu('Sheet Tools')
    .addItem('Refresh All Data', 'refreshData')
    .addItem('Update SDE Data', 'importSDE')
    .addItem('Authorize Script (First Run)', 'forceAuthorization')
    .addItem("Recalculate/Refresh", "Full_Recalculate_Cycle")
    .addToUi();
}

function getStructureNames(structureIDs) {
  if (!(Array.isArray(structureIDs))) { structureIDs = [[structureIDs]] };
  var output = [];
  for (var i = 0; i < structureIDs.length; i++) {
    var data = GESI.universe_structures_structure(structureIDs[i][0], GESI.getMainCharacter(), false);
    output.push(data[0][0]);
  }
  return output;
}

function pull_SDE() {
  // Lock Formulas from running
  const haltFormulas = [[0, 0]];

  var thisSpreadSheet = SpreadsheetApp.getActiveSpreadsheet();
  var loadingHelper = thisSpreadSheet.getRangeByName("'Utility'!B3:C3");
  const backupSettings = loadingHelper.getValues();
  loadingHelper.setValues(haltFormulas);

  try {

    const sdePages = [
      new SdePage(
        "SDE_invTypes",
        "invTypes.csv",
        // Optional headers,  
        // invTypes is 100+ megabytes. Select columns needed to help it load faster. 
        ["typeID", "groupID", "typeName", "volume"]
      ),
      new SdePage(
        "SDE_staStations",
        "staStations.csv",
        // Optional headers,  
        // invTypes is 100+ megabytes. Select columns needed to help it load faster. 
        ["stationID", "security", "stationTypeID", "corporationID", "solarSystemID", "regionID", "stationName"]
      ),
      new SdePage(
        "SDE_industryActivityProducts",
        "industryActivityProducts.csv",
        []
      )
    ];
    sdePages.forEach(buildSDEs);
  }
  finally {
    // release lock
    loadingHelper.setValues(backupSettings);
  }
}

/**
 * Rebuilds the 'Market_Control' sheet.
 * NEW VERSION: Adds a 'last_updated' column to track the status of each item.
 */
function updateControlSheet() {
  const log = LoggerEx.withTag('CONTROL_GEN');
  log.info('Starting Market_Control sheet rebuild with last_updated column...');

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const itemMasterSheetName = 'Item List';
  const sdeSheetName = 'SDE_invTypes';
  const locationListSheetName = 'Location List';
  const controlSheetName = 'Market_Control';

  const itemSheet = ss.getSheetByName(itemMasterSheetName);
  const sdeSheet = ss.getSheetByName(sdeSheetName);
  const locationSheet = ss.getSheetByName(locationListSheetName);
  const controlSheet = ss.getSheetByName(controlSheetName);

  if (!itemSheet || !sdeSheet || !locationSheet || !controlSheet) {
    throw new Error(`One or more required sheets are missing.`);
  }

  // 1. Create a lookup map from the SDE (typeName -> typeID)
  const sdeData = sdeSheet.getRange('A2:C' + sdeSheet.getLastRow()).getValues();
  const typeIdMap = new Map(sdeData.map(row => [String(row[2]).trim().toLowerCase(), row[0]]));

  // 2. Read Item Names from the master 'Item List' sheet
  const itemNamesRange = itemSheet.getRange('B2:B' + itemSheet.getLastRow());
  const itemIds = itemNamesRange.getValues()
    .flat()
    .map(name => name ? typeIdMap.get(name.trim().toLowerCase()) : null)
    .filter(id => Number.isFinite(id) && id > 0);

  const uniqueItemIds = Array.from(new Set(itemIds));
  log.info(`Found ${uniqueItemIds.length} unique item IDs from '${itemMasterSheetName}'.`);

  // 3. Read and Deduplicate Location IDs
  const locHeaders = locationSheet.getRange('A5:G5').getValues()[0];
  const stationColIndex = locHeaders.indexOf('Station');
  const systemColIndex = locHeaders.indexOf('System');
  const regionColIndex = locHeaders.indexOf('Region');

  const locData = locationSheet.getRange(6, 1, locationSheet.getLastRow() - 5, locHeaders.length).getValues();
  const uniqueLocationStrings = new Set();
  locData.forEach(row => {
    if (Number(row[stationColIndex]) > 0) uniqueLocationStrings.add(`station_${row[stationColIndex]}`);
    if (Number(row[systemColIndex]) > 0) uniqueLocationStrings.add(`system_${row[systemColIndex]}`);
    if (Number(row[regionColIndex]) > 0) uniqueLocationStrings.add(`region_${row[regionColIndex]}`);
  });

  const locations = Array.from(uniqueLocationStrings).map(locString => {
    const [type, id] = locString.split('_');
    return { type, id: Number(id) };
  });
  log.info(`Found ${locations.length} unique market locations.`);

  // 4. Generate and Write the Control Table Data
  withSheetLock(function () {
    controlSheet.clear();
    const headers = [['type_id', 'location_type', 'location_id', 'last_updated']];
    controlSheet.getRange(1, 1, 1, 4).setValues(headers);

    const outputRows = [];

    // REVERSED LOOP ORDER: Iterate over locations first, then items.
    for (const loc of locations) {
      for (const item_id of uniqueItemIds) {
        // Add a blank placeholder for the 'last_updated' timestamp
        outputRows.push([item_id, loc.type, loc.id, '']);
      }
    }

    if (outputRows.length > 0) {
      controlSheet.getRange(2, 1, outputRows.length, 4).setValues(outputRows);
      log.info(`Successfully wrote ${outputRows.length} control rows.`);
    }
  });
  SpreadsheetApp.getUi().alert(`'${controlSheetName}' has been updated successfully.`);
}

/**
 * Function to run manually to force the authorization prompt.
 */
function forceAuthorization() {
  // This function runs a service that requires authorization (UrlFetchApp)
  // and is accessible via the custom menu. Running it guarantees the prompt appears.
  try {

    _deleteExistingTriggers();
    UrlFetchApp.fetch("https://google.com");
    SpreadsheetApp.getUi().alert('Authorization granted successfully!');
  } catch (e) {
    if (e.message.includes('Authorization is required')) {
      SpreadsheetApp.getUi().alert('Authorization failed. Please follow the prompt in the editor after running this function.');
    } else {
      // Check if the script needs permissions beyond basic Spreadsheet access
      const propertiesService = PropertiesService.getUserProperties();
      propertiesService.setProperty('AUTH_CHECK', 'RUNNING');
      propertiesService.deleteProperty('AUTH_CHECK');
      SpreadsheetApp.getUi().alert('Authorization check failed. Please run this function again and check the console/editor for prompts.');
    }
  }
}

function importSDE() {

  // Display an alert box with a title, message, input field, and "Yes" and "No" buttons. The
  // user can also close the dialog by clicking the close button in its title bar.
  var ui = SpreadsheetApp.getUi();
  var response = ui.alert('Updating the SDE',
    'Updating the SDE may take a few minutes. In the meantime do not close the window otherwise you will have to restart. Continue?',
    ui.ButtonSet.YES_NO);

  // Process the user's response.
  if (response == ui.Button.YES) {

    pull_SDE();
  } else if (response == ui.Button.NO) {
    ui.alert('SDE unchanged.');
  } else {
    ui.alert('SDE unchanged.');
  }
}

/**
 *Get Character Names from thier ID
 *
 * @param {*} charIds
 * @return {*} 
 */
function getChacterNameFromID(charIds, show_column_headings = true) {
  if (!charIds) throw "undefined charIds";
  if (!Array.isArray(charIds)) charIds = [charIds];
  charIds = charIds.filter(Number);

  let chars = [];
  if (show_column_headings) chars = chars.concat("chacacter_name");
  const rowIdx = show_column_headings ? 1 : 0;

  for (I = 0; I < charIds.length; I++) {
    try {
      const char = GESI.characters_character(Number(charIds[I]), show_column_headings);
      chars = chars.concat(char[rowIdx][7]);
    }
    catch (e) {
      throw e;
    }
  }
  Logger.log(chars);
  return chars;
}



/**
 * Replace [Header Name] tokens in a QUERY-like SQL with ColN,
 * based on the first row (header) of the given range.
 *
 * This version uses CacheService to avoid re-fetching the header
 * when called multiple times in a single formula.
 *
 * @param {string} rangeName    A string representing a Named Range (e.g., "myRange")
 * OR an A1 notation range (e.g., "Sheet1!A1:G10").
 * @param {string} queryString  SQL containing bracketed headers: [Item], [Goal], ...
 * @param {boolean} [useColNums=true]  (This param is now ignored, defaults to ColN)
 * @returns {string}
 */
function sqlFromHeaderNamesEx(rangeName, queryString, useColNums) {
  
  if (typeof rangeName !== 'string' || !rangeName) {
     throw new Error(`sqlFromHeaderNamesEx: First argument must be the name of a Named Range (as a string) or an A1 notation string.`);
  }

  // Get the script cache
  const cache = CacheService.getScriptCache();
  // The cache key is the string itself, whether it's a named range
  // or A1 notation.
  const cacheKey = `headerMap_${rangeName}`;
  let map = {};

  // Try to get the map from cache
  const cachedMap = cache.get(cacheKey);

  if (cachedMap) {
    // Found it in the cache, parse it and use it
    map = JSON.parse(cachedMap);
  } else {
    // Not in cache, so we must build it
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    let range = null;

    // 1. Try to resolve as a Named Range first
    range = ss.getRangeByName(rangeName); // This returns null if not found, doesn't throw.

    // 2. If not a Named Range, try to resolve as A1 notation
    if (!range) {
      try {
        range = ss.getRange(rangeName); // This *will* throw an error if invalid
      } catch (e) {
        // If both fail, throw a clear error.
        throw new Error(`sqlFromHeaderNamesEx: Could not resolve range. "${rangeName}" is not a valid Named Range or A1 notation range.`);
      }
    }
    
    // --- Build the Header Map ---
    const headerWidth = range.getNumColumns();
    // This is the expensive API call we want to run only once
    const headerRow = range.offset(0, 0, 1, headerWidth).getValues()[0];

    for (let i = 0; i < headerRow.length; i++) {
      const raw = headerRow[i];
      if (raw == null) continue;
      const h = String(raw).trim();
      if (!h) continue;

      const replacement = `Col${i + 1}`;
      map[h] = replacement;
    }
    
    // Store the newly built map in the cache for 5 minutes (300 seconds)
    cache.put(cacheKey, JSON.stringify(map), 300);
  }
  
  // --- Replace Tokens in Query String ---
  
  // Find all [Header] tokens and replace them with their ColN equivalent
  const rewritten = String(queryString).replace(/\[([^\]]+)\]/g, (m, label) => {
    const key = String(label || "").trim();
    if (map.hasOwnProperty(key)) return map[key];
    
    // Fallback to case-insensitive check
    const found = Object.keys(map).find(k => k.toLowerCase() === key.toLowerCase());
    return found ? map[found] : m; // leave untouched if no match
  });

  return rewritten;
}

