/**
 * Fuz API Fallback Method for the primary Hub.
 * Shortcut to fetch market data using the 'Hub' configuration defined in the 'Location List' sheet.
 * Default: Minimum Sell Price.
 *
 * @example =hubFallBack(A2:A20)
 * @example =hubFallBack(A2, "buy", "max")
 * * @param {number[][]} typeIDs The range of Type IDs to look up.
 * @param {string} [orderType="sell"] The market side ('buy' or 'sell').
 * @param {string} [orderLevel="min"] The price metric (e.g., 'min', 'max', 'fivepercent').
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} [ss] Optional spreadsheet context.
 * @return {object[][]} Prices formatted to the input range shape.
 * @customfunction
 */
function hubFallBack(typeIDs, orderType = "sell", orderLevel = "min", ss) {
  const { locationID, locationType } = getMarketConfig("hub", ss);
  return marketStatData(typeIDs, locationType, locationID, orderType, orderLevel);
}

/**
 * Fuz API Fallback Method for secondary Feeds.
 * Shortcut to fetch market data using the 'Feed' configuration defined in the 'Location List' sheet.
 * Default: Minimum Sell Price.
 *
 * @example =feedFallBack(A2:A20)
 * * @param {number[][]} typeIDs The range of Type IDs to look up.
 * @param {string} [orderType="sell"] The market side ('buy' or 'sell').
 * @param {string} [orderLevel="min"] The price metric (e.g., 'min', 'max', 'fivepercent').
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} [ss] Optional spreadsheet context.
 * @return {object[][]} Prices formatted to the input range shape.
 * @customfunction
 */
function feedFallBack(typeIDs, orderType = "sell", orderLevel = "min", ss) {
  const { locationID, locationType } = getMarketConfig("feed", ss);
  return marketStatData(typeIDs, locationType, locationID, orderType, orderLevel);
}

/**
 * Internal helper to retrieve location settings from the 'Location List' sheet.
 * * @param {string} source The configuration key to look up ('hub' or 'feed').
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} [ss] The spreadsheet to pull data from.
 * @return {Object} An object containing {locationID, locationType}.
 * @throws {Error} If the sheet, named range, or source key is missing.
 */
// 1. Rename the cache variable to avoid naming collisions
var _marketConfigCache = {}; 

function getMarketConfig(source, ss) {
  // 2. Return cached value immediately
  if (_marketConfigCache[source]) return _marketConfigCache[source];
  
  if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // 3. Cache the sheet object so we don't fetch it every time
  if (!_marketConfigCache._sheet) {
    _marketConfigCache._sheet = ss.getSheetByName("Location List");
    _marketConfigCache._typeRange = ss.getRangeByName("setting_market_range");
  }

  const sheet = _marketConfigCache._sheet;
  const typeRange = _marketConfigCache._typeRange;

  if (!sheet) throw new Error("Sheet 'Location List' not found.");
  if (!typeRange) throw new Error("Named Range 'setting_market_range' not found.");

  // Fetch only once
  const locationType = typeRange.getValue();

  const configs = {
    "hub": { 
      locationID: sheet.getRange("C3").getValue(), 
      locationType: locationType 
    },
    "feed": { 
      locationID: sheet.getRange("D3").getValue(), 
      locationType: locationType
    }
  };

  const config = configs[source.toLowerCase()];
  if (!config) throw new Error(`Source "${source}" undefined.`);
  
  // 4. Save to cache and return
  _marketConfigCache[source] = config;
  return config;
}

/**
 * Reads the 'Market_Control' table and returns a clean, structured array of market requests.
 * Uses a single-pass pre-allocated loop for high performance.
 * * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} [ss] Optional spreadsheet context.
 * @returns {Array<{type_id: number, market_type: string, market_id: number}>} Array of request objects.
 */
function getMasterBatchFromControlTable(ss = null) {
  try {
    ss = ss || SpreadsheetApp.getActiveSpreadsheet();
    const controlSheet = ss.getSheetByName('Market_Control');
    if (!controlSheet) throw new Error("Sheet 'Market_Control' not found.");

    const lastRow = controlSheet.getLastRow();
    
    if (lastRow < 2) {
      console.warn("MasterReader: Control table is empty.");
      return [];
    }

    const values = controlSheet.getRange(2, 1, lastRow - 1, 3).getValues();
    const marketRequests = [];
    
    for (let i = 0; i < values.length; i++) {
      const row = values[i];
      
      // Force conversion to numbers immediately
      const tId = Number(row[0]);
      const mId = Number(row[2]);
      
      // Strict validation: Both must resolve to positive numbers.
      // This automatically destroys #N/A, strings, spaces, and empty cells.
      if (tId > 0 && mId > 0) {
        marketRequests.push({
          type_id: tId,
          market_type: String(row[1]).trim(),
          market_id: mId
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