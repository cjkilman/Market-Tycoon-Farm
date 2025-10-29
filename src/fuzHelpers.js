/**
 * Reads the Control Table and returns a clean, structured array of market requests.
 * This is the single source of truth for what to process.
 * @returns {Array<Object>} An array of objects, e.g., [{type_id: 34, market_id: 60003760, market_type: 'station'}]
 */
function getMasterBatchFromControlTable() {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const controlSheet = ss.getSheetByName('Market_Control'); // Your sheet's name
    if (!controlSheet) throw new Error("Sheet 'Market_Control' not found.");

    // Assumes headers in row 1, data starts in row 2.
    // Columns: type_id (A), market_id (B), market_type (C)
    const range = controlSheet.getRange('A2:C' + controlSheet.getLastRow());
    const values = range.getValues();

    const marketRequests = values
      .filter(row => row[0] && row[1] && row[2]) // Filter out incomplete rows
      .map(row => ({
        type_id: parseInt(row[0], 10), // Ensure IDs are numbers
        market_id: parseInt(row[2], 10),
        market_type: String(row[1]).toLowerCase()
      }));

    console.log(`MasterReader: Loaded ${marketRequests.length} requests from Control Table.`);
    return marketRequests;
  } catch (e) {
    console.error(`Failed to read from Control Table: ${e.message}`);
    // --- FIX: Re-throw the error so the calling function can fail gracefully ---
    throw e; 
    // --- END FIX ---
  }
}

/**
 * A data model class that acts as a smart parser and data accessor for Fuzzwork API responses.
 * It creates a standardized record and provides a safe method to retrieve specific stats.
 */
class FuzDataObject {
  /**
   * Private helper to safely parse numbers and provide a default. Volums Should return 0 for no data
   */
  _normalizeNumber(value, defaultValue = 0) {
    const num = parseInt(value);
    return isNaN(num) ? defaultValue : num;
  }
    /**
   * Private helper to safely parse Prices and provide a default. Prices can be Blank representing no data
   */
    _normalizeFloat(value, defaultValue = "") {
    const num = parseFloat(value);
    return isNaN(num) ? defaultValue : num;
  }

  /**
   * @param {string|number} typeId The EVE Online type ID.
   * @param {Object} rawItemData The complete, raw item object from Fuzzwork.
   */
  constructor(typeId, rawItemData) {
    const buyData = rawItemData?.buy || {};
    const sellData = rawItemData?.sell || {};

    this.type_id = parseInt(typeId, 10);
    this.last_updated = new Date();

    this.buy = {
      avg: this._normalizeFloat(buyData.weightedAverage,""),
      max: this._normalizeFloat(buyData.max,""),
      min: this._normalizeFloat(buyData.min,""),
      stddev: this._normalizeFloat(buyData.stddev,""),
      median: this._normalizeFloat(buyData.median,""),
      volume: this._normalizeNumber(buyData.volume,0),
      orderCount: this._normalizeNumber(buyData.orderCount, 0)
    };

    this.sell = {
      avg: this._normalizeFloat(sellData.weightedAverage,""),
      max: this._normalizeFloat(sellData.max,""),
      min: this._normalizeFloat(sellData.min,""),
      stddev: this._normalizeFloat(sellData.stddev,""),
      median: this._normalizeFloat(sellData.median,""),
      volume: this._normalizeNumber(sellData.volume,0),
      orderCount: this._normalizeNumber(sellData.orderCount, 0)
    };
  }


 /**
   * Safely gets a specific statistic from the data object.
   */
  getStat(order_type, order_level) {
    let type = (order_type != null) ? String(order_type).toLowerCase() : null;
    let level = (order_level != null) ? String(order_level).toLowerCase() : null;

    if (type === "bid") type = "buy";
    if (type === "ask") type = "sell";

    const levelAliases = {
      mean: "avg", average: "avg", med: "median", vol: "volume", qty: "volume",
      quantity: "volume", weightedavg: "avg"
    };

    if (level && levelAliases[level]) level = levelAliases[level];

    const validTypes = ["buy", "sell"];
    if (!type && !level) { type = "sell"; level = "min"; }
    else if (!type && level) { type = (level === "max") ? "buy" : "sell"; }
    else if (type && !level) { level = (type === "buy") ? "max" : "min"; }

    if (!validTypes.includes(type)) return null;
    if (!this[type] || this[type][level] === undefined) return null;

    const value = this[type][level];
    const priceLevels = ["avg", "max", "min", "median"];
    
    // FIX: If the value is a price and it's not positive, return "" instead of the value
    // This ensures VLOOKUP/XLOOKUP doesn't see a 0 when data is missing.
    if (priceLevels.includes(level)) {
        const numValue = Number(value);
        // If the value is not a positive number, return the clean empty string (which _normalizeFloat already defaults to)
        // or ensure it's treated as a number > 0.
        return (isFinite(numValue) && numValue > 0) ? value : "";
    }
    
    return value;
  }
}

/**
 * Executes a function with automatic retries for temporary network errors.
 * Implements exponential backoff.
 */
function withRetries(fn, tries = 3, base = 300) {
  for (let i = 0; i < tries; i++) {
    try { return fn(); }
    catch (e) {
      const s = String(e && e.message || e);
      if (!/429|420|5\d\d|temporar|rate|timeout/i.test(s) || i === tries - 1) throw e;
      const sleepTime = base * Math.pow(2, i) + Math.floor(Math.random() * 200);
      console.warn(`Retry attempt ${i + 1}/${tries} failed: ${s}. Sleeping for ${sleepTime}ms...`);
      Utilities.sleep(sleepTime);
    }
  }
}