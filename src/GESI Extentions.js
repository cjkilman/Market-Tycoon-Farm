// ContractItems_Fetchers.gs.js
// Robust, GAS-safe contract sync for EVE (GESI):
//    * Two-phase listing: CHARACTER -> CORPORATION (no mixing of scopes)
//    * Items fetched with HEADERS (boolean; default true in GESI)
//    * Single canonical endpoints: positional arguments
//    * Per-doc cache for auth names; per-user cache for items (scope-partitioned)
//    * LoggerEx integration (marketTracker style)
//    * All major functions now accept an optional 'ss' (Spreadsheet) argument.
//    * Uses executeLocked pattern for top-level locking and retry.
//
/* global GESI, CacheService, SpreadsheetApp, LockService, Utilities, Session, LoggerEx, ML, getOrCreateSheet, PT, _charIdMap, _getData_, _toNumberISK_, executeLocked, scheduleOneTimeTrigger, deleteTriggersByName, _measureSpreadsheetLatency */

// ==========================================================================================
// CONFIG & CONSTANTS
// ==========================================================================================

// Optional override for corp auth character. May be:
//  - a Named Range (e.g., "CORP_AUTH_CHAR"),
//  - a Sheet!A1 range (e.g., "Utility!B3"),
//  - or a literal character name (e.g., "CJ Kilman").
// If omitted/blank/invalid, we default to GESI.name, then to first authed name.
var CORP_AUTH_CHARACTER = "setting_director";

// NEW PERSISTENT PROPERTY KEY
const _CORP_AUTH_CHAR_PROP = 'GESI_PERSISTED_CORP_AUTH_CHAR';

// Rolling lookback (days) for finished item_exchange contracts.
var CONTRACT_LOOKBACK_DAYS = 30;

// Maximum number of RAW sheet rows (excluding header) to read and process for CPU safety.
const MAX_RAW_ROWS_TO_PROCESS = 50000;

// --- CRITICAL CONSTANTS (Must be defined in the module file) ---
const CONTRACTS_RAW_SHEET = "Contracts (RAW)";
const CONTRACT_ITEMS_RAW_SHEET = "Contract Items (RAW)";
const CONTRACT_RAW_COLUMNS = 16; // Number of columns in Contracts (RAW)
const ITEMS_RAW_COLUMNS = 6;     // Number of columns in Contract Items (RAW)

// Recommended Update:
const CONTRACT_STATUSES = [
  "outstanding",
  "in_progress",
  "finished",
  "finished_issuer",
  "finished_contractor",
];

const PROP_KEY_COGS_STEP = 'cogsJobStep';
const STATE_FLAGS_COGS = { FINALIZING: 'FINALIZING' };
const PROP_KEY_LAST_CONTRACT_ID = 'lastProcessedContractId';

const PROP_KEY_CONTRACT_LEASE = 'contractJobLeaseUntil';
const CONTRACT_LEASE_DURATION_MS = 60 * 60 * 1000; // 1 hour lease in milliseconds

// LEDGER SHEET CONSTANTS
const LEDGER_BUY_SHEET = 'Material_Ledger';
const LEDGER_SALE_SHEET = 'Sales_Ledger';
const LEDGER_CORP_SALE_SOURCE = 'CORP_SALE'; // New source label for corp sales
const CORP_JOURNAL_RESUME_PROP = 'CORP_JOURNAL_DIV_RESUME'; // Property for resume logic

// NEW: Property to store the transaction ID of the most recently fetched (newest) record.
const CORP_JOURNAL_LAST_ID = 'CORP_JOURNAL_LAST_TRANSACTION_ID';

// --- Raw_loot (rolling 30d total) -> Material_Ledger (post deltas) ------------
const RAW_LOOT_SHEET = 'Raw_loot';
const SNAP_KEY = 'raw_loot:snapshot:v2'; // doc properties key

// NEW EXTERNAL LOOT SOURCE CONFIG (Replaces IMPORTRANGE formula)
const EXTERNAL_LOOT_SHEET_ID = "1qESXdN_BabqiJmwHS7fHkmQxntEkU7_Zfh6mhUwLfIg";
const EXTERNAL_LOOT_RANGE = "Raw_loot!A:D";


// ENDPOINTS (canonical; let GESI handle versioning)
var EP_LIST_CHAR = "characters_character_contracts";
var EP_LIST_CORP = "corporations_corporation_contracts";

var EP_ITEMS_CHAR = "characters_character_contracts_contract_items";
var EP_ITEMS_CORP = "corporations_corporation_contracts_contract_items";


// Contract list (headerless) column order fallback, if needed
var GESI_CONTRACT_COLS = [
  "acceptor_id", "assignee_id", "availability", "buyout", "collateral", "contract_id",
  "date_accepted", "date_completed", "date_expired", "date_issued", "days_to_complete",
  "end_location_id", "for_corporation", "issuer_corporation_id", "issuer_id", "price",
  "reward", "start_location_id", "status", "title", "type", "volume", "character_name"
];

// Cache TTLs (seconds)
var GESI_TTL = (GESI_TTL != null && typeof GESI_TTL === 'object') ? GESI_TTL : {};
GESI_TTL.chars = (GESI_TTL.chars != null) ? GESI_TTL.chars : 21600; // 6h (document cache)

// UPDATED: Set Contracts and Items to 60 minutes (3600 seconds)
GESI_TTL.contracts = (GESI_TTL.contracts != null) ? GESI_TTL.contracts : 3600;    // 60m
GESI_TTL.items = (GESI_TTL.items != null) ? GESI_TTL.items : 3600;    // 60m

// ADDED: Module-level cache variable to store the authenticated character name
// only once per script execution.
var _cachedAuthChar = null;
// ADDED: Cache for Named Ranges to avoid slow API lookups
var _cachedNamedRanges = {};
// NEW: Cache for authenticated GESI names (expensive call)
var _cachedAuthNames = null;
// NEW: Cache for character ID map
var _cachedCharIdMap = null;


function FORCE_WAKEUP_FORMULAS() {
  // Pass the spreadsheet directly to completely bypass any custom function traps
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const authName = getCorpAuthChar(ss);

  // Double-check the save
  if (authName) {
    PropertiesService.getScriptProperties().setProperty('GESI_PERSISTED_CORP_AUTH_CHAR', authName);
    console.log("✅ SUCCESS: Saved " + authName + " to the Fast-Path Cache.");
  } else {
    console.error("❌ FAILED: Could not find Corp Auth Character.");
  }
}

// ==========================================================================================
// UTILITIES (GAS-SAFE)
// ==========================================================================================

function FORCE_RESET_CACHES() {
  const props = PropertiesService.getScriptProperties();
  props.deleteProperty('GESI_PERSISTED_CORP_AUTH_CHAR');

  // RESET ALL JOURNAL ANCHORS
  props.deleteProperty('CORP_JOURNAL_DIV_RESUME');
  props.deleteProperty('CORP_JOURNAL_LAST_TRANSACTION_ID');
  props.deleteProperty('CORP_JOURNAL_LAST_ID_BUYS');  // New
  props.deleteProperty('CORP_JOURNAL_LAST_ID_SELLS'); // New
  props.deleteProperty('CORP_JOURNAL_PHASE');

  console.log("✅ ALL CACHES AND ANCHORS FLUSHED.");
}


/**
 * Utility function to read all data from a sheet (rows 2+) and return header map.
 * @param {GoogleAppsScript.Spreadsheet.Spreadsheet} ss
 * @param {string} sheetName
 */
function _getData_(ss, sheetName) { // ADDED ss ARGUMENT
  var sh = ss.getSheetByName(sheetName); // USE ss ARGUMENT
  if (!sh) throw new Error('Missing sheet: ' + sheetName);
  var vals = sh.getDataRange().getValues();

  // ROBUSTNESS FIX: Check for empty data before accessing header row (A1 is excluded from getDataRange if only headers exist)
  if (vals.length < 1) {
    return { sh: sh, header: [], rows: [], h: {} };
  }

  var header = vals[0] || [];
  var rows = vals.slice(1);
  var h = {};
  for (var i = 0; i < header.length; i++) { h[String(header[i]).trim()] = i; } // 0-based index map

  return { sh: sh, header: header, rows: rows, h: h };
}


/**
 * Nuclear Rewrite: Forces the sheet to match the data schema.
 */
function _rewriteData_(sh, header, rows) {
  // 1. Clear everything (Headers + Data)
  sh.clear();

  // 2. Set the correct headers
  sh.getRange(1, 1, 1, header.length).setValues([header]).setFontWeight("bold");

  // 3. Write data if it exists
  if (rows && rows.length > 0) {
    sh.getRange(2, 1, rows.length, header.length).setValues(rows);
  }
}

/**
 * Historical Ledger: Appends new rows to the bottom without destroying history.
 */
function _appendData_(sh, header, rows) {
  // 1. If the sheet is completely empty, write the headers first
  if (sh.getLastRow() === 0) {
    sh.getRange(1, 1, 1, header.length).setValues([header]).setFontWeight("bold");
  }

  // 2. Append the new data to the bottom
  if (rows && rows.length > 0) {
    const nextRow = sh.getLastRow() + 1;
    sh.getRange(nextRow, 1, rows.length, header.length).setValues(rows);
  }
}

/**
 * Resets the Loot Delta Snapshot. 
 * Forces the next run to treat ALL current loot in the external sheet as 'New'
 * and import it to the ledger.
 * WARNING: This may cause duplicates if the data is already in the ledger!
 */
function resetLootSnapshot() {
  const PROP_KEY = 'raw_loot:snapshot:v2'; // Must match SNAP_KEY in GESI Extentions.js
  const props = PropertiesService.getDocumentProperties();

  const lock = LockService.getScriptLock();
  if (lock.tryLock(5000)) {
    try {
      props.deleteProperty(PROP_KEY);
      Logger.log("✅ Loot Snapshot Reset. Next run will import all external quantities as new deltas.");
      if (typeof SpreadsheetApp !== 'undefined') SpreadsheetApp.getUi().alert("Success: Loot Snapshot Reset.");
    } catch (e) {
      Logger.log("❌ Error resetting snapshot: " + e.message);
    } finally {
      lock.releaseLock();
    }
  } else {
    Logger.log("⚠️ Could not acquire lock. Try again.");
  }
}



/**
 * Master Sync Controller
 * Run this function on a single trigger (e.g., every 15-30 mins)
 */
function TransactionsAndJournalSync(ss) {
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();

  try {
    // 1. Capture the boolean return values from the feeders
    const txAdded = executeWithWaitLock(() => Feed_Transactions_To_Buffer(ss), 'Feed_Transactions_To_Buffer', 300000);
    const journalAdded = executeWithWaitLock(() => Feed_Journal_To_Buffer(ss), 'Feed_Journal_To_Buffer', 300000);

    // 2. Gatekeeper: Only process if at least one feeder brought in new data
    if (txAdded || journalAdded) {
      processInternalBuffer(ss);
    } else {
      console.log("No new transactions or journal entries. Skipping buffer processing to save quota.");
    }

  } catch (e) {
    // If the 5-minute lock fails, quietly abort and let the next trigger handle it
    console.warn("Sync deferred due to heavy traffic: " + e.message);
  }
}
function Reset_Sync_Anchors() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  // Nuke the bookmarks completely. 
  // Setting to 0 forces the API to pull maximum available history (usually 30 days for ESI).
  // Your deduplicator will safely ignore the overlap.
  SCRIPT_PROP.setProperty('CORP_LAST_TRANSACTION_ID', '0');
  SCRIPT_PROP.setProperty('CORP_LAST_JOURNAL_ID', '0');

  // FIXED: Removed the floating variable.
  // FIXED: Removed quotes around GLOBAL_STATE_KEY so it uses your global constant.
  // Set to 'RUNNING' to ensure the maintenance lock is fully lifted.
  SCRIPT_PROP.setProperty(GLOBAL_STATE_KEY, 'RUNNING');

  console.log("Anchors set to 0 (Maximum Rewind). Maintenance Lock lifted. Run Master Sync now.");
}

function Feed_Transactions_To_Buffer(ss) {
  const log = LoggerEx.withTag('TXN_FEEDER');
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const LAST_TXN_KEY = 'CORP_LAST_TRANSACTION_ID';

  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const bufferSheet = ss.getSheetByName("_Internal_Ledger_Buffer");
  if (!bufferSheet) {
    log.error("Missing _Internal_Ledger_Buffer sheet.");
    return false;
  }

  // Check circuit state before attempting network I/O
  if (ESI.isLocked()) {
    log.error("Sync aborted: ESI Circuit Locked.");
    return false;
  }

  const authToon = getCorpAuthChar(ss);
  const lastProcessedId = Number(SCRIPT_PROP.getProperty(LAST_TXN_KEY) || 0);

  // UPGRADE: Utilize ESI module
  const authClient = GESI.getClient(authToon);
  const service = ESI.forEndpoint(authClient, 'corporations_corporation_wallets_division_transactions', {
    onLock: () => log.error("Quota lock triggered during Transaction fetch.")
  });

  const result = service.get({ division: 3 });

  // Handle errors
  if (result.error) {
    if (result.error === "LOCKED") {
      SCRIPT_PROP.setProperty('DAILY_QUOTA_EXHAUSTED', 'true');
    }
    log.error(`ESI Module Failure: ${result.error}`);
    return false;
  }

  // Process data
  let newTxns = (Array.isArray(result.data) ? result.data : [])
    .filter(t => t.transaction_id > lastProcessedId);

  if (newTxns.length === 0) {
    log.info("No new transactions to buffer.");
    return false;
  }

  newTxns.sort((a, b) => a.transaction_id - b.transaction_id);
  const NOW = new Date().getTime();

  const bufferRows = newTxns.map(t => [
    Number(t.transaction_id),
    JSON.stringify({ data: { ...t, source: 'TRANSACTION' }, tax: 0, status: 'WAITING', ts: NOW }),
    NOW
  ]);

  const startRow = Math.max(2, bufferSheet.getLastRow() + 1);
  bufferSheet.getRange(startRow, 1, bufferRows.length, 3).setValues(bufferRows);

  SCRIPT_PROP.setProperty(LAST_TXN_KEY, String(newTxns[newTxns.length - 1].transaction_id));

  log.info(`Parked ${bufferRows.length} new transactions in the Waiting Room.`);
  return true;
}

function Feed_Journal_To_Buffer(ss) {
  const log = LoggerEx.withTag('JOURNAL_FEEDER');
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const LAST_JOURNAL_KEY = 'CORP_LAST_JOURNAL_ID';

  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const bufferSheet = ss.getSheetByName("_Internal_Ledger_Buffer");
  if (!bufferSheet) {
    log.error("Missing _Internal_Ledger_Buffer sheet.");
    return false;
  }

  // 1. Quota Circuit Breaker
  if (ESI.isLocked()) {
    log.error("Sync aborted: ESI Circuit Locked.");
    return false;
  }

  const authToon = getCorpAuthChar(ss);
  const authClient = GESI.getClient(authToon);
  let lastProcessedId = Number(SCRIPT_PROP.getProperty(LAST_JOURNAL_KEY) || 0);

  // 2. ESI Module Integration
  const service = ESI.forEndpoint(authClient, 'corporations_corporation_wallets_division_journal', {
    onLock: () => SCRIPT_PROP.setProperty('DAILY_QUOTA_EXHAUSTED', 'true')
  });

  const result = service.get({ division: 3 });

  // 3. Centralized Error Handling
  if (result.error) {
    log.error(`ESI Module Failure: ${result.error}`);
    return false;
  }

  // 4. Process Data
  let newEntries = (Array.isArray(result.data) ? result.data : [])
    .filter(j => j.id > lastProcessedId && j.ref_type !== 'market_escrow');

  if (newEntries.length === 0) {
    log.info("No new (valid) journal entries to buffer.");
    return false;
  }

  // 5. Sort and Map to Buffer Rows
  newEntries.sort((a, b) => a.id - b.id);
  const NOW = new Date().getTime();

  const bufferRows = newEntries.map(j => [
    Number(j.id),
    JSON.stringify({ data: { ...j, source: 'JOURNAL' }, status: 'WAITING', ts: NOW }),
    NOW
  ]);

  // 6. Commit to Sheet
  const startRow = Math.max(2, bufferSheet.getLastRow() + 1);
  bufferSheet.getRange(startRow, 1, bufferRows.length, 3).setValues(bufferRows);

  // 7. Update Anchor
  SCRIPT_PROP.setProperty(LAST_JOURNAL_KEY, String(newEntries[newEntries.length - 1].id));

  log.info(`Parked ${bufferRows.length} new valid journal entries in the Waiting Room.`);
  return true;
}


function processInternalBuffer(ss) {
  const log = LoggerEx.withTag('BUFFER_PROCESS');

  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const bufferSheet = ss.getSheetByName("_Internal_Ledger_Buffer");
  if (!bufferSheet) {
    log.warn("Could not find _Internal_Ledger_Buffer sheet.");
    return;
  }

  const data = bufferSheet.getDataRange().getValues();
  if (data.length <= 1) {
    log.info("Buffer is empty. Nothing to process.");
    return;
  }

  const headers = data.shift();
  log.info(`Loaded ${data.length} raw rows from buffer.`);

  const pending = new Map();

  // --- 1. GROUPING PHASE ---
  data.forEach((row, rowIndex) => {
    if (!row[0]) return;
    const entry = JSON.parse(row[1]);
    const d = entry.data;

    const isTx = (d.source === 'TRANSACTION');
    const isJournal = (d.source === 'JOURNAL');

    let id = 0;
    if (isTx) {
      id = Number(d.transaction_id || 0);
    } else if (isJournal) {
      const cType = String(d.context_id_type || "").toLowerCase();
      if (cType === 'transaction_id' || cType === 'market_transaction_id') {
        id = Number(d.context_id || 0);
      } else {
        return;
      }
    }

    id = Math.floor(Number(id));
    if (!id || isNaN(id) || id === 0) return;

    if (!pending.has(id)) pending.set(id, { tx: null, fees: 0, ts: entry.ts, journalFound: false, journalAmount: 0 });

    const record = pending.get(id);

    if (isTx) {
      record.tx = d;
    } else if (isJournal) {
      if (['broker_fee', 'transaction_tax'].includes(d.ref_type)) {
        record.fees += Math.abs(Number(d.amount || 0));
      } else if (['market_transaction'].includes(d.ref_type)) {
        record.journalFound = true;
        record.journalAmount = Number(d.amount || 0);
      }
    }
  });

  // --- 2. PROCESSING PHASE ---
  const sells = [];
  const buys = [];
  const processedIds = new Set();

  pending.forEach((p, id) => {
    if (p.tx && p.journalFound) {
      const isCorpPurchase = (p.journalAmount < 0);
      const perUnitFee = p.tx.quantity > 0 ? (p.fees / Number(p.tx.quantity)) : 0;
      const finalUnitValue = isCorpPurchase ? (Number(p.tx.unit_price) + perUnitFee) : (Number(p.tx.unit_price) - perUnitFee);

      const ledgerObj = {
        date: new Date(p.tx.date),
        type_id: Number(p.tx.type_id),
        qty: isCorpPurchase ? Number(p.tx.quantity) : -Number(p.tx.quantity),
        source: 'TRANSACTION',
        contract_id: id,
        unit_value_filled: finalUnitValue,
        char: "Corp Wallet"
      };

      if (isCorpPurchase) buys.push(ledgerObj);
      else sells.push(ledgerObj);

      processedIds.add(id);
    }
  });

  log.info(`Processing complete. Cleanly routed ${processedIds.size} verified transactions.`);

  // --- 3. UPSERT & BUFFER CLEANUP ---
  let needsWakeUp = false;
  try {
    if (typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

    if (sells.length > 0) {
      ML.forSheet("Sales_Ledger").upsert(['date', 'source', 'type_id', 'contract_id'], sells, true);
    }

    if (buys.length > 0) {
      ML.forSheet("Material_Ledger").upsert(['date', 'source', 'type_id', 'contract_id'], buys, true);
    }

    // Keep rows that haven't been processed
    const remainingRows = data.filter(row => {
      if (!row[0]) return false;
      const entry = JSON.parse(row[1]);
      const d = entry.data;

      let id = 0;
      if (d.source === 'TRANSACTION') id = Math.floor(Number(d.transaction_id || 0));
      else if (d.source === 'JOURNAL' && (String(d.context_id_type || "").toLowerCase() === 'transaction_id' || String(d.context_id_type || "").toLowerCase() === 'market_transaction_id')) id = Math.floor(Number(d.context_id || 0));

      return !processedIds.has(id);
    });

    bufferSheet.getRange(2, 1, Math.max(1, bufferSheet.getLastRow() - 1), 3).clearContent();
    if (remainingRows.length > 0) {
      bufferSheet.getRange(2, 1, remainingRows.length, 3).setValues(remainingRows);
    }

    log.info(`Execution Summary - Held: ${remainingRows.length} | Buys: ${buys.length} | Sells: ${sells.length}`);

  } finally {
    if (needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss);
  }
}



function Recover_All_Historical_Data() {
  const log = LoggerEx.withTag('RECOVERY');
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const bufferSheet = ss.getSheetByName("_Internal_Ledger_Buffer");
  const client = GESI.getClient(getCorpAuthChar(ss));

  const bufferRows = [];
  const NOW = new Date().getTime();

  log.info("Starting brute-force Journal recovery...");
  client.setFunction('corporations_corporation_wallets_division_journal');
  let page = 1;
  while (true) {
    let req = client.buildRequest({ division: 3, page: page });
    let resp = UrlFetchApp.fetch(req.url, { method: 'get', headers: req.headers, muteHttpExceptions: true });

    if (resp.getResponseCode() !== 200) break;
    let data = JSON.parse(resp.getContentText());
    if (!data || data.length === 0) break;

    data.forEach(j => {
      // FIX: Added ts: NOW inside the JSON object
      bufferRows.push([String(j.id), JSON.stringify({ data: { ...j, source: 'JOURNAL' }, status: 'RECOVERED', ts: NOW }), NOW]);
    });
    log.info(`Fetched Journal Page ${page} (${data.length} records)`);
    page++;
  }

  log.info("Starting brute-force Transaction recovery...");
  client.setFunction('corporations_corporation_wallets_division_transactions');
  let fromId = null;
  while (true) {
    let params = { division: 3 };
    if (fromId) params.from_id = fromId; // This is the crucial fix for Transactions

    let req = client.buildRequest(params);
    let resp = UrlFetchApp.fetch(req.url, { method: 'get', headers: req.headers, muteHttpExceptions: true });

    if (resp.getResponseCode() !== 200) break;
    let data = JSON.parse(resp.getContentText());
    if (!data || data.length === 0) break;

    data.forEach(t => {
      // FIX: Added ts: NOW inside the JSON object
      bufferRows.push([String(t.transaction_id), JSON.stringify({ data: { ...t, source: 'TRANSACTION' }, status: 'RECOVERED', ts: NOW }), NOW]);
    });

    // Find the oldest ID in this batch to walk backward on the next loop
    let oldestIdInBatch = Math.min(...data.map(t => t.transaction_id));
    fromId = oldestIdInBatch;
    log.info(`Fetched Transactions down to ID ${fromId} (${data.length} records)`);
  }

  // Dump it all into the buffer at once
  if (bufferRows.length > 0) {
    const startRow = Math.max(2, bufferSheet.getLastRow() + 1);
    bufferSheet.getRange(startRow, 1, bufferRows.length, 3).setValues(bufferRows);
    log.info(`SUCCESS: Dumped ${bufferRows.length} historical records into the Buffer.`);
  } else {
    log.info("No data found to recover.");
  }
}

/**
 * CALCULATE ESI CACHE TTL
 * Returns seconds remaining until ESI refresh.
 */
function _getEsiCacheTTL(response) {
  const headers = response.getHeaders();
  const expires = headers['Expires'] || headers['expires'];

  if (!expires) return 3600; // Default to 1 hour if header is missing

  const now = new Date().getTime();
  const expiry = new Date(expires).getTime();
  const secondsLeft = Math.floor((expiry - now) / 1000);

  // Google CacheService limit is 21600 (6 hours). ESI is usually 3600.
  return Math.max(0, Math.min(secondsLeft, 21600));
}


function _fetchCorpOrdersConcurrently(authName) {
  const log = LoggerEx ? LoggerEx.withTag('CORP_ORDERS') : console;
  const STANDARD_ORDER_HEADERS = [
    "duration", "escrow", "is_buy", "issued", "issued_by",
    "location_id", "min_volume", "order_id", "price", "range",
    "region_id", "type_id", "volume_remain", "volume_total", "wallet_division"
  ];

  // 1. Auth & Corp ID Resolution
  const authClient = GESI.getClient(authName);
  const charData = GESI.getCharacterData ? GESI.getCharacterData(authName) : null;
  const corpId = charData ? charData.corporation_id : null;
  
  if (!corpId) {
    log.error(`Could not resolve Corp ID for '${authName}'.`);
    return [STANDARD_ORDER_HEADERS];
  }

  // 2. The One-Liner (ESI Module handles pagination internally)
  const result = ESI.forEndpoint(authClient, 'corporations_corporation_orders').get({
    corporation_id: corpId
  });

  if (result.error) {
    log.error(`ESI Fetch failed: ${result.error}`);
    return [STANDARD_ORDER_HEADERS];
  }

  // 3. Format and Return
  const formatRow = (obj) => {
    return STANDARD_ORDER_HEADERS.map(key => {
      const val = obj[key];
      if (key === "issued") return val ? new Date(val) : "";
      if (key === "is_buy") {
        const buyFlag = obj.hasOwnProperty('is_buy_order') ? obj.is_buy_order : obj.is_buy;
        return (buyFlag === true || buyFlag === 1 || String(buyFlag).toLowerCase() === "true") ? "TRUE" : "FALSE";
      }
      return val !== undefined ? val : "";
    });
  };

  return [STANDARD_ORDER_HEADERS].concat(result.data.map(formatRow));
}

/**
 * Reads external loot sheet, filters for non-null items, and sorts the result.
 * This completely replaces the slow QUERY(IMPORTRANGE()) formula.
 * @returns {Object|null} { header: string[], rows: any[][], h: Object } or null on failure.
 */
function _fetchProcessedLootData() {
  const log = LoggerEx.withTag('LOOT_SYNC');

  try {
    // 1. Open external sheet
    const externalSs = SpreadsheetApp.openById(EXTERNAL_LOOT_SHEET_ID);
    const sourceSheetName = EXTERNAL_LOOT_RANGE.split('!')[0];
    const sourceRange = EXTERNAL_LOOT_RANGE.split('!')[1];

    if (!externalSs) {
      log.error('External Loot Sheet not found.', { id: EXTERNAL_LOOT_SHEET_ID });
      return null;
    }

    // 2. Read the entire required range from the external source
    const externalSheet = externalSs.getSheetByName(sourceSheetName);
    if (!externalSheet) {
      log.error('External Loot Sheet not found.', { name: sourceSheetName });
      return null;
    }

    // Use the specified range (A:D in this case)
    const values = externalSheet.getRange(sourceRange).getValues();

    if (values.length < 2) { // Need at least header + 1 row
      log.warn('External loot source returned insufficient data (less than 1 data row).');
      return { sh: externalSheet, header: values[0] || [], rows: [], h: {} };
    }

    const header = values[0];
    let rows = values.slice(1);

    // Determine which column is Col1 (the first column, index 0)
    const Col1_Index = 0;

    // 3. Filter: WHERE Col1 IS NOT NULL
    const filteredRows = rows.filter(row => row[Col1_Index] != null && row[Col1_Index] !== "");

    // 4. Sort: Order By Col1 DESC
    filteredRows.sort((a, b) => {
      const valA = a[Col1_Index];
      const valB = b[Col1_Index];

      // Simple descending comparison (assumes sortable data type)
      if (valA > valB) return -1;
      if (valA < valB) return 1;
      return 0;
    });

    log.info('Successfully fetched, filtered, and sorted external loot data.', { rows: filteredRows.length });

    // 5. Return in the same structure as _getData_
    const h = {};
    for (let i = 0; i < header.length; i++) { h[String(header[i]).trim()] = i; } // 0-based index map

    return { sh: externalSheet, header: header, rows: filteredRows, h: h };

  } catch (e) {
    log.error('Failed to fetch and process external loot data:', e);
    return null; // Return null on any catastrophic failure (Sheet/File not found)
  }
}


function _isoDate(d) {
  if (!d) return ""; // Do not hallucinate 'Now' for blank ESI dates
  const validDate = new Date(d);
  return !isNaN(validDate.getTime()) ? validDate : "";
}

function FORCE_RESCAN_LAST_YEAR() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const LAST_TXN_KEY = 'CORP_LAST_TRANSACTION_ID';

  // EVE Transaction IDs are sequential. 
  // If your current ID is, say, 6,800,000,000, 
  // one year ago was roughly 5,500,000,000.
  const ONE_YEAR_AGO_ID = "5500000000";

  SCRIPT_PROP.setProperty(LAST_TXN_KEY, ONE_YEAR_AGO_ID);
  console.log("Anchor reset to " + ONE_YEAR_AGO_ID + ". The next maintenance tick will now fetch historical data.");
}

function _toIntOrNull(v) {
  if (v == null) return null;
  var s = String(v).trim().replace(/[^\d]/g, '');
  if (!s) return null;
  var n = parseInt(s, 10);
  return (Number(n) === n && isFinite(n)) ? n : null;
}

/** Reads a config value from a Named Range, falling back to a default value. */
function _getNamedOr_(name, fallback) {
  try {
    const ss = SpreadsheetApp.getActiveSpreadsheet();
    const r = ss.getRangeByName(name);
    if (!r) return fallback;
    const v = String(r.getValue()).trim();
    return v !== '' ? v : fallback;
  } catch (e) { return fallback; }
}


// Lookback days resolver (Named Range "LOOKBACK_DAYS" -> default)
function getLookbackDays(ss) { // ADDED ss ARGUMENT
  ss = ss || SpreadsheetApp.getActiveSpreadsheet(); // Fallback to ensure 'ss' is defined
  var v = null;
  try {
    var nr = ss.getRangeByName('LOOKBACK_DAYS');
    if (nr) v = nr.getValue();
  } catch (_) { }
  var n = parseInt(v, 10);
  if (!(Number(n) === n && isFinite(n))) n = CONTRACT_LOOKBACK_DAYS;
  if (n < 1) n = 1;
  if (n > 365) n = 365;
  return n;
}

/* Per-DOCUMENT cache for authenticated character names */
function getCharNamesFast() {
  // NEW: Return cached value if available during this execution
  if (_cachedAuthNames) {
    return _cachedAuthNames;
  }

  // Directly call the global GESI function. GESI handles its own caching
  // via ScriptProperties or other mechanisms.
  var namesFn =
    (GESI && typeof GESI.getAuthenticatedCharacterNames === 'function')
      ? GESI.getAuthenticatedCharacterNames
      : (typeof getAuthenticatedCharacterNames === 'function'
        ? getAuthenticatedCharacterNames
        : null);

  if (!namesFn) throw new Error('getAuthenticatedCharacterNames not found (GESI or global).');

  const names = namesFn() || [];
  _cachedAuthNames = names; // Cache for rest of execution
  return names;
}

// Resolve corp auth character (override -> GESI.name -> NamedRange/Utility -> first authed)
function getCorpAuthChar(ss) { // ADDED ss ARGUMENT
  var log = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('GESI') : console;
  // --- PHASE 1: FASTEST EXIT (In-Memory Cache / Persistent Property) ---
  if (_cachedAuthChar) {
    return _cachedAuthChar;
  }

  const props = PropertiesService.getScriptProperties();
  const persistedChar = props.getProperty(_CORP_AUTH_CHAR_PROP);

  if (persistedChar) {
    _cachedAuthChar = persistedChar;
    return persistedChar;
  }

  // --- CUSTOM FUNCTION SAFETY CHECK ---
  // If we are in a custom function and have no cache, DO NOT try the slow path.
  if (!ss) {
    try {
      // This explicitly throws an error when run from a sheet cell
      SpreadsheetApp.getActiveRange();
    } catch (e) {
      // The error proves we are in a custom function! Bail out fast.
      return (GESI && GESI.name) || '';
    }
  }

  // --- PHASE 2: EXPENSIVE RESOLUTION (Sheet I/O / API Calls) ---
  const SAFE_CONSOLE_SHIM = {
    log: console.log,
    info: console.log, // <-- CRITICAL FIX: Ensures log.info() is callable
    warn: console.warn,
    error: console.error,
    startTimer: () => ({ stamp: () => { } })
  };
  const GESI_LOG = (typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('GESI_AUTH') : SAFE_CONSOLE_SHIM);
  const t = GESI_LOG.startTimer('getCorpAuthChar_SlowPath');

  try {

    const spreadsheet = ss || SpreadsheetApp.getActiveSpreadsheet(); // Fallback if ss is null/undefined
    log.info("Checking for Authized Corp Character (SLOW PATH)");

    // *** NEW: Spreadsheet Latency Check ***
    if (typeof _measureSpreadsheetLatency !== 'undefined') {
      const ssLatency = _measureSpreadsheetLatency();
      log.info(`[PERF] Spreadsheet Latency at start of SLOW PATH: ${ssLatency}ms`);
    }
    // *** END NEW ***

    var desired = "";

    // Helper function optimized for speed by caching Named Range lookups
    function _resolve(sh, spec) {
      if (!spec) return null;
      spec = String(spec).trim();

      // 1. Check value cache first (Fastest)
      if (_cachedNamedRanges[spec] !== undefined) {
        return _cachedNamedRanges[spec] != null ? String(_cachedNamedRanges[spec]).trim() : null;
      }

      var got = null;

      // 2. Perform expensive Sheet API calls

      // 2a. Try Named range lookup
      try {
        var nr = sh.getRangeByName(spec);
        if (nr) got = nr.getValue();
      } catch (_) {
        // Ignore
      }

      // 2b. Try Sheet!A1 reference lookup if 2a failed
      if (got == null && spec.indexOf('!') > 0) {
        var cut = spec.indexOf('!');
        var shn = spec.slice(0, cut);
        var a1 = spec.slice(cut + 1);

        if (sh) {
          try {
            got = sh.getSheetByName(shn).getRange(a1).getValue();
          } catch (_) { }
        }
      }

      // 3. Cache the resulting value and return
      // Use null to indicate "not found" or "no value" explicitly in the cache
      const resultValue = got != null && got !== "" ? got : null;
      _cachedNamedRanges[spec] = resultValue;

      return resultValue != null ? String(resultValue).trim() : null;
    }

    // 1. Try config override (fast, no GESI)
    if (typeof CORP_AUTH_CHARACTER !== 'undefined' && CORP_AUTH_CHARACTER != null) {
      desired = _resolve(spreadsheet, CORP_AUTH_CHARACTER);
    }

    // Convert null result from _resolve back to empty string for subsequent checks
    if (desired === null) desired = "";

    // 2. Try GESI's internal default (fast, usually a PropertyService lookup)
    if (!desired && GESI && GESI.getMainCharacter) {
      desired = String(GESI.getMainCharacter()).trim();
    }

    // 3. Try secondary config location (fast, no GESI)
    if (!desired) {
      desired = _resolve(spreadsheet, 'CORP_AUTH_CHAR');
      if (desired === null) desired = ""; // Ensure result is not null
    }

    // --- OPTIMIZED FALLBACK LOGIC ---

    // 4. Verification/Fallback: Only execute the slow GESI check if needed.
    // This check relies on the newly optimized getCharNamesFast()
    var names = getCharNamesFast();
    var fallback = names[0] || "";

    if (!desired) {
      // Case 1: No name found via fast methods (Steps 1-3). Use the GESI fallback.
      desired = fallback;

    } else {
      // Case 2: A name was found via config.
      // We still need to verify this name is authenticated by GESI.

      // If the name from the config (desired) is not in the official list, revert to default.
      if (names.indexOf(desired) === -1) {
        log.warn('Corp auth override not in authenticated names; falling back', { wanted: desired, using: fallback, list: names });
        desired = fallback;
      }
    }


    log.debug('corp auth character', { using: desired });

    // --- PHASE 3: CACHE AND PERSIST ---
    if (desired) {
      _cachedAuthChar = desired;
      props.setProperty(_CORP_AUTH_CHAR_PROP, desired); // Persist for future fast runs
    }

    t.stamp('Auth_Resolved'); // <-- ADDED STAMP

    return desired;
  } catch (e) {
    // If an error occurs (e.g. network/GESI), rely on GESI.name fallback
    LoggerEx.withTag('GESI').error('getCorpAuthChar failed during slow path:', e);
    t.stamp('Auth_Failed'); // <-- ADDED STAMP
    return (GESI && GESI.name) || '';
  }
}

function _charIdMap(ss) {
  if (typeof _cachedCharIdMap !== 'undefined' && _cachedCharIdMap !== null) return _cachedCharIdMap;
  if (ESI.isLocked()) return {};

  const cache = CacheService.getScriptCache();
  const storedMap = cache.get('CORP_CHAR_MAP');
  if (storedMap) {
    _cachedCharIdMap = JSON.parse(storedMap);
    return _cachedCharIdMap;
  }

  const log = LoggerEx.withTag('CHAR_MAP');
  const authToon = getCorpAuthChar(ss);
  if (!authToon) return {};

  const charIdMap = {};
  const authClient = GESI.getClient(authToon);
  const charData = GESI.getCharacterData(authToon);

  try {
    // 1. Fetch Roster
    const rosterService = ESI.forEndpoint(authClient, 'corporations_corporation_members');
    const rosterRes = rosterService.get({ corporation_id: charData.corporation_id });

    if (rosterRes.error) throw new Error(rosterRes.error);

    const memberIds = Array.isArray(rosterRes.data) ? rosterRes.data.filter(Number.isFinite) : [];
    if (memberIds.length === 0) throw new Error("No ESI member IDs found.");

    // 2. Resolve Names via POST (Fixed: Passing the array directly, no {ids: ...} wrapper)
    const nameService = ESI.forEndpoint(authClient, 'universe_names');
    const chunkSize = 1000;

    for (let i = 0; i < memberIds.length; i += chunkSize) {
      const chunk = memberIds.slice(i, i + chunkSize);

      // Fix: Passing chunk array directly as the payload
      const nameRes = nameService.post(chunk);

      if (nameRes.error) {
        log.error(`Name resolution failed: ${nameRes.error}`);
        continue;
      }

      if (Array.isArray(nameRes.data)) {
        nameRes.data.forEach(entry => {
          if (entry && entry.category === 'character' && entry.name && entry.id) {
            charIdMap[entry.name] = entry.id;
          }
        });
      }
    }
  } catch (e) {
    log.error('Error building character ID map:', e.message);
    const fallbackId = parseInt(_getNamedOr_('CORP_AUTH_CHAR_ID', null), 10);
    if (authToon && fallbackId && Number.isFinite(fallbackId)) charIdMap[authToon] = fallbackId;
  }

  _cachedCharIdMap = charIdMap;
  cache.put('CORP_CHAR_MAP', JSON.stringify(charIdMap), 21600);

  log.info(`Built and cached character ID map for ${Object.keys(charIdMap).length} members.`);
  return charIdMap;
}

// ==========================================================================================
// NORMALIZERS
//==========================================================================================

// Normalize CONTRACT LIST results -> [{ ch, c }]
function _normalizeCharContracts(res, names, idNameMap) { // NOTE: idNameMap is now required
  const LOG = Logger;
  var tuples = [];

  if (!res || !res.length) {
    LOG.warn('CHAR_NORM: No contract results found for normalization.');
    return tuples;
  }

  LOG.log(`CHAR_NORM: Starting normalization for ${names.length} authenticated tokens.`);

  // *** FIX 1: Per-Char Arrays (Primary GESI Output) ***
  if (Array.isArray(res[0]) && (res[0].length === 0 || (res[0].length > 0 && typeof res[0][0] === 'object'))) {
    LOG.log('CHAR_NORM: Using Object Array Normalization Logic.');
    for (var a = 0; a < names.length; a++) {
      var arr = res[a] || [];
      var fetchingCharName = names[a] || '';

      LOG.log(`CHAR_NORM: Processing token holder: ${fetchingCharName} (Found ${arr.length} contracts)`);

      for (var b = 0; b < arr.length; b++) {
        var cA = arr[b];
        if (!cA || typeof cA !== 'object') continue;
        if (cA.for_corporation === true) {
          continue;
        }
        // CRITICAL FIX: Ensure the ESI IDs are strings for lookup consistency
        const acceptorId = String(cA.acceptor_id);
        const issuerId = String(cA.issuer_id);

        // Default to the token holder's name (safety)
        let chA = fetchingCharName;

        // CRITICAL FIX: Prioritize attribution to the known ESI ID party
        if (idNameMap[acceptorId]) {
          // Priority 1: Contract is linked to an authenticated character via the Acceptor role.
          chA = idNameMap[acceptorId];
        } else if (idNameMap[issuerId]) {
          // Priority 2: Linked via the Issuer role.
          chA = idNameMap[issuerId];
        }

        // Final Push with the resolved name
        LOG.log(`CHAR_NORM: Contract ID ${cA.contract_id}: Resolved to ${chA}. Issuer ESI: ${issuerId}. Acceptor ESI: ${acceptorId}.`);
        tuples.push({ ch: String(chA), c: cA });
      }
    }
    return tuples;
  }

  // ... (The rest of the normalization logic should be reviewed but is not the source of the critical bug) ...

  LOG.log(`CHAR_NORM: Finished normalization. Generated ${tuples.length} tuples.`);
  return tuples;
}

// Corp list: same mapping, but force auth name (corp lists usually lack char names)
function _normalizeCorpContracts(res, corpAuthName) {
  var tuples = [];
  if (!res || !res.length) return tuples;

  // Headerless rows (Positional Array Output)
  if (Array.isArray(res[0]) && typeof res[0][0] !== 'string') {
    for (var r = 0; r < res.length; r++) {
      var row = res[r], c = {};
      var n = Math.min(row.length, GESI_CONTRACT_COLS.length);
      for (var k = 0; k < n; k++) c[GESI_CONTRACT_COLS[k]] = row[k];
      // FIX: Force attribution to corpAuthName
      tuples.push({ ch: String(corpAuthName), c: c });
    }
    return tuples;
  }

  // Tabular (Header row)
  if (Array.isArray(res[0]) && typeof res[0][0] === 'string') {
    var hdr = res[0];
    for (var i = 1; i < res.length; i++) {
      var row2 = res[i]; if (!Array.isArray(row2)) continue;
      var c2 = {};
      for (var j = 0; j < hdr.length; j++) c2[String(hdr[j]).trim()] = row2[j];
      // FIX: Force attribution to corpAuthName
      tuples.push({ ch: String(corpAuthName), c: c2 });
    }
    return tuples;
  }

  // Flat objects (Most common GESI format)
  if (typeof res[0] === 'object') {
    for (var m = 0; m < res.length; m++) {
      var cB = res[m]; if (!cB || typeof cB !== 'object') continue;
      // FIX: Force attribution to corpAuthName, regardless of GESI data
      tuples.push({ ch: String(corpAuthName), c: cB });
    }
  }
  return tuples;
}

function normalizeItemRows(rows) {
  if (!rows || !rows.length) return [];

  return rows.map(x => {
    return {
      is_included: !!x.is_included,
      is_singleton: !!x.is_singleton,
      quantity: Number(x.quantity || x.qty || 0),
      type_id: Number(x.type_id || x.typeId || 0),
      // Mapped directly to top-level ESI keys as per the schema
      runs: Number(x.runs || 0),
      me: Number(x.material_efficiency || 0),
      te: Number(x.time_efficiency || 0)
    };
  });
}


/**
 * UPGRADED: Character Contract Item Fetcher
 * Now returns the full esiClient result object, inheriting all quota management.
 */
function _fetchCharContractItems(charName, contractId) {
  const cid = _toIntOrNull(contractId);
  if (cid === null) return { error: "Invalid Contract ID", data: null };

  return esiClient(charName, 'characters_character_contracts_contract_items', {
    contract_id: cid
  });
}

/**
 * UPGRADED: Corporation Contract Item Fetcher
 * Now returns the full esiClient result object, inheriting all quota management.
 */
function _fetchCorpContractItems(charName, contractId) {
  const cid = _toIntOrNull(contractId);
  if (cid === null) return { error: "Invalid Contract ID", data: null };

  return esiClient(authToon, 'corporations_corporation_contracts_contract_items', {
    contract_id: cid
  });
}


// Per-USER cached items (partition by scope + auth name; HEADERS shape)
function getContractItemsCached(charName, contractId, force, forCorp) {
  if (force === void 0) force = false;
  var log = LoggerEx.withTag('GESI');

  var cid = _toIntOrNull(contractId);
  if (cid == null) {
    log.warn('getContractItemsCached: invalid contract_id', { char: charName, contractId: contractId });
    return [];
  }

  var authName = forCorp ? getCorpAuthChar() : String(charName);
  var scope = forCorp ? 'corp' : 'char';

  var c = CacheService.getUserCache();
  var k = 'gesi:items:' + (forCorp ? ('CORP:' + authName) : authName) + ':' + cid + ':' + scope + ':hdr';
  if (!force) {
    var hit = c.get(k);
    if (hit) return JSON.parse(hit);
  }

  var items = forCorp
    ? _fetchCorpContractItems(authName, cid)
    : _fetchCharContractItems(authName, cid);

  c.put(k, JSON.stringify(items || []), GESI_TTL.items);
  return items || [];
}

// ==========================================================================================
// RAW SHEET FUNCTIONS (mirror the same signature/shape as invoke fetchers)
// ==========================================================================================
function raw_characters_character_contract_items(contract_id, charName) {
  const cid = _toIntOrNull(contract_id);
  if (cid === null) return [];

  const authClient = GESI.getClient(charName);
  const service = ESI.forEndpoint(authClient, 'characters_character_contracts_contract_items');
  const result = service.get({ contract_id: cid });

  return result.error ? [] : result.data;
}

function raw_corporations_corporation_contracts_contract_items(contract_id, charName) {
  const cid = _toIntOrNull(contract_id);
  if (cid === null) return [];

  const authClient = GESI.getClient(charName);
  const service = ESI.forEndpoint(authClient, 'corporations_corporation_contracts_contract_items');
  const result = service.get({ contract_id: cid });

  return result.error ? [] : result.data;
}

function _pickCharForContract(candidates, contractRow, idMap) {
  // candidates: array of { ch, c } for the same contract_id
  // contractRow: any one of those (for acceptor_id, etc.)
  // idMap: { name -> character_id } from CharIDMap()
  if (!candidates || !candidates.length) return '';

  var acc = String(contractRow.acceptor_id || '').trim();
  if (acc && idMap) {
    for (var i = 0; i < candidates.length; i++) {
      var ch = candidates[i].ch || '';
      if (ch && idMap[ch] && String(idMap[ch]) === acc) return ch;
    }
  }
  // fallback: first seen
  return candidates[0].ch || '';
}


// ==========================================================================================
// START LEDGER FUNCTIONS
// ==========================================================================================

/**
 * --- Raw_loot -> Material_Ledger (UUID Delta Mode) ---
 * Generates unique Transaction IDs for every delta.
 * CRITICAL FIX: Only saves the snapshot if the ledger write succeeds.
 */
function _runLootDeltaImport(ss, lootData, asOfDate, sourceLabel, writeNegatives) {
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const dateStr = asOfDate ? _isoDate(asOfDate) : _isoDate(Date.now());
  const source = sourceLabel || 'LOOT';
  const allowNeg = !!writeNegatives;
  const charName = getCorpAuthChar(ss);
  const log = LoggerEx.withTag('LOOT_DELTA');

  if (!lootData || lootData.rows.length === 0) {
    log.log('loot_import', { status: 'Skipped: No fresh external loot data available.' });
    return 0;
  }
  const loot = lootData;
  const MaterialLedger = ML.forSheet(LEDGER_BUY_SHEET);

  const h = loot.h;
  const cTid = h['type_id'], cQty = h['total_quantity'], cBuy = h['weighted_average_buy'], cVal = h['weighted_average_value'];

  if ([cTid, cQty, cBuy, cVal].some(v => v == null)) {
    throw new Error(`'${RAW_LOOT_SHEET}' must have headers: type_id, total_quantity, weighted_average_buy, weighted_average_value`);
  }

  // 1. Calculate Deltas
  const curr = new Map();
  for (const r of loot.rows) {
    const tid = Number(r[cTid]) || 0;
    if (!tid) continue;
    const qty = Number(String(r[cQty]).replace(/[^\d.\-]/g, '')) || 0;
    const sBuy = String(r[cBuy] == null ? '' : r[cBuy]).replace(/[^\d.\-]/g, '').replace(/,/g, '');
    const buy = isFinite(Number(sBuy)) ? Number(sBuy) : 0;
    const sVal = String(r[cVal] == null ? '' : r[cVal]).replace(/[^\d.\-]/g, '').replace(/,/g, '');
    const val = isFinite(Number(sVal)) ? Number(sVal) : 0;
    curr.set(tid, { qty, val, buy });
  }

  const props = PropertiesService.getDocumentProperties();
  const prevRaw = props.getProperty(SNAP_KEY);
  const prev = prevRaw ? JSON.parse(prevRaw) : {};

  const allTids = new Set([...curr.keys(), ...Object.keys(prev).map(x => Number(x) || 0)]);
  const outRows = [];

  for (const tid of allTids) {
    const cur = curr.get(tid) || { qty: 0, val: 0, buy: 0 };
    const p = prev[String(tid)] || { qty: 0, val: 0 };
    const dq = cur.qty - (Number(p.qty) || 0);

    if (dq === 0) continue;
    if (!allowNeg && dq < 0) continue;

    let unit = (isFinite(cur.val) && cur.qty > 0) ? (cur.val / cur.qty) : (cur.buy || 0);
    if (!(unit > 0)) unit = cur.buy || 0;

    // GENERATE UUID for every delta event
    outRows.push({
      date: dateStr,
      type_id: tid,
      qty: dq,
      unit_value_filled: unit,
      source: source,
      char: charName,
      contract_id: Utilities.getUuid() // Unique Transaction ID
    });
  }

  if (outRows.length === 0) {
    log.log('loot_import', { status: 'Skipped ledger update: No deltas found.', processed: allTids.size, date: dateStr });
    return 0;
  }

  // 2. Write to Ledger (Using UUID key = Append)
  // FIX: Check the result object to ensure success BEFORE saving snapshot.
  const result = MaterialLedger.upsert(['date', 'source', 'type_id', 'contract_id'], outRows);
  const count = result.rows || 0;

  // 3. Safe Snapshot Save
  if (result.status === "SUCCESS" || count > 0) {
    const nextSnap = {};
    for (const [tid, cur] of curr.entries()) {
      nextSnap[String(tid)] = { qty: cur.qty, val: cur.val };
    }
    props.setProperty(SNAP_KEY, JSON.stringify(nextSnap));

    log.log('loot_import', { appended: count, status: "SUCCESS", date: dateStr });
  } else {
    log.warn('loot_import', { status: "WRITE_FAILED_SNAPSHOT_NOT_SAVED", error: result.errorMerssage });
  }

  return count;
}

/**
 * PERMANENT SHARDING (PropertiesService)
 * Mirror of Utility logic but for permanent storage.
 * Uses 8000 byte chunks to stay safely under the 9KB individual property limit.
 */
function _chunkAndPut_Permanent(key, content) {
  const props = PropertiesService.getScriptProperties();
  const MAX_SIZE = 8000;

  const chunks = [];
  let offset = 0;
  while (offset < content.length) {
    chunks.push(content.substr(offset, MAX_SIZE));
    offset += MAX_SIZE;
  }

  const payload = {};
  chunks.forEach((c, i) => { payload[key + "_" + i] = c; });
  payload[key + "_chunks"] = chunks.length.toString();

  props.setProperties(payload);
  return true;
}

function _getAndDechunk_Permanent(key) {
  const props = PropertiesService.getScriptProperties();
  const countStr = props.getProperty(key + "_chunks");

  if (!countStr) return props.getProperty(key);

  const count = parseInt(countStr, 10);
  let full = "";
  for (let i = 0; i < count; i++) {
    const part = props.getProperty(key + "_" + i);
    if (!part) return null;
    full += part;
  }
  return full;
}

function emergencyPropertyCleanup() {
  const props = PropertiesService.getScriptProperties();
  const allKeys = props.getKeys();
  // Find all shards related to the ledger buffer
  const shards = allKeys.filter(k => k.startsWith('LEDGER_PENDING_BUFFER'));
  shards.forEach(k => props.deleteProperty(k));
  console.log(`✅ Cleanup Complete. Deleted ${shards.length} property shards. Quota restored.`);
}







/**
 * NEW: Helper function to run all loot delta processing steps
 * Assumes lock is held by caller.
 */
function runLootDeltaPhase(ss) {
  const log = LoggerEx.withTag('MASTER_SYNC');
  let lootData = null;
  try {
    log.info('Running _fetchProcessedLootData (External Data Sync)...');
    lootData = _fetchProcessedLootData();
  } catch (e) {
    log.error('_fetchProcessedLootData FAILED', e.message);
  }

  try {
    if (lootData) {
      log.info('Executing loot delta calculation and import...');
      // Assumes _runLootDeltaImport no longer uses internal withSheetLock
      _runLootDeltaImport(ss, lootData, null, null, false);
    } else {
      log.warn('Skipping loot delta import: Loot data could not be fetched/processed.');
    }
  } catch (e) {
    log.error('Loot Delta Phase FAILED', e.message);
  }
}

/**
 * Resets the Contract Sync Anchor and Locks.
 * Forces the next 'runContractLedgerPhase' to re-scan ALL contracts
 * within the lookback window (e.g., 30 days) and re-process them.
 */
function resetContractSync() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const lock = LockService.getScriptLock();
  if (!lock.tryLock(5000)) {
    Logger.log("⚠️ Reset failed: Script Lock busy. Try again.");
    return;
  }

  try {
    const SCRIPT_PROP = PropertiesService.getScriptProperties();

    // Keys defined in GESI Extentions.js
    const ANCHOR_KEY = 'lastProcessedContractId'; // PROP_KEY_LAST_CONTRACT_ID
    const LEASE_KEY = 'contractJobLeaseUntil';    // PROP_KEY_CONTRACT_LEASE
    const COGS_FLAG = 'cogsJobStep';              // PROP_KEY_COGS_STEP

    // 1. Delete the Anchor (Forces re-scan of old contracts)
    SCRIPT_PROP.deleteProperty(ANCHOR_KEY);

    // 2. Clear Lease (Unblocks execution if stuck)
    SCRIPT_PROP.deleteProperty(LEASE_KEY);

    // 3. Clear COGS Flag (Resets finalizer state)
    SCRIPT_PROP.deleteProperty(COGS_FLAG);
    SCRIPT_PROP.deleteProperty('PROCESSED_CONTRACT_IDS');

    // Add this inside your try block, before the releaseLock()
    const sheetsToClear = [CONTRACTS_RAW_SHEET, CONTRACT_ITEMS_RAW_SHEET];
    sheetsToClear.forEach(name => {
      const sh = ss.getSheetByName(name);
      if (sh) {
        const lastRow = sh.getLastRow();
        if (lastRow > 1) {
          sh.getRange(2, 1, lastRow - 1, sh.getLastColumn()).clearContent();
          Logger.log(`✅ Cleared data in ${name}`);
        }
      }
    });

    Logger.log("✅ Contract Sync Reset Complete.");
    Logger.log("Next run will process ALL contracts in the lookback window.");

    if (typeof SpreadsheetApp !== 'undefined') {
      SpreadsheetApp.getUi().alert("Contract Sync Reset. The next run will be a full re-scan.");
    }



  } catch (e) {
    Logger.log("❌ Error resetting contract sync: " + e.message);
  } finally {
    lock.releaseLock();
  }
}

function getEndpointForContract(availability) {
  // If the contract is public, we use the specific endpoint 
  // that you've confirmed provides the BP data.
  return (availability === 'public')
    ? 'contracts_public_items_contract_id'
    : 'corporations_corporation_contracts_contract_items';
}

/**
 * Purges Ledgered and Toxic contracts from RAW sheets.
 * Re-architected to scan Parents first, ensuring empty/deleted contracts are caught.
 */
function purgeContractsWithLedgeredStatus(ss) {
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const log = LoggerEx.withTag('PURGE_CONTRACTS');

  const itemsSheet = ss.getSheetByName('Contract Items (RAW)');
  const contractsSheet = ss.getSheetByName('Contracts (RAW)');

  if (!itemsSheet || !contractsSheet) {
    log.error("Could not find required sheets (Items/Contracts RAW).");
    return;
  }

  const processedCids = new Set();
  const toxicStatuses = ['deleted', 'expired', 'cancelled', 'rejected', 'failed'];

  // --- 1. SCAN CONTRACTS (RAW) AS THE SOURCE OF TRUTH ---
  const contData = contractsSheet.getDataRange().getValues();
  if (contData.length > 1) {
    const hCont = contData[0].map(h => String(h).trim().toLowerCase());
    const cCidIdx = hCont.indexOf('contract_id');
    const cSyncStatIdx = hCont.indexOf('sync_status');
    const cStatIdx = hCont.indexOf('status'); // The ESI Status

    if (cCidIdx === -1 || cSyncStatIdx === -1 || cStatIdx === -1) {
      log.error("Critical Failure: Missing 'contract_id', 'sync_status', or 'status' in Contracts (RAW).");
      return;
    }

    const contractsToKeep = [contData[0]]; // Keep Headers

    for (let i = 1; i < contData.length; i++) {
      const cid = String(contData[i][cCidIdx]).trim();
      const syncStatus = String(contData[i][cSyncStatIdx]).toUpperCase();
      const eveStatus = String(contData[i][cStatIdx]).toLowerCase();

      // THE HIT CRITERIA: Successfully Ledgered OR an ESI Trash Status
      if (syncStatus === 'LEDGERED' || toxicStatuses.includes(eveStatus)) {
        processedCids.add(cid);
      } else {
        contractsToKeep.push(contData[i]);
      }
    }

    // --- ATOMIC WRITE FOR CONTRACTS ---
    if (contData.length !== contractsToKeep.length) {
      contractsSheet.getRange(1, 1, contractsToKeep.length, contData[0].length).setValues(contractsToKeep);
      contractsSheet.getRange(contractsToKeep.length + 1, 1, contData.length - contractsToKeep.length, contData[0].length).clearContent();
      log.info(`Purged ${processedCids.size} parent contracts (Ledgered + Toxic).`);
    } else {
      log.info("No parent contracts required purging.");
    }
  }

  // --- 2. CLEAN UP CONTRACT ITEMS (RAW) ---
  // Only bother scanning items if we actually found parents to delete
  if (processedCids.size > 0) {
    const itemsData = itemsSheet.getDataRange().getValues();
    if (itemsData.length <= 1) return; // Nothing to clean

    const hItems = itemsData[0].map(h => String(h).trim().toLowerCase());
    const cidIdxItems = hItems.indexOf('contract_id');
    const statIdxItems = hItems.indexOf('sync_status'); // Fallback check

    if (cidIdxItems === -1) {
      log.error("Critical Failure: 'contract_id' missing in Items sheet.");
      return;
    }

    const itemsToKeep = [itemsData[0]]; // Keep Headers
    let purgedItemsCount = 0;

    for (let i = 1; i < itemsData.length; i++) {
      const cid = String(itemsData[i][cidIdxItems]).trim();
      const itemSyncStatus = statIdxItems !== -1 ? String(itemsData[i][statIdxItems]).toUpperCase() : '';

      // Destroy if the parent was destroyed, OR if it's somehow marked LEDGERED directly
      if (processedCids.has(cid) || itemSyncStatus === 'LEDGERED') {
        purgedItemsCount++;
      } else {
        itemsToKeep.push(itemsData[i]);
      }
    }

    // --- ATOMIC WRITE FOR ITEMS ---
    if (itemsData.length !== itemsToKeep.length) {
      itemsSheet.getRange(1, 1, itemsToKeep.length, itemsData[0].length).setValues(itemsToKeep);
      itemsSheet.getRange(itemsToKeep.length + 1, 1, itemsData.length - itemsToKeep.length, itemsData[0].length).clearContent();
      log.info(`Purged ${purgedItemsCount} child items linked to removed contracts.`);
    }
  }
}

function syncContracts(ss, charIdMap) {
  const log = LoggerEx.withTag('GESI_CONTRACTS');
  const SCRIPT_PROP = PropertiesService.getScriptProperties();

  if (ESI.isLocked()) {
    log.error("Aborting Sync: Global ESI Quota Locked.");
    return { contracts: 0 };
  }

  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const authToon = getCorpAuthChar(ss);

  // Create ONLY the Corp client here, because the Corp is always the authToon
  const corpClient = GESI.getClient(authToon);
  const corpService = ESI.forEndpoint(corpClient, EP_LIST_CORP);

  const LAST_CID = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_LAST_CONTRACT_ID) || '0', 10);
  let maxContractId = LAST_CID;

  // Cleaners: n for numbers (defaults to 0), s for strings (defaults to '')
  const n = (v) => Number(v) || 0;
  const s = (v) => String(v || '');

  // Using the corrected sync_status header
  const hdrC = ["char", "contract_id", "type", "sync_status", "issuer_id", "acceptor_id", "date_issued", "date_expired", "price", "reward", "collateral", "volume", "title", "availability", "start_location_id", "end_location_id"];
  const hdrI = ["char", "contract_id", "type_id", "quantity", "is_included", "is_singleton", "runs", "me", "te"];

  if (!charIdMap) charIdMap = _charIdMap(ss);
  const allNames = getCharNamesFast();
  const idNameMap = {};
  Object.entries(charIdMap).forEach(([name, id]) => { idNameMap[String(id)] = name; });

  let allTuples = [];
  log.info(`Syncing contract lists for ${allNames.length} characters + Corp...`);

  // --- PASS 1-3: DISCOVERY (FIXED SOURCE ROUTING) ---
  CONTRACT_STATUSES.forEach(status => {

    // 1. Fetch Personal Contracts using individual character tokens
    allNames.forEach(charName => {
      const localCharClient = GESI.getClient(charName);
      const charService = ESI.forEndpoint(localCharClient, EP_LIST_CHAR);
      const result = charService.get({ status: status });

      if (result.error) {
        if (result.error.includes("404")) {
          log.info(`[SKIP] No contracts for ${charName} (${status}) - 404 expected.`);
        } else {
          log.error(`[ESI_ERROR] ${charName} on ${status}: ${result.error}`);
        }
        return;
      }
      if (Array.isArray(result.data)) {
        // TAG AS PERSONAL
        const cTuples = _normalizeCharContracts([result.data], [charName], idNameMap);
        cTuples.forEach(t => t.isCorp = false);
        allTuples = allTuples.concat(cTuples);
      }
    });

    // 2. Fetch Corp Contracts using the Director token
    const corpResult = corpService.get({ status: status });
    if (corpResult.error) {
      if (!corpResult.error.includes("404")) log.error(`[ESI_ERROR] Corp failed on ${status}: ${corpResult.error}`);
    } else if (Array.isArray(corpResult.data)) {
      // TAG AS CORP
      const corpTuples = _normalizeCorpContracts(corpResult.data, authToon);
      corpTuples.forEach(t => t.isCorp = true);
      allTuples = allTuples.concat(corpTuples);
    }
  });

  // --- PASS 4: COMPILE ITEM REQUESTS (WITH TOXIC FILTER) ---
  const itemRequests = [];
  const validTuples = [];
  const seenCids = new Set();

  // The Hit List
  const toxicStatuses = ['deleted', 'expired', 'cancelled', 'rejected', 'failed'];

  for (const tuple of allTuples) {
    const cid = _toIntOrNull(tuple.c.contract_id);
    const status = String(tuple.c.status || '').toLowerCase();

    // IF IT IS TOXIC, SKIP IT ENTIRELY
    if (!cid || seenCids.has(cid) || (LAST_CID > 0 && cid <= LAST_CID) || toxicStatuses.includes(status)) {
      continue;
    }

    // Look at the exact source, completely ignore 'availability' strings.
    const endpoint = tuple.isCorp ? EP_ITEMS_CORP : EP_ITEMS_CHAR;

    itemRequests.push({
      cid: cid,
      char: tuple.isCorp ? authToon : tuple.ch,
      endpoint: endpoint,
      attributedChar: _getAttributedChar(tuple, idNameMap)
    });

    validTuples.push(tuple);
    seenCids.add(cid);
    if (cid > maxContractId) maxContractId = cid;
  }

  // --- PASS 5: ITEM FETCHING (FIXED AUTHENTICATION) ---
  const outC_Combined = [];
  const outI_Combined = [];

  // Format the Contract Headers safely
  validTuples.forEach(tuple => {
    const c = tuple.c;
    outC_Combined.push([
      tuple.ch, n(c.contract_id), s(c.type), s(c.status), n(c.issuer_id), n(c.acceptor_id),
      _isoDate(c.date_issued), _isoDate(c.date_expired), n(c.price), n(c.reward),
      n(c.collateral), n(c.volume), s(c.title), s(c.availability), n(c.start_location_id), n(c.end_location_id)
    ]);
  });

  itemRequests.forEach(req => {
    log.info(`[FETCHING] CID ${req.cid} via ${req.endpoint} for ${req.attributedChar}`);

    // GET THE CLIENT FOR THE SPECIFIC CHARACTER WHO OWNS THIS CONTRACT
    const specificClient = GESI.getClient(req.char);
    const itemService = ESI.forEndpoint(specificClient, req.endpoint);
    const res = itemService.get({ contract_id: req.cid });

    if (res.error && res.error.includes("404")) {
      log.info(`[EMPTY] CID ${req.cid} returned 404. Skipping.`);
      return;
    }

    if (res.error) {
      log.warn(`[FETCH_FAIL] CID ${req.cid} failed: ${res.error}`);
    } else if (Array.isArray(res.data) && res.data.length > 0) {
      log.info(`[FETCH_SUCCESS] CID ${req.cid} retrieved ${res.data.length} items.`);
      normalizeItemRows(res.data).forEach(item => {
        outI_Combined.push([req.attributedChar, req.cid, n(item.type_id), n(item.quantity), item.is_included ? 'TRUE' : 'FALSE', item.is_singleton ? 'TRUE' : 'FALSE', n(item.runs), n(item.me), n(item.te)]);
      });
    } else {
      log.info(`[EMPTY] CID ${req.cid} returned no item data.`);
    }
  });

  // --- PASS 6: SHEET WRITES (WITH ANESTHESIA) ---
  let needsWakeUp = false;
  try {
    if (typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

    if (outC_Combined.length > 0) {
      _appendData_(getOrCreateSheet(ss, CONTRACTS_RAW_SHEET, hdrC), hdrC, outC_Combined);
    }
    if (outI_Combined.length > 0) {
      _appendData_(getOrCreateSheet(ss, CONTRACT_ITEMS_RAW_SHEET, hdrI), hdrI, outI_Combined);
    }

    SCRIPT_PROP.setProperty(PROP_KEY_LAST_CONTRACT_ID, String(maxContractId));

  } catch (e) {
    log.error('Failed to append raw contract data:', e.message);
  } finally {
    if (needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss);
  }

  return { contracts: outC_Combined.length, buyData: { contracts: outC_Combined, items: outI_Combined }, saleData: { contracts: [], items: [] } };
}

function _getAttributedChar(tuple, idNameMap) {
  const c = tuple.c;
  const isInternalIssuer = idNameMap[String(c.issuer_id)];
  const isInternalAcceptor = idNameMap[String(c.acceptor_id)];
  const isCorpAcceptor = (c.availability === 'corporation');
  return (isInternalIssuer && (isInternalAcceptor || isCorpAcceptor))
    ? (isInternalAcceptor ? idNameMap[String(c.acceptor_id)] : idNameMap[String(c.issuer_id)])
    : tuple.ch;
}

/**
 * Worker function that executes the expensive COGS allocation logic.
 * NOW CHECKS DEPENDENCIES: Loot, Journal, and Contracts must have run at least once.
 */
function _runRebuildContractUnitCostsWorker() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const funcName = '_runRebuildContractUnitCostsWorker';

  const workerFunc = () => {
    const log = LoggerEx.withTag('COGS_WORKER');

    // Check against the exact keys the Orchestrator writes
    const getTs = (jobName) => parseInt(SCRIPT_PROP.getProperty('LAST_RUN_' + jobName) || '0', 10);

    const lastLoot = getTs('runLootDeltaPhase');
    const lastJournal = getTs('TransactionsAndJournalSync');
    const lastContracts = getTs('runContractLedgerPhase');

    const now = Date.now();
    const MAX_AGE_MS = 24 * 60 * 60 * 1000; // 24 Hours in milliseconds

    const isStale = (ts) => ts === 0 || (now - ts > MAX_AGE_MS);

    // Strict Freshness Dependency check
    if (isStale(lastLoot) || isStale(lastJournal) || isStale(lastContracts)) {
      log.warn('ABORTING COGS: Dependencies missing or too old (Stale Data).');
      log.info(`Debug: LootStale=${isStale(lastLoot)}, JournalStale=${isStale(lastJournal)}, ContractsStale=${isStale(lastContracts)}`);
      SCRIPT_PROP.deleteProperty('cogsJobStep');
      return;
    }

    if (SCRIPT_PROP.getProperty('cogsJobStep') !== 'FINALIZING') {
      log.warn('COGS worker called outside FINALIZING state. Aborting.');
      return;
    }

    rebuildContractUnitCosts(ss);
    SCRIPT_PROP.deleteProperty('cogsJobStep');
    log.info('COGS Finalization complete.');
  };

  executeWithTryLock(workerFunc, funcName);
}

/**
 * Checks for and restarts the COGS unit cost worker if the flag is still set.
 * Assumes scheduleOneTimeTrigger is defined.
 */
function _nudgeCogsFinalizer() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  // NOTE: Assuming PROP_KEY_COGS_STEP and STATE_FLAGS_COGS are defined or globally available.

  if (SCRIPT_PROP.getProperty('cogsJobStep') === 'FINALIZING') {
    const lock = LockService.getScriptLock();

    // Check if the worker's lock is currently available (meaning the worker is not running).
    if (lock.tryLock(0)) {
      lock.releaseLock();
      console.log(`Orchestrator: COGS Finalizer flag found. Re-queuing worker.`);
      // Assumes _runRebuildContractUnitCostsWorker and scheduleOneTimeTrigger are global.
      scheduleOneTimeTrigger("_runRebuildContractUnitCostsWorker", 5000);
      return true;
    } else {
      console.log(`Orchestrator: COGS Finalizer flag set but worker lock is busy. Skipping nudge.`);
    }
  }
  return false;
}

// ==========================================================================================
// UNIVERSAL CONTRACT ROUTER (DRY)
// ==========================================================================================

function _routeContractsToLedger(ss, charIdMap, dataObj, isSale, holdAnesthesia) {
  const log = LoggerEx.withTag('GESI');
  const actionTag = isSale ? 'sales_ledger' : 'material_ledger';

  if (!dataObj || !dataObj.contracts || dataObj.contracts.length === 0 || !dataObj.items || dataObj.items.length === 0) {
    log.log(`contracts->${actionTag}`, { status: 'Skipped: In-memory data is empty.' });
    return 0;
  }

  ss = ss || SpreadsheetApp.getActiveSpreadsheet();

  // 1. DYNAMIC CONFIGURATION
  const targetSheetName = isSale ? LEDGER_SALE_SHEET : LEDGER_BUY_SHEET;
  const targetSourceTag = isSale ? "SALE" : "CONTRACT";
  const qtyMultiplier = isSale ? -1 : 1;

  const LedgerAPI = ML.forSheet(targetSheetName, ss);

  const hC_Names = ["char", "contract_id", "type", "status", "issuer_id", "acceptor_id", "date_issued", "date_expired", "price"];
  const hI_Names = ["char", "contract_id", "type_id", "quantity", "is_included"];

  const ix = (arr, name) => arr.indexOf(name);

  // --- FIX THIS LINE ---
  const colC = {
    char: ix(hC_Names, "char"),
    contract_id: ix(hC_Names, "contract_id"),
    date_issued: ix(hC_Names, "date_issued"),
    price: ix(hC_Names, "price"),
    status: ix(hC_Names, "status") // ADD THIS
  };

  const colI = { contract_id: ix(hI_Names, "contract_id"), type_id: ix(hI_Names, "type_id"), quantity: ix(hI_Names, "quantity"), is_included: ix(hI_Names, "is_included") };

  const LOGGED_IN_CHARS = new Set(getCharNamesFast());
  const cids = new Set(dataObj.contracts.map(c => c[colC.contract_id]));

  const itemsByCid = {};
  for (const rowI of dataObj.items) {
    const cid = rowI[colI.contract_id];
    if (!cids.has(cid)) continue;
    if (!itemsByCid[cid]) itemsByCid[cid] = [];
    itemsByCid[cid].push({
      type_id: rowI[colI.type_id],
      qty: Number(rowI[colI.quantity] || 0),
      is_included: String(rowI[colI.is_included]).toUpperCase() === 'TRUE'
    });
  }

  const outRows = [];

  for (const rowC of dataObj.contracts) {
    const contractChar = String(rowC[colC.char] || "");
    const cid = String(rowC[colC.contract_id]);


    const status = String(rowC[colC.status] || "").toLowerCase();
    const toxicStatuses = ['deleted', 'expired', 'cancelled', 'rejected', 'failed'];
    if (toxicStatuses.includes(status)) {
      console.warn(`[REJECTED] Contract ${cid} (Status: ${status}) blocked from Ledger.`);
      continue; // Immediately skip to the next contract
    }
    // ---------------------------------

    if (!LOGGED_IN_CHARS.has(contractChar)) {
      console.warn(`[REJECTED] Contract ${cid}: Char '${contractChar}' not in active auth list.`);
      continue;
    }

    if (!itemsByCid[cid]) {
      console.warn(`[REJECTED] Contract ${cid}: No items found in itemsByCid map.`);
      continue;
    }

    const items = itemsByCid[cid];
    const issued = rowC[colC.date_issued] ? _isoDate(rowC[colC.date_issued]) : "";
    const rawPrice = Number(rowC[colC.price]) || 0;

    for (const it of items) {
      if (!it.is_included) {
        console.warn(`[SKIPPED ITEM] Contract ${cid}, Item ${it.type_id}: is_included is false.`);
        continue;
      }
      if (it.qty <= 0) {
        console.warn(`[SKIPPED ITEM] Contract ${cid}, Item ${it.type_id}: qty ${it.qty} <= 0.`);
        continue;
      }

      let base_unit_cost = (items.length > 0 && it.qty > 0) ? (rawPrice / items.length) / it.qty : 0;

      outRows.push({
        date: issued,
        type_id: Number(it.type_id),
        qty: Number(it.qty) * qtyMultiplier,
        unit_value_filled: base_unit_cost,
        source: targetSourceTag,
        contract_id: cid,
        char: contractChar
      });
    }
  }

  if (outRows.length === 0) return 0;

  let needsWakeUp = false;
  if (!holdAnesthesia && typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

  try {
    const keys = ['source', 'char', 'contract_id', 'type_id'];
    const upsertResult = LedgerAPI.upsert(keys, outRows, true);
    const count = upsertResult.rows !== undefined ? upsertResult.rows : upsertResult;

    log.log(`contracts->${actionTag}`, { appended_or_updated: count, processed_rows: outRows.length });
    return count;
  } catch (e) {
    log.error(`_routeContractsToLedger (${actionTag}) WRITE FAILED`, e.message);
    throw e;
  } finally {
    if (!holdAnesthesia && needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss);
  }
}

// ==========================================================================================
// CONTRACT UNIT COST ALLOCATION HELPERS
// ==========================================================================================

/**
 * Helper to build the reference price map (Tier 1 & 2 prices).
 * Reads the 'market price Tracker' sheet. (Resolves missing dependency)
 */
function _buildRefPriceMap_(ss) {
  const log = LoggerEx.withTag('CONTRACT_ALLOC');
  const TRACKER_SHEET_NAME = 'market price Tracker';
  const HEADERS = ['type_id_filtered', 'Median Buy', 'Median Sell'];

  // Assumes _getData_ is robust (returns {rows, h})
  const dataObj = _getData_(ss, TRACKER_SHEET_NAME);
  if (dataObj.rows.length === 0) {
    log.warn(`[RefPrice] Tracker sheet is empty. Cannot allocate costs.`);
    return new Map();
  }

  const h = dataObj.h;
  const refMap = new Map();

  const cTid = h[HEADERS[0]];
  const cBuy = h[HEADERS[1]];
  const cSell = h[HEADERS[2]];

  dataObj.rows.forEach(row => {
    const type_id = Number(row[cTid]);
    const medianBuyStr = String(row[cBuy]).replace(/[^0-9.]/g, '');
    const medianSellStr = String(row[cSell]).replace(/[^0-9.]/g, '');

    const buy = parseFloat(medianBuyStr) || 0;
    const sell = parseFloat(medianSellStr) || 0;

    if (type_id > 0 && (buy > 0 || sell > 0)) {
      refMap.set(type_id, { buy, sell });
    }
  });

  log.info(`[RefPrice] Built price map for ${refMap.size} items.`);
  return refMap;
}

/**
 * Reads the user's primary market configuration from the spreadsheet cells.
 * Uses 'Location List'!C3 for Location ID and 'Market Overview'!C8 for Location Type.
 */
function _getPrimaryMarketConfig(ss) {
  const log = LoggerEx.withTag('MARKET_CONFIG');
  let locationId = 0;
  let locationType = 'Region'; // Default safe assumption

  try {
    // 1. Location ID from 'Location List'!C3 
    const locSheet = ss.getSheetByName('Location List');
    if (locSheet) {
      locationId = Number(locSheet.getRange('C3').getValue());
    }

    // 2. Location Type from 'Market Overview'!C8 
    const marketSheet = ss.getSheetByName('Market Overview');
    if (marketSheet) {
      locationType = String(marketSheet.getRange('C8').getValue()).trim() || locationType;
    }

    if (!locationId || isNaN(locationId)) {
      // Fallback to Amarr Region ID if user's ID is missing (better than Jita)
      locationId = 10000043;
      log.warn(`Location ID from 'Location List'!C3 was invalid. Defaulting to ${locationId}.`);
    }

  } catch (e) {
    log.error(`Error reading market config: ${e.message}`);
  }

  return { locationId, locationType };
}



/**
 * Robustly cleans and converts a price string to a positive number.
 */
function _cleanPrice_(value) {
  if (value == null || value === 0 || value === '') return 0;
  // Aggressively strip all non-digit, non-decimal characters (e.g., commas, ISK).
  const cleaned = String(value).replace(/[^\d.]/g, '');
  const num = Number(cleaned);
  return (Number.isFinite(num) && num > 0) ? num : 0;
}

/**
 * Helper to fetch prices for items missing from the initial _buildRefPriceMap_
 * Checks Tier 2 (Tracker) and Tier 3 (Fuzzwork API, using dynamic location).
 *
 * NOTE: Assumes _getPrimaryMarketConfig is available and functional.
 */
function _getContractPriceFallbackMap(ss, missingTids) {
  const log = LoggerEx.withTag('CONTRACT_FALLBACK');
  const fallbackMap = new Map();

  if (missingTids.length === 0) return fallbackMap;

  // --- Tier 2: Read Local Market Tracker for Missing Items ---
  const TRACKER_SHEET_NAME = "market price Tracker";
  const ID_HEADER = 'type_id_filtered';
  const BUY_HEADER = 'Median Buy';
  const requiredTids = new Set(missingTids);
  const tidsForFuzzwork = [];

  try {
    // Read data for Tier 2 check (local tracker)
    const allData = _getData_(ss, TRACKER_SHEET_NAME);
    const h = allData.h;
    const cTid = h[ID_HEADER];
    const cBuy = h[BUY_HEADER];

    if (cTid != null && cBuy != null) {
      for (const row of allData.rows) {
        const type_id = Number(row[cTid]);

        if (requiredTids.has(type_id)) {
          // Normalize price string
          const priceStr = String(row[cBuy]).replace(/ISK/gi, '').replace(/,/g, '').trim();
          const buyPrice = Number(priceStr);

          if (type_id > 0 && buyPrice > 0) {
            fallbackMap.set(type_id, { buy: buyPrice, sell: 0 });
            requiredTids.delete(type_id); // Item resolved locally
          }
        }
      }
    }

    // --- Tier 3: Prepare the remaining TIDs for API Call ---
    requiredTids.forEach(tid => tidsForFuzzwork.push(tid));

    if (tidsForFuzzwork.length > 0) {
      log.info(`Attempting Tier 3 Fuzzwork fallback for ${tidsForFuzzwork.length} missing items.`);

      // Get the dynamically configured market location
      const { locationId, locationType } = _getPrimaryMarketConfig(ss);

      // FIX: ONE-STEP CALL using fuzAPI.requestItems
      // Assumes this returns an array of item objects directly.
      const rawFuzResults = fuzAPI.requestItems(locationId, locationType, tidsForFuzzwork);

      // Process the resulting array of item objects directly.
      if (Array.isArray(rawFuzResults)) {
        rawFuzResults.forEach(item => {
          const tid = item.type_id;
          // Use item.buy.max to get the highest buy order (best acquisition cost)
          const maxBuyPrice = item.buy?.max || 0;

          if (tid > 0 && maxBuyPrice > 0) {
            fallbackMap.set(tid, { buy: maxBuyPrice, sell: 0 });
            log.debug(`Resolved Tier 3 cost for ${tid}: ${maxBuyPrice}`);
          }
        });
      }
    }

  } catch (e) {
    log.error(`Contract Price Fallback FAILED: ${e.message}`);
  }

  return fallbackMap;
}

/**
 * NEW: Wraps _getContractPriceFallbackMap to cache expensive Tier 3 prices.
 */
function _getContractPricesCached(ss, missingTids) {
  const log = LoggerEx.withTag('CONTRACT_CACHE');
  const cache = CacheService.getScriptCache();
  const cacheKey = 'CONTRACT_FALLBACK_PRICES_V1'; // Static key for all items
  const CACHE_TTL = 3600; // Cache these fallback prices for 1 hour

  // 1. Check for cached fallback map
  const cachedJson = cache.get(cacheKey);
  let cachedFallbackMap = new Map();

  if (cachedJson) {
    // Rebuild the Map from the cached JSON array
    const parsedArray = JSON.parse(cachedJson);
    cachedFallbackMap = new Map(parsedArray);
    log.info(`[CONTRACT_CACHE] Loaded ${cachedFallbackMap.size} fallback prices from cache.`);
  }

  // 2. Identify TIDs *still* missing after checking local and cache
  const finalMissingTids = missingTids.filter(tid => !cachedFallbackMap.has(tid));

  if (finalMissingTids.length > 0) {
    log.info(`[CONTRACT_CACHE] Running API for ${finalMissingTids.length} uncached items.`);

    // 3. Run the slow, API-dependent function only for items still missing
    const newFallbackMap = _getContractPriceFallbackMap(ss, finalMissingTids);

    if (newFallbackMap.size > 0) {
      // 4. Merge new results with the cache
      newFallbackMap.forEach((v, k) => cachedFallbackMap.set(k, v));

      // 5. Store the entire merged map back into the cache
      const jsonToCache = JSON.stringify(Array.from(cachedFallbackMap.entries()));
      cache.put(cacheKey, jsonToCache, CACHE_TTL);
      log.info(`[CONTRACT_CACHE] Cached and merged ${newFallbackMap.size} new prices. Total cached: ${cachedFallbackMap.size}.`);
    }
  }

  // 6. Return the consolidated map for lookup
  return cachedFallbackMap;
}

/**
 * Helper to build a map of contract prices for allocation reference.
 * Reads the Contracts (RAW) sheet. (Resolves missing dependency)
 */
function _buildContractPriceMap_(ss) {
  const log = LoggerEx.withTag('CONTRACT_ALLOC');
  const RAW_SHEET = "Contracts (RAW)";
  const HEADERS = ['contract_id', 'price', 'collateral', 'reward', 'char', 'date_issued'];

  const dataObj = _getData_(ss, RAW_SHEET);
  if (dataObj.rows.length === 0) {
    log.warn(`[PriceMap] Contracts RAW sheet is empty. Skipping.`);
    return new Map();
  }

  const h = dataObj.h;
  const priceMap = new Map();

  const cContractId = h[HEADERS[0]];
  const cPrice = h[HEADERS[1]];
  const cCollateral = h[HEADERS[2]];
  const cReward = h[HEADERS[3]];
  const cChar = h[HEADERS[4]];
  const cDate = h[HEADERS[5]];

  dataObj.rows.forEach(row => {
    const contract_id = String(row[cContractId]);
    const price = Number(String(row[cPrice]).replace(/[^\d.]/g, '')) || 0;
    const collateral = Number(String(row[cCollateral]).replace(/[^\d.]/g, '')) || 0;
    const reward = Number(String(row[cReward]).replace(/[^\d.]/g, '')) || 0;
    const char = String(row[cChar]);

    if (contract_id) {
      priceMap.set(contract_id, { price, collateral, reward, char, date: String(row[cDate]) });
    } // <--- This closing bracket was missing
  });

  log.info(`[PriceMap] Built price map for ${priceMap.size} contracts.`);
  return priceMap;
}

// ==========================================================================================
// UNIT COST ALLOCATION FUNCTION
// ==========================================================================================


function rebuildContractUnitCosts(ss, items, priceMap, holdAnesthesia) {
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const log = LoggerEx.withTag('CONTRACT_UNIT_COST');
  const LEDGER_BUY_SHEET = 'Material_Ledger';

  if (!items || items.length === 0) {
    console.warn("No items provided to rebuildContractUnitCosts.");
    return;
  }

  const allocMode = String(_getNamedOr_('setting_contract_alloc_mode', 'REF')).toUpperCase();
  const bpcMap = _buildInternalBpcMap_(ss);
  const refMap = _buildRefPriceMap_(ss);

  // NOTE: priceMap is now accepted as an argument, no need to rebuild it here.

  const ci = _getData_(ss, 'Contract Items (RAW)');
  if (!ci || ci.rows.length === 0) return 0;

  const itemsByCid = new Map();
  const allUniqueTids = new Set();


  const { contract_id: hCid, type_id: hTid, quantity: hQty, is_included: hInc, runs: hRuns, me: hMe, te: hTe, sync_status: hStat } = ci.h;

  // PASS 0: Grouping & Unique TID collection
  ci.rows.forEach(row => {
    if (String(row[hStat]).toUpperCase() === 'LEDGERED') return;

    const qty = Number(row[hQty] || 0);
    if (String(row[hInc]).toUpperCase() === 'TRUE' && qty > 0) {
      const cid = String(row[hCid]);
      const tid = Number(row[hTid]);

      if (!itemsByCid.has(cid)) itemsByCid.set(cid, []);
      itemsByCid.get(cid).push({
        tid,
        qty,
        runs: Number(row[hRuns] || 0),
        me: Number(row[hMe] || 0),
        te: Number(row[hTe] || 0)
      });

      if (!refMap.has(tid) && !bpcMap.has(tid)) {
        allUniqueTids.add(tid);
      }
    }
  });

  if (itemsByCid.size === 0) {
    log.info("No new unledgered items to process.");
    return 0;
  }

  const fallbackMap = _getContractPricesCached(ss, Array.from(allUniqueTids));
  const outRows = [];

  for (const [cid, items] of itemsByCid.entries()) {
    const meta = priceMap.get(cid);
    if (!meta) continue;

    const totalContractValue = meta.price + meta.collateral - meta.reward;
    let totalReferenceValue = 0;

    items.forEach(item => {
      const internalVal = bpcMap.get(item.tid);
      const priceObj = refMap.get(item.tid) || fallbackMap.get(item.tid);

      item.resolvedPrice = internalVal || priceObj?.buy || 0;
      totalReferenceValue += (item.resolvedPrice * item.qty);
    });

    const pricePerRefUnit = (totalReferenceValue > 0) ? (totalContractValue / totalReferenceValue) : 0;
    const simpleVolumeSplit = (items.length > 0) ? (totalContractValue / items.length) : 0;

    items.forEach(item => {
      let unitCost = 0;
      if (allocMode === 'REF' && totalReferenceValue > 0 && item.resolvedPrice > 0) {
        unitCost = item.resolvedPrice * pricePerRefUnit;
      } else {
        unitCost = simpleVolumeSplit / item.qty;
      }

      outRows.push({
        date: meta.date,
        source: "CONTRACT",
        char: meta.char,
        contract_id: cid,
        type_id: item.tid,
        qty: item.qty,
        unit_value_filled: unitCost,
        metadata: { runs: item.runs, me: item.me, te: item.te }
      });
    });
  }

  if (outRows.length === 0) return 0;

  // --- ADD THE HOLD LOGIC HERE ---
  let needsWakeUp = false;
  try {
    // Only sleep if we aren't being held by the master controller
    if (!holdAnesthesia && typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

    const MaterialLedger = ML.forSheet(LEDGER_BUY_SHEET);
    const keys = ['source', 'char', 'contract_id', 'type_id'];

    const result = MaterialLedger.upsert(keys, outRows, true);

    const appendedCount = result.appended || 0;
    const upsertedCount = result.upserted || 0;
    log.info(`Upserted ${appendedCount + upsertedCount} rows to ${LEDGER_BUY_SHEET}.`);

    const processedIds = Array.from(itemsByCid.keys());
    _markContractsLedgered_(ss, processedIds);

    return result.totalRows || 0;

  } catch (e) {
    log.error('rebuildContractUnitCosts WRITE FAILED', e.message);
    throw e;
  } finally {
    // Only wake up if WE were the ones who put it to sleep
    if (!holdAnesthesia && needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss);
  }
}

/**
 * Utility function to batch-update the Status column to 'LEDGERED'
 * for specific contracts without causing Apps Script timeouts.
 */
function _markContractsLedgered_(ss, processedIds) {
  const log = LoggerEx.withTag('STAMP_SYNC');
  const idSet = new Set(processedIds.map(String));

  // --- 1. HANDLE CONTRACT ITEMS (RAW) ---
  const itemSh = ss.getSheetByName('Contract Items (RAW)');
  if (itemSh) {
    const data = itemSh.getDataRange().getValues();
    const h = data[0].map(h => String(h).trim()); // Preserve case
    const cidIdx = h.indexOf('contract_id');
    const statIdx = h.indexOf('sync_status'); // Capitalized only

    if (cidIdx !== -1 && statIdx !== -1) {
      for (let i = 1; i < data.length; i++) {
        if (idSet.has(String(data[i][cidIdx]))) {
          data[i][statIdx] = 'LEDGERED';
        }
      }
      itemSh.getDataRange().setValues(data);
      log.info(`Stamped Contract Items (RAW)`);
    }
  }

  // --- 2. HANDLE CONTRACTS (RAW) - THE COLLISION SHEET ---
  const contSh = ss.getSheetByName('Contracts (RAW)');
  if (contSh) {
    const data = contSh.getDataRange().getValues();
    const h = data[0].map(h => String(h).trim());
    const cidIdx = h.indexOf('contract_id');
    // FORCE: Look for Capitalized 'Status', skip lowercase 'status'
    const statIdx = h.lastIndexOf('sync_status');

    if (cidIdx !== -1 && statIdx !== -1) {
      for (let i = 1; i < data.length; i++) {
        if (idSet.has(String(data[i][cidIdx]))) {
          data[i][statIdx] = 'LEDGERED';
        }
      }
      contSh.getDataRange().setValues(data);
      log.info(`Stamped Contracts (RAW) using column index ${statIdx}`);
    } else {
      log.error(`STAMP FAILED: Could not find 'contract_id' or 'Status' in Contracts (RAW)`);
    }
  }
}

function runContractLedgerPhase(ss) {
  const log = LoggerEx.withTag('MASTER_SYNC');
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();

  // 1. Sync
  const charIdMap = _charIdMap(ss);
  const syncResult = syncContracts(ss, charIdMap);

  if (syncResult.contracts > 0) {
    log.info(`Sync found ${syncResult.contracts} NEW contracts.`);

    // --- START GLOBAL ANESTHESIA ---
    let needsWakeUp = false;
    if (typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

    try {
      // 2. Route Data (Passing TRUE to hold internal anesthesia)
      if (syncResult.buyData.contracts.length > 0) {
        _routeContractsToLedger(ss, charIdMap, syncResult.buyData, false, true);
      }
      if (syncResult.saleData.contracts.length > 0) {
        _routeContractsToLedger(ss, charIdMap, syncResult.saleData, true, true);
      }

      // 3. Finalize (Passing TRUE to hold internal anesthesia)
      const priceMap = _buildContractPriceMap_(ss);
      rebuildContractUnitCosts(ss, syncResult.buyData.items, priceMap, true);

      // 4. Stamp
      const allIds = syncResult.buyData.contracts.map(c => String(c[1]));
      _markContractsLedgered_(ss, allIds);

    } finally {
      // --- END GLOBAL ANESTHESIA ---
      // Sheet wakes up ONLY after all Ledger routing and stamping is finished
      if (needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss);
    }
  }

  // 5. Purge
  // Runs outside the sleep cycle since it only touches RAW transit sheets
  purgeContractsWithLedgeredStatus(ss);
}

function triggerContractUnitCostsFinalization() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const LOG = LoggerEx.withTag('COGS_TRIGGER');
  const FINALIZER_FUNC = '_runRebuildContractUnitCostsWorker';

  // 1. Set the finalize flag before scheduling
  SCRIPT_PROP.setProperty(PROP_KEY_COGS_STEP, STATE_FLAGS_COGS.FINALIZING);

  // 2. Schedule the worker to run soon after the main ledger phase exits.
  scheduleOneTimeTrigger(FINALIZER_FUNC, 5000); // 5 seconds delay
  LOG.info(`Scheduled heavy COGS finalization: ${FINALIZER_FUNC}. Flag set.`);
}







