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
const CONTRACT_STATUSES = ["finished", "completed", "outstanding"]; // Fetch all relevant states
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
 * Helper to call GESI with exponential backoff for rate limits.
 * Protects against HTTP 429 "Bandwidth quota exceeded" errors.
 */
function _invokeGesiWithRetry(endpoint, params, maxRetries = 3) {
  let attempt = 0;
  let delayMs = 2000; // Start with a 2-second delay if rate limited

  while (attempt < maxRetries) {
    try {
      return GESI.invokeRaw(endpoint, params);
    } catch (e) {
      attempt++;
      const errorMsg = e.message || String(e);

      // Only retry on rate limit / bandwidth errors
      if (errorMsg.includes("Bandwidth quota exceeded") || errorMsg.includes("429")) {
        if (attempt >= maxRetries) throw e; // Give up if max retries hit

        const log = (typeof LoggerEx !== 'undefined') ? LoggerEx.withTag('GESI_RETRY') : console;
        log.warn(`Rate limit hit for ${params.name || 'API'}. Retrying in ${delayMs}ms... (Attempt ${attempt} of ${maxRetries})`);

        Utilities.sleep(delayMs);
        delayMs *= 2; // Exponential backoff (2s, 4s, 8s...)
      } else {
        // Throw immediately for auth drops (e.g., "Access not granted")
        throw e;
      }
    }
  }
}

// A leaner reset for your new Single-Pass architecture
function resetCorpJournalImport() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const LAST_ID_KEY = 'CORP_JOURNAL_LAST_TRANSACTION_ID';
  SCRIPT_PROP.deleteProperty(LAST_JOURNAL_KEY);
  SCRIPT_PROP.deleteProperty(LAST_ID_KEY);
  console.log("✅ Journal anchor reset.");
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
  // Force anchors back 24 hours (roughly 10,000 IDs for journal/txn)
  const txId = Number(SCRIPT_PROP.getProperty('CORP_LAST_TRANSACTION_ID') || 0);
  const joId = Number(SCRIPT_PROP.getProperty('CORP_LAST_JOURNAL_ID') || 0);

  SCRIPT_PROP.setProperty('CORP_LAST_TRANSACTION_ID', String(Math.max(0, txId - 10000)));
  SCRIPT_PROP.setProperty('CORP_LAST_JOURNAL_ID', String(Math.max(0, joId - 10000)));
  console.log("Anchors reset. Run Feeder functions now.");
}

function Feed_Transactions_To_Buffer(ss) {
  const log = LoggerEx.withTag('TXN_FEEDER');
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const LAST_TXN_KEY = 'CORP_LAST_TRANSACTION_ID';

  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const bufferSheet = ss.getSheetByName("_Internal_Ledger_Buffer");
  if (!bufferSheet) {
    log.error("Missing _Internal_Ledger_Buffer sheet.");
    return false; // <--- STATUS ADDED
  }

  const authToon = getCorpAuthChar(ss);
  let lastProcessedId = Number(SCRIPT_PROP.getProperty(LAST_TXN_KEY) || 0);
  const client = GESI.getClient(authToon);
  client.setFunction('corporations_corporation_wallets_division_transactions');

  const req1 = client.buildRequest({ division: 3, page: 1 });
  const respArray = _robustFetchAll([{ url: req1.url, method: 'get', headers: req1.headers, muteHttpExceptions: true }]);
  const resp1 = respArray[0];

  if (!resp1 || resp1.getResponseCode() !== 200) {
    log.error("Failed to fetch Transaction Page 1.");
    if (resp1) {
      log.error("Status Code: " + resp1.getResponseCode());
      log.error("Response: " + resp1.getContentText());
    }
    return false; // <--- STATUS ADDED
  }

  const maxPages = Number(resp1.getHeaders()['X-Pages'] || resp1.getHeaders()['x-pages'] || 1);
  let newTxns = JSON.parse(resp1.getContentText()).filter(t => t.transaction_id > lastProcessedId);

  if (maxPages > 1) {
    const requests = [];
    for (let p = 2; p <= maxPages; p++) {
      const req = client.buildRequest({ division: 3, page: p });
      requests.push({ url: req.url, method: 'get', headers: req.headers, muteHttpExceptions: true });
    }

    const responses = _robustFetchAll(requests, 5, 3);
    responses.forEach(res => {
      if (res.getResponseCode() === 200) {
        const pageData = JSON.parse(res.getContentText());
        newTxns = newTxns.concat(pageData.filter(t => t.transaction_id > lastProcessedId));
      }
    });
  }

  if (newTxns.length === 0) {
    log.info("No new transactions to buffer.");
    return false; // <--- STATUS ADDED
  }

  newTxns.sort((a, b) => a.transaction_id - b.transaction_id);
  const NOW = new Date().getTime();
  const bufferRows = newTxns.map(t => [
    String(t.transaction_id),
    JSON.stringify({ data: t, tax: 0, status: 'WAITING', ts: NOW }),
    NOW
  ]);

  const startRow = Math.max(2, bufferSheet.getLastRow() + 1);
  bufferSheet.getRange(startRow, 1, bufferRows.length, 3).setValues(bufferRows);
  SCRIPT_PROP.setProperty(LAST_TXN_KEY, String(newTxns[newTxns.length - 1].transaction_id));

  log.info(`Parked ${bufferRows.length} new transactions in the Waiting Room.`);
  return true; // <--- STATUS ADDED (Success)
}

function Feed_Journal_To_Buffer(ss) {
  const log = LoggerEx.withTag('JOURNAL_FEEDER');
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const LAST_JOURNAL_KEY = 'CORP_LAST_JOURNAL_ID';

  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const bufferSheet = ss.getSheetByName("_Internal_Ledger_Buffer");
  if (!bufferSheet) {
    log.error("Missing _Internal_Ledger_Buffer sheet.");
    return false; // <--- STATUS ADDED
  }

  const authToon = getCorpAuthChar(ss);
  let lastProcessedId = Number(SCRIPT_PROP.getProperty(LAST_JOURNAL_KEY) || 0);
  const client = GESI.getClient(authToon);
  client.setFunction('corporations_corporation_wallets_division_journal');

  const req1 = client.buildRequest({ division: 3, page: 1 });
  const respArray = _robustFetchAll([{ url: req1.url, method: 'get', headers: req1.headers, muteHttpExceptions: true }]);
  const resp1 = respArray[0];

  if (!resp1 || resp1.getResponseCode() !== 200) {
    log.error("Failed to fetch Journal Page 1.");
    if (resp1) {
      log.error("Status Code: " + resp1.getResponseCode());
      log.error("Response: " + resp1.getContentText());
    }
    return false; // <--- STATUS ADDED
  }

  const maxPages = Number(resp1.getHeaders()['X-Pages'] || resp1.getHeaders()['x-pages'] || 1);
  let newEntries = JSON.parse(resp1.getContentText()).filter(j => j.id > lastProcessedId);

  if (maxPages > 1) {
    const requests = [];
    for (let p = 2; p <= maxPages; p++) {
      const req = client.buildRequest({ division: 3, page: p });
      requests.push({ url: req.url, method: 'get', headers: req.headers, muteHttpExceptions: true });
    }

    const responses = _robustFetchAll(requests, 5, 3);
    responses.forEach(res => {
      if (res.getResponseCode() === 200) {
        const pageData = JSON.parse(res.getContentText());
        newEntries = newEntries.concat(pageData.filter(j => j.id > lastProcessedId));
      } else {
        log.error(`A Journal page failed even after retries with code ${res.getResponseCode()}.`);
      }
    });
  }

  if (newEntries.length === 0) {
    log.info("No new journal entries to buffer.");
    return false; // <--- STATUS ADDED
  }

  newEntries.sort((a, b) => a.id - b.id);
  const NOW = new Date().getTime();
  const bufferRows = newEntries.map(j => [
    String(j.id),
    JSON.stringify({ data: { ...j, source: 'JOURNAL' }, status: 'WAITING', ts: NOW }),
    NOW
  ]);

  const startRow = Math.max(2, bufferSheet.getLastRow() + 1);
  bufferSheet.getRange(startRow, 1, bufferRows.length, 3).setValues(bufferRows);
  SCRIPT_PROP.setProperty(LAST_JOURNAL_KEY, String(newEntries[newEntries.length - 1].id));

  log.info(`Parked ${bufferRows.length} new journal entries in the Waiting Room.`);
  return true; // <--- STATUS ADDED (Success)
}


/**
 * PROCESS INTERNAL BUFFER
 * FIXED: Fallback detection for unlabeled Transactions.
 * FIXED: Mapped ESI's 'market_transaction_id' for tax pairing.
 * FIXED: Safely parses ISO Timestamps and purges 24-hour orphans.
 */
function processInternalBuffer(ss) {
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const bufferSheet = ss.getSheetByName("_Internal_Ledger_Buffer");
  if (!bufferSheet) return;

  const data = bufferSheet.getDataRange().getValues();
  if (data.length <= 1) return; // Nothing in buffer
  const headers = data.shift(); // Remove headers

  const pending = new Map();
  const HOLD_DURATION_MS = 300000; // 5 minutes wait for journal sync
  const now = new Date().getTime();

  // 1. GROUPING PHASE
  data.forEach((row, rowIndex) => {
    if (!row[0]) return;
    const entry = JSON.parse(row[1]);
    const d = entry.data;

    // Bulletproof Source Detection
    const isTx = (d.source === 'TRANSACTION' || (d.transaction_id !== undefined && !d.ref_type));
    const isJournal = (d.source === 'JOURNAL' || d.ref_type !== undefined);

    let id = "";
    if (isTx) {
      id = String(d.transaction_id || "");
    } else if (isJournal) {
      const cType = String(d.context_id_type || "").toLowerCase();
      if (cType === 'transaction_id' || cType === 'market_transaction_id') {
        id = String(d.context_id || "");
      } else {
        id = String(d.first_id || d.second_id || d.context_id || "");
      }
    }

    id = id.trim();
    if (id.indexOf('.') > -1) id = id.split('.')[0];

    if (!id || id === "undefined" || id === "null") return;

    if (!pending.has(id)) pending.set(id, { tx: null, fees: 0, ts: entry.ts, journalFound: false });

    const record = pending.get(id);

    if (isTx) {
      record.tx = d;
    } else if (isJournal) {
      if (['broker_fee', 'transaction_tax'].includes(d.ref_type)) {
        record.fees += Math.abs(Number(d.amount || 0));
        record.journalFound = true;
      } else if (['market_transaction', 'market_escrow'].includes(d.ref_type)) {
        record.journalFound = true;
      }
    }
  });

  // 2. PROCESSING PHASE
  const sells = [];
  const buys = [];
  const processedIds = new Set();

  pending.forEach((p, id) => {
    const pTime = new Date(p.ts).getTime() || 0;
    const isReady = p.tx && (p.journalFound || (now - pTime > HOLD_DURATION_MS));

    if (isReady) {
      const isBuy = (String(p.tx.is_buy).toLowerCase() === 'true' || p.tx.is_buy === true || p.tx.is_buy === 1);
      const perUnitFee = p.tx.quantity > 0 ? (p.fees / Number(p.tx.quantity)) : 0;
      const finalUnitValue = isBuy ? (Number(p.tx.unit_price) + perUnitFee) : (Number(p.tx.unit_price) - perUnitFee);

      const ledgerObj = {
        date: p.tx.date,
        type_id: Number(p.tx.type_id),
        qty: isBuy ? Number(p.tx.quantity) : -Number(p.tx.quantity),
        source: 'TRANSACTION',
        contract_id: id,
        unit_value_filled: finalUnitValue,
        char: p.tx.client_id || p.tx.character_id // Failsafe for client ID
      };

      if (isBuy) buys.push(ledgerObj);
      else sells.push(ledgerObj);

      processedIds.add(id);
    }
  });

  // 3. BATCH UPSERT
  if (sells.length > 0) ML.forSheet("Sales_Ledger").upsert(['date', 'source', 'type_id', 'contract_id'], sells, true);
  if (buys.length > 0) ML.forSheet("Material_Ledger").upsert(['date', 'source', 'type_id', 'contract_id'], buys, true);

  // 4. SURGICAL CLEANUP
  const remainingRows = data.filter(row => {
    if (!row[0]) return false;
    const entry = JSON.parse(row[1]);
    const d = entry.data;

    const isTx = (d.source === 'TRANSACTION' || (d.transaction_id !== undefined && !d.ref_type));
    const isJournal = (d.source === 'JOURNAL' || d.ref_type !== undefined);

    let id = "";
    if (isTx) {
      id = String(d.transaction_id || "");
    } else if (isJournal) {
      const cType = String(d.context_id_type || "").toLowerCase();
      if (cType === 'transaction_id' || cType === 'market_transaction_id') {
        id = String(d.context_id || "");
      } else {
        id = String(d.first_id || d.second_id || d.context_id || "");
      }
    }

    id = id.trim();
    if (id.indexOf('.') > -1) id = id.split('.')[0];

    if (processedIds.has(id)) return false;

    const entryTime = new Date(entry.ts).getTime() || 0;
    if (isJournal && (now - entryTime > 86400000)) return false;

    return true;
  });

  bufferSheet.getRange(2, 1, Math.max(1, bufferSheet.getLastRow() - 1), 3).clearContent();
  if (remainingRows.length > 0) {
    bufferSheet.getRange(2, 1, remainingRows.length, 3).setValues(remainingRows);
  }

  console.log(`Buffer Processed. Paired: ${processedIds.size}. Held: ${remainingRows.length}. Buys: ${buys.length} | Sells: ${sells.length}`);
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

/**
 * THE FINAL V12 FERRARI ENGINE - CORPORATION ORDERS
 * Complete Version: Dynamic Paging, ESI-Synced Cache, and Strict Data Typing.
 */
function _fetchCorpOrdersConcurrently(authName) {

  const SCRIPT_NAME = '_fetchCorpOrdersConcurrently';
  const client = GESI.getClient().setFunction('corporations_corporation_orders');
  const cache = CacheService.getUserCache();
  const cacheKey = "CORP_ORDERS_" + authName;


  // 1. HEADER DEFINITION (Matches your sheet structure precisely)
  const STANDARD_ORDER_HEADERS = [
    "duration", "escrow", "is_buy", "issued", "issued_by",
    "location_id", "min_volume", "order_id", "price", "range",
    "region_id", "type_id", "volume_remain", "volume_total", "wallet_division"
  ];

  // 2. CACHE CHECK - Skips API if ESI hasn't refreshed yet
  const cachedData = cache.get(cacheKey);
  if (cachedData) {
    console.log(`[CACHE] Serving fresh data from CacheService for ${authName}.`);
    return JSON.parse(cachedData);
  }

  let allOrders = [STANDARD_ORDER_HEADERS];
  let rawObjects = [];
  let corpId = 0;

  // 3. RESOLVE CORP ID
  try {
    const charData = GESI.getCharacterData(authName);
    if (charData) corpId = charData.corporation_id;
  } catch (e) {
    console.error(`[${SCRIPT_NAME}] Could not resolve Corp ID.`);
  }
  if (!corpId) return allOrders;

  // 4. THE STRICT FORMATTER (Handles types, dates, and boolean strings)
  const formatRow = (obj) => {
    return STANDARD_ORDER_HEADERS.map(key => {
      const val = obj[key];

      // DATE: Force real JS Date Object so Sheets can do math on them
      if (key === "issued") return val ? new Date(val) : "";

      // BOOLEAN: Normalizes is_buy_order vs is_buy into literal "TRUE"/"FALSE" strings
      // This is vital for your (buys = "TRUE") filter in the LET formula.
      if (key === "is_buy") {
        const buyFlag = obj.hasOwnProperty('is_buy_order') ? obj.is_buy_order : obj.is_buy;
        return (buyFlag === true || buyFlag === 1 || String(buyFlag).toLowerCase() === "true") ? "TRUE" : "FALSE";
      }

      // FLOATS: Forced precision for Price and Escrow
      if (key === "price" || key === "escrow") return val !== undefined ? parseFloat(val) : 0.0;

      // INTEGERS: Forced whole numbers for Volumes and TypeIDs
      if (["volume_remain", "volume_total", "min_volume", "duration", "type_id", "wallet_division"].includes(key)) {
        return val !== undefined ? parseInt(val, 10) : 0;
      }

      // STRINGS: IDs as strings to prevent Scientific Notation rounding errors
      if (["location_id", "order_id", "issued_by", "range", "region_id"].includes(key)) {
        return val !== undefined ? String(val) : "";
      }

      return val !== undefined ? val : "";
    });
  };

  try {
    // 5. FETCH PAGE 1 - Establishes cache sync and page count
    const req1 = client.buildRequest({ corporation_id: corpId, page: 1, name: authName });
    const resp1 = UrlFetchApp.fetch(req1.url, { method: 'get', headers: req1.headers, muteHttpExceptions: true });

    if (resp1.getResponseCode() !== 200) throw new Error("ESI HTTP Error " + resp1.getResponseCode());

    const page1Data = JSON.parse(resp1.getContentText());
    rawObjects = rawObjects.concat(page1Data);

    // 6. SYNC CACHE TTL WITH ESI HEADERS
    const headers = resp1.getHeaders();
    const expires = headers['Expires'] || headers['expires'];
    let ttl = 3600; // Default 1 hour
    if (expires) {
      const now = new Date().getTime();
      const expiry = new Date(expires).getTime();
      ttl = Math.max(1, Math.min(Math.floor((expiry - now) / 1000), 21600));
    }

    // 7. PARALLEL PAGING - Fetches every single order in existence for the corp
    const maxPages = Number(headers['X-Pages'] || headers['x-pages']) || 1;
    if (maxPages > 1) {
      const requests = [];
      for (let p = 2; p <= maxPages; p++) {
        const req = client.buildRequest({ corporation_id: corpId, page: p, name: authName });
        requests.push({ url: req.url, method: 'get', headers: req.headers, muteHttpExceptions: true });
      }
      const responses = UrlFetchApp.fetchAll(requests);
      responses.forEach(res => {
        if (res.getResponseCode() === 200) rawObjects = rawObjects.concat(JSON.parse(res.getContentText()));
      });
    }

    console.log(`[SUCCESS] Retrieved ${rawObjects.length} orders. ESI Refresh in ${ttl}s.`);

    // 8. FINAL MAPPING AND CACHING
    const finalOutput = allOrders.concat(rawObjects.map(obj => formatRow(obj)));

    // Store in cache to prevent hitting ESI until the next refresh
    cache.put(cacheKey, JSON.stringify(finalOutput), ttl);

    return finalOutput;

  } catch (e) {
    console.error(`[CRITICAL] Engine failure: ${e.message}`);
    return allOrders;
  }
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
  return Utilities.formatDate(new Date(d), "UTC", "yyyy-MM-dd");
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


/** Build Char name -> ID map (Implementation) */
function _charIdMap(ss) {
  // --- IMPLEMENTATION OF NAME-TO-ID MAP (Based on Corp Members) ---
  if (_cachedCharIdMap) {
    return _cachedCharIdMap;
  }

  const log = LoggerEx.withTag('CHAR_MAP');
  const authToon = getCorpAuthChar(ss);

  if (!authToon) {
    log.warn('No authorized character found for building character map.');
    _cachedCharIdMap = {};
    return {};
  }

  const charIdMap = {};

  try {
    // 1. Get all member IDs for the corporation tied to the authenticated character.
    // FIX: Use GESI.invokeRaw with parameter object for robust script execution.
    const memberIdsRaw = GESI.invokeRaw(
      'corporations_corporation_members',
      {
        name: authToon,
        show_column_headings: false,
        version: null
      }
    );

    const memberIds = Array.isArray(memberIdsRaw) ? memberIdsRaw.filter(Number.isFinite) : [];

    if (memberIds.length === 0) {
      log.warn('No member IDs returned from GESI.corporations_corporation_members.');
      // Throw an error to ensure the subsequent fallback logic is executed
      throw new Error("No ESI member IDs found.");
    }

    // 2. Resolve those IDs to Names.
    // FIX: Use GESI.invokeRaw with the standard 'universe_names' alias.
    // IMPLEMENTING USER'S EXPLICIT INSTRUCTION: ids: [memberIds]
    const nameResolutions = GESI.invokeRaw('universe_names',
      {
        ids: memberIds, // Implementing user's explicit instruction
        show_column_headings: false,
        version: null
      }
    );
    LoggerEx.info(JSON.stringify(nameResolutions));
    // 3. Build the final Name -> ID map
    if (Array.isArray(nameResolutions)) {
      for (const entry of nameResolutions) {
        if (entry && entry.category === 'character' && entry.name && entry.id) {
          // The map is NAME -> ID
          charIdMap[entry.name] = entry.id;
        }
      }
    }

  } catch (e) {
    log.error('Error building character ID map:', e.message);

    // CRITICAL STABILITY FALLBACK (to handle external API failures)
    const fallbackIdRaw = _getNamedOr_('CORP_AUTH_CHAR_ID', null);
    const fallbackId = parseInt(fallbackIdRaw, 10);

    if (authToon && fallbackId && Number.isFinite(fallbackId)) {
      charIdMap[authToon] = fallbackId;
      log.warn(`[CHAR_MAP] ESI call failed. Using configured ID ${fallbackId} from Named Range 'CORP_AUTH_CHAR_ID' for ${authToon}.`);
    } else {
      log.warn(`[CHAR_MAP] ESI call failed. No valid fallback ID found in Named Range 'CORP_AUTH_CHAR_ID'.`);
    }

    _cachedCharIdMap = charIdMap;
    return charIdMap;
  }

  log.info(`Built character ID map for ${Object.keys(charIdMap).length} members.`);
  _cachedCharIdMap = charIdMap;
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


function _fetchCharContractItems(charName, contractId) {
  const client = GESI.getClient(charName);
  const req = client.setFunction('characters_character_contracts_contract_items')
    .buildRequest({ contract_id: _toIntOrNull(contractId) });
  // Add metadata so _robustFetchAll can track what this is
  return { ...req, name: charName, type: 'CHAR', cid: contractId };
}

function _fetchCorpContractItems(charName, contractId) {
  const client = GESI.getClient(charName);
  const req = client.setFunction('corporations_corporation_contracts_contract_items')
    .buildRequest({ contract_id: _toIntOrNull(contractId) });
  return { ...req, name: charName, type: 'CORP', cid: contractId };
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
function raw_characters_character_contract_items(contract_id, name, show_column_headings) {
  if (show_column_headings == null) show_column_headings = true;
  var cid = _toIntOrNull(contract_id);
  var cache = CacheService.getUserCache();
  var key = "gesiCharContractItmz:" + name + ":" + cid + ":" + (show_column_headings ? 1 : 0);
  var hit = cache.get(key);
  if (hit !== null) return JSON.parse(hit);

  var data = GESI.invoke(
    EP_ITEMS_CHAR,
    [String(name)],
    { contract_id: cid, show_column_headings: !!show_column_headings }
  ) || [];

  cache.put(key, JSON.stringify(data), 3600);
  return data;
}

function raw_corporations_corporation_contracts_contract_items(contract_id, name, show_column_headings) {
  if (show_column_headings == null) show_column_headings = true;
  var cid = _toIntOrNull(contract_id);
  var cache = CacheService.getUserCache();
  var key = "gesiCorpContractItmz:" + name + ":" + cid + ":" + (show_column_headings ? 1 : 0);
  var hit = cache.get(key);
  if (hit !== null) return JSON.parse(hit);

  var data = GESI.invoke(
    EP_ITEMS_CORP,
    [String(name)],
    { contract_id: cid, show_column_headings: !!show_column_headings }
  ) || [];

  cache.put(key, JSON.stringify(data), 3600);
  return data;
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
 * Robust Fetch All Utility
 * Chunks requests to prevent throttling and automatically retries failed ESI pages.
 * Explicitly drops 403/404 errors. Includes specific 429 Penalty Box pauses.
 */
function _robustFetchAll(requests, chunkSize = 5, maxRetries = 3) {
  const allResponses = [];
  const log = LoggerEx.withTag('ROBUST_FETCH');

  for (let i = 0; i < requests.length; i += chunkSize) {
    const chunk = requests.slice(i, i + chunkSize);
    let attempts = 0;
    let currentChunk = [...chunk];
    let chunkResponses = [];

    while (attempts < maxRetries && currentChunk.length > 0) {
      attempts++;
      const responses = UrlFetchApp.fetchAll(currentChunk);
      const nextChunk = [];

      responses.forEach((res, index) => {
        const code = res.getResponseCode();

        if (code === 200) {
          chunkResponses.push(res);
        } else if (code === 429) {
          // Properly extract the ESI wait time
          const retryAfter = res.getHeaders()['Retry-After'] || res.getHeaders()['retry-after'];
          const waitTime = retryAfter ? Number(retryAfter) * 1000 : 30000;

          log.warn(`ESI 429: Rate limited. Sleeping ${waitTime / 1000}s.`);
          Utilities.sleep(waitTime);
          nextChunk.push(currentChunk[index]); // Retry this specific request
        } else if (code >= 500) {
          log.warn(`ESI ${code}: Transient error. Backing off.`);
          nextChunk.push(currentChunk[index]);
        } else {
          log.error(`ESI ${code}: Non-recoverable. Dropping ${currentChunk[index].url}`);
        }
      });

      currentChunk = nextChunk;
      if (currentChunk.length > 0) {
        Utilities.sleep(2000 * attempts); // Linear backoff for remaining
      }
    }
    allResponses.push(...chunkResponses);
  }
  return allResponses;
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
 * Hardened Garbage Collector: Cleans out old 'LEDGERED' rows from BOTH staging buffers.
 */
function purgeContractsWithLedgeredStatus(ss) {
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const log = LoggerEx.withTag('PURGE_CONTRACTS');
  const itemsSheet = ss.getSheetByName('Contract Items (RAW)');
  const contractsSheet = ss.getSheetByName('Contracts (RAW)');

  if (!itemsSheet || !contractsSheet) {
    log.error("Could not find required sheets.");
    return;
  }

  // 1. FILTER & PREPARE CONTRACT ITEMS (RAW)
  const itemsData = itemsSheet.getDataRange().getValues();
  const hItems = itemsData[0].map(h => String(h).trim().toLowerCase());
  const cidIdxItems = hItems.indexOf('contract_id');
  const statIdxItems = hItems.indexOf('status');

  if (cidIdxItems === -1 || statIdxItems === -1) {
    log.error("Critical Failure: Missing columns in Items sheet.");
    return;
  }

  const processedCids = new Set();
  const itemsToKeep = [itemsData[0]]; // Initialize with headers

  for (let i = 1; i < itemsData.length; i++) {
    if (String(itemsData[i][statIdxItems]).toUpperCase() === 'LEDGERED') {
      processedCids.add(String(itemsData[i][cidIdxItems]).trim());
    } else {
      itemsToKeep.push(itemsData[i]);
    }
  }

  log.info(`Found ${processedCids.size} LEDGERED contract IDs to wipe.`);

  // 2. FILTER CONTRACTS (RAW) BASED ON ENCOUNTERED IDS
  const contData = contractsSheet.getDataRange().getValues();
  const hCont = contData[0].map(h => String(h).trim().toLowerCase());
  const cCidIdx = hCont.indexOf('contract_id');

  if (cCidIdx === -1) {
    log.error("Critical Failure: 'contract_id' not found in Contracts (RAW) sheet.");
    return;
  }

  const contractsToKeep = contData.filter((row, index) => {
    if (index === 0) return true;
    return !processedCids.has(String(row[cCidIdx]).trim());
  });

  // 3. WRITE CLEAN DATA BACK TO CONTRACT ITEMS (RAW)
  if (itemsData.length > itemsToKeep.length) {
    itemsSheet.clearContents();
    itemsSheet.getRange(1, 1, itemsToKeep.length, itemsData[0].length).setValues(itemsToKeep);
    log.info(`Successfully cleared ${itemsData.length - itemsToKeep.length} processed rows from Contract Items (RAW).`);
  } else {
    log.info("No rows required purging from Contract Items (RAW).");
  }

  // 4. WRITE CLEAN DATA BACK TO CONTRACTS (RAW)
  if (contData.length > contractsToKeep.length) {
    contractsSheet.clearContents();
    contractsSheet.getRange(1, 1, contractsToKeep.length, contData[0].length).setValues(contractsToKeep);
    log.info(`Successfully cleared ${contData.length - contractsToKeep.length} old parent contracts from Contracts (RAW).`);
  } else {
    log.info("No matching records found to purge from Contracts (RAW).");
  }
}
function debugPurgeMismatch() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const itemsSheet = ss.getSheetByName('Contract Items (RAW)');
  const contractsSheet = ss.getSheetByName('Contracts (RAW)');

  // 1. Get processed IDs
  const itemsData = itemsSheet.getDataRange().getValues();
  const hItems = itemsData[0];
  const cidIdxItems = hItems.indexOf('contract_id');
  const statIdx = hItems.indexOf('Status');

  const processedCids = new Set();
  for (let i = 1; i < itemsData.length; i++) {
    if (String(itemsData[i][statIdx]).toUpperCase() === 'LEDGERED') {
      // FORCE STRING and TRIM
      processedCids.add(String(itemsData[i][cidIdxItems]).trim());
    }
  }

  // 2. Sample first 5 IDs in Contracts (RAW)
  const contData = contractsSheet.getDataRange().getValues();
  const hCont = contData[0];
  const cCidIdx = hCont.indexOf('contract_id');

  Logger.log("--- ID COMPARISON ---");
  Logger.log("Total LEDGERED IDs found in Items: " + processedCids.size);

  for (let i = 1; i < Math.min(6, contData.length); i++) {
    const rawId = String(contData[i][cCidIdx]).trim();
    const isMatch = processedCids.has(rawId);
    Logger.log(`Row ${i} ID: '${rawId}' | Match Found in Set: ${isMatch}`);
  }
}
function debugMismatch() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const itemsSh = ss.getSheetByName('Contract Items (RAW)');
  const contSh = ss.getSheetByName('Contracts (RAW)');

  const itemsData = itemsSh.getDataRange().getValues();
  const contData = contSh.getDataRange().getValues();

  const cidIdxItems = itemsData[0].indexOf('contract_id');
  const statIdx = itemsData[0].indexOf('Status');
  const cidIdxCont = contData[0].indexOf('contract_id');

  // Collect 3 sample ledgered IDs
  const sampleIds = [];
  for (let i = 1; i < itemsData.length; i++) {
    if (String(itemsData[i][statIdx]).toUpperCase() === 'LEDGERED') {
      sampleIds.push(String(itemsData[i][cidIdxItems]).trim());
      if (sampleIds.length >= 3) break;
    }
  }

  // Check if they exist in Contracts (RAW)
  const contIds = contData.map(r => String(r[cidIdxCont]).trim());

  Logger.log("--- MATCHING DIAGNOSTIC ---");
  Logger.log("Sample Ledgered IDs from Items: " + JSON.stringify(sampleIds));
  Logger.log("First 10 IDs in Contracts (RAW): " + JSON.stringify(contIds.slice(1, 11)));

  sampleIds.forEach(id => {
    Logger.log(`Is ID ${id} in Contracts sheet? ${contIds.includes(id) ? "YES" : "NO"}`);
  });
}

function syncContracts(ss, charIdMap) {
  var log = LoggerEx.withTag('GESI_CONTRACTS');
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  var authToon = getCorpAuthChar(ss);
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  const LAST_CID = parseInt(SCRIPT_PROP.getProperty(PROP_KEY_LAST_CONTRACT_ID) || '0', 10);
  let maxContractId = LAST_CID;

  var hdrC = ["char", "contract_id", "type", "status", "issuer_id", "acceptor_id", "date_issued", "date_expired", "price", "reward", "collateral", "volume", "title", "availability", "start_location_id", "end_location_id"];
  var hdrI = ["char", "contract_id", "type_id", "quantity", "is_included", "is_singleton", "runs", "me", "te"];

  if (!charIdMap) charIdMap = _charIdMap(ss);
  var allNames = getCharNamesFast();
  const idNameMap = {};
  Object.entries(charIdMap).forEach(([name, id]) => { idNameMap[String(id)] = name; });

  // --- PASS 1: CONCURRENT QUEUE FOR PAGE 1 LISTS ---
  const page1Requests = [];
  for (const status of CONTRACT_STATUSES) {
    allNames.forEach(charName => {
      const req = GESI.getClient(charName)
        .setFunction(EP_LIST_CHAR)
        .buildRequest({ status: status, show_column_headings: false, page: 1 });
      page1Requests.push({ ...req, name: charName, type: 'CHAR', status: status, page: 1 });
    });

    const corpReq = GESI.getClient(authToon)
      .setFunction(EP_LIST_CORP)
      .buildRequest({ status: status, show_column_headings: false, page: 1 });
    page1Requests.push({ ...corpReq, name: authToon, type: 'CORP', status: status, page: 1 });
  }

  log.info(`Dispatching Pass 1: Fetching page 1 lists (${page1Requests.length} endpoints)...`);
  const page1Responses = _robustFetchAll(page1Requests, 10, 3);

  let allTuples = [];
  const overflowRequests = [];

  // --- PASS 2: PARSE PAGE 1 & SEED OVERFLOW QUEUE ---
  page1Responses.forEach((res, i) => {
    if (res.getResponseCode() !== 200) return;

    const data = JSON.parse(res.getContentText());
    const req = page1Requests[i];

    if (Array.isArray(data) && data.length > 0) {
      allTuples = allTuples.concat(
        req.type === 'CHAR'
          ? _normalizeCharContracts([data], [req.name], idNameMap)
          : _normalizeCorpContracts(data, req.name)
      );
    }

    // Trace true sheet depth from ESI matrix response headers
    const headers = res.getHeaders();
    const maxPages = Number(headers['X-Pages'] || headers['x-pages']) || 1;

    if (maxPages > 1) {
      log.info(`Pagination overflow found: ${req.type} '${req.name}' [Status: ${req.status}] has ${maxPages} pages. Queuing remainder.`);

      const client = GESI.getClient(req.name).setFunction(req.type === 'CHAR' ? EP_LIST_CHAR : EP_LIST_CORP);
      for (let p = 2; p <= maxPages; p++) {
        const overflowReq = client.buildRequest({ status: req.status, show_column_headings: false, page: p });
        overflowRequests.push({ ...overflowReq, name: req.name, type: req.type, status: req.status, page: p });
      }
    }
  });

  // --- PASS 3: FETCH AND CONSOLIDATE OVERFLOW PAGES ---
  if (overflowRequests.length > 0) {
    log.info(`Dispatching Pass 2: Fetching overflow pages (${overflowRequests.length} requests)...`);
    const overflowResponses = _robustFetchAll(overflowRequests, 10, 3);

    overflowResponses.forEach((res, i) => {
      if (res.getResponseCode() !== 200) return;

      const data = JSON.parse(res.getContentText());
      const req = overflowRequests[i];

      if (Array.isArray(data) && data.length > 0) {
        allTuples = allTuples.concat(
          req.type === 'CHAR'
            ? _normalizeCharContracts([data], [req.name], idNameMap)
            : _normalizeCorpContracts(data, req.name)
        );
      }
    });
  }

  // --- PASS 4: COMPILE ITEM REQUESTS FROM CONSOLIDATED TUPLES ---
  const itemRequests = [];
  const validTuples = [];
  const seenCids = new Set();

  for (const tuple of allTuples) {
    const cid = _toIntOrNull(tuple.c.contract_id);
    if (!cid || seenCids.has(cid) || (LAST_CID > 0 && cid <= LAST_CID)) continue;

    let req;
    switch (tuple.c.availability) {
      case 'corporation':
        req = GESI.getClient(tuple.ch).setFunction(EP_ITEMS_CORP).buildRequest({ contract_id: cid });
        break;
      case 'personal':
      case 'character':
      case 'public':
        req = GESI.getClient(tuple.ch).setFunction(EP_ITEMS_CHAR).buildRequest({ contract_id: cid });
        break;
      default:
        continue;
    }

    itemRequests.push({
      ...req,
      cid: cid,
      attributedChar: _getAttributedChar(tuple, idNameMap)
    });

    validTuples.push(tuple);
    seenCids.add(cid);
    if (cid > maxContractId) maxContractId = cid;
  }

  log.info(`Compiled contract pool. Requesting items for ${itemRequests.length} new contracts...`);
  const itemResponses = _robustFetchAll(itemRequests, 10, 3);

  var outC_Combined = [];
  var outI_Combined = [];

  validTuples.forEach(tuple => {
    const c = tuple.c;
    outC_Combined.push([tuple.ch, c.contract_id, c.type || '', c.status || '', c.issuer_id || 0, c.acceptor_id || 0, _isoDate(c.date_issued), _isoDate(c.date_expired), c.price || 0, c.reward || 0, c.collateral || 0, c.volume || 0, c.title || '', c.availability || '', c.start_location_id || 0, c.end_location_id || 0]);
  });

  itemResponses.forEach((res, i) => {
    if (res.getResponseCode() === 200) {
      const itemsRaw = JSON.parse(res.getContentText());
      const req = itemRequests[i];
      normalizeItemRows(itemsRaw).forEach(item => {
        outI_Combined.push([req.attributedChar, req.cid, item.type_id || 0, item.quantity || 0, item.is_included ? 'TRUE' : 'FALSE', item.is_singleton ? 'TRUE' : 'FALSE', item.runs || 0, item.me || 0, item.te || 0]);
      });
    }
  });

  // --- PASS 5: COMMIT UN-TRUNCATED DATA BATCHES TO SHEETS ---
  _appendData_(getOrCreateSheet(ss, CONTRACTS_RAW_SHEET, hdrC), hdrC, outC_Combined);
  _appendData_(getOrCreateSheet(ss, CONTRACT_ITEMS_RAW_SHEET, hdrI), hdrI, outI_Combined);
  SCRIPT_PROP.setProperty(PROP_KEY_LAST_CONTRACT_ID, String(maxContractId));

  log.info(`Sync sequence comprehensive. Procured ${outC_Combined.length} total contracts across all pages.`);

  return {
    contracts: outC_Combined.length,
    buyData: { contracts: outC_Combined, items: outI_Combined },
    saleData: { contracts: [], items: [] }
  };
}

function resetContractBookmark() {
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  // Ensure you use the exact string used in your script for PROP_KEY_LAST_CONTRACT_ID
  SCRIPT_PROP.setProperty('PROP_KEY_LAST_CONTRACT_ID', '0');
  Logger.log("Bookmark reset to 0. Run syncContracts() now.");
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

function contractsToMaterialLedger(ss, charIdMap, buyData, holdAnesthesia) {
  const log = LoggerEx.withTag('GESI');

  if (!buyData || !buyData.contracts || buyData.contracts.length === 0 || !buyData.items || buyData.items.length === 0) {
    log.log('contracts->ledger', { status: 'Skipped: In-memory data is empty.' });
    return 0;
  }

  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const MaterialLedger = ML.forSheet(LEDGER_BUY_SHEET);

  const hC_Names = ["char", "contract_id", "type", "status", "issuer_id", "acceptor_id", "date_issued", "date_expired", "price"];
  const hI_Names = ["char", "contract_id", "type_id", "quantity", "is_included"];

  const ix = (arr, name) => arr.indexOf(name);
  const colC = { char: ix(hC_Names, "char"), contract_id: ix(hC_Names, "contract_id"), date_issued: ix(hC_Names, "date_issued"), price: ix(hC_Names, "price") };
  const colI = { contract_id: ix(hI_Names, "contract_id"), type_id: ix(hI_Names, "type_id"), quantity: ix(hI_Names, "quantity"), is_included: ix(hI_Names, "is_included") };

  const LOGGED_IN_CHARS = new Set(getCharNamesFast());
  const buyCids = new Set(buyData.contracts.map(c => c[colC.contract_id]));

  const itemsByCid = {};
  for (const rowI of buyData.items) {
    const cid = rowI[colI.contract_id];
    if (!buyCids.has(cid)) continue;
    if (!itemsByCid[cid]) itemsByCid[cid] = [];
    itemsByCid[cid].push({
      type_id: rowI[colI.type_id],
      qty: Number(rowI[colI.quantity] || 0),
      is_included: String(rowI[colI.is_included]).toUpperCase() === 'TRUE'
    });
  }

  const outRows = [];

  for (const rowC of buyData.contracts) {
    const contractChar = String(rowC[colC.char] || "");
    if (!LOGGED_IN_CHARS.has(contractChar)) continue;
    const cid2 = rowC[colC.contract_id];
    const issued = rowC[colC.date_issued] ? _isoDate(rowC[colC.date_issued]) : "";
    const items = itemsByCid[cid2] || [];

    const rawPrice = Number(rowC[colC.price]) || 0;

    for (const it of items) {
      if (!it.is_included || it.qty <= 0) continue;

      let base_unit_cost = (items.length > 0 && it.qty > 0) ? (rawPrice / items.length) / it.qty : 0;

      outRows.push({
        date: issued,
        type_id: Number(it.type_id),
        qty: Number(it.qty),
        unit_value_filled: base_unit_cost,
        source: "CONTRACT",
        contract_id: String(cid2),
        char: contractChar
      });
    }
  }

  if (outRows.length === 0) return 0;

  let needsWakeUp = false;
  if (!holdAnesthesia && typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

  try {
    const keys = ['source', 'char', 'contract_id', 'type_id'];
    const upsertResult = MaterialLedger.upsert(keys, outRows, true);
    const count = upsertResult.rows !== undefined ? upsertResult.rows : upsertResult;

    log.log('contracts->ledger', { appended_or_updated: count, processed_rows: outRows.length });
    return count;
  } catch (e) {
    log.error('contractsToMaterialLedger WRITE FAILED', e.message);
    throw e;
  } finally {
    if (!holdAnesthesia && needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss);
  }
}




/**
 * Schedules a new job to run the heavy COGS finalization step later.
 */
function triggerContractUnitCostsFinalization() {
  const LOG = LoggerEx.withTag('COGS_TRIGGER');
  const FINALIZER_FUNC = '_runRebuildContractUnitCostsWorker';

  // Schedule the worker to run soon after the main ledger phase exits.
  scheduleOneTimeTrigger(FINALIZER_FUNC, 5000); // 5 seconds delay
  LOG.info(`Scheduled heavy COGS finalization: ${FINALIZER_FUNC}`);
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

function contractsToSalesLedger(ss, charIdMap, saleData, holdAnesthesia) {
  const log = LoggerEx.withTag('GESI');

  if (!saleData || !saleData.contracts || saleData.contracts.length === 0 || !saleData.items || saleData.items.length === 0) {
    log.log('contracts->sales_ledger', { status: 'Skipped: In-memory data is empty.' });
    return 0;
  }

  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const SalesLedger = ML.forSheet(LEDGER_SALE_SHEET);

  const hC_Names = ["char", "contract_id", "type", "status", "issuer_id", "acceptor_id", "date_issued", "date_expired", "price"];
  const hI_Names = ["char", "contract_id", "type_id", "quantity", "is_included"];

  const ix = (arr, name) => arr.indexOf(name);
  const colC = { char: ix(hC_Names, "char"), contract_id: ix(hC_Names, "contract_id"), date_issued: ix(hC_Names, "date_issued"), price: ix(hC_Names, "price") };
  const colI = { contract_id: ix(hI_Names, "contract_id"), type_id: ix(hI_Names, "type_id"), quantity: ix(hI_Names, "quantity"), is_included: ix(hI_Names, "is_included") };

  const LOGGED_IN_CHARS = new Set(getCharNamesFast());
  const saleCids = new Set(saleData.contracts.map(c => c[colC.contract_id]));

  const itemsByCid = {};
  for (const rowI of saleData.items) {
    const cid = rowI[colI.contract_id];
    if (!saleCids.has(cid)) continue;
    if (!itemsByCid[cid]) itemsByCid[cid] = [];
    itemsByCid[cid].push({
      type_id: rowI[colI.type_id],
      qty: Number(rowI[colI.quantity] || 0),
      is_included: String(rowI[colI.is_included]).toUpperCase() === 'TRUE'
    });
  }

  const outRows = [];
  for (const rowC of saleData.contracts) {
    const contractChar = String(rowC[colC.char] || "");
    if (!LOGGED_IN_CHARS.has(contractChar)) continue;

    const cid2 = rowC[colC.contract_id];
    const issued = rowC[colC.date_issued] ? _isoDate(rowC[colC.date_issued]) : "";
    const items = itemsByCid[cid2] || [];

    const price = Number(rowC[colC.price]) || 0;

    for (const it of items) {
      if (!it.is_included || it.qty <= 0) continue;
      let unit_price_filled = (items.length > 0 && it.qty > 0) ? (price / items.length) / it.qty : 0;

      outRows.push({
        date: issued,
        type_id: it.type_id,
        qty: -it.qty,
        unit_value: '',
        unit_value_filled: unit_price_filled,
        source: "SALE",
        contract_id: cid2,
        char: contractChar
      });
    }
  }

  if (outRows.length === 0) return 0;

  let needsWakeUp = false;
  if (!holdAnesthesia && typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

  try {
    const keys = ['source', 'char', 'contract_id', 'type_id'];
    const upsertResult = SalesLedger.upsert(keys, outRows, true);
    const count = upsertResult.rows !== undefined ? upsertResult.rows : upsertResult;

    log.log('contracts->sales_ledger', { appended_or_updated: count, processed_rows: outRows.length });
    return count;
  } catch (e) {
    log.error('contractsToSalesLedger WRITE FAILED', e.message);
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


function rebuildContractUnitCosts(ss) {
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const log = LoggerEx.withTag('CONTRACT_UNIT_COST');
  const LEDGER_BUY_SHEET = 'Material_Ledger';

  const allocMode = String(_getNamedOr_('setting_contract_alloc_mode', 'REF')).toUpperCase();
  const bpcMap = _buildInternalBpcMap_(ss); // 1. LOAD INTERNAL BPC VALUES
  const refMap = _buildRefPriceMap_(ss);
  const priceMap = _buildContractPriceMap_(ss);

  const ci = _getData_(ss, 'Contract Items (RAW)');
  if (!ci || ci.rows.length === 0) return 0;

  const itemsByCid = new Map();
  const allUniqueTids = new Set();

  // Added 'Status' to the destructured headers
  const { contract_id: hCid, type_id: hTid, quantity: hQty, is_included: hInc, runs: hRuns, me: hMe, te: hTe, Status: hStat } = ci.h;

  // PASS 0: Grouping & Unique TID collection
  ci.rows.forEach(row => {
    // --- THE BUFFER SHIELD: Skip rows that are already safely in the ledger ---
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

      // 2. OPTIMIZATION: Do not send TIDs to Fuzzwork if they are already in the BPC map
      if (!refMap.has(tid) && !bpcMap.has(tid)) {
        allUniqueTids.add(tid);
      }
    }
  });

  // If there are no new unprocessed rows, exit cleanly
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

    // PASS 1: Resolve Prices & Calculate Total Reference Value
    items.forEach(item => {
      // 3. RESOLUTION CHAIN: Internal BPC -> Local Tracker -> Fuzzwork
      const internalVal = bpcMap.get(item.tid);
      const priceObj = refMap.get(item.tid) || fallbackMap.get(item.tid);

      item.resolvedPrice = internalVal || priceObj?.buy || 0;
      totalReferenceValue += (item.resolvedPrice * item.qty);
    });

    const pricePerRefUnit = (totalReferenceValue > 0) ? (totalContractValue / totalReferenceValue) : 0;
    const simpleVolumeSplit = (items.length > 0) ? (totalContractValue / items.length) : 0;

    // PASS 2: Final Cost Calculation
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
        metadata: { runs: item.runs, me: item.me, te: item.te } // <--- THE FIX
      });
    });
  }

  if (outRows.length === 0) return 0;

  // --- 5. SHEET WRITE OPERATION ---
  try {
    const MaterialLedger = ML.forSheet(LEDGER_BUY_SHEET);
    const keys = ['source', 'char', 'contract_id', 'type_id'];
    const result = MaterialLedger.upsert(keys, outRows);

    log.info("Upserted " + result.rows + " rows to " + LEDGER_BUY_SHEET + ".");

    // --- 6. SAFE BUFFER UPDATE ---
    const processedIds = Array.from(itemsByCid.keys());
    _markContractsLedgered_(ss, processedIds);

    return result.rows;
  } catch (e) {
    log.error('rebuildContractUnitCosts WRITE FAILED', e.message);
    throw e;
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
    const statIdx = h.indexOf('Status'); // Capitalized only

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
    const statIdx = h.lastIndexOf('Status');

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
  const SCRIPT_PROP = PropertiesService.getScriptProperties();
  ss = ss || SpreadsheetApp.getActiveSpreadsheet();
  const charIdMap = _charIdMap(ss);

  // 1. Sync and capture the result
  const syncResult = syncContracts(ss, charIdMap);

  // 2. LOGIC: If NEW contracts found, perform Ledger and Finalization
  if (syncResult.contracts > 0) {
    log.info(`Sync found ${syncResult.contracts} NEW contracts. Processing...`);

    // Process the data
    if (syncResult.buyData?.contracts?.length > 0) {
      contractsToMaterialLedger(ss, charIdMap, syncResult.buyData, true);
    }
    if (syncResult.saleData?.contracts?.length > 0) {
      contractsToSalesLedger(ss, charIdMap, syncResult.saleData, true);
    }

    // ONLY HERE: Run the finalizer
    log.info('New contracts detected. Running COGS Finalizer.');
    rebuildContractUnitCosts(ss); // Run directly to avoid trigger delay
  } else {
    log.info('No new contracts found. Sync finished. System idle.');
  }
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







