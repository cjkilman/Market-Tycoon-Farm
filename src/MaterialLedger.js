function runDowntimeMaintenance() {
  var ss = SpreadsheetApp.getActiveSpreadsheet();
  var LOG = typeof LoggerEx !== 'undefined' ? LoggerEx.withTag('MAINTENANCE') : console;

  LOG.info("Starting Daily Ledger Maintenance...");

  if (typeof pauseSheet === 'function') pauseSheet(ss);

  try {
    // Material Ledger: Split by Item, Source (Loot vs Journal), and Character
    ML.forSheet("Material_Ledger").condenseHistory(30, ['type_id', 'source', 'char'], true);
    LOG.info("Material Ledger Condensed.");

    // Sales Ledger: Split by Item, Source (Market vs Contract), and Character
    ML.forSheet("Sales_Ledger").condenseHistory(30, ['type_id', 'source', 'char'], true);
    LOG.info("Sales Ledger Condensed.");

  } catch (e) {
    LOG.error("Maintenance Error: " + e.message);
  } finally {
    if (typeof wakeUpSheet === 'function') wakeUpSheet(ss);
    LOG.info("Maintenance Complete. Sheet Awake.");
  }
}

function purgeSalesFromMaterialLedger() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const log = LoggerEx.withTag('SURGICAL_PURGE');

  const matSheet = ss.getSheetByName("Material_Ledger");
  const salesSheet = ss.getSheetByName("Sales_Ledger");

  if (!matSheet || !salesSheet) {
    log.error("Required ledger sheets are missing.");
    return;
  }

  // 1. Collect all valid transaction contract_ids from the clean Sales_Ledger
  const salesData = salesSheet.getDataRange().getValues();
  if (salesData.length <= 1) {
    log.warn("Sales Ledger has no data rows to match against.");
    return;
  }
  const salesHeaders = salesData.shift().map(h => String(h).trim().toLowerCase());
  const idxSalesCid = salesHeaders.indexOf('contract_id');

  if (idxSalesCid === -1) {
    log.error("Could not find 'contract_id' column in Sales Ledger.");
    return;
  }

  const salesCidSet = new Set();
  salesData.forEach(row => {
    const cid = String(row[idxSalesCid]).trim();
    if (cid) salesCidSet.add(cid);
  });

  log.info(`Found ${salesCidSet.size} unique contract IDs in clean Sales Ledger.`);

  // 2. Read Material_Ledger and filter out matching rows
  const matData = matSheet.getDataRange().getValues();
  if (matData.length <= 1) return;
  const matHeaders = matData[0]; // Keep header row intact

  const matHeadersLower = matHeaders.map(h => String(h).trim().toLowerCase());
  const idxMatCid = matHeadersLower.indexOf('contract_id');
  const idxMatSource = matHeadersLower.indexOf('source');

  if (idxMatCid === -1 || idxMatSource === -1) {
    log.error("Missing critical columns in Material Ledger headers.");
    return;
  }

  const keptMaterialRows = [matHeaders];
  let purgeCount = 0;

  for (let i = 1; i < matData.length; i++) {
    const row = matData[i];
    const cid = String(row[idxMatCid]).trim();
    const source = String(row[idxMatSource]).toUpperCase();

    // CRITICAL MATCH: If it's labeled TRANSACTION and its ID is in our Sales Set, drop it.
    if (source === 'TRANSACTION' && salesCidSet.has(cid)) {
      purgeCount++;
    } else {
      keptMaterialRows.push(row);
    }
  }

  log.info(`Identified ${purgeCount} misplaced sales records inside Material Ledger.`);

  // 3. Re-write the clean material data array back to the sheet
  if (purgeCount > 0) {
    matSheet.getRange(1, 1, matSheet.getLastRow(), matHeaders.length).clearContent();
    matSheet.getRange(1, 1, keptMaterialRows.length, matHeaders.length).setValues(keptMaterialRows);
    log.info(`SUCCESS: Purged duplicates. Material Ledger is now clean.`);
  } else {
    log.info("No matching misplaced transaction rows were found in Material Ledger.");
  }
}

function runDeduplication() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  // Run this on both ledgers
  deduplicateLedger(ss, "Material_Ledger");
  deduplicateLedger(ss, "Sales_Ledger");
}

function deduplicateLedger(ss, sheetName) {
  const sh = ss.getSheetByName(sheetName);
  if (!sh) return;
  const data = sh.getDataRange().getValues();
  if (data.length <= 1) return;

  const header = data[0];
  const rows = data.slice(1);

  // These keys MUST match your headers
  const idxDate = header.indexOf("date");
  const idxSource = header.indexOf("source");
  const idxContractId = header.indexOf("contract_id");
  const idxTypeId = header.indexOf("type_id");

  const seenKeys = new Set();
  const uniqueRows = [];

  rows.forEach(row => {
    const key = [row[idxDate], row[idxSource], row[idxContractId], row[idxTypeId]].join('|');
    if (!seenKeys.has(key)) {
      uniqueRows.push(row);
      seenKeys.add(key);
    }
  });

  sh.getRange(2, 1, sh.getLastRow() - 1, header.length).clearContent();
  sh.getRange(2, 1, uniqueRows.length, header.length).setValues(uniqueRows);
  console.log(`Deduplicated ${sheetName}. Removed ${rows.length - uniqueRows.length} duplicates.`);
}

function directMaterialLedgerCleanup() {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  const matSheet = ss.getSheetByName("Material_Ledger");

  if (!matSheet) {
    console.error("Material_Ledger sheet not found!");
    return;
  }

  const data = matSheet.getDataRange().getValues();
  if (data.length <= 1) return;

  const headers = data[0];
  const hLower = headers.map(h => String(h).trim().toLowerCase());
  const idxSource = hLower.indexOf('source');
  const idxDate = hLower.indexOf('date');

  const cleanRows = [headers];
  let removedCount = 0;

  // Get today's date string to target the bad rows written during our test runs
  const todayStr = Utilities.formatDate(new Date(), Session.getScriptTimeZone(), "yyyy-MM-dd");

  for (let i = 1; i < data.length; i++) {
    const row = data[i];
    const source = String(row[idxSource]).toUpperCase();

    // FIX 1: Safely handle Date objects vs strings to ensure a perfect match
    const rawDate = row[idxDate];
    const dateStr = rawDate instanceof Date ?
      Utilities.formatDate(rawDate, Session.getScriptTimeZone(), "yyyy-MM-dd") :
      String(rawDate).trim();

    // TARGET: Any 'TRANSACTION' row written with today's date inside the Material_Ledger
    if (source === 'TRANSACTION' && dateStr === todayStr) {
      removedCount++;
    } else {
      cleanRows.push(row);
    }
  }

  if (removedCount > 0) {
    matSheet.getRange(1, 1, matSheet.getLastRow(), headers.length).clearContent();
    matSheet.getRange(1, 1, cleanRows.length, headers.length).setValues(cleanRows);

    // FIX 2: Destroy the stale cache for this sheet since we just modified the physical data
    if (typeof GLOBALS !== 'undefined' && GLOBALS.dataCache) {
      GLOBALS.dataCache.delete("NR_MATERIAL_LEDGER");
    }

    console.log(`SUCCESS: Surgically removed ${removedCount} dirty test entries from Material_Ledger.`);
  } else {
    console.log("No dirty test entries found in Material_Ledger.");
  }
}

// --- GLOBAL SCOPE ---
const GLOBALS = {
  dataCache: new Map(), // Stores parsed data arrays
  rangeCache: new Map()  // Stores NamedRange objects
};

/**
 * Ensures we only fetch data once per execution
 */
function getCachedData(ss, rangeName) {
  if (!GLOBALS.dataCache.has(rangeName)) {
    // Lazy resolve the NamedRange and fetch data
    const nr = ss.getNamedRanges().find(r => r.getName() === rangeName);
    if (!nr) return null;

    // Store both the range and the values
    const data = nr.getRange().getValues();
    GLOBALS.dataCache.set(rangeName, data);
  }
  return GLOBALS.dataCache.get(rangeName);
}



/**
 * Market Ledger (ML) Manager.
 * Handles the ingestion, merging, and historical compression of high-volume market data.
 * Prevents Apps Script timeouts by utilizing bulk array operations and selective pruning.
 *
 * @namespace ML
 */
var ML = (function () {
  const requiredHeaders = ['date', 'type_id', 'item_name', 'qty', 'unit_value', 'source', 'contract_id', 'char', 'unit_value_filled', 'metadata'];

  function getSS_() { return SpreadsheetApp.getActiveSpreadsheet(); }

  /** * Forces any date input (Date Object, ESI String, Timestamp, or YYYY-MM-DD string)
 * into a standard 'yyyy-MM-dd' format.
 */
  function _standardizeDate(val) {
    if (!val) return "";

    // 1. If it's already a clean 'yyyy-mm-dd' string, return it
    if (typeof val === 'string' && /^\d{4}-\d{2}-\d{2}$/.test(val)) return val;

    // 2. Otherwise, use JS Date to parse it, then format it
    const d = new Date(val);
    if (isNaN(d.getTime())) return String(val).trim().toLowerCase(); // Fallback if invalid

    return Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
  }

  function forSheet(sheetName) {
    const ss = getSS_();
    var sh = ss.getSheetByName(sheetName);

    // Auto-create if missing
    if (!sh) {
      sh = ss.insertSheet(sheetName);
      sh.getRange(1, 1, 1, requiredHeaders.length).setValues([requiredHeaders]);
    }

    // 1. DYNAMIC HEAD RESOLUTION
    const rawHead = sh.getRange(1, 1, 1, sh.getLastColumn()).getValues()[0];
    let HEAD = rawHead.map(h => String(h).trim());
    let HEAD_LOWER = HEAD.map(h => h.toLowerCase()); // Keep underscores here

    // 2. CORRECTED INDEX LOOKUPS (Must match requiredHeaders exactly)
    const idxDate = HEAD_LOWER.indexOf('date');
    const idxQty = HEAD_LOWER.indexOf('qty');
    const idxTypeId = HEAD_LOWER.indexOf('type_id'); // Changed from typeid
    const idxSource = HEAD_LOWER.indexOf('source');
    const idxUnitValue = HEAD_LOWER.indexOf('unit_value'); // Changed from unitvalue
    const idxUnitValueFilled = HEAD_LOWER.indexOf('unit_value_filled'); // Changed from unitvaluefilled

    // 3. FAIL-FAST CHECK (Add this immediately after lookups)
    if ([idxTypeId, idxQty, idxUnitValueFilled].includes(-1)) {
      throw new Error(`Critical Header Mismatch: Ensure sheet "${sheetName}" has 'type_id', 'qty', and 'unit_value_filled' columns. Found: ${JSON.stringify(HEAD)}`);
    }

    function normalizeRow_(r) {
      const isArray = Array.isArray(r);
      const row = isArray ? {} : r;
      if (isArray) {
        HEAD.forEach((h, i) => row[h] = r[i]);
      }

      const parsedId = Number(row.type_id);
      if (isNaN(parsedId) || parsedId === 0) throw new Error("Invalid type_id: " + row.type_id);

      // LEAK 3 FIXED: Date Safety Net. Do not shift UTC strings to local time!
      let dateStr;
      if (typeof row.date === 'string' && /^\d{4}-\d{2}-\d{2}/.test(row.date)) {
        dateStr = row.date.substring(0,10); // Capture exact yyyy-mm-dd directly
      } else {
        let d = row.date ? new Date(row.date) : new Date();
        if (isNaN(d.getTime())) d = new Date();
        dateStr = Utilities.formatDate(d, Session.getScriptTimeZone(), "yyyy-MM-dd");
      }

      let meta = row.metadata;
      if (typeof meta === 'string' && meta.startsWith('{')) {
        // Keep as stringified JSON
      } else {
        meta = JSON.stringify(meta || {});
      }

      const out = {
        date: dateStr,
        type_id: parsedId,
        item_name: String(row.item_name || ''),
        qty: Number(String(row.qty || 0).replace(/,/g, '')) || 0,
        unit_value: (parseFloat(String(row.unit_value || 0).replace(/,/g, '')) || 0).toFixed(4),
        unit_value_filled: (parseFloat(String(row.unit_value_filled || 0).replace(/,/g, '')) || 0).toFixed(6),
        source: String(row.source || ''),
        contract_id: String(row.contract_id || ''),
        char: String(row.char || ''),
        metadata: meta
      };

      return HEAD.map(k => (out[k] == null ? '' : out[k]));
    }

    function _updateBlendedSummary(totals) {
      // 1. UPDATED HEADER: Added "total_qty" so it has 4 columns
      const blendedPriceSummary = [["type_id", "total_sum", "total_qty", "unit_weighted_average"]];

      Object.keys(totals).forEach(id => {
        const typeIdNum = Number(id); // Cast string back to integer
        const totalCost = totals[id].i;
        const totalQty = totals[id].q;

        // Guard against zero to prevent division by zero errors
        const weightedAvg = totalQty > 0 ? (totalCost / totalQty) : 0;

        // 2. UPDATED PUSH: Just pass the variable directly, no colons
        blendedPriceSummary.push([
          typeIdNum,
          totalCost,
          Math.round(totalQty), // Just the variable name
          Number(weightedAvg.toFixed())
        ]);
      });

      const tName = (sheetName === "Material_Ledger") ? "Blended_Cost" : "Blended_Sales";
      const bName = (tName === "Blended_Cost") ? "NR_BLENDED_COST" : "NR_BLENDED_SALES";
      const tSh = ss.getSheetByName(tName);

      if (tSh) {
        tSh.getDataRange().clearContent();

        // 3. UPDATED RANGE: Changed the '3' to a '4' to account for the new quantity column
        const range = tSh.getRange(1, 1, blendedPriceSummary.length, 4);
        range.setValues(blendedPriceSummary);

        const existingRanges = ss.getNamedRanges();
        existingRanges.forEach(nr => {
          if (nr.getName() === bName) nr.remove();
        });

        ss.setNamedRange(bName, range);
      }
    }

    function upsertBy(keys, rows, holdAnesthesia) {
      if (!rows || !rows.length) return { rows: 0, status: "SUCCESS" };

      // 1. READ EXISTING DATA (Using Cache First)
      const rangeName = (sheetName === "Material_Ledger") ? "NR_MATERIAL_LEDGER" : "NR_SALES_LEDGER";

      // ss is defined in the parent ML module
      const ss = SpreadsheetApp.getActiveSpreadsheet();
      const cachedData = getCachedData(ss, rangeName);

      let existingRaw = [];
      if (cachedData && cachedData.length > 1) {
        existingRaw = cachedData.slice(1); // Skip headers
      } else {
        // Fallback if cache is empty or range doesn't exist yet
        const lastRow = sh.getLastRow();
        existingRaw = (lastRow >= 2) ? sh.getRange(2, 1, lastRow - 1, HEAD.length).getValues() : [];
      }


      // 2. NORMALIZE EXISTING (The Defacto Format)
      let ghostCount = 0;
      
      let normalizedExisting = existingRaw
        .filter(row => {
          const rawId = row[idxTypeId];
          const isValid = rawId !== undefined && rawId !== null && String(rawId).trim() !== '';
          
          if (!isValid) ghostCount++; // Tally the garbage
          return isValid;
        })
        .map(row => normalizeRow_(row));

      if (ghostCount > 0) {
        console.log(`[UPSERT CAUTION] Safely ignored ${ghostCount} blank ghost rows from the sheet bottom.`);
      }

     // 3. INDEX MAP (LEAK 2 FIXED: Use HEAD_LOWER for case-immune lookups)
      const indexMap = new Map();
      normalizedExisting.forEach((rowArr, i) => {
        const key = keys.map(k => {
          const idx = HEAD_LOWER.indexOf(String(k).trim().toLowerCase());
          return idx > -1 ? String(rowArr[idx]).trim().toLowerCase() : 'MISSING';
        }).join('|');
        indexMap.set(key, i);
      });

      // 4. PROCESS INCOMING (LEAK 1 FIXED: Add appended rows to the map)
      rows.forEach(obj => {
        const rowArr = normalizeRow_(obj);
        const key = keys.map(k => {
          const idx = HEAD_LOWER.indexOf(String(k).trim().toLowerCase());
          return idx > -1 ? String(rowArr[idx]).trim().toLowerCase() : 'MISSING';
        }).join('|');

        if (indexMap.has(key)) {
          // It exists! Overwrite it cleanly.
          normalizedExisting[indexMap.get(key)] = rowArr; 
        } else {
          // It's new! Append it AND index it so same-batch duplicates overwrite this.
          const newIdx = normalizedExisting.length;
          normalizedExisting.push(rowArr);
          indexMap.set(key, newIdx); 
        }
      });

      // 5. WRITE BACK & INVALIDATE CACHE
      if (sh.getLastRow() > 1) {
        sh.getRange(2, 1, sh.getLastRow() - 1, HEAD.length).clearContent();
      }

      if (normalizedExisting.length > 0) {
        sh.getRange(2, 1, normalizedExisting.length, HEAD.length).setValues(normalizedExisting);

        // CRITICAL FIX: Destroy the stale cache so the next function gets fresh data
        GLOBALS.dataCache.delete(rangeName);
      }

      // 6. TOTALS & SUMMARY
      const totals = {};
      normalizedExisting.forEach(row => {
        const typeId = row[idxTypeId];
        if (!typeId) return;
        const qty = Number(row[idxQty]) || 0;
        const cost = Number(row[idxUnitValueFilled]) || Number(row[idxUnitValue]) || 0;
        if (!totals[typeId]) totals[typeId] = { q: 0, i: 0 };
        totals[typeId].q += qty;
        totals[typeId].i += (qty * cost);
      });

      _updateBlendedSummary(totals);

      // 7. OBSERVE & RETURN
      const logMsg = `[ML UPSERT] Incoming Lines: ${rows.length} | Successful Final Rows: ${normalizedExisting.length} | Ghosts Purged: ${ghostCount}`;
      
      if (typeof LoggerEx !== 'undefined') {
        LoggerEx.withTag('LEDGER').info(logMsg);
      } else {
        console.log(logMsg);
      }

      return { rows: normalizedExisting.length, status: "SUCCESS" };
    }

    /**
    * Condensed history logic with Date Safety Net and Map-based grouping
    */
    function condenseHistory(cutoffDays, keys, holdAnesthesia) {
      const lastRow = sh.getLastRow();
      if (lastRow <= 2) return { rows: 0, status: "SUCCESS" };

      const rangeName = (sheetName === "Material_Ledger") ? "NR_MATERIAL_LEDGER" : "NR_SALES_LEDGER";
      const data = getCachedData(ss, rangeName);
      if (!data || data.length < 2) return { rows: 0, status: "SUCCESS" };

      const rows = data.slice(1);
      const cutoffDate = new Date();
      cutoffDate.setDate(cutoffDate.getDate() - cutoffDays);

      const keptRows = [];
      const condensedGroups = new Map(); // Using Map for O(1) lookup performance

      rows.forEach(row => {
        // 1. DATE SAFETY NET: If date is NaN or empty, default to epoch (0) or today
        const rawDate = row[idxDate];
        const rowDate = (rawDate && rawDate !== "") ? new Date(rawDate) : new Date(0);

        if (rowDate < cutoffDate) {
          // Create a unique key for grouping
          const k = keys.map(key => {
            const idx = HEAD_LOWER.indexOf(String(key).toLowerCase());
            return row[idx];
          }).join('|');

          if (!condensedGroups.has(k)) {
            // Initialize this group
            let newRow = [...row];
            // Normalize the date of the condensed row to the cutoff
            newRow[idxDate] = Utilities.formatDate(cutoffDate, Session.getScriptTimeZone(), "yyyy-MM-dd");
            // Track the running cost for weighted average later
            newRow.total_cost = Number(row[idxQty]) * Number(row[idxUnitValueFilled]);
            condensedGroups.set(k, newRow);
          } else {
            // Update existing group
            let group = condensedGroups.get(k);
            const newQty = Number(row[idxQty]) || 0;
            const newCost = newQty * (Number(row[idxUnitValueFilled]) || 0);

            group[idxQty] = Number(group[idxQty]) + newQty;
            group.total_cost += newCost;

            // Recalculate Weighted Avg on the fly
            const totalQty = Number(group[idxQty]);
            group[idxUnitValueFilled] = totalQty > 0 ? (group.total_cost / totalQty) : 0;
            condensedGroups.set(k, group);
          }
        } else {
          keptRows.push(row);
        }
      });

      // Reconstruct the final list
      const condensedRows = Array.from(condensedGroups.values()).map(r => {
        delete r.total_cost; // Clean up helper property
        return r;
      });

      const finalRows = keptRows.concat(condensedRows);

      // Write Back
      let needsWakeUp = false;
      try {
        if (!holdAnesthesia && typeof pauseSheet === 'function') needsWakeUp = pauseSheet(ss);

        // Clear only relevant data area
        sh.getRange(2, 1, sh.getLastRow() - 1, HEAD.length).clearContent();

        // Batch write
        if (finalRows.length > 0) {
          sh.getRange(2, 1, finalRows.length, HEAD.length).setValues(finalRows);
          GLOBALS.dataCache.delete(rangeName);
        }

        return { rows: finalRows.length, status: "SUCCESS" };
      } finally {
        if (!holdAnesthesia && needsWakeUp && typeof wakeUpSheet === 'function') wakeUpSheet(ss);
      }
    }

    function query(criteria) {
      const rangeName = (sheetName === "Material_Ledger") ? "NR_MATERIAL_LEDGER" : "NR_SALES_LEDGER";
      const data = getCachedData(ss, rangeName);
      if (!data || data.length < 2) return [];

      return data.slice(1).map(row => {
        const obj = {};
        HEAD.forEach((h, i) => obj[h] = row[i]);

        const metaIdx = HEAD_LOWER.indexOf('metadata');
        if (metaIdx > -1 && typeof obj.metadata === 'string' && obj.metadata.startsWith('{')) {
          try { obj.metadata = JSON.parse(obj.metadata); } catch (e) { obj.metadata = {}; }
        } else { obj.metadata = {}; }

        if (typeof criteria === 'object') {
          for (let key in criteria) {
            if (Array.isArray(criteria[key])) {
              // Check if the first item in the criteria array is a string or number to handle casting safely
              const isStringQuery = typeof criteria[key][0] === 'string';
              const checkVal = isStringQuery ? String(obj[key]) : Number(obj[key]);

              if (!criteria[key].includes(checkVal)) return null;
            } else if (obj[key] != criteria[key]) {
              return null;
            }
          }
        }
        return obj;
      }).filter(item => item !== null);
    }

    return { upsert: upsertBy, condenseHistory: condenseHistory, query: query };
  }

  return { forSheet: forSheet };
})();