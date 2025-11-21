// ----------------------------------------------------------------------
// --- DATA HELPERS (The "Work" you didn't want to do) ---
// ----------------------------------------------------------------------

/**
 * Generic helper to read a sheet into a Map.
 * @param {string} sheetName - Name of the sheet to read.
 * @param {string} keyHeader - Header name for the Key (e.g., 'typeID').
 * @param {string|string[]} valHeaders - Header name(s) for the Value.
 */
function _readSheetToMap(ss, sheetName, keyHeader, valHeaders) {
    if (!ss) ss = SpreadsheetApp.getActiveSpreadsheet();
    const sheet = ss.getSheetByName(sheetName);
    if (!sheet) {
        console.warn(`Sheet '${sheetName}' not found. Returning empty map.`);
        return new Map();
    }

    const data = sheet.getDataRange().getValues();
    if (data.length < 2) return new Map();

    const headers = data[0];
    const keyIdx = headers.indexOf(keyHeader);
    
    // Resolve value indices
    const valIndices = Array.isArray(valHeaders) 
        ? valHeaders.map(h => headers.indexOf(h)) 
        : [headers.indexOf(valHeaders)];

    if (keyIdx === -1 || valIndices.some(i => i === -1)) {
        console.warn(`Missing headers in '${sheetName}'. Found: ${headers}`);
        return new Map();
    }

    const map = new Map();
    for (let i = 1; i < data.length; i++) {
        const row = data[i];
        const key = row[keyIdx];
        if (!key) continue;

        if (Array.isArray(valHeaders)) {
            // Return object if multiple values requested
            const obj = {};
            valHeaders.forEach((h, n) => {
                obj[h] = row[valIndices[n]];
            });
            map.set(key, obj);
        } else {
            // Return single value
            map.set(key, row[valIndices[0]]);
        }
    }
    return map;
}

function _getSdeNameMap(ss) {
    // Maps typeID -> typeName
    return _readSheetToMap(ss, "SDE_invTypes", "typeID", "typeName");
}

function _getConfigPresetRuns(ss) {
    // Maps bp_type_id -> preset_runs
    return _readSheetToMap(ss, "Config_BPC_Runs", "bp_type_id", "preset_runs");
}

function _getMarketMedianMap(ss) {
    // Maps type_id_filtered -> Median Buy (Safe default for material cost)
    return _readSheetToMap(ss, "market price Tracker", "type_id_filtered", "Median Buy");
}

function _getBpoAmortizationMap(ss) {
    // Maps bp_type_id -> Amortization_Runs
    return _readSheetToMap(ss, "BPO_Amortization", "bp_type_id", "Amortization_Runs");
}

/**
 * Fetches Corp Blueprints from ESI to get ME/TE levels.
 * Maps Blueprint Type ID -> { material_efficiency, time_efficiency }
 * Uses the 'Best' (Max ME) blueprint if multiple exist for a type.
 */
function _getBpoAttributesMapFromEsi() {
    const blueprints = _getCorporateBlueprintsRaw(false);
    const map = new Map();

    if (!blueprints) return map;

    blueprints.forEach(bp => {
        // Only care about BPOs (quantity -2 means copy, usually, but BPOs are unique items)
        // Actually, quantity -1 is BPO, -2 is BPC in some contexts, but ESI has 'quantity'.
        // ESI 'quantity' is -1 for singleton BPOs inside corp hangars usually.
        
        const typeId = bp.type_id;
        const me = bp.material_efficiency;
        
        // If we have multiple BPOs of same type, assume we use the best one
        if (!map.has(typeId) || map.get(typeId).material_efficiency < me) {
            map.set(typeId, { 
                material_efficiency: me,
                time_efficiency: bp.time_efficiency
            });
        }
    });
    return map;
}

/**
 * Fetches Corporate Blueprints from ESI.
 * Uses the same GESI pattern as the Job fetcher.
 */
function _getCorporateBlueprintsRaw(forceRefresh) {
    const authToon = getCorpAuthChar();
    const cacheKey = 'CORP_BLUEPRINTS_V1:' + authToon;
    
    // 1. Cache Check (Utility.js)
    if (!forceRefresh) {
        const cached = _getAndDechunk(cacheKey);
        if (cached) return JSON.parse(cached);
    }

    // 2. Fetch
    try {
        const client = GESI.getClient();
        let corpId = 0;
        
        // Resolve Corp ID (Simplified)
        const charData = GESI.getCharacterData ? GESI.getCharacterData(authToon) : null;
        if (charData) corpId = charData.corporation_id;
        
        if (!corpId) throw new Error("Corp ID not found for BP fetch.");

        // Use invokeRaw for simplicity here as it handles pagination automatically for some endpoints,
        // but for safety we use the standard pattern.
        // Blueprints can be heavy, so we use the Parallel Fetcher pattern if needed, 
        // but usually there are fewer BPs than Assets. Let's use a simple fetchAll loop.
        
        let allBps = [];
        let page = 1;
        let pages = 1;
        
        do {
           const req = client.buildRequest({ 
               corporation_id: corpId, 
               page: page,
               name: authToon 
           });
           // manually override URL to blueprints endpoint if client defaults to jobs
           req.url = req.url.replace('/industry/jobs/', '/blueprints/'); 
           
           // Actually, safer to just use GESI.corporations_corporation_blueprints directly if available
           // But let's use UrlFetch for consistency.
           // The endpoint is /corporations/{corporation_id}/blueprints/
           
           const endpoint = `https://esi.evetech.net/v3/corporations/${corpId}/blueprints/?datasource=tranquility&page=${page}`;
           const params = {
               headers: { "Authorization": `Bearer ${GESI.getAccessToken(authToon)}` },
               muteHttpExceptions: true
           };
           
           const resp = UrlFetchApp.fetch(endpoint, params);
           if (resp.getResponseCode() !== 200) break;
           
           allBps.push(...JSON.parse(resp.getContentText()));
           pages = Number(resp.getHeaders()['x-pages'] || 1);
           page++;
        } while (page <= pages);

        // Cache it
        _chunkAndPut(cacheKey, JSON.stringify(allBps), 3600);
        return allBps;

    } catch (e) {
        console.warn("Failed to fetch blueprints:", e);
        return [];
    }
}