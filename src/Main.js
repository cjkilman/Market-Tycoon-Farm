function onOpen() {
  var ui = SpreadsheetApp.getUi();
  // Or DocumentApp or FormApp.
  ui.createMenu('Sheet Tools')
      .addItem('Refresh All Data','refreshData')
      .addItem('Update SDE Data', 'importSDE')
      .addToUi();
}

function getStructureNames(structureIDs){
  if(!(Array.isArray(structureIDs))){structureIDs=[[structureIDs]]};
  var output=[];
  for(var i=0;i<structureIDs.length;i++){
    var data=GESI.universe_structures_structure(structureIDs[i][0],GESI.getMainCharacter(),false);
    output.push(data[0][0]);
  }
  return output;
}

function pull_SDE()
{
       // Lock Formulas from running
      const haltFormulas = [[0,0]];

      var thisSpreadSheet = SpreadsheetApp.getActiveSpreadsheet();
      var loadingHelper= thisSpreadSheet.getRangeByName("'Utility'!B3:C3");
      const  backupSettings = loadingHelper.getValues();
      loadingHelper.setValues(haltFormulas); 

      try{

    const sdePages = [
        new SdePage(
        "SDE_invTypes",
        "invTypes.csv",
           // Optional headers,  
           // invTypes is 100+ megabytes. Select columns needed to help it load faster. 
          [ "typeID","groupID","typeName","volume"]
          ),
      new SdePage(
        "SDE_staStations",
        "staStations.csv",
           // Optional headers,  
           // invTypes is 100+ megabytes. Select columns needed to help it load faster. 
          ["stationID",	"security",	"stationTypeID",	"corporationID",	"solarSystemID", "regionID",	"stationName"	]
          ),
      new SdePage(
        "SDE_industryActivityProducts",
        "industryActivityProducts.csv",
          []
          )
      ];
      sdePages.forEach(buildSDEs);
      }
    finally{
          // release lock
          loadingHelper.setValues(backupSettings); 
        }
}

function importSDE()
{
  
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
function getChacterNameFromID(charIds,show_column_headings=true)
{
    if(!charIds)throw "undefined charIds";
    if(!Array.isArray(charIds)) charIds=[charIds];
    charIds = charIds.filter(Number) ;

    let chars=[];
    if(show_column_headings) chars = chars.concat("chacacter_name");
    const rowIdx =  show_column_headings ? 1:0;

    for(I=0;I<charIds.length;I++)
    {
      try{
        const char = GESI.characters_character(Number(charIds[I]),show_column_headings);
        chars = chars.concat(char[rowIdx][7]);
      }
      catch(e)
      {
        throw e;
      }
    }
    Logger.log(chars);
    return chars;
}

/**
 * Replace [Header Name] tokens in a QUERY-like SQL with ColN or A1 letters,
 * based on the first row (header) of the given range.
 *
 * @param {Range|string} rangeRef  Range object, A1 string ("Sheet!B1:J1"), or Named Range
 * @param {string} queryString     SQL containing bracketed headers: [Item], [Goal], ...
 * @param {boolean} [useColNums=true]  true -> Col1, Col2... ; false -> letters A,B,...
 * @returns {string}
 */
function sqlFromHeaderNamesEx(rangeRef, queryString, useColNums) {
  if (useColNums == null) useColNums = true;

  const ss = SpreadsheetApp.getActiveSpreadsheet();
  let range = null;

  // 1) If a Range object was passed
  if (rangeRef && typeof rangeRef.getA1Notation === 'function') {
    range = rangeRef;
  } else if (typeof rangeRef === 'string') {
    // 2) Try Named Range first (doesn't throw; returns null if missing)
    range = ss.getRangeByName(rangeRef);
    if (!range) {
      // 3) Try A1 notation (may throw)
      try {
        range = ss.getRange(rangeRef); // Supports "Sheet!A1:B" as well
      } catch (e) {
        // Fall through; we'll error below with a clear message
      }
    }
  }

  if (!range) {
    throw new Error(`sqlFromHeaderNamesEx: could not resolve range from "${rangeRef}". 
Pass a Range, a valid A1 like "Sheet!B1:J1", or an existing Named Range.`);
  }

  // Build header row (first row of the range)
  const headerWidth = range.getNumColumns();
  const headerRow = range.offset(0, 0, 1, headerWidth).getValues()[0];

  // Map header text -> replacement (ColN or letters)
  const map = {};
  for (let i = 0; i < headerRow.length; i++) {
    const raw = headerRow[i];
    if (raw == null) continue;
    const h = String(raw).trim();
    if (!h) continue;

    const replacement = useColNums
      ? `Col${i + 1}`
      : range.getCell(1, i + 1).getA1Notation().replace(/\d+/g, ""); // letters only

    // keep last-seen on duplicates
    map[h] = replacement;
  }

  // Replace [Header Name] tokens (case-insensitive label match)
  const rewritten = String(queryString).replace(/\[([^\]]+)\]/g, (m, label) => {
    const key = String(label || "").trim();
    if (map.hasOwnProperty(key)) return map[key];
    const found = Object.keys(map).find(k => k.toLowerCase() === key.toLowerCase());
    return found ? map[found] : m; // leave untouched if no match
  });

  return rewritten;
}


/**
 * Enhances Google Sheets' native "query" method.  Allows you to specify column-names instead of using the column letters in the SQL statement (no spaces allowed in identifiers)
 * 
 * Sample : =query(data!A1:I,SQL("data!A1:I1","SELECT Owner-Name,Owner-Email,Type,Account-Name",false),true)
 *  
 * Params : useColNums (boolean) : false/default = generate "SELECT A, B, C" syntax 
 *                                 true = generate "SELECT Col1, Col2, Col3" syntax
 * reference: https://productforums.google.com/forum/#!topic/docs/vTgy3hgj4M4
 * by: Matthew Quinlan
 */
function sqlFromHeaderNames(rangeName, queryString, useColNums){
 
  let ss = SpreadsheetApp.getActiveSpreadsheet();

  let range;
  try{
    range = ss.getRange(rangeName);
  }
  catch(e){
    range = ss.getRangeByName(rangeName);
  }

  let headers = range.getValues()[0];
  
  for (var i=0; i<headers.length; i++) {
    if (headers[i].length < 1) continue;
    var re = new RegExp("\\b"+headers[i]+"\\b","gm");
    if (useColNums) {
      var columnName="Col"+Math.floor(i+1);
      queryString = queryString.replace(re,columnName);
    }
    else {
      var columnLetter=range.getCell(1,i+1).getA1Notation().split(/[0-9]/)[0];
      queryString = queryString.replace(re,columnLetter);
    }
  }
  //Logger.log(queryString);
  return queryString;
}
