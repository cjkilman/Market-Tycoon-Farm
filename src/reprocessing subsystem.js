
/**
 * REPROCESSING ENGINE - Market-Tycoon Farm
 * Calculates the ISK value of an item if melted down.
 * * @param {number} typeID The ID of the item to reprocess.
 * @param {number} stationYield Station efficiency (e.g., 0.50).
 * @param {number} playerSkill Skill multiplier (e.g., 1.15).
 * @return {number} The ISK value PER SINGLE UNIT of the item.
 */
function getReprocessValue(typeID, stationYield, playerSkill) {
  const ss = SpreadsheetApp.getActiveSpreadsheet();
  
  // 1. Access Required Sheets
  const matSheet = ss.getSheetByName("SDE_invTypeMaterials");
  const typeSheet = ss.getSheetByName("SDE_invTypes");
  const priceSheet = ss.getSheetByName("Market_Data_Raw");
  
  if (!matSheet || !typeSheet || !priceSheet) return 0;

  const matData = matSheet.getDataRange().getValues();
  const typeData = typeSheet.getDataRange().getValues();
  const priceData = priceSheet.getDataRange().getValues();

  // 2. Get portionSize (Batch Size) from SDE_invTypes
  // Note: Ensure your GET_SDE_CONFIG includes 'portionSize' at index 6
  let portionSize = 1;
  const typeEntry = typeData.find(row => row[0] == typeID);
  if (typeEntry) {
    portionSize = typeEntry[6] || 1;
  }

  // 3. Create a Price Map for speed (Buy_Max in Hub)
  const priceMap = new Map();
  priceData.forEach(row => {
    priceMap.set(row[1], row[5]); // type_id -> buy_max
  });

  // 4. Calculate the total value of the batch
  let totalBatchIsk = 0;
  
  // Filter materials for this specific item
  const materials = matData.filter(row => row[0] == typeID);
  
  materials.forEach(mat => {
    const materialTypeID = mat[1];
    const baseQuantity = mat[2];
    const unitPrice = priceMap.get(materialTypeID) || 0;
    
    // Math: Base Qty * Station Yield * Skill Bonus
    const outputQty = baseQuantity * stationYield * playerSkill;
    totalBatchIsk += (outputQty * unitPrice);
  });

  // 5. Return value per unit (Batch Value divided by Batch Size)
  return totalBatchIsk / portionSize;
}