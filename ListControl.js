/**
 * MasterControl IIFE to safely manage an in-memory list of items
 * with concurrency control via LockService.
 *
 * NOTE: This is intended for Google Apps Script where LockService is available.
 */



/**
 * 
 * TODO:
 * add list of items if not exists
 * time Stamps when Items are Accessed or read. This will Allow Fuz Cache Tasks
 * to filter needs based on Time
 */


var MasterControl = (() => {
  // Private variables
  const lock = LockService.getScriptLock();
  // Renamed to 'lock_timeout_ms' for clarity, used in tryLock (milliseconds)
  const LOCK_TIMEOUT_MS = 10000; // 10 seconds for a safer, non-short lock

  // TTL (Time To Live) for items in the list, in hours.
  // This will be used to determine if an item is "stale" and needs to be refreshed.
  const DEFAULT_ITEM_TTL_HOURS = 6;
  let item_ttl_hours = DEFAULT_ITEM_TTL_HOURS; // settable via setExpireTime

  // The core list, storing objects with at least:
  // { type_id: number, market_id: number, market_type: string, last_updated: Date }
  let list = [];

  // --- Utility Functions ---

  /**
   * Generates a unique key for an item based on its core identifiers.
   * @param {Object} item The item object.
   * @returns {string} A unique composite key.
   */
  function getItemKey(item) {
    // Assuming the composite key is type_id + market_id + market_type
    return `${item.type_id}-${item.market_id}-${item.market_type}`;
  }

  // --- Public/Exposed Methods Implementations ---

  /**
   * Adds an item to the list if it doesn't exist

   * @param {Object} item The item to add/update. Must have type_id, market_id, market_type.
   * @returns {boolean} True if the item was added/updated, false if the lock failed.
   */
  function Add(item) {
    if (!lock.tryLock(LOCK_TIMEOUT_MS)) //TODO: Consider Lock.Wait()
         {
      console.warn("Failed to acquire lock for Add operation.");
      return false;
    }

    try {

      // 2. Check for an existing item to update (de-duplication logic)
      const key = getItemKey(item);
      const existingIndex = list.findIndex(i => getItemKey(i) === key);

      if (existingIndex > -1) {
        // Update the existing item
        list[existingIndex] = item;
        console.log(`Updated existing item with key: ${key}`);
      } else {
        // Add the new item
        list.push(item);
        console.log(`Added new item with key: ${key}`);
      }
      return true;
    } finally {
      lock.releaseLock();
    }
  }

  /**
   * Removes an item from the list based on its core identifiers.
   * @param {Object} item The item to remove (requires type_id, market_id, market_type).
   * @returns {boolean} True if the item was removed, false if lock failed or item not found.
   */
  function Remove(item) {
    if (!lock.tryLock(LOCK_TIMEOUT_MS)) { //Coonsider Lock.Wait
      console.warn("Failed to acquire lock for Remove operation.");
      return false;
    }

    try {
      const keyToRemove = getItemKey(item);
      const initialLength = list.length;

      // Filter out the item to remove
      list = list.filter(i => getItemKey(i) !== keyToRemove);

      return list.length < initialLength; // Returns true if an item was actually removed
    } finally {
      lock.releaseLock();
    }
  }

  /**
   * Returns a *copy* of the current list to prevent external modification.
   * @returns {Array<Object>} A copy of the master list.
   */
  function getList() { //Set Time Stamps here on read??
    // Return a shallow copy to prevent external code from modifying the private 'list'
    return [...list];
  }

  /**
   * Replaces the entire list with a new list of items.
   * @param {Array<Object>} newList The new list to set.
   * @returns {boolean} True if the list was set, false if the lock failed.
   */
  function setList(newList) {
    if (!lock.tryLock(LOCK_TIMEOUT_MS)) {
      console.warn("Failed to acquire lock for setList operation.");
      return false;
    }
    try {
      // Ensure the new list is an array and make a copy
      list = Array.isArray(newList) ? [...newList] : [];
      return true;
    } finally {
      lock.releaseLock();
    }
  }

  /**
   * Clears items whose 'last_updated' timestamp is older than the configured TTL.
   * @returns {boolean} True if the operation was successful, false if the lock failed.
   */
  function clearOld() {
    if (!lock.tryLock(LOCK_TIMEOUT_MS)) {
      console.warn("Failed to acquire lock for clearOld operation.");
      return false;
    }

    try {
      const expirationDate = new Date();
      // Calculate the time in milliseconds ago
      const ttl_ms = item_ttl_hours * 60 * 60 * 1000;
      expirationDate.setTime(expirationDate.getTime() - ttl_ms);

      const initialCount = list.length;
      list = list.filter(item => {
        // Keep items that don't have a timestamp or whose timestamp is newer than the expiration date
        return !item.last_updated || item.last_updated.getTime() >= expirationDate.getTime();
      });

      console.log(`Cleared ${initialCount - list.length} stale items.`);
      return true;
    } finally {
      lock.releaseLock();
    }
  }

  /**
   * Sets the Time To Live (TTL) for items in hours.
   * @param {number} hours The new expiration time in hours.
   * @returns {boolean} True if the TTL was set.
   */
  function setExpireTime(hours) {
    if (typeof hours === 'number' && hours > 0) {
      item_ttl_hours = hours;
      console.log(`Item TTL set to ${hours} hours.`);
      return true;
    }
    return false;
  }

  // NOTE: The deDuplicate function is essentially handled by the Add function's logic.
  // A separate function would be needed to clean up an *already* corrupted list
  // or a list set from an external source without using Add().

  // --- Expose Public Methods (Module Pattern) ---
  return {
    addItem: Add,
    removeItem: Remove,
    getList: getList,
    setList: setList,
    clearOld: clearOld,
    setExpireTime: setExpireTime,
  };
})();


