import ballerina/system;
import ballerina/task;
import ballerina/time;

type LocalCacheEntry record {
    any value;
    int lastAccessedTime;
};
public type LocalCache object {

    private int capacity;
    map<LocalCacheEntry> entries;
    private float evictionFactor;

    public new(capacity = 100, evictionFactor = 0.25) {
        // Cache capacity must be a positive value.
        if (capacity <= 0) {
            error e = { message: "Capacity must be greater than 0." };
            throw e;
        }
        // Cache eviction factor must be between 0.0 (exclusive) and 1.0 (inclusive).
        if (evictionFactor <= 0 || evictionFactor > 1) {
            error e = { message: "Cache eviction factor must be between 0.0 (exclusive) and 1.0 (inclusive)." };
            throw e;
        }
    }

    public function hasKey(string key) returns (boolean) {
        return entries.hasKey(key);
    }

    public function size() returns (int) {
        return lengthof entries;
    }


    public function put(string key, any value) {
        // We need to synchronize this process otherwise concurrecy might cause issues.
        lock {
            int cacheCapacity = capacity;
            int cacheSize = lengthof entries;

            // If the current cache is full, evict cache.
            if (cacheCapacity <= cacheSize) {
                evict();
            }
            // Add the new cache entry.
            int time = time:currentTime().time;
            LocalCacheEntry entry = { value: value, lastAccessedTime: time };
            entries[key] = entry;
        }
    }

    function evict() {
        int maxCapacity = capacity;
        float ef = evictionFactor;
        int numberOfKeysToEvict = <int>(maxCapacity * ef);
        // Get the above number of least recently used cache entry keys from the cache
        string[] cacheKeys = getLRUCacheKeys(numberOfKeysToEvict);
        // Iterate through the map and remove entries.
        foreach c in cacheKeys {
            // These cache values are ignred. So it is not needed to check the return value for the remove function.
            _ = entries.remove(c);
        }
    }


    public function get(string key) returns any? {
        // Check whether the requested cache is available.
        if (!hasKey(key)){
            return ();
        }
        // Get the requested cache entry from the map.
        LocalCacheEntry? cacheEntry = entries[key];

        match cacheEntry {
            LocalCacheEntry entry => {
                // Modify the last accessed time and return the cache if it is not expired.
                entry.lastAccessedTime = time:currentTime().time;
                return entry.value;
            }
            () => {
                return ();
            }
        }
    }


    public function remove(string key) {
        // Cache might already be removed by the cache clearing task. So no need to check the return value.
        _ = entries.remove(key);
    }


    public function keys() returns string[] {
        return entries.keys();
    }

    function getLRUCacheKeys(int numberOfKeysToEvict) returns (string[]) {
        // Create new arrays to hold keys to be removed and hold the corresponding timestamps.
        string[] cacheKeysToBeRemoved = [];
        int[] timestamps = [];
        string[] keys = self.entries.keys();
        // Iterate through the keys.
        foreach key in keys {
            LocalCacheEntry? cacheEntry = entries[key];
            match cacheEntry {
                LocalCacheEntry entry => {
                    // Check and add the key to the cacheKeysToBeRemoved if it matches the conditions.
                    checkAndAdd(numberOfKeysToEvict, cacheKeysToBeRemoved, timestamps, key, entry.lastAccessedTime);
                }
                () => {
                    // If the key is not found in the map, that means that the corresponding cache is already removed
                    // (possibly by a another worker).
                }
            }
        }
        // Return the array.
        return cacheKeysToBeRemoved;
    }
};

function checkAndAdd(int numberOfKeysToEvict, string[] cacheKeys, int[] timestamps, string key, int lastAccessTime) {
    string myKey = key;
    int myLastAccessTime = lastAccessTime;

    // Iterate while we count all values from 0 to numberOfKeysToEvict exclusive of numberOfKeysToEvict since the
    // array size should be numberOfKeysToEvict.
    foreach index in 0..<numberOfKeysToEvict {
        // If we have encountered the end of the array, that means we can add the new values to the end of the
        // array since we haven’t reached the numberOfKeysToEvict limit.
        if (lengthof cacheKeys == index) {
            cacheKeys[index] = myKey;
            timestamps[index] = myLastAccessTime;
            // Break the loop since we don't have any more elements to compare since we are at the end
            break;
        } else {
            // If the timestamps[index] > lastAccessTime, that means the cache which corresponds to the 'key' is
            // older than the current entry at the array which we are checking.
            if (timestamps[index] > myLastAccessTime) {
                // Swap the values. We use the swapped value to continue to check whether we can find any place to
                // add it in the array.
                string tempKey = cacheKeys[index];
                int tempTimeStamp = timestamps[index];
                cacheKeys[index] = myKey;
                timestamps[index] = myLastAccessTime;
                myKey = tempKey;
                myLastAccessTime = tempTimeStamp;
            }
        }
    }
}
