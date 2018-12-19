import ballerina/system;
import ballerina/task;
import ballerina/time;

map<LocalCache> localCacheMap={};

# Cache cleanup task starting delay in ms.
const int LOCAL_CACHE_CLEANUP_START_DELAY = 0;

# Cache cleanup task invoking interval in ms.
const int LOCAL_CACHE_CLEANUP_INTERVAL = 5000;

# Cleanup task which cleans the cache periodically.
task:Timer localCacheCleanupTimer = createlocalCacheCleanupTask();

# Represents a cache entry.
#
# + value - cache value
# + lastAccessedTime - last accessed time in ms of this value which is used to remove LRU cached values
type LocalCacheEntry record {
    anydata value;
    int lastAccessedTime;
    !...
};
type LocalCacheConfig record {
    int capacity;
    float evictionFactor;
    int expiryTimeMillis;
};

public type LocalCache object {

    map<LocalCacheEntry> entries={};
    LocalCacheConfig config={expiryTimeMillis:900000,capacity:100,evictionFactor:0.25};

    public function __init(int expiryTimeMillis,int capacity,float evictionFactor) {
        // Cache expiry time must be a positive value.
        if (expiryTimeMillis <= 0) {
            error e = error("Expiry time must be greater than 0.");
            panic e;
        }
        // Cache capacity must be a positive value.
        if (capacity <= 0) {
            error e = error("Capacity must be greater than 0.");
            panic e;
        }
        // Cache eviction factor must be between 0.0 (exclusive) and 1.0 (inclusive).
        if (evictionFactor <= 0 || evictionFactor > 1) {
            error e = error("Cache eviction factor must be between 0.0 (exclusive) and 1.0 (inclusive).");
            panic e;
        }

        self.config.expiryTimeMillis = expiryTimeMillis;
        self.config.capacity = capacity;
        self.config.evictionFactor = evictionFactor;
        localCacheMap[system:uuid()]=self;
    }

    // public function __init(LocalCacheConfig cfg) {
    //     self.config = cfg;
    // }

    public function getLocalCacheConfig () returns json {
        json|error localCacheConfig = json.convert(self.config);
        if (localCacheConfig is json){
            return localCacheConfig;
        }else {
            return {};
        }
    }
    public function hasKey(string key) returns (boolean) {
        return self.entries.hasKey(key);
    }

    public function size() returns (int) {
        return self.entries.length();
    }


    public function put(string key, anydata value) {
        // We need to synchronize this process otherwise concurrecy might cause issues.
        lock {
            int cacheSize = self.entries.length();
            // If the current cache is full, evict cache.
            if (self.config.capacity <= cacheSize) {
                self.evict();
            }
            // Add the new cache entry.
            int time = time:currentTime().time;
            LocalCacheEntry entry = { value: value, lastAccessedTime: time };
            self.entries[key] = entry;
        }
    }

    function evict() {
        int maxCapacity = self.config.capacity;
        float ef = self.config.evictionFactor;
        int numberOfKeysToEvict = <int>(maxCapacity * ef);
        // Get the above number of least recently used cache entry keys from the cache
        string[] cacheKeys = self.getLRUCacheKeys(numberOfKeysToEvict);
        // Iterate through the map and remove entries.
        foreach var c in cacheKeys {
            // These cache values are ignred. So it is not needed to check the return value for the remove function.
            _ = self.entries.remove(c);
        }
    }


    public function get(string key) returns anydata? {
        // Check whether the requested cache is available.
        if (!self.hasKey(key)) {
            return ();
        }
        // Get the requested cache entry from the map.
        LocalCacheEntry? cacheEntry = self.entries[key];

        if (cacheEntry is LocalCacheEntry) {
                int currentSystemTime = time:currentTime().time;
                if (currentSystemTime >= cacheEntry.lastAccessedTime + self.config.expiryTimeMillis) {
                    // If it is expired, remove the cache and return nil.
                    self.remove(key);
                    return ();
                }
                // Modify the last accessed time and return the cache if it is not expired.
                cacheEntry.lastAccessedTime = currentSystemTime;
                return cacheEntry.value;

        } else {
            return ();
        }
    }


    public function remove(string key) {
        // Cache might already be removed by the cache clearing task. So no need to check the return value.
        _ = self.entries.remove(key);
    }


    public function keys() returns string[] {
        return self.entries.keys();
    }

    function getLRUCacheKeys(int numberOfKeysToEvict) returns (string[]) {
        // Create new arrays to hold keys to be removed and hold the corresponding timestamps.
        string[] cacheKeysToBeRemoved = [];
        int[] timestamps = [];
        string[] keys = self.entries.keys();
        // Iterate through the keys.
        foreach var key in keys {
            LocalCacheEntry? cacheEntry = self.entries[key];
            if (cacheEntry is LocalCacheEntry){
                    // Check and add the key to the cacheKeysToBeRemoved if it matches the conditions.
                    checkAndAdd(numberOfKeysToEvict, cacheKeysToBeRemoved, timestamps, key, cacheEntry.lastAccessedTime);
            }
        }
        // Return the array.
        return cacheKeysToBeRemoved;
    }
};

# Removes expired cache entries from all caches.
#
# + return - Any error which occured during cache expiration
function runCacheExpiry() returns error? {
    // Iterate through all caches.
    foreach var (currentCacheKey, currentCache) in localCacheMap {
        // Get the expiry time of the current cache
        int currentCacheExpiryTime = currentCache.config.expiryTimeMillis;
        // Create a new array to store keys of cache entries which needs to be removed.
        string[] cachesToBeRemoved = [];
        int cachesToBeRemovedIndex = 0;
        // Iterate through all keys.
        foreach var (key, entry) in currentCache.entries {
            // Get the current system time.
            int currentSystemTime = time:currentTime().time;
            // Check whether the cache entry needs to be removed.
            if (currentSystemTime >= entry.lastAccessedTime + currentCacheExpiryTime) {
                cachesToBeRemoved[cachesToBeRemovedIndex] = key;
                cachesToBeRemovedIndex = cachesToBeRemovedIndex + 1;
            }
        }
        // Iterate through the key list which needs to be removed.
        foreach var currentKeyIndex in 0..<cachesToBeRemovedIndex {
            string key = cachesToBeRemoved[currentKeyIndex];
            // Remove the cache entry.
            _ = currentCache.entries.remove(key);
        }
    }
    return ();
}

function checkAndAdd(int numberOfKeysToEvict, string[] cacheKeys, int[] timestamps, string key, int lastAccessTime) {
    string myKey = key;
    int myLastAccessTime = lastAccessTime;

    // Iterate while we count all values from 0 to numberOfKeysToEvict exclusive of numberOfKeysToEvict since the
    // array size should be numberOfKeysToEvict.
    foreach var index in 0..<numberOfKeysToEvict {
        // If we have encountered the end of the array, that means we can add the new values to the end of the
        // array since we haven’t reached the numberOfKeysToEvict limit.
        if (cacheKeys.length() == index) {
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

# Creates a new cache cleanup task.
#
# + return - cache cleanup task ID
function createlocalCacheCleanupTask() returns task:Timer {
    (function () returns error?) onTriggerFunction = runCacheExpiry;
    task:Timer localTimer = new(onTriggerFunction, (), LOCAL_CACHE_CLEANUP_INTERVAL, delay = LOCAL_CACHE_CLEANUP_START_DELAY);
    localTimer.start();
    return localTimer;
}
