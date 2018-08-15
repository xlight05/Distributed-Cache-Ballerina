import ballerina/time;


type CacheEntry record {
    any value;
    int lastAccessedTime;
    int timesAccessed;
    int createdTime;
};


public type Cache object {

    private map<CacheEntry> entries;
    private float evictionFactor;

    public new(evictionFactor = 0.25) {

        // Cache eviction factor must be between 0.0 (exclusive) and 1.0 (inclusive).
        if (evictionFactor <= 0 || evictionFactor > 1) {
            error e = { message: "Cache eviction factor must be between 0.0 (exclusive) and 1.0 (inclusive)." };
            throw e;
        }

    }

    public function put(string key, any value) {
        int currentTime = time:currentTime().time;
        CacheEntry entry = {value:value,lastAccessedTime:currentTime,timesAccessed:0,createdTime:currentTime};
        entries[key]=entry;

    }

    public function get(string key) returns any? {
        if (!hasKey(key)){
            return  ();
        }
        map temp;
        int currentTime = time:currentTime().time;
        CacheEntry ent;
        CacheEntry entry = entries[key] ?: ent;
        entry.lastAccessedTime = currentTime;
        //entries[key]= entry;
        return entry.value;
    }

    public function size() returns int {
        return lengthof entries;
    }
    public function hasKey (string key) returns boolean {
        return entries.hasKey(key);
    }

};
