import ballerina/http;
import ballerina/log;
import ballerina/io;
import ballerina/math;
//This service is used to store data in nodes. Each service in the node acts as a local in memory store
map<CacheEntry> cacheEntries;

//Returns single cache entry according to given key
function getCacheEntry(string key) returns json {
    CacheEntry default;
    //CacheEntry obj = cacheEntries[key] ?: default;
    json payload;
    CacheEntry? cacheEntry = cacheEntries[key];
    match cacheEntry {
        CacheEntry entry => {
            entry.lastAccessedTime = time:currentTime().time;
            payload = check <json>entry;
        }
        () => {
            payload = check <json>default;
        }
    }
    //json payload = check <json>obj;
    return payload;
}

//Adds a single cache entry to the store
function setCacheEntry(json jsObj) returns json {
    if (cacheCapacity <= lengthof cacheEntries +1){
        evictEntries();
    }
    string key = jsObj["key"].toString();
    //jsObj.remove(key);
    cacheEntries[key] = check <CacheEntry>jsObj;
    return jsObj;
}

//Returns all the cache entries avaialbe in the node
function getAllEntries() returns json {
    json payload = check <json>cacheEntries;
    return payload;
}

//Returns all the changed entries of the node catagorized according to node IP.
function getChangedEntries() returns json {
    json entries;
    string currentNodeIP = currentNode;
    //Node catagorize
    foreach node in clientMap {
        if (node.config.url != currentNodeIP){
            entries[node.config.url] = [];
        }
    }
    isRelocationOrEvictionRunning = true;
    foreach key, value in cacheEntries {

        if (value.replica){
            string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
            boolean remove = true;
            foreach item in replicaNodes {
                if (item != currentNodeIP){
                    //value["key"] = key;
                    entries[item][lengthof entries[item]] = check <json>value;
                }
                else {
                    remove = false;
                }
            }
            if (remove){
                _ = cacheEntries.remove(key);
            }

        } else {
            string correctNodeIP = hashRing.get(key);
            //Checks if the node is changed
            if (correctNodeIP != currentNodeIP){
                //value["key"] = key;
                entries[correctNodeIP][lengthof entries[correctNodeIP]] = check <json>value;
                _ = cacheEntries.remove(key); //Assuming the response was recieved :/
            }
        }
    }
    isRelocationOrEvictionRunning = false;
    return entries;
}
//Adds multiple entries to the cache.
function storeMultipleEntries(json jsonObj) {
    log:printInfo("Recieved entries");
    if (cacheCapacity <= lengthof cacheEntries + lengthof jsonObj){
        evictEntries();
    }
    isRelocationOrEvictionRunning = true;
    foreach item in jsonObj {
        json nodeItem = item;
        string key = nodeItem["key"].toString();
        nodeItem.remove(key);
        cacheEntries[key] = check <CacheEntry>nodeItem;
    }
    isRelocationOrEvictionRunning = false;
}

function evictEntries() {
    int keyCountToEvict = <int>(cacheCapacity * cacheEvictionFactor);
    // Create new arrays to hold keys to be removed and hold the corresponding timestamps.
    string[] cacheKeysToBeRemoved = [];
    int[] timestamps = [];
    string[] keys = cacheEntries.keys();
    // Iterate through the keys.
    isRelocationOrEvictionRunning = true;
    foreach key in keys {
        CacheEntry? cacheEntry = cacheEntries[key];
        match cacheEntry {
            CacheEntry entry => {
                if (entry.replica){
                    continue;
                }
                // Check and add the key to the cacheKeysToBeRemoved if it matches the conditions.
                checkAndAdd(keyCountToEvict, cacheKeysToBeRemoved, timestamps, key, entry.lastAccessedTime);
            }
            () => {
                // If the key is not found in the map, that means that the corresponding cache is already removed
                // (possibly by a another worker).
            }
        }
    }

    // Return the array.
    //io:println(cacheKeysToBeRemoved);

    json entries;
    string currentNodeIP = currentNode;
    //Node catagorize
    foreach node in clientMap{
        if (node.config.url != currentNodeIP){
            entries[node.config.url] = [];
        }
    }
    foreach c in cacheKeysToBeRemoved {
        // These cache values are ignred. So it is not needed to check the return value for the remove function.
        string[] replicaNodes = hashRing.GetClosestN(c, replicationFact);
        // CacheEntry default;
        // CacheEntry obj = cacheEntries[c] ?: default;
        foreach node in replicaNodes {
            if (node != currentNodeIP){
                //value["key"] = key;
                //json test = {key:c};
                entries[node][lengthof entries[node]] = c;
            }
        }
        _ = cacheEntries.remove(c);
        log:printInfo(c + " Entry Evicted");
    }
    io:println(entries);
    foreach nodeItem in clientMap {
        if (nodeItem.config.url == currentNode){ //Ignore if its the current node
            continue;
        }

        nodeEndpoint = nodeItem;

        var res = nodeEndpoint->delete("/data/evict", untaint entries[nodeItem.config.url]);
        //sends changed entries to correct node
        match res {
            http:Response resp => {
                var msg = resp.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        log:printInfo("Entries sent to " + nodeItem.config.url);
                    }
                    error err => {
                        log:printError(err.message, err = err);
                    }
                }
            }
            error err => {
                log:printError(err.message, err = err);
            }
        }
    }
    isRelocationOrEvictionRunning = false;
}


