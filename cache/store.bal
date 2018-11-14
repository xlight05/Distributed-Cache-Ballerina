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
    if (cacheCapacity <= lengthof cacheEntries ){
        evictEntries();
    }
    boolean isReplica= check <boolean> jsObj["replica"];
    string key;
    //jsObj.remove(key);
    if (isReplica){
         key = "R:"+jsObj["key"].toString();
    }else {
        key = "O:"+jsObj["key"].toString();
    }
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
            string[] replicaNodes = hashRing.GetClosestN(value.key, replicationFact);
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
            string correctNodeIP = hashRing.get(value.key);
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
    log:printInfo (jsonObj.toString());
    if (cacheCapacity <= lengthof cacheEntries + lengthof jsonObj){
        evictEntries();
    }
    isRelocationOrEvictionRunning = true;
    foreach nodeItem in jsonObj {
        boolean isReplica= check <boolean> nodeItem["replica"];
        string key;
        if (isReplica){
            key = "R:"+nodeItem["key"].toString();
        }else {
            key = "O:"+nodeItem["key"].toString();
        }

        //nodeItem.remove(key);
        cacheEntries[key] = check <CacheEntry>nodeItem;
    }
    isRelocationOrEvictionRunning = false;
}

//TODO FIX according to new cache entry ids.
function evictEntries() {
    int keyCountToEvict = <int>(cacheCapacity * cacheEvictionFactor);
    // Create new arrays to hold keys to be removed and hold the corresponding timestamps.
    string[] cacheKeysToBeRemoved = [];
    int[] timestamps = [];
    //string[] keys = cacheEntries.keys();
    // Iterate through the keys.
    isRelocationOrEvictionRunning = true;
    //foreach key in keys {
    //    CacheEntry? cacheEntry = cacheEntries[key];
    //    match cacheEntry {
    //        CacheEntry entry => {
    //            if (entry.replica){
    //                continue;
    //            }
    //            // Check and add the key to the cacheKeysToBeRemoved if it matches the conditions.
    //            checkAndAddEntries(keyCountToEvict, cacheKeysToBeRemoved, timestamps, key, entry.lastAccessedTime);
    //        }
    //        () => {
    //            // If the key is not found in the map, that means that the corresponding cache is already removed
    //            // (possibly by a another worker).
    //        }
    //    }
    //}
    foreach key,value in cacheEntries {
                    if (value.replica){
                        continue;
                    }
                    // Check and add the key to the cacheKeysToBeRemoved if it matches the conditions.
                    checkAndAddEntries(keyCountToEvict, cacheKeysToBeRemoved, timestamps, key, value.lastAccessedTime);
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
                entries[node][lengthof entries[node]] = "R:"+c;
            }
        }
        _ = cacheEntries.remove("O:"+c);
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


function checkAndAddEntries(int numberOfKeysToEvict, string[] cacheKeys, int[] timestamps, string key, int lastAccessTime) {
    string plainKey = key.substring(2,lengthof key);
    int myLastAccessTime = lastAccessTime;

    // Iterate while we count all values from 0 to numberOfKeysToEvict exclusive of numberOfKeysToEvict since the
    // array size should be numberOfKeysToEvict.
    foreach index in 0..<numberOfKeysToEvict {
        // If we have encountered the end of the array, that means we can add the new values to the end of the
        // array since we haven’t reached the numberOfKeysToEvict limit.
        if (lengthof cacheKeys == index) {
            cacheKeys[index] = plainKey;
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
                cacheKeys[index] = plainKey;
                timestamps[index] = myLastAccessTime;
                plainKey = tempKey;
                myLastAccessTime = tempTimeStamp;
            }
        }
    }
}
