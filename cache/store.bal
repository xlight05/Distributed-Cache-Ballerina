import ballerina/http;
import ballerina/log;
import ballerina/io;
import ballerina/math;
import ballerina/time;
import ballerina/task;
import consistent_bound;

# cacheEntries contains all the entries in the current node
map<CacheEntry> cacheEntries={};
consistent_bound:Consistent hashRing = new();
# Cache cleanup task starting delay in ms.
const int CACHE_CLEANUP_START_DELAY = 0;

# Cache cleanup task invoking interval in ms.
const int CACHE_CLEANUP_INTERVAL = 5000;

task:Timer cacheCleanupTimer = createCacheCleanupTask();

//Returns single cache entry according to given key
function getCacheEntry(string key) returns CacheEntry? {
    CacheEntry? cacheEntry = cacheEntries[key];
    if (cacheEntry is CacheEntry){
        int currentSystemTime = time:currentTime().time;
        //checks if entry is expired
        if (currentSystemTime >= cacheEntry.lastAccessedTime + cacheEntry.expiryTimeMillis) {
            // If it is expired, remove the cache and return nil.
            _ = cacheEntries.remove(key);
            _ = start removeReplicas(key, currentNode);
            return ();
        }
        //updates last accessed time
        cacheEntry.lastAccessedTime = time:currentTime().time;
        return cacheEntry;
    }else {
        return ();
    }
}

//Adds a single cache entry to the store
function setCacheEntry(CacheEntry entry) returns string {
    //checks if node size is exceeded
    if (cacheCapacity <= cacheEntries.length()) {
        evictEntries();
    }
    string key = entry.cacheName + ":" + entry.key;
    //checks for replica
    if (entry.replica) {
        key = "R:" + key;
    } else {
        key = "O:" + key;
    }
    cacheEntries[key] = entry;
    return entry.key;
}

//Returns all the cache entries avaialbe in the node
function getAllEntries() returns json {
    json|error payload = json.convert (cacheEntries);
    if (payload is json){
        return payload;
    }else {
        return {};
    }
}

//Returns all the changed entries of the node catagorized according to node IP.
function getChangedEntries() returns json {
    json entries={};
    //Init json according to nodes
    foreach var (index,node) in cacheClientMap {
        entries[node.ip] = [];
    }
    isRelocationOrEvictionRunning = true;
    foreach var (key, value) in cacheEntries {
        if (value.replica) {
            string[] replicaNodes = hashRing.GetClosestN(value.key, replicationFact);
            boolean remove = true;
            foreach var replicaNode in replicaNodes {
                if (replicaNode != currentNode) {
                    json|error valueJSON = json.convert(value);
                    if (valueJSON is json){
                        entries[replicaNode][entries[replicaNode].length()] = valueJSON;
                    }else {
                        //will always convert
                    }
                }
                else {
                    remove = false;
                }
            }
            if (remove) {
                _ = cacheEntries.remove(key);
            }
            string correctNodeIP = hashRing.get(value.key);
            if (correctNodeIP == currentNode){
                string newKey = "O:"+value.cacheName + ":" + value.key;
                CacheEntry newEntry = {cacheName:value.cacheName,value:value.value,key:value.key,lastAccessedTime:value.lastAccessedTime,expiryTimeMillis:value.expiryTimeMillis,replica:false};
                cacheEntries[newKey]=newEntry;
            }

        } else {
            string correctNodeIP = hashRing.get(value.key);
            //Checks if the node is changed
            if (correctNodeIP != currentNode) {
                json|error valueJSON = json.convert(value);
                    if (valueJSON is json){
                        entries[correctNodeIP][entries[correctNodeIP].length()] = valueJSON;
                    }else {
                        //will always convert
                    }
                _ = cacheEntries.remove(key); //Assuming the response was recieved :/
            }
            string[] replicaNodes = hashRing.GetClosestN(value.key, replicationFact);
            boolean found=false;
            foreach var i in replicaNodes {
                if (i == currentNode){
                    found = true;
                }
            }
            if (found) {
                string newKey = "R:" + value.cacheName + ":" + value.key;
                CacheEntry newEntry = { cacheName: value.cacheName, value: value.value, key: value.key, lastAccessedTime
                : value.lastAccessedTime, expiryTimeMillis: value.expiryTimeMillis, replica: true };
                cacheEntries[newKey] = newEntry;
            }
        }
    }
    isRelocationOrEvictionRunning = false;
    return entries;
}
//Adds multiple entries to the cache.
function storeMultipleEntries(CacheEntry[] jsonObj) {
    //log:printInfo("Recieved entries" + jsonObj);
    if (cacheCapacity <= cacheEntries.length() +jsonObj.length()) {
        evictEntries();
    }
    lock {
        isRelocationOrEvictionRunning = true;
        foreach var entry in jsonObj {//TODO revisit
            string key = entry.cacheName + ":" + entry.key;
            if (entry.replica) {
                key = "R:" + key;
            } else {
                key = "O:" + key;
            }
            cacheEntries[key] = entry;
        }
        isRelocationOrEvictionRunning = false;
    }
}

function evictEntries() {
    int keyCountToEvict = <int>(cacheCapacity * cacheEvictionFactor);
    // Create new arrays to hold keys to be removed and hold the corresponding timestamps.
    string[] cacheKeysToBeRemoved = [];
    int[] timestamps = [];
    // Iterate through the keys.
    isRelocationOrEvictionRunning = true;
    foreach var (key, value) in cacheEntries {
        if (value.replica) {
            continue;
        }
        // Check and add the key to the cacheKeysToBeRemoved if it matches the conditions.
        checkAndAddEntries(keyCountToEvict, cacheKeysToBeRemoved, timestamps, key, value.lastAccessedTime);
    }
    // Return the array.
    json entries={};
    //Node catagorize
    foreach var (index,node) in cacheClientMap{
            entries[node.ip] = [];
    }
    foreach var c in cacheKeysToBeRemoved {
        string plainKey = c.split(":")[2];
        // These cache values are ignred. So it is not needed to check the return value for the remove function.
        string[] replicaNodes = hashRing.GetClosestN(plainKey, replicationFact);
        foreach var node in replicaNodes {
                entries[node][entries[node].length()] = "R:" + c.split(":")[1]+":"+c.split(":")[2];
        }
        _ = cacheEntries.remove(c);
        log:printInfo(c + " Entry Evicted");
    }
    io:println(entries);
    evictReplicas(entries);
    isRelocationOrEvictionRunning = false;
}

function evictReplicas(json entries) {
    foreach var (index,nodeItem) in cacheClientMap {
        if (entries[nodeItem.ip].length()== 0){
            //if not replicas evicted no need to send the request
            continue;
        }
        nodeClientEndpoint = nodeItem.nodeEndpoint;
        var res = nodeClientEndpoint->delete("/cache/entries", untaint entries[nodeItem.ip]);
        //sends changed entries to correct node
        if (res is http:Response){
            var msg = res.getJsonPayload();
            if (msg is json){
                log:printInfo("Entries sent to " + nodeItem.ip);
            }else {
                log:printError("Invalid JSON", err = msg);
            }
        }else {
                log:printError("Response not recieved ", err = res);
        }
    }
}

function checkAndAddEntries(int numberOfKeysToEvict, string[] cacheKeys, int[] timestamps, string key, int
    lastAccessTime) {
    //string plainKey = key.substring(2, lengthof key);
    int myLastAccessTime = lastAccessTime;
    string myKey = key;

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

function cacheExpiry()returns error?  {
    json entries={};
    foreach var (index,node) in cacheClientMap{
            entries[node.ip] = [];
    }
    foreach var (key, value) in cacheEntries {
        int currentSystemTime = time:currentTime().time;
        if (currentSystemTime >= value.lastAccessedTime + value.expiryTimeMillis) {
            string[] replicaNodes = hashRing.GetClosestN(value.key, replicationFact);
            foreach var node in replicaNodes {
                    entries[node][entries[node].length()] = "R:" + key.split(":")[1];
            }
            _ = cacheEntries.remove(key);
        }
    }
    evictReplicas(entries);
    return ();
}


function createCacheCleanupTask() returns task:Timer {
    (function () returns error?) onTriggerFunction = cacheExpiry;
    task:Timer cleanerTimer = new(onTriggerFunction, (), CACHE_CLEANUP_INTERVAL, delay = CACHE_CLEANUP_START_DELAY);
    cleanerTimer.start();
    return cleanerTimer;
}

function removeReplicas(string key, string originalNode) {
    string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
    foreach var node in replicaNodes {
        Node? replicaNode = cacheClientMap[node];
        if (replicaNode is Node){
                json entryJSON = { "key": key };
                nodeClientEndpoint =replicaNode.nodeEndpoint;
                var response = nodeClientEndpoint->delete("/cache/entries"+key, entryJSON);
                if (response is http:Response){
                        var msg = response.getJsonPayload();
                        if (msg is json){
                            log:printInfo("Cache entry replica remove " + msg["status"].toString());
                        }else {
                            log:printError("Invalid JSON ", err = msg);
                        }
                }else {
                    log:printError("Response not recieved ", err = response);
                }
        }else {
            log:printError("Client not found");
        }
    }
}

//TODO maintain counter in both sender and reciver to ensure request is recieved. or MB
function relocateData() {
    lock{
        json changedJson = getChangedEntries();
        foreach var (index,nodeItem) in relocationClientMap {
            string nodeIP = nodeItem.ip;
            log:printInfo("Relocating data");
            nodeClientEndpoint = nodeItem.nodeEndpoint;
            var res = nodeClientEndpoint->post("/cache/entries", untaint changedJson[nodeIP]);
            //sends changed entries to correct node
            if (res is http:Response){
                    var msg = res.getJsonPayload();
                    if (msg is json){
                        log:printInfo("Entries sent to " + nodeIP);
                    }else {
                        log:printError("Invalid JSON",err = msg);
                    }
            }else {
                log:printError("Response not recieved");
            }
        }
    }
}