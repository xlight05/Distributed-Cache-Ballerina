import ballerina/http;
import ballerina/log;
import ballerina/io;

//This service is used to store data in nodes. Each service in the node acts as a local in memory store
map<CacheEntry> cacheEntries;

//Returns single cache entry according to given key
function getCacheEntry(string key) returns json {
    CacheEntry default;
    CacheEntry obj = cacheEntries[key] ?: default;
    json payload = check <json>obj;
    return payload;
}

//Adds a single cache entry to the store
function setCacheEntry(json jsObj) returns json {
    string key = jsObj["key"].toString();
    jsObj.remove(key);
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
    //Node catagorize
    foreach node in nodeList {
        if (node.ip != currentNode.ip){
            entries[node.ip] = [];
        }
    }
    foreach key, value in cacheEntries {
        string correctNodeIP = hashRing.get(key);
        string currentNodeIP = currentNode.ip;
        //Checks if the node is changed
        if (correctNodeIP != currentNodeIP){
            value["key"] = key;
            entries[correctNodeIP][lengthof entries[correctNodeIP]] = check <json>value;
            boolean x = cacheEntries.remove(key); //Assuming the response was recieved :/
        }
    }
    return entries;
}
//Adds multiple entries to the cache.
function storeMultipleEntries(json jsonObj) {
    foreach item in jsonObj {
        json nodeItem = item;
        string key = nodeItem["key"].toString();
        nodeItem.remove(key);
        cacheEntries[key] = check <CacheEntry>nodeItem;
    }
}