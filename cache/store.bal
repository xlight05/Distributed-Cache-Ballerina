import ballerina/http;
import ballerina/log;
import ballerina/io;


map<CacheEntry> cacheEntries;

//This service is used to store data in nodes. Each service in the node acts as a local in memory store
function getCacheEntry(string key) returns json {
    CacheEntry default;
    CacheEntry obj = cacheEntries[key] ?: default;
    json payload = check <json>obj;
    return payload;
}

function setCacheEntry(json jsObj) returns json {
    string key = jsObj["key"].toString();
    jsObj.remove(key);
    cacheEntries[key] = check <CacheEntry>jsObj;
    return jsObj;
}

function getAllEntries() returns json {
    json payload = check <json>cacheEntries;
    return payload;
}