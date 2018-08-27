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

function getChangedEntries () returns json{
    json entries;
    foreach node in nodeList {
        if(node.ip!=currentNode.ip){
        entries[node.ip]=[];
        }
    }
    foreach key,value in cacheEntries {
        string correctNodeIP = c.get(key);
        string currentNodeIP = currentNode.ip;
        if(correctNodeIP!=currentNodeIP){
            value["key"]=key;
            entries[correctNodeIP][lengthof entries[correctNodeIP]] = check <json>value;  
            boolean x =cacheEntries.remove(key);
        }

    }
    return entries;
}

function storeMultipleEntries (json jsonObj) {
    foreach item in jsonObj {
        json nodeItem = item;
        string key = nodeItem["key"].toString();
        nodeItem.remove(key);
        cacheEntries[key]= check <CacheEntry> nodeItem;
    }
}