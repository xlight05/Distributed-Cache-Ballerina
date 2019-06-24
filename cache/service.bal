import ballerina/time;
import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;

//listener http:Listener raftListner = new(config:getAsInt("raft.port", default = 7000));

@http:ServiceConfig { basePath: "/cache" }
service cacheService on cacheListner {
    //Allows users to get cache objects created by other nodes.
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/{key}"
    }
    resource function cacheGet(http:Caller caller, http:Request req, string key) {
        http:Response response = new;
        json resp={};
        Cache? entry = cacheMap[key];
        if (entry is Cache){
            resp = {name:entry.name,expiryTimeMillis:entry.expiryTimeMillis,LocalCacheConfig:entry.nearCache.getLocalCacheConfigAsJSON ()};
        }else {
            response.statusCode = 204;
            resp = { "message": "Cache not found" };
        }
        response.setJsonPayload(untaint resp, contentType = "application/json");
        var result =caller->respond(response);
        if (result is error){
            log:printError("Error sending response", err = result);
        }
    }

    //List all entries in the node
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/entries"
    }
    resource function storelist(http:Caller caller, http:Request req) {
        http:Response res = new;
        json payload = getAllEntries();
        res.setJsonPayload(untaint payload, contentType = "application/json");
        var result =caller->respond(res);
        if (result is error){
            log:printError("Error sending response", err = result);
        }
    }

    //Allows users to retrive data from a given key
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/entries/{key}"
    }
    resource function get(http:Caller caller, http:Request req, string key) {
        CacheEntry? entry = getCacheEntry(untaint key);
        http:Response res = new;
        json resp={};
        if (entry is CacheEntry){
            json|error responseJSON = json.convert(entry);
            if (responseJSON is json){
                resp = responseJSON;
            }
        }else {//TODO Revisit
            res.statusCode = 204;
            resp = { "message": "Entry not found"};
        }
        res.setJsonPayload(untaint resp, contentType = "application/json");
        var result =caller->respond(res);
        if (result is error){
            log:printError("Error sending response", err = result);
        }
    }

    //Allows users to store data in the node.
    @http:ResourceConfig {//TODO recheck
        methods: ["POST"],
        path: "/entries/{key}"
    }
    resource function store(http:Caller caller, http:Request req) {
        http:Response res = new;
        CacheEntry|error obj = CacheEntry.convert(req.getJsonPayload());
        json resp;
        if (obj is CacheEntry){
            string key = setCacheEntry(obj);
            resp = { "message": "Entry Added","key":key};
        }else {
            res.statusCode = 400;
            resp = { "message": "Invalid JSON"};
            log:printError("Error recieving response", err = obj);
        }
        res.setJsonPayload(untaint resp);
        var result =caller->respond(res);
        if (result is error){
            log:printError("Error sending response", err = result);
        }
    }

    //Allows users to store data in the node.
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/entries"
    }
    resource function multipleStore(http:Caller caller, http:Request req) {
        http:Response res = new;
        CacheEntry[]|error obj = CacheEntry[].stamp (req.getJsonPayload());
        json resp ;
        if (obj is CacheEntry[]){
            storeMultipleEntries(obj);
            resp = { "message": "Entries Added"};
        }else {
            res.statusCode = 400;
            resp = { "message": "Invalid JSON"};
            log:printError("Error recieving response", err = obj);
        }
        res.setJsonPayload(untaint resp,contentType = "application/json");
        var result =caller->respond(res);
        if (result is error){
            log:printError("Error sending response", err = result);
        }
    }

    //Allows users to store data in the node.
    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/entries/{key}"
    }
    resource function dataRemove(http:Caller caller, http:Request req) {
        http:Response res = new;
        json|error obj = req.getJsonPayload();
        json resp;
        if (obj is json){
            boolean status = cacheEntries.remove(obj["key"].toString());
            if (!status){
                res.statusCode = 204;
            }
            resp = { "message": "Entry executed "+status};
        }else {
            res.statusCode = 400;
            resp = { "message": "Invalid JSON"};
        }
        res.setJsonPayload(untaint resp,contentType = "application/json");
        var result =caller->respond(res);
        if (result is error){
            log:printError("Error sending response", err = result);
        }
    }

    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/entries"
    }
    resource function evictData(http:Caller caller, http:Request req) { //replicas only !!
        http:Response res = new;
        json|error jsonData = req.getJsonPayload();

        json resp;
        if (jsonData is json){
                string[]|error entryKeyArr = string[].stamp (jsonData);
                if (entryKeyArr is string[]){
                    foreach var i in entryKeyArr {
                        _ = cacheEntries.remove(i);
                    }
                    resp = { "message": "Entries evicted"};
                }else {
                    res.statusCode = 400;
                    resp = { "message": "Invalid JSON"};
                }
        }else {
                res.statusCode = 400;
                resp = { "message": "Invalid JSON"};
        }
        res.setJsonPayload(untaint resp, contentType = "application/json");
        var result =caller->respond(res);
        if (result is error){
            log:printError("Error sending response", err = result);
        }
    }
}
