import ballerina/time;
import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;

@http:ServiceConfig { basePath: "/cache" }
service<http:Service> cacheService bind listener {
    //Allows users to get cache objects created by other nodes.
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/{key}"
    }
    cacheGet(endpoint caller, http:Request req, string key) {
        http:Response res = new;
        json resp;
        if (cacheMap.hasKey(key)) {
            resp = check <json>cacheMap[key];
        }
        else {
            res.statusCode = 204;
            resp = { "message": "Cache not found" };
        }
        res.setJsonPayload(untaint resp, contentType = "application/json");
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //List all entries in the node
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/entries"
    }
    storelist(endpoint caller, http:Request req) {
        http:Response res = new;
        json payload = getAllEntries();
        res.setJsonPayload(untaint payload, contentType = "application/json");
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //Allows users to retrive data from a given key
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/entries/{key}"
    }
    get(endpoint caller, http:Request req, string key) {
        json? entryRes = getCacheEntry(key);
        http:Response res = new;
        json resp;
        match entryRes { //TODO recheck
            () => {
                res.statusCode = 204;
                resp = { "message": "Entry not found"};
            }
            json entry => {
                resp = entry;
            }
        }
        res.setJsonPayload(untaint resp, contentType = "application/json");
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //Allows users to store data in the node.
    @http:ResourceConfig {//TODO recheck
        methods: ["POST"],
        path: "/entries/{key}"
    }
    store(endpoint caller, http:Request req) {
        http:Response res = new;
        json|error obj = req.getJsonPayload();
        json resp;
        match obj {
            json jsonObj => {
                string key = setCacheEntry(jsonObj);
                resp = { "message": "Entry Added","key":key};
            }
            error err => {
                res.statusCode = 400;
                resp = { "message": "Invalid JSON"};
                log:printError("Error recieving response", err = err);
            }
        }
        res.setJsonPayload(untaint resp);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //Allows users to store data in the node.
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/entries"
    }
    multipleStore(endpoint caller, http:Request req) {
        http:Response res = new;
        json|error obj = req.getJsonPayload();
        json resp ;
        match obj {
            json jsonObj => {
                storeMultipleEntries(jsonObj);
                resp = { "message": "Entries Added"};
            }
            error err => {
                res.statusCode = 400;
                resp = { "message": "Invalid JSON"};
                log:printError("Error recieving response", err = err);
            }
        }
        res.setJsonPayload(untaint resp,contentType = "application/json");
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //Allows users to store data in the node.
    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/entries/{key}"
    }
    dataRemove(endpoint caller, http:Request req) {
        http:Response res = new;
        json|error obj = req.getJsonPayload();
        json resp;
        match obj {
            json entryJSON => {
                boolean status = cacheEntries.remove(entryJSON["key"].toString());
                if (!status){
                    res.statusCode = 204;
                }
                resp = { "message": "Entry executed "+status};
            }
            error err => {
                res.statusCode = 400;
                resp = { "message": "Invalid JSON"};
            }
        }
        res.setJsonPayload(untaint resp,contentType = "application/json");
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/entries"
    }
    evictData(endpoint caller, http:Request req) { //replicas only !!
        http:Response res = new;
        json|error jsonData = req.getJsonPayload();
        string[] entryKeyArr;
        json resp;
        match jsonData {
            json entryKeyListJSON => {
                entryKeyArr = check <string[]>entryKeyListJSON;
                foreach i in entryKeyArr {
                    _ = cacheEntries.remove(i);
                }
                resp = { "message": "Entries evicted"};
            }
            error err => {
                res.statusCode = 400;
                resp = { "message": "Invalid JSON"};
            }
        }
        res.setJsonPayload(untaint resp, contentType = "application/json");
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }
}
