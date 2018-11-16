import ballerina/time;
import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;


@http:ServiceConfig { basePath: "/" }
service<http:Service> api bind listener {

    //Allows you to list the nodes from the cluster
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/node/list"
    }
    nodeList(endpoint caller, http:Request req) {
        json jsonObj = getNodeList();
        http:Response res = new;
        res.setJsonPayload(untaint jsonObj);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }


    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

    //Allows users to retrive data from a given key
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/data/get/{key}"
    }
    get(endpoint caller, http:Request req, string key) {
        json payload = getCacheEntry(key);
        http:Response res = new;
        res.setJsonPayload(untaint payload, contentType = "application/json");
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }


    //Allows users to store data in the node.
    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/data/remove"
    }
    dataRemove(endpoint caller, http:Request req) {
        http:Response res = new;
        json|error obj = req.getJsonPayload();
        json jsObj;
        match obj {
            json jsonObj => {
                boolean status = cacheEntries.remove(jsonObj["key"].toString());
                jsObj = { "status": status };
            }
            error err => {
                log:printError("Error recieving response", err = err);
            }
        }
        res.setJsonPayload(untaint jsObj);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //Allows users to store data in the node.
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/data/store"
    }
    store(endpoint caller, http:Request req) {
        http:Response res = new;
        json|error obj = req.getJsonPayload();
        json jsObj;
        match obj {
            json jsonObj => {
                jsObj = setCacheEntry(jsonObj);
            }
            error err => {
                log:printError("Error recieving response", err = err);
            }
        }
        res.setJsonPayload(untaint jsObj);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //Allows users to store data in the node.
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/data/multiple/store"
    }
    multipleStore(endpoint caller, http:Request req) {
        http:Response res = new;
        json|error obj = req.getJsonPayload();
        json testJson = { "message": "Entries Added", "status": 200 };
        match obj {
            json jsonObj => {
                storeMultipleEntries(jsonObj);
            }
            error err => {
                log:printError("Error recieving response", err = err);
            }
        }
        res.setJsonPayload(untaint testJson);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //List all entries in the node
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/data/list"
    }
    storelist(endpoint caller, http:Request req) {
        http:Response res = new;
        json payload = getAllEntries();
        res.setJsonPayload(untaint payload, contentType = "application/json");
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //Allows users to store data in the node.
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/cache/add"
    }
    cacheAdd(endpoint caller, http:Request req) {
        http:Response res = new;
        json|error obj = req.getJsonPayload();
        json testJson = { "message": "Nodes Added", "status": 200 };
        match obj {
            json jsonObj => {
                cacheMap = check <map<Cache>>untaint jsonObj;
            }
            error err => {
                log:printError("Error recieving response", err = err);
            }
        }
        res.setJsonPayload(untaint testJson);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //Allows users to get cache objects created by other nodes.
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/cache/get/{key}"
    }
    cacheGet(endpoint caller, http:Request req, string key) {
        http:Response res = new;
        json resp;
        if (cacheMap.hasKey(key)) {
            resp = check <json>cacheMap[key];
        }
        else {
            resp = { "status": "Not found" };
        }

        res.setJsonPayload(untaint resp);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/data/clear"
    }
    clear(endpoint caller, http:Request req) {

        http:Response res = new;
        cacheEntries.clear();
        json testJson = { "message": "Node entries Removed", "status": 200 };

        res.setJsonPayload(testJson);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };

    }

    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/data/evict"
    }
    evictData(endpoint caller, http:Request req) { //replicas only !!
        json|error jsonData = req.getJsonPayload();
        string[] strArr;
        match jsonData {
            json js => {
                strArr = check <string[]>js;
            }
            error err => {
                log:printWarn("Error recieving json");
            }
        }
        foreach i in strArr {
            _ = cacheEntries.remove(i);
            log:printInfo(i + " Replica Evicted");
        }
        http:Response res = new;
        json testJson = { "message": "Node entries evicted", "status": 200 };
        res.setJsonPayload(untaint testJson, contentType = "application/json");
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }
}

//
//function initCache() returns boolean {
//    io:println("In init cache");
//    true -> cacheReady;
//    return true;
//}