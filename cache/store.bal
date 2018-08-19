import ballerina/http;
import ballerina/log;


map<CacheEntry> cacheEntries;

//This service is used to store data in nodes. Each service in the node acts as a local in memory store
service<http:Service> data bind { port: 6969 } {

    //Allows users to retrive data from a given key
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/get/{key}"
    }
    get(endpoint caller, http:Request req, string key) {
        CacheEntry default;
        CacheEntry obj = cacheEntries[key] ?: default;
        http:Response res = new;
        json payload = check <json>obj;
        res.setJsonPayload(payload, contentType = "application/json");
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //Allows users to store data in the node.
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/store"
    }
    store(endpoint caller, http:Request req) {
        http:Response res = new;
        json|error obj = req.getJsonPayload();
        json jsObj;
        match obj {
            json jsonObj => {
                jsObj = jsonObj;
            }
            error err => {
                io:println(err);
            }
        }
        string key = jsObj["key"].toString();
        jsObj.remove(key);
        cacheEntries[key] = check <CacheEntry>jsObj;
        res.setJsonPayload(untaint jsObj);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //List all entries in the node
    @http:ResourceConfig {
    methods: ["GET"],
    path: "/list"
    }
    list(endpoint caller, http:Request req) {
        http:Response res = new;
        json payload = check <json>cacheEntries;
        res.setJsonPayload(untaint payload, contentType = "application/json");
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

}