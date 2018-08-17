import ballerina/http;
import ballerina/log;


map<CacheEntry> cacheEntries;


service<http:Service> data bind { port: 6969 } {

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

}