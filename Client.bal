import ballerina/io;
import cache;
import ballerina/runtime;
import ballerina/log;
import ballerina/http;
import ballerina/config;

// function main(string... args) {

//     //cache:createCluster();
//     _ =cache:initNodeConfig();
//     cache:Cache oauthCache = new("oauthCache");

//     oauthCache.put("Name", "Ballerina");

//     string x = <string>oauthCache.get("Name");
//     io:println(x);
//     runtime:sleep(100000000);
// }


endpoint http:Listener listen {
    port: config:getAsInt("api", default = 8080)
};
//cache:Cache? activeCache= (); //can't declare without initing ?
cache:Cache? activeCache;

@http:ServiceConfig { basePath: "/api" }
service<http:Service> api bind listen {

boolean init = cache:initNodeConfig();
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/cache"
    }
    setCache(endpoint caller, http:Request req) {
        json|error obj = req.getJsonPayload();
        match obj {
            json jsonObj => {
                string cache = untaint check <string> jsonObj["cache"];
                 match activeCache {
                     cache:Cache c=> {
                        activeCache = new cache:Cache(cache);
                     }
                     () => {
                        activeCache = new cache:Cache(cache);
                     }
                 }
                //activeCache = activeCache but {() => new cache:Cache(cache)};
                //activeCache = new cache:Cache(cache);
                http:Response res = new;
                json testJson = { "message": "Cache Active", "status": 200 };
                res.setJsonPayload(untaint testJson);

        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
            }
            error err => {
                log:printError("Error recieving response", err = err);
            }
        }
    }

    @http:ResourceConfig {
        methods: ["PUT"],
        path: "/put"
    }
    put(endpoint caller, http:Request req) {
        json|error obj = req.getJsonPayload();
        match obj {
            json jsonObj => {
                string key = untaint check <string> jsonObj["key"];
                string value = untaint check <string> jsonObj["value"];
                activeCache.put(key, value);
                http:Response res = new;
                json testJson = { "message": "Nodes Added", "status": 200 };
                res.setJsonPayload(untaint testJson);

        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
            }
            error err => {
                log:printError("Error recieving response", err = err);
            }
        }
    }


    @http:ResourceConfig {
        methods: ["GET"],
        path: "/get/{key}"
    }
    get(endpoint caller, http:Request req, string key) {
        http:Response res = new;
        any? x =untaint activeCache.get(key);
        //any x =  "";
        //json y = {key:x};
        string y = <string> x;
        json resp;
        // try{
         resp  =  y;
        // }catch( error err){
        //     resp="null";
        // }
        res.setJsonPayload(untaint resp);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }


    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/remove"
    }
    remove(endpoint caller, http:Request req) {
        json|error obj = req.getJsonPayload();
        match obj {
            json jsonObj => {
                string key = untaint check <string> jsonObj["key"];
                string value = untaint check <string> jsonObj["value"];
                activeCache.put(key, value);
                http:Response res = new;
                json testJson = { "message": "Nodes Added", "status": 200 };
                res.setJsonPayload(untaint testJson);

        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
            }
            error err => {
                log:printError("Error recieving response", err = err);
            }
        }
    }

    //     @http:ResourceConfig {
    //     methods: ["POST"],
    //     path: "/put"
    // }
    // put(endpoint caller, http:Request req) {
    //     json|error obj = req.getJsonPayload();
    //     match obj {
    //         json jsonObj => {

    //         }
    //         error err => {
    //             log:printError("Error recieving response", err = err);
    //         }
    //     }
    // }
    
 
}