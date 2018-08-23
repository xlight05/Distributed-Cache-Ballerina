import ballerina/time;
import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;


@http:ServiceConfig { basePath: "/" }
service<http:Service> api bind listner {

    // Allows you to add a node to the cluster
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/node/add"
    }
    add(endpoint caller, http:Request req) {
        json|error obj = req.getJsonPayload();
        match obj {
            json jsonObj => {
                Node node = check <Node> jsonObj;
                json jsonNodeList = addServer(node);
                http:Response res = new;
                res.setJsonPayload(untaint jsonNodeList);
                caller->respond(res) but { error e => log:printError(
                                                          "Error sending response", err = e) };
            }
            error err => {
                io:println(err);
            }
        }
    }

    //Allows you to remove a node from the cluster
    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/node/remove"
    }
    remove(endpoint caller, http:Request req) {

        http:Response res = new;
        boolean isRemoved = removeServer("test");
        if (isRemoved){
            json testJson = { "message": "Node Removed", "status": 200 };

            res.setJsonPayload(testJson);
            caller->respond(res) but { error e => log:printError(
                                                      "Error sending response", err = e) };
        }
        else {
            json testJson = { "message": "Node not found", "status": 500 };

            res.setJsonPayload(testJson);
            caller->respond(res) but { error e => log:printError(
                                                      "Error sending response", err = e) };
        }
    }

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

    //Allows you to set multiple nodes for the cluster
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/node/set"
    }
    setNodes(endpoint caller, http:Request req) {
        json|error reqJson = req.getJsonPayload();
        match reqJson {
            json reqPayload => {
                foreach item in reqPayload {
                    nodeList[lengthof nodeList] = check <Node> item;
                    updateLoadBalancerConfig();
                }
            }
            error err => {
                io:println(err);
            }
        }
        json testJson = { "message": "Nodes Added", "status": 200 };
        http:Response res = new;
        res.setJsonPayload(untaint testJson);
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
                io:println(err);
            }
        }
        res.setJsonPayload(untaint jsObj);
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
                cacheMap = check <map<Cache>> untaint jsonObj;
            }
            error err => {
                io:println(err);
            }
        }
        res.setJsonPayload(untaint testJson);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    @http:ResourceConfig {
        methods: ["GET"],
        path: "/cache/get/{key}"
    }
    cacheGet(endpoint caller, http:Request req,string key) {
        http:Response res = new;
        json resp;
        if (cacheMap.hasKey(key)){
            resp = check <json>cacheMap[key];
        }
        else {
            resp = {"status":"Not found"};
        }

        res.setJsonPayload(untaint resp);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }
}


