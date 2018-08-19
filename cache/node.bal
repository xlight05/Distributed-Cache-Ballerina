import ballerina/io;
import ballerina/http;
import ballerina/log;
//not used but might be useful later
type Node record {
    string ip;

};

//Load Balancer endpoint that uses round robin data distribution
endpoint http:LoadBalanceClient lbBackendEP {
    targets: [
        { url: "http://localhost:6969/data/store" }
    ],
    algorithm: http:ROUND_ROBIN,
    timeoutMillis: 5000
};
//Node endpoint
endpoint http:Client nodeEP {
    url: "http://localhost:7000"
};

public string[] nodeList;

function getNodeList() returns string[] {
    return nodeList;
}

function addServer(string ip) {
    nodeList[lengthof nodeList] = ip;
    io:println(string `{{ip}} Added`);
}

function removeServer(string ip) returns boolean {
    boolean found = false;
    foreach k, v in nodeList{
        if (v == ip){
            v = "";
            found = true;
        }
    }
    return found;
}
service<http:Service> node bind { port: 7000 } {

    // Allows you to add a node to the cluster
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/add"
    }
    add(endpoint caller, http:Request req) {
        json|error obj = req.getJsonPayload();
        match obj {
            json jsonObj => {
                addServer(jsonObj["ip"].toString());
                http:Response res = new;
                json jsonNodeList;
                foreach k, v in nodeList {
                    jsonNodeList[k] = v;
                }
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
        path: "/remove"
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
        path: "/list"
    }
    list(endpoint caller, http:Request req) {
        json jsonObj;
        foreach k, v in nodeList {
            jsonObj[k] = v;
        }
        http:Response res = new;
        res.setJsonPayload(untaint jsonObj);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    //Allows you to set multiple nodes for the cluster
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/set"
    }
    setNodes(endpoint caller, http:Request req) {
        json|error reqJson = req.getJsonPayload();
        match reqJson {
            json reqPayload => {
                foreach item in reqPayload {
                    nodeList[lengthof nodeList] = item.toString();
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
}

//Load Balancer is used to distribute data across the nodes in the cluster
@http:ServiceConfig {
    basePath: "/lb"
}
service<http:Service> loadBalancerDemoService bind { port: 9998 } {

    @http:ResourceConfig {
        path: "/"
    }
    invokeEndpoint(endpoint caller, http:Request req) {
        //Takes node list in case a node is added recently
        var ress = nodeEP->get("/node/list");
        json serverListJSON;
        match ress {
            http:Response resps => {
                var msg = resps.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        serverListJSON = jsonPayload;
                    }
                    error err => {
                        log:printError(err.message, err = err);
                    }
                }
            }
            error err => {
                log:printError(err.message, err = err);
            }
        }
        //Load balancer targets should be updated in runtime. 
        //Constructing target service with node list.
        http:TargetService[] tar;
        int counter = 0;
        foreach item in serverListJSON {
            http:TargetService serv = { url: item.toString() + ":6969" };
            tar[counter] = serv;
            counter++;
        }
        //Updating LoadBalacnerClientEndpointConfig
        http:LoadBalanceClientEndpointConfiguration cfg = {
            targets: tar,
            algorithm: http:ROUND_ROBIN,
            timeoutMillis: 5000
        };
        json|error obj = req.getJsonPayload();
        json requestPayload;
        match obj {
            json jsonObj => {
                requestPayload = jsonObj;
            }
            error err => {
                io:println(err);
            }
        }
        http:Request outRequest = new;
        outRequest.setPayload(untaint requestPayload);
        var response = lbBackendEP->post("/", outRequest);
        match response {
            http:Response resp => {
                caller->respond(resp) but {
                    error e => log:printError("Error sending response", err = e)
                };
            }
            error responseError => {
                http:Response outResponse = new;
                outResponse.statusCode = 500;
                outResponse.setPayload(responseError.message);
                caller->respond(outResponse) but {
                    error e => log:printError("Error sending response", err = e)
                };
            }
        }
    }
}



