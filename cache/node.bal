import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;

//Load Balancer endpoint that uses round robin data distribution
endpoint http:LoadBalanceClient lbBackendEP {
    targets: [
        { url: "http://localhost:"+config:getAsString("port", default = "7000") }
    ],
    algorithm: http:ROUND_ROBIN,
    timeoutMillis: 5000
};

public string[] nodeList;

function getNodeList() returns json {
    json jsonObj;
    foreach k, v in nodeList {
        jsonObj[k] = v;
    }
    return jsonObj;
}

function addServer(string ip) returns json{
    nodeList[lengthof nodeList] = ip;
    updateLoadBalancerConfig();
    json jsonNodeList;
    foreach k, v in nodeList {
        jsonNodeList[k] = v;
    }
    io:println(string `{{ip}} Added`);
    return jsonNodeList;
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

function updateLoadBalancerConfig() {
    //Populating Target Service
    http:TargetService[] tar;
    foreach k, v in nodeList {
        http:TargetService serv = { url: v};
        tar[k] = serv;
    }
    io:println(tar);
    //Updating LoadBalacnerClientEndpointConfig
    http:LoadBalanceClientEndpointConfiguration cfg = {
        targets: tar,
        algorithm: http:ROUND_ROBIN,
        timeoutMillis: 5000
    };
    lbBackendEP.init(cfg);
}


//Load Balancer is used to distribute data across the nodes in the cluster
@http:ServiceConfig {
    basePath: "/lb"
}
service<http:Service> loadBalancerDemoService bind listner {

    @http:ResourceConfig {
        path: "/"
    }
    invokeEndpoint(endpoint caller, http:Request req) {
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
        var response = lbBackendEP->post("/data/store", outRequest);
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



