import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/runtime;
import ballerina/math;

endpoint http:Client clientEP {
    url: "http://wwww.google.lk"
};


service<http:Service> init bind { port: 9000 } {
    @http:ResourceConfig {
        methods: ["PUT"],
        path: "/join"
    }
    connect(endpoint caller, http:Request req) {
        json|error obj = req.getJsonPayload();
        //
        string currentIP;
        string nodePort = "7000";
        json nodeArr;
        match obj {
            json jsonObj => {
                nodeArr = jsonObj.nodeArr;
                currentIP = jsonObj["currentIP"].toString();
            }
            error err => {
                io:println(err);
            }
        }
        string currentIpWithPort = currentIP + ":" + nodePort;

        int i = 0;
        int nodeLength = lengthof nodeArr;
        io:println(nodeLength);
        json serverList={"0":currentIP};
        while (i < nodeLength) {
            http:ClientEndpointConfig config = { url: nodeArr[i].toString() + ":" + nodePort };
            clientEP.init(config);

            json serverDetailsJSON = { "ip": currentIP };
            var response = clientEP->post("/node/add", untaint serverDetailsJSON);

            match response {
                http:Response resp => {
                    var msg = resp.getJsonPayload();
                    match msg {
                        json jsonPayload => {
                            serverList = jsonPayload;
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
            i = i + 1;
        }

        io:println(currentIpWithPort);
        http:ClientEndpointConfig config = { url: currentIpWithPort };
        clientEP.init(config);
        io:println(serverList);
        var response = clientEP->post("/node/set", untaint serverList);

        match response {
            http:Response resp => {
                var msg = resp.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        http:Response res = new;
                        res.setJsonPayload(untaint jsonPayload);
                        caller->respond(res) but { error e => log:printError(
                                                                  "Error sending response", err = e) };
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
    }
}
