import ballerina/io;

type Node record {
   string ip;

};

map <string> nodeList;

function getNodeList() returns map<string> {
    return nodeList;
}

function addrServer(string id,string ip){
    nodeList["id"] = "ip";
    io:println(string `{{ip}} Added`);
}

function removeServer(string id,string ip) {
    _ = nodeList.remove("id");
    io:println(string `{{ip}} Removed`);
}

service<http:Service> node bind { port: 7000 } {

    @http:ResourceConfig {
        methods: ["POST"],
        path: "/add"
    }
    add(endpoint caller, http:Request req) {

        http:Response res = new;

        res.setPayload("Hello, Node Added");

        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/remove"
    }
    remove(endpoint caller, http:Request req) {

        http:Response res = new;

        res.setPayload("Hello, Node Removed");

        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }
}


