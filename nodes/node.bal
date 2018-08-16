import ballerina/io;
import ballerina/http;
import ballerina/log;

type Node record {
   string ip;

};

string [] nodeList;

function getNodeList() returns string[] {
    return nodeList;
}

function addServer(string ip){
    nodeList[lengthof nodeList] = ip;
    io:println(string `{{ip}} Added`);

}

function removeServer(string ip) returns boolean{
    boolean found = false;
    foreach k,v in nodeList{
        if (v==ip){
            v="";
            found=true;
        }
    }
    return found;
}

service<http:Service> node bind { port: 7000 } {

    @http:ResourceConfig {
        methods: ["POST"],
        path: "/add"
    }
    add(endpoint caller, http:Request req) {
        json|error obj = req.getJsonPayload();
        match obj{
            json  jsonObj=> {
                addServer(jsonObj["ip"].toString());
                http:Response res = new;

                json testJson = {"message":"Node Added","status":200};

                res.setJsonPayload(testJson);

                caller->respond(res) but { error e => log:printError(
                                                          "Error sending response", err = e) };
            }
            error err => {io:println(err);}
        }
    }

    @http:ResourceConfig {
        methods: ["DELETE"],
        path: "/remove"
    }
    remove(endpoint caller, http:Request req) {

        http:Response res = new;
        boolean isRemoved = removeServer("test");
        if (isRemoved){
            json testJson = {"message":"Node Removed","status":200};

            res.setJsonPayload(testJson);
            caller->respond(res) but { error e => log:printError(
                                                      "Error sending response", err = e) };
        }
        else {
            json testJson = {"message":"Node not found","status":500};

            res.setJsonPayload(testJson);
            caller->respond(res) but { error e => log:printError(
                                                      "Error sending response", err = e) };
        }



    }
    @http:ResourceConfig {
        methods: ["GET"],
        path: "/list"
    }
    list(endpoint caller, http:Request req) {
        json jsonObj;
        foreach k,v in nodeList {
            jsonObj[k]=v;
        }
        http:Response res = new;
        res.setJsonPayload(untaint jsonObj);
        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }
}


