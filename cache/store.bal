import ballerina/http;
import ballerina/log;


//map<CacheEntry> entries;


service<http:Service> data bind { port:  6969 } {

    @http:ResourceConfig {
        methods: ["GET"],
        path: "/get"
    }
    get(endpoint caller, http:Request req) {


        http:Response res = new;

        res.setPayload("Hello, World! GET Data");

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
        match obj{
            json jsonObj=> {jsObj=jsonObj;}
            error err => {io:println(err);}
        }

        res.setJsonPayload(jsObj);

        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }
}