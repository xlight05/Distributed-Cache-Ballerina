import ballerina/http;
import ballerina/log;



service<http:Service> data bind { port: 6969 } {

    @http:ResourceConfig {
        methods: ["GET"],
        path: "/recieve"
    }
    recieve(endpoint caller, http:Request req) {

        http:Response res = new;

        res.setPayload("Hello, World! GET Data");

        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }

    @http:ResourceConfig {
        methods: ["POST"],
        path: "/send"
    }

    send(endpoint caller, http:Request req) {

        http:Response res = new;

        res.setPayload("Hello, World! SEND Data");

        caller->respond(res) but { error e => log:printError(
                                                  "Error sending response", err = e) };
    }
}