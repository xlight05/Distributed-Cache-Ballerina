import ballerina/io;
import ballerina/http;
import ballerina/log;
import nodes; //invoking services. need a better way
import cache; //invoking services. need a better way
import ballerina/runtime;

endpoint http:Client clientEP {
    url: "http://wwww.google.lk"
};


service<http:Service> init bind { port: 9090 } {
    @http:ResourceConfig {
        methods: ["PUT"],
        path: "/join"
    }
    sayHello(endpoint caller, http:Request req) {
        http:Response res = new;

        res.setPayload("Hello, World!");

        caller->respond(res) but {
            error e => log:printError(
                           "Error sending response", err = e)
        };


        http:ClientEndpointConfig config = { url: "https://postman-echo.com" };
        clientEP.init(config);

        var response = clientEP->get("/get?test=123");

        match response {
            http:Response resp => {
                io:println("GET request:");
                var msg = resp.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        io:println(jsonPayload);
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
