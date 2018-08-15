import ballerina/io;
import ballerina/http;
import ballerina/log;
import nodes; //invoking services. need a better way
import cache; //invoking services. need a better way
import ballerina/runtime;

endpoint http:Client clientEP {
    url:"http://wwww.google.lk"
};



function main(string... args) {

    http:ClientEndpointConfig config = {url:"https://postman-echo.com"};
    clientEP.init(config);
    http:Request req = new;

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
        error err => { log:printError(err.message, err = err); }
    }

    runtime:sleep(60000); // temp coz services closes.
}

