import ballerina/io;
import cache;
import ballerina/runtime;
import ballerina/log;


function main(string... args) {

    cache:createCluster();
    cache:createCache("oauthCache");
    var cacheVar = cache:getCache("oauthCache");
    cache:Cache oauthCache = new ("local");

    match cacheVar {
        cache:Cache cache=> {
            oauthCache = cache;
        }
        () err=> {
             log:printError("Error sending response", err = err);
        }
    }


    oauthCache.put("Name", "Ballerina");

    string x = <string>oauthCache.get("Name");
    io:println(x);

    runtime:sleep(100000000);
}
