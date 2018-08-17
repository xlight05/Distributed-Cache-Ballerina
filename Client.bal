import ballerina/io;
import cache;
import ballerina/system;
import ballerina/runtime;


function main(string... args) {
    cache:Cache cache = new("http://localhost");

    cache.put("Name", "Ballerina");

     string x = <string>cache.get("Name");
     io:println(x);

    runtime:sleep(10000);
}
