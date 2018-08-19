import ballerina/io;
import cache;
import ballerina/runtime;


function main(string... args) {
    cache:Cache cache = new ("http://192.168.1.101");

    cache.put("Name", "Ballerina");

     string x = <string>cache.get("Name");
     io:println(x);

    runtime:sleep(100000000);
}
