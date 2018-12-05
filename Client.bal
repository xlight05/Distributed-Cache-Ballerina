import ballerina/io;
import cache;
import ballerina/runtime;
import ballerina/log;
import ballerina/http;
import ballerina/config;

public function main(string... args) {

    _ = cache:initNodeConfig();
    cache:Cache oauthCache = new("oauthCache",expiryTimeMillis=30000);

    oauthCache.put("1", "1");
    oauthCache.put("2", "2");
    oauthCache.put("3", "3");
    //io:println(<string>oauthCache.get("1"));
    oauthCache.put("4", "4");
    oauthCache.put("5", "5");
    oauthCache.put("6", "6");
    oauthCache.put("7", "7");
    oauthCache.put("8", "8");
    oauthCache.put("9", "9");
    oauthCache.put("10", "10");
    oauthCache.put("11", "11");
    oauthCache.put("12", "12");

    //io:println(<string>oauthCache.get("1"));
    //oauthCache.remove ("1");
    io:println(<string>oauthCache.get("1"));
    io:println(<string>oauthCache.get("2"));
    io:println(<string>oauthCache.get("3"));
    io:println(<string>oauthCache.get("4"));
    io:println(<string>oauthCache.get("5"));
    io:println(<string>oauthCache.get("6"));
    io:println(<string>oauthCache.get("7"));
    io:println(<string>oauthCache.get("8"));
    io:println(<string>oauthCache.get("9"));
    io:println(<string>oauthCache.get("10"));
    io:println(<string>oauthCache.get("11"));
    io:println(<string>oauthCache.get("12"));




    //io:println(<string>oauthCache.get("Name"));
    runtime:sleep(100000000);
}
