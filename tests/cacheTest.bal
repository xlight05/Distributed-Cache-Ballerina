import ballerina/io;
import ballerina/test;
import cache;

// The `BeforeSuite` function is executed before all test functions in this package. 
@test:BeforeSuite
function init() {
    io:println("I'm the before suite function!");
        _ =cache:initNodeConfig();
    cache:Cache oauthCache = new("testCache");
}

// Test function.
@test:Config
function testFunction1() {
    io:println("I'm in test function 1!");
    test:assertTrue(true, msg = "Failed");
}

// Test function.
@test:Config
function testFunction2() {
    io:println("I'm in test function 2!");
    test:assertTrue(true, msg = "Failed");
}