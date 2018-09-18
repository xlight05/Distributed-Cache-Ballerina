import ballerina/io;
import ballerina/test;

// The `BeforeSuite` function is executed before all test functions in this package. 
@test:BeforeSuite
function init() {
    io:println("I'm the before suite function!");

}

// Test function.
@test:Config
function geWithOneNodetTest() {
    Consistent hashRing = new ();
    hashRing.add("http://localhost:3000");
    string key = "oauth"; //Key here
    string node = hashRing.get(key);
    string actualNode = "http://localhost:3000";
    test:assertEquals(node,actualNode, msg = "Failed");
}

@test:Config
function geWithThreeNodetTest() {
    Consistent hashRing = new ();
    hashRing.add("http://localhost:3000");
    hashRing.add("http://localhost:4000");
    hashRing.add("http://localhost:5000");
    string key = "oauth"; //Key here
    string node = hashRing.get(key);
    string actualNode = "http://localhost:5000";
    test:assertEquals(node,actualNode, msg = "Failed");


    key = "testKey";
    node = hashRing.get(key);
    actualNode = "http://localhost:4000";
    test:assertEquals(node,actualNode, msg = "Failed");

}

// Test function.
@test:Config
function testFunction2() {
    io:println("I'm in test function 2!");
    test:assertTrue(true, msg = "Failed");
}
