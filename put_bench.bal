import cache;
import ballerina/math;
import ballerina/io;
import ballerina/log;
//import ballerina/observe;
import ballerina/runtime;
import ballerina/time;
import ballerina/config;

public function main(string... args) {
    boolean localCacheEnabled = config:getAsBoolean("cache.local.cache");
    int capacity = config:getAsInt("cache.capacity");
    float evictionFact = config:getAsFloat("cache.eviction.factor");
    int partitions = config:getAsInt("consistent.hashing.partitions");
    int replicationFact = config:getAsInt ("cache.replication.fact");
    int iteration = config:getAsInt ("benchmark.iteration");
    int nodes = config:getAsInt("benchmark.nodes");

    float nanosecondsPerOneEntry = runTest(1000000);
    float requestsPerSecond = (1.0/nanosecondsPerOneEntry)*1000000000;
    //writeToJson(1000000, requestsPerSecond, localCacheEnabled,capacity,evictionFact,partitions,replicationFact,iteration,nodes);
}




//function repeatRun(int entryCount, int runCount) returns int {
//    int sum;
//    foreach i in 1...runCount {
//        int nanoPerEntry = runTest(entryCount);
//        sum += nanoPerEntry;
//    }
//    return (sum / runCount);
//}

function runTest(int entryCount) returns float {
    cache:Cache benchCache = new("oauthCache");
    string[] keyArr=[];
    string[] valueArr=[];
    // int spanId = observe:startRootSpan("Parent Span");
    // int spanId2 = check observe:startSpan("Child Span", parentSpanId = spanId);
    foreach var i in 0...entryCount - 1{
        keyArr[i] = randomString();
    }
    // _ = observe:finishSpan(spanId2);
    // _ = observe:finishSpan(spanId);
    foreach var i in 0...entryCount - 1{
        valueArr[i] = randomString();
    }

    io:println("Starting");
    //time:Time time1 = time:currentTime();
    int startingTime = time:nanoTime();
    //start timer

    foreach var i in 0...entryCount - 1{
        benchCache.put(keyArr[i], valueArr[i]);
    }
    //stop timer
    //time:Time time2 = time:currentTime();
    //runtime:sleep(8000000000000);
    //benchCache.clearAllEntries();
    int endingTime = time:nanoTime();
    float entryCountFloat = <float> entryCount;
    return ((endingTime - startingTime) / entryCountFloat);

}
function randomString() returns string {
    string[] letterArr = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
    "T", "U", "V", "W", "X", "Y", "Z", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0"];
    string randomStr="";
    foreach var letter in letterArr {
        int randomIndex = math:randomInRange(0,letterArr.length());
        string randomLetter = letterArr[randomIndex];
        randomStr += randomLetter;
    }
    return randomStr;
}


