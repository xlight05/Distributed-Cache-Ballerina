import cache;
import ballerina/math;
import ballerina/io;
import ballerina/time;
import ballerina/log;
import ballerina/config;
import ballerina/runtime;


public function main(string... args) {
    boolean localCacheEnabled = config:getAsBoolean("cache.local.cache");
    int capacity = config:getAsInt("cache.capacity");
    float evictionFact = config:getAsFloat("cache.eviction.factor");
    int partitions = config:getAsInt("consistent.hashing.partitions");
    int replicationFact = config:getAsInt ("cache.replication.fact");
    int iteration = config:getAsInt ("benchmark.iteration");
    int nodes = config:getAsInt("benchmark.nodes");
    float nanosecondsPerOneEntry = runTest(10000);
    float requestsPerSecond = (1.0/nanosecondsPerOneEntry)*1000000000;
    writeToJson(10000, requestsPerSecond, localCacheEnabled,capacity,evictionFact,partitions,replicationFact,iteration,nodes);
    runtime:sleep (1000000);
}


public function writeToJson(int entryCount, float requestsPerSecond, boolean localCacheEnabled,int capacity,float evictionFactor,int partitions,int replicationFact,int iteration,int nodes) {
    string filePath = "./reports/get.json";
    json|error existingContent = read(filePath);
    if (existingContent is json ){
        time:Time time = time:currentTime();
        int currentTime = time.time;
        json newContent = {
            "currentTime": currentTime,
            "entryCount": entryCount,
            "Requests per second": requestsPerSecond,
            "localCache": localCacheEnabled,
            "capacity":capacity,
            "evictionFactor":evictionFactor,
            "partitions":partitions,
            "replicationFact":replicationFact,
            "iteration":iteration,
            "nodeCount":nodes
        };

        existingContent[existingContent.length()] = newContent;
        _=write(existingContent, filePath);
    }
}
function runTest(int entryCount) returns float {
    cache:Cache benchCache = new("oauthCache");
    string[] keyArr=[];
    // int spanId = observe:startRootSpan("Parent Span");
    // int spanId2 = check observe:startSpan("Child Span", parentSpanId = spanId);
    io:println("Starting populating cache");
    foreach var i in 0...entryCount - 1{
        string str = <string> i;
        keyArr[i] = str;
         _=benchCache.put(str,str);
    }
    io:println("Starting GET Benchmark");
    //time:Time time1 = time:currentTime();
    int startingTime = time:nanoTime();
    //start timer

    foreach var i in 0...entryCount - 1{
        var x = benchCache.get(keyArr[i]);
        if (x==()){
            io:println (x);
            _=benchCache.put(<string>i,<string>i);
            _=benchCache.get(keyArr[i]);
        }
    }
    //stop timer
    //time:Time time2 = time:currentTime();
    int endingTime = time:nanoTime();
    float entryCountFloat = <float> entryCount;
    return ((endingTime - startingTime) / entryCountFloat);
}
// function runTest(int entryCount) returns float {
//     cache:Cache benchCache = new("oauthCache");
//     string[] keyArr;
//     // int spanId = observe:startRootSpan("Parent Span");
//     // int spanId2 = check observe:startSpan("Child Span", parentSpanId = spanId);
//     io:println("Starting populating cache");
//     foreach i in 0...entryCount - 1{
//         string str = randomString();
//         keyArr[i] = str;
//          _=benchCache.put(str,str);
//     }
//     runtime:sleep(10000);
//     io:println("Starting GET Benchmark");
//     //time:Time time1 = time:currentTime();
//     int startingTime = time:nanoTime();
//     //start timer

//     foreach i in 0...entryCount - 1{
//         _ = benchCache.get(keyArr[i]);
//     }
//     //stop timer
//     //time:Time time2 = time:currentTime();
//     int endingTime = time:nanoTime();
//     float entryCountFloat = <float> entryCount;
//     return ((endingTime - startingTime) / entryCountFloat);
// }
function randomString() returns string {
    string[] letterArr = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
    "T", "U", "V", "W", "X", "Y", "Z", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0"];
    string randomStr="";
    foreach var letter in letterArr {
        int randomIndex = math:randomInRange(0, letterArr.length());
        string randomLetter = letterArr[randomIndex];
        randomStr += randomLetter;
    }
    return randomStr;
}

function closeRc(io:ReadableCharacterChannel rc) {
    var result = rc.close();
    if (result is error) {
        log:printError("Error occurred while closing character stream",
                        err = result);
    }
}

function closeWc(io:WritableCharacterChannel wc) {
    var result = wc.close();
    if (result is error) {
        log:printError("Error occurred while closing character stream",
                        err = result);
    }
}

function read(string path) returns json|error {

    io:ReadableByteChannel rbc = io:openReadableFile(path);

    io:ReadableCharacterChannel rch = new(rbc, "UTF8");
    var result = rch.readJson();
    if (result is error) {
        closeRc(rch);
        return result;
    } else {
        closeRc(rch);
        return result;
    }
}

function write(json content, string path) returns error? {

    io:WritableByteChannel wbc = io:openWritableFile(path);

    io:WritableCharacterChannel wch = new(wbc, "UTF8");
    var result = wch.writeJson(content);
    if (result is error) {
        closeWc(wch);
        return result;
    } else {
        closeWc(wch);
        return result;
    }
}
