import cache;
import ballerina/math;
import ballerina/io;
import ballerina/time;
import ballerina/log;
import ballerina/config;
import ballerina/runtime;


public function main(string... args) {
    boolean localCacheEnabled = config:getAsBoolean("cache.localCache");
    int capacity = config:getAsInt("cache.capacity");
    float evictionFact = config:getAsFloat("cache.evictionFactor");
    int partitions = config:getAsInt("consistent.hashing.partitions");
    int replicationFact = config:getAsInt ("cache.replicationFact");
    int iteration = config:getAsInt ("benchmark.iteration");
    int nodes = config:getAsInt("benchmark.nodes");
    float nanosecondsPerOneEntry = runTest(10000);
    float requestsPerSecond = (1.0/nanosecondsPerOneEntry)*1000000000;
    writeToJson(10000, requestsPerSecond, localCacheEnabled,capacity,evictionFact,partitions,replicationFact,iteration,nodes);
    runtime:sleep (1000000);
}


public function writeToJson(int entryCount, float requestsPerSecond, boolean localCacheEnabled,int capacity,float evictionFactor,int partitions,int replicationFact,int iteration,int nodes) {
    string filePath = "./reports/get.json";
    json existingContent;
    try {
        existingContent = read(filePath);
    }
    catch (error e) {
        //content = [];
    }

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

    existingContent[lengthof existingContent] = newContent;
    write(existingContent, filePath);
}
function runTest(int entryCount) returns float {
    cache:Cache benchCache = new("oauthCache");
    string[] keyArr;
    // int spanId = observe:startRootSpan("Parent Span");
    // int spanId2 = check observe:startSpan("Child Span", parentSpanId = spanId);
    io:println("Starting populating cache");
    foreach i in 0...entryCount - 1{
        string str = <string> i;
        keyArr[i] = str;
         _=benchCache.put(str,str);
    }
    io:println("Starting GET Benchmark");
    //time:Time time1 = time:currentTime();
    int startingTime = time:nanoTime();
    //start timer

    foreach i in 0...entryCount - 1{
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
    string randomStr;
    foreach letter in letterArr {
        int randomIndex = math:randomInRange(0, lengthof letterArr);
        string randomLetter = letterArr[randomIndex];
        randomStr += randomLetter;
    }
    return randomStr;
}


function close(io:ReadableCharacterChannel|io:WritableCharacterChannel
               characterChannel) {
    match characterChannel {
        io:ReadableCharacterChannel readableCharChannel => {
            readableCharChannel.close() but {
                error e =>
                log:printError("Error occurred while closing character stream",
                    err = e)
            };
        }
        io:WritableCharacterChannel writableCharChannel => {
            writableCharChannel.close() but {
                error e =>
                log:printError("Error occurred while closing character stream",
                    err = e)
            };
        }
    }
}

function write(json content, string path) {
    io:WritableByteChannel byteChannel = io:openWritableFile(path);

    io:WritableCharacterChannel ch = new io:WritableCharacterChannel(byteChannel, "UTF8");

    match ch.writeJson(content) {
        error err => {
            close(ch);
            throw err;
        }
        () => {
            close(ch);
            io:println("Content written successfully");
        }
    }
}


function read(string path) returns json {
    io:ReadableByteChannel byteChannel = io:openReadableFile(path);

    io:ReadableCharacterChannel ch = new io:ReadableCharacterChannel(byteChannel, "UTF8");

    match ch.readJson() {
        json result => {
            close(ch);
            return result;
        }
        error err => {
            close(ch);
            throw err;
        }
    }
}