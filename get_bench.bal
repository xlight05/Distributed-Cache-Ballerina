//import cache;
//import ballerina/math;
//import ballerina/io;
////import ballerina/observe;
//import ballerina/time;
//import ballerina/log;
//public function main(string... args) {
//    _ = cache:initNodeConfig();
//    int avgNanoPerRecord1000000 = runTest(1000000);
//    io:println(avgNanoPerRecord1000000);
//    writeToJson(1000000, avgNanoPerRecord1000000, false,1000000,0.1,7,1,1,3);
//
//}
//
//
//
//function runTest(int entryCount) returns int {
//    string randomCache = randomString();
//    cache:Cache benchCache = new(randomCache);
//    string[] keyArr;
//    // int spanId = observe:startRootSpan("Parent Span");
//    // int spanId2 = check observe:startSpan("Child Span", parentSpanId = spanId);
//    foreach i in 0...entryCount - 1{
//        keyArr[i] = randomString();
//    }
//
//    io:println("Starting");
//    //time:Time time1 = time:currentTime();
//    int startingTime = time:nanoTime();
//    //start timer
//
//    foreach i in 0...entryCount - 1{
//        _ = benchCache.get(keyArr[i]);
//    }
//    //stop timer
//    //time:Time time2 = time:currentTime();
//    int endingTime = time:nanoTime();
//    return ((endingTime - startingTime) / entryCount);
//}
//function randomString() returns string {
//    string[] letterArr = ["A", "B", "C", "D", "E", "F", "G", "H", "I", "J", "K", "L", "M", "N", "O", "P", "Q", "R", "S",
//    "T", "U", "V", "W", "X", "Y", "Z", "1", "2", "3", "4", "5", "6", "7", "8", "9", "0"];
//    string randomStr;
//    foreach letter in letterArr {
//        int randomIndex = math:randomInRange(0, lengthof letterArr);
//        string randomLetter = letterArr[randomIndex];
//        randomStr += randomLetter;
//    }
//    return randomStr;
//}
//
//function writeToJson(int entryCount, int timeTaken, boolean localCacheEnabled,int capacity,float evictionFactor,int partitions,int replicationFact,int iteration,int nodes) {
//    string filePath = "./reports/get.json";
//    json existingContent;
//    try {
//        existingContent = read(filePath);
//    }
//    catch (error e) {
//        //content = [];
//    }
//
//    time:Time time = time:currentTime();
//    int currentTime = time.time;
//    json newContent = {
//        "currentTime": currentTime,
//        "entryCount": entryCount,
//        "timeTaken": timeTaken,
//        "localCache": localCacheEnabled,
//        "capacity":capacity,
//        "evictionFactor":evictionFactor,
//        "partitions":partitions,
//        "replicationFact":replicationFact,
//        "iteration":iteration
//    };
//
//    existingContent[lengthof existingContent] = newContent;
//    write(existingContent, filePath);
//}
//
//function close(io:CharacterChannel characterChannel) {
//
//    characterChannel.close() but {
//        error e =>
//        log:printError("Error occurred while closing character stream",
//            err = e)
//    };
//}
//
//function write(json content, string path) {
//
//    io:ByteChannel byteChannel = io:openFile(path, io:WRITE);
//
//    io:CharacterChannel ch = new io:CharacterChannel(byteChannel, "UTF8");
//
//    match ch.writeJson(content) {
//        error err => {
//            close(ch);
//            throw err;
//        }
//        () => {
//            close(ch);
//        }
//    }
//}
//
//
//function read(string path) returns json {
//
//    io:ByteChannel byteChannel = io:openFile(path, io:READ);
//
//    io:CharacterChannel ch = new io:CharacterChannel(byteChannel, "UTF8");
//
//    match ch.readJson() {
//        json result => {
//            close(ch);
//            return result;
//        }
//        error err => {
//            close(ch);
//            throw err;
//        }
//    }
//}
