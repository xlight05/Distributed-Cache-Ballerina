import cache;
import ballerina/math;
import ballerina/io;
import ballerina/log;
//import ballerina/observe;
import ballerina/runtime;
import ballerina/time;
function main(string... args) {
    _ = cache:initNodeConfig();
    int avgNanoPerRecord1000000 = runTest(1000000);
    io:println(avgNanoPerRecord1000000);
    writeToJson(1000000, avgNanoPerRecord1000000, false,1000000,0.1,7,1,1,3);


}


function close(io:CharacterChannel characterChannel) {

    characterChannel.close() but {
        error e =>
        log:printError("Error occurred while closing character stream",
            err = e)
    };
}

function write(json content, string path) {

    io:ByteChannel byteChannel = io:openFile(path, io:WRITE);

    io:CharacterChannel ch = new io:CharacterChannel(byteChannel, "UTF8");

    match ch.writeJson(content) {
        error err => {
            close(ch);
            throw err;
        }
        () => {
            close(ch);
        }
    }
}


function read(string path) returns json {

    io:ByteChannel byteChannel = io:openFile(path, io:READ);

    io:CharacterChannel ch = new io:CharacterChannel(byteChannel, "UTF8");

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


//function repeatRun(int entryCount, int runCount) returns int {
//    int sum;
//    foreach i in 1...runCount {
//        int nanoPerEntry = runTest(entryCount);
//        sum += nanoPerEntry;
//    }
//    return (sum / runCount);
//}

function runTest(int entryCount) returns int {
    string randomCache = randomString();
    cache:Cache benchCache = new(randomCache);

    string[] keyArr;
    string[] valueArr;
    // int spanId = observe:startRootSpan("Parent Span");
    // int spanId2 = check observe:startSpan("Child Span", parentSpanId = spanId);
    foreach i in 0...entryCount - 1{
        keyArr[i] = randomString();
    }
    // _ = observe:finishSpan(spanId2);
    // _ = observe:finishSpan(spanId);
    foreach i in 0...entryCount - 1{
        valueArr[i] = randomString();
    }

    io:println("Starting");
    //time:Time time1 = time:currentTime();
    int startingTime = time:nanoTime();
    //start timer

    foreach i in 0...entryCount - 1{
        benchCache.put(keyArr[i], valueArr[i]);
    }
    //stop timer
    //time:Time time2 = time:currentTime();
    //runtime:sleep(8000000000000);
    benchCache.clearAllEntries();
    int endingTime = time:nanoTime();
    return ((endingTime - startingTime) / entryCount);

}
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

    
