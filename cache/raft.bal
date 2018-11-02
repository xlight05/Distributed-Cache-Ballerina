import ballerina/io;
import ballerina/runtime;
import ballerina/task;
import ballerina/math;
import ballerina/grpc;
import ballerina/log;
import ballerina/config;
import ballerina/http;

//TODO make proper election timeout with MIN MAX timeout
//TODO make proper hearbeat timeout
//TODO Test dynamic node changes
//TODO LOCKS
//TODO LAG Test
//TODO Partition Test
//TODO Merge with cache


//endpoint raftBlockingClient blockingEp {
//    url: "http://localhost:3000"
//};

endpoint http:Client blockingEp {
    url: "http://localhost:3000"
};

map<http:Client> clientMap;
int MIN_ELECTION_TIMEOUT = 2000;
int MAX_ELECTION_TIMEOUT = 2250;
int HEARTBEAT_TIMEOUT = 1000;

string leader;
string state = "Follower";
int currentTerm;
LogEntry[] log = [{}];
string votedFor = "None";
string currentNode = config:getAsString("ip") + ":" + config:getAsString("port");
int commitIndex = 0;
int lastApplied = 0;
task:Timer? timer;
task:Timer? heartbeatTimer;
map<int> candVoteLog;

map<int> nextIndex;
map<int> matchIndex;
channel<boolean> raftReadyChan;

type SuspectNode record {
    http:Client client;
    string ip;
    int suspectRate;
};

//failure detector
int SUSPECT_VALUE = 10; // 1 - 100;
int FAILURE_TIMEOUT_MILS = 1000;
map<SuspectNode> suspectNodes;
boolean isRelocationRunning;

public function startRaft() {
    //cheat for first node lol
    log[lengthof log] = { term: 1, command: "NA " + currentNode };
    apply("NA " + currentNode);
    commitIndex = commitIndex +1;
    lastApplied = lastApplied +1;
    //raftBlockingClient client;
    //grpc:ClientEndpointConfig cc = { url: currentNode };
    //client.init(cc);
    //clientMap[currentNode] = client;
    nextIndex[currentNode] = 1;
    matchIndex[currentNode] = 0;


    int interval = math:randomInRange(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT);

    (function () returns error?) onTriggerFunction = electLeader;

    function (error) onErrorFunction = timerError;

    timer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval);
    timer.start();
    //io:println("After starting cluster");
    //printStats();
    boolean ready;
    ready <- raftReadyChan;
}

//function printStats() {
//    io:println("State :" + state);
//    io:println("Current Term :" + currentTerm);
//    io:print("Log :");
//    io:println(log);
//    io:println("Commit Index :" + commitIndex);
//    io:println("Leader Vars ");
//    io:println("Next Index :");
//    foreach k, v in nextIndex {
//        io:println(k + " : " + v);
//    }
//    io:println("Match Index :");
//    foreach k, v in matchIndex {
//        io:println(k + " : " + v);
//    }
//    io:println("Client list :");
//    foreach i in clientMap{
//        io:println(i.config.url);
//    }
//    io:println();
//}

public function joinRaft() {
    nextIndex[currentNode] = 1;
    matchIndex[currentNode] = 0;


    int interval = math:randomInRange(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT);

    (function () returns error?) onTriggerFunction = electLeader;

    function (error) onErrorFunction = timerError;

    timer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval, delay = interval);
    timer.start();
    //io:println("After joining cluster");
    //printStats();
    boolean ready;
    ready <- raftReadyChan;
}

function electLeader() {

    //addNodes();//temp
    if (state == "Leader") {
        //timer.stop();
        return;
    }
    log:printInfo("Starting Leader Election by " + currentNode);
    currentTerm = currentTerm +1;
    int electionTerm = currentTerm;
    votedFor = currentNode;
    state = "Candidate";
    VoteRequest req = { term: currentTerm, candidateID: currentNode, lastLogIndex: (lengthof log) - 1, lastLogTerm: log[(
        lengthof log) - 1].term };
    //io:print("Sending vote Request :");
    //io:println(req);
    future<int> voteResp = start sendVoteRequests(untaint req);
    int voteCount = await voteResp;
    //check if another appendEntry came
    if (currentTerm != electionTerm) {
        return;
    }
    int quoram = <int>math:ceil(lengthof clientMap / 2.0);
    log:printInfo(voteCount + " out of " + lengthof clientMap);
    //0 for first node
    if (voteCount < quoram) {
        state = "Follower";
        votedFor = "None";
        //heartbeatTimer.stop();
        //not sure if started
        //stepdown
    } else {
        state = "Leader";
        //timer.stop();
        foreach i in clientMap {
            nextIndex[i.config.url] = lengthof log;
        }
        startHeartbeatTimer();
        //startProcessingSuspects();
    }
    resetElectionTimer();
    log:printInfo(currentNode + " is a " + state);
    true -> raftReadyChan;
    //io:println("After electing Leader");
    //printStats();
    return ();
}

function sendVoteRequests(VoteRequest req) returns int {
    //votes for itself
    future[] futureVotes;
    foreach node in clientMap {
        if (node.config.url == currentNode) {
            continue;
        }
        candVoteLog[node.config.url] = -1;
        future asyncRes = start seperate(node, req);
        futureVotes[lengthof futureVotes] = asyncRes;
        //ignore current Node
    }
    foreach i in futureVotes { //change this in to quoram
        _ = await i;
    }
    int count = 1;
    foreach item in candVoteLog {
        if (item == 1) {
            count= count +1;
        }
        if (item == -2) {
            candVoteLog.clear();
            return 0;
        }
    }
    candVoteLog.clear();
    //int count;
    ////busy while :/ fix using future map
    //while (true) {
    //    runtime:
    //    sleep(50);
    //    count = 0;
    //    foreach item in candVoteLog {
    //        if (item == 1) {
    //            count++;
    //        }
    //    }
    //    if (count > math:floor(<int>lengthof clientMap / 2.0)) {
    //        break;
    //    } else {
    //        //wait a bit
    //        runtime:sleep(100);
    //        //change // 150/2
    //        //check again
    //        break;
    //    }
    //}

    return count;
}

function seperate(http:Client node, VoteRequest req) {
    blockingEp = node;

    var unionResp = blockingEp->post("/raft/vote", check <json>req);
    match unionResp {
        http:Response payload => {
            VoteResponse result = check <VoteResponse>check payload.getJsonPayload();
            if (result.term > currentTerm) {
                candVoteLog[node.config.url] = -2;
                return;
                //stepdown
            }
            if (result.granted) {
                candVoteLog[node.config.url] = 1;
            }
            else {
                candVoteLog[node.config.url] = 0;
            }

        }
        error err => {
            log:printError("Error from Connector: " + err.message + "\n");
            candVoteLog[node.config.url] = 0;
        }
    }
}

function sendHeartbeats() {
    if (state != "Leader") {
        return;
    }
    future[] heartbeatAsync;
    foreach node in clientMap {
        if (node.config.url == currentNode) {
            continue;
        }
        future asy = start heartbeatChannel(node);
        heartbeatAsync[lengthof heartbeatAsync] = asy;
    }
    foreach item in heartbeatAsync {
        var x = await item;
    }
    commitEntry();
    //io:println("After sending all heartbeats");
    //io:println(printStats());
}

function heartbeatChannel(http:Client node) {
    if (state != "Leader") {
        return;
    }
    string peer = node.config.url;
    int nextIndexOfPeer = nextIndex[peer] ?: 0;
    int prevLogIndex = nextIndexOfPeer - 1;
    int prevLogTerm = 0;
    if (prevLogIndex > 0) {
        prevLogTerm = log[prevLogIndex].term;
    }
    //        int lastEntry = min(lengthof log,nextIndexOfPeer);
    LogEntry[] entryList;
    foreach i in prevLogIndex...lengthof log - 1 {
        entryList[lengthof entryList] = log[i];
    }
    AppendEntries appendEntry = {
        term: currentTerm,
        leaderID: currentNode,
        prevLogIndex: prevLogIndex,
        prevLogTerm: log[prevLogIndex].term,
        entries: entryList,
        leaderCommit: commitIndex
    };
    //io:println(appendEntry);
    //io:println("To " + node.config.url);
    blockingEp = node;
    var heartbeatResp = blockingEp->post("/raft/append", check <json>untaint appendEntry);
    match heartbeatResp {
        http:Response payload => {
            AppendEntriesResponse result = check <AppendEntriesResponse>check payload.getJsonPayload();
            if (result.sucess) {
                matchIndex[peer] = result.followerMatchIndex;
                nextIndex[peer] = result.followerMatchIndex + 1; //atomicc
            } else {
                nextIndex[peer] = max(1, nextIndexOfPeer - 1);
                heartbeatChannel(node);
            }
        }
        error err => {
            log:printError("Error from Connector: " + err.message + "\n");
            //commit suspect
            //boolean commited = clientRequest("NSA "+node.config.url);
            //log:printInfo(node.config.url +" added to suspect list");
            //commited?
        }
    }
    commitEntry();
}

function startProcessingSuspects() {
    foreach suspect in suspectNodes {
        _ = start checkSuspectedNode(suspect);
    }
}
//executed ones few appendRPC fails
//assuming nodes are in suspect state
function checkSuspectedNode(SuspectNode node) {
    http:Client? clients = getHealthyNode();
    match clients {
        http:Client client => {
            blockingEp = client;
            json req = { ip: node.client.config.url };
            //change
            //increase timeout
            var resp = blockingEp->post("/raft/indirect/", req);
            match resp {
                http:Response payload => {
                    json|error result = payload.getJsonPayload();
                    match result {
                        json j => {
                            boolean status = check <boolean>j.status;
                            if (status) {
                                //up
                                boolean relocate = check <boolean>j.relocate;
                                if (!relocate) {
                                    //not relocating just slow lol or up noww !
                                    node.suspectRate = node.suspectRate - SUSPECT_VALUE;
                                    if (node.suspectRate <= -50) {
                                        //commit remove from suspect
                                        boolean commited = clientRequest("NSR " + node.client.config.url);
                                        log:printInfo(node.client.config.url +" Recovred from suspection");
                                        //??commited
                                        return;
                                    }
                                }
                            } else {
                                //not responding
                                node.suspectRate = node.suspectRate + SUSPECT_VALUE;
                                if (node.suspectRate >= 100) {
                                    //commit dead
                                    boolean commited = clientRequest("NR " + node.client.config.url);
                                    log:printInfo(node.client.config.url +" Removed from the cluster");
                                    return;
                                }
                            }
                            runtime:sleep(FAILURE_TIMEOUT_MILS);
                            checkSuspectedNode(node);
                        }
                        error e => {
                            log:printError("Error from Connector: " + e.message + "\n");
                        }
                    }

                }
                error err => {
                    //if healthy node didnt respond
                    log:printError("Error from Connector: " + err.message + "\n");
                }
            }

        }
        () => {
            log:printWarn("No Healthy nodes. LOL");
        }
    }
}

function getHealthyNode() returns http:Client? {
    foreach i in clientMap {
        boolean inSuspect = false;
        foreach j in suspectNodes {
            if (i.config.url == j.ip) {
                inSuspect = true;
            }
        }
        if (!inSuspect) {
            return i;
        }
    }
    return ();
}

function commitEntry() {
    if (state != "Leader") {
        return;
    }
    int item = lengthof log - 1;
    while (item > commitIndex) {
        int replicatedCount = 1;
        foreach server in clientMap {
            if (server.config.url == currentNode) {
                continue;
            }
            if (matchIndex[server.config.url] == item) {
                replicatedCount = replicatedCount +1;
            }
        }
        if (replicatedCount >= math:ceil(lengthof clientMap / 2.0)) {
            commitIndex = item;
            apply(log[item].command);
            break;
        }
        item = item - 1;
    }
}


function min(int x, int y) returns int {
    if (x < y) {
        return x;
    } else {
        return y;
    }
}

function max(int x, int y) returns int {
    if (x > y) {
        return x;
    }
    else {
        return y;
    }
}
function timerError(error e) {
    io:println(e);
}


function stepDown() {

}

function resetElectionTimer() {
    int interval = math:randomInRange(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT);
    lock {
        timer.stop();
        (function () returns error?) onTriggerFunction = electLeader;

        function (error) onErrorFunction = timerError;
        timer = new task:Timer(onTriggerFunction, onErrorFunction,
            interval);
        timer.start();
    }
}

function startHeartbeatTimer() {
    int interval = HEARTBEAT_TIMEOUT;
    (function () returns error?) onTriggerFunction = sendHeartbeats;

    function (error) onErrorFunction = timerError;
    heartbeatTimer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval);
    heartbeatTimer.start();
}

function startElectionTimer() {
    int interval = math:randomInRange(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT);
    (function () returns error?) onTriggerFunction = electLeader;

    function (error) onErrorFunction = timerError;
    timer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval, delay = interval);
    timer.start();
}


function clientRequest(string command) returns boolean {
    if (state == "Leader") {
        int entryIndex = lengthof log;
        log[entryIndex] = { term: currentTerm, command: command };
        future ee = start sendHeartbeats();
        _ = await ee;
        //check if commited moree
        if (commitIndex >= entryIndex) {
            return true;
        } else {
            return false;
        }

    } else {
        return false;
    }
}


function addNode(string ip) returns ConfigChangeResponse {
    if (state != "Leader") {
        return { sucess: false, leaderHint: leader };
    } else {
        string command = "NA " + ip;
        //or commit
        boolean sucess = clientRequest(command);
        return { sucess: sucess, leaderHint: leader };
    }
}

function apply(string command) {
    if (command.substring(0, 2) == "NA") { //NODE ADD
        string ip = command.split(" ")[1];
        foreach item in clientMap { // temp. check heartbeat commiting agian
            if (item.config.url == ip) {
                return;
            }
        }
        http:Client client;
        http:ClientEndpointConfig cc = {
            url: ip,
            timeoutMillis: HEARTBEAT_TIMEOUT / 3
            //retryConfig: {
            //    interval: HEARTBEAT_TIMEOUT/2,
            //    count: 3,
            //    backOffFactor: 1.0,
            //    maxWaitInterval: HEARTBEAT_TIMEOUT/2
            //}
        };
        client.init(cc);
        clientMap[ip] = client;
        nextIndex[ip] = 1;
        matchIndex[ip] = 0;
        hashRing.add(ip);
        relocateData();
        // async?  //incositant while catching up a new node?
        io:println(lengthof clientMap);
    }

    if (command.substring(0, 3) == "NSA") { //NODE SUSPECT Add
        string ip = command.split(" ")[1];
        foreach item in suspectNodes { // temp. check heartbeat commiting agian
            if (item.ip == ip) {
                return;
            }
        }
        http:Client client;
        http:ClientEndpointConfig cc = { url: ip, timeoutMillis: 60000 };
        client.init(cc);
        SuspectNode node = { ip: ip, client: client, suspectRate: 0 };
        suspectNodes[ip] = node;
        _ = start checkSuspectedNode(node);
    }

    if (command.substring(0, 3) == "NSR") { //NODE SUSPECT Remove
        string ip = command.split(" ")[1];
        _ = suspectNodes.remove(ip);
    }

    if (command.substring(0, 2) == "NR") { //NODE Remove
        string ip = command.split(" ")[1];
        //_ = suspectNodes.remove(ip);
        boolean sucess = clientRequest("NSR " + ip);
        if (sucess) {
            _ = clientMap.remove(ip);
            relocateData(); // async?
        }

    }
    log:printInfo(command + " Applied!!");
}

// function isQuoram(int count) returns boolean {
//     if (count==0){
//         return true;
//     }
// }