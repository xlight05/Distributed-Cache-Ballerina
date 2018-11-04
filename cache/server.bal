import ballerina/grpc;
import ballerina/io;
import ballerina/config;
import ballerina/log;
import ballerina/http;

//endpoint grpc:Listener listener {
//    host: "localhost",
//    port: config:getAsInt("port", default = 7000)
//};listener listner
endpoint http:Listener listener {
    port: config:getAsInt("port", default = 7000)
};
//map<boolean> voteLog;
//boolean initVoteLog = voteLogInit();

// public type VoteRequest record{
//     int term;
//     string candidateID;
//     int lastLogIndex;
//     int lastLogTerm;
// };

// public type VoteResponse record{
//     boolean granted;
//     int term;
// };

// public type AppendEntries record {
//     int term;
//     string leaderID;
//     int prevLogIndex;
//     int prevLogTerm;
//     LogEntry[] entries;
//     int leaderCommit;
// };

// public type AppendEntriesResponse record {
//     int term;
//     boolean sucess;
//     int followerMatchIndex;

// };
//  public type LogEntry record {
//      int term;
//      string command;
//  };
//  type ConfigChangeResponse record{
//     boolean sucess;
//     string leaderHint;
// };
//
//AppendEntries(term, leaderID, prevLogIndex, prevLogTerm, entries[], leaderCommit)
//-> (term, conflictIndex, conflictTerm, success)
@http:ServiceConfig { basePath: "/raft" }
service<http:Service> raft bind listener {
    //Internal
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/vote"
    }
    voteResponseRPC(endpoint client, http:Request request) {
        //io:println("Before responding to vote");
        //printStats();
        json jsonPayload = check request.getJsonPayload();
        VoteRequest voteReq = check <VoteRequest>jsonPayload;
        log:printInfo("Vote request came from " + voteReq.candidateID);
        boolean granted = voteResponseHandle(voteReq);
        VoteResponse res = { granted: granted, term: currentTerm };
        log:printInfo("Vote status for " + voteReq.candidateID + " is " + res.granted);
        http:Response response;
        response.setJsonPayload(check <json>res);

        client->respond(response) but {
            error e => log:printError("Error in responding to vote req", err = e)
        };
    }
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/append"
    }
    appendEntriesRPC(endpoint client, http:Request request) {
        //io:println("Before respondin to append RPC");
        //io:println(printStats());
        json jsonPayload = check request.getJsonPayload();
        AppendEntries appendEntry = check <AppendEntries>jsonPayload;
        AppendEntriesResponse res = heartbeatHandle(appendEntry);
        //io:println("After respondin to append RPC");
        //io:println(printStats());
        //io:println(res);
        http:Response response;
        response.setJsonPayload(untaint check <json>res);

        client->respond(response) but {
            error e => log:printError("Error in responding to append req", err = e)
        };
    }

    //External
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/server"
    }
    addServerRPC(endpoint client, http:Request request) {
        string ip = check request.getTextPayload();
        //io:println(ip);
        ConfigChangeResponse res = addNode(ip);
        http:Response response;
        response.setJsonPayload(check <json>res);

        client->respond(response) but {
            error e => log:printError("Error in responding to add server", err = e)
        };
    }

    @http:ResourceConfig {
        methods: ["POST"],
        path: "/client"
    }
    clientRequestRPC(endpoint client, http:Request request) {
        string command = check request.getTextPayload();
        boolean sucess = clientRequest(command);
        ConfigChangeResponse res = { sucess: sucess, leaderHint: leader };
        http:Response response;
        response.setJsonPayload(check <json>res);

        client->respond(response) but {
            error e => log:printError("Error in responding to vote req", err = e)
        };
    }

    @http:ResourceConfig {
        methods: ["POST"],
        path: "/indirect"
    }
    indirectRPC(endpoint client, http:Request request) {
        json reqq = check request.getJsonPayload();
        string targetIP = check <string>reqq.ip;
        foreach i in clientMap {
            if (i.config.url == targetIP) {
                io:println("Target IP :"+i.config.url);
                blockingEp = i;
                break;
            }
        }
        //TODO High timeout coz data relocation might be affected
        var resp = blockingEp->get("/raft/failCheck/");
        json j1;
        match resp {
            http:Response payload => {
                io:println("Target is up");
                string result = check payload.getTextPayload();
                boolean relocate;
                if (result == "true") {
                    relocate = true;
                } else {
                    relocate = false;
                }
                j1 = { "status": true, "relocate": relocate };
            }
            error err => {
                io:println("Nop, still down");
                j1 = { "status": false, "relocate": false };
            }
        }
        http:Response response;
        response.setJsonPayload(j1);

        client->respond(response) but {
            error e => log:printError("Error in responding to vote req", err = e)
        };
    }

    @http:ResourceConfig {
        methods: ["GET"],
        path: "/failCheck"
    }
    failCheckRPC(endpoint client, http:Request request) {
        string res;
        if (isRelocationRunning) {
            res = "true";
        } else {
            res = "false";
        }
        http:Response response;
        response.setTextPayload(res);

        client->respond(response) but {
            error e => log:printError("Error in responding to vote req", err = e)
        };
    }
}

function heartbeatHandle(AppendEntries appendEntry) returns AppendEntriesResponse {
    //initLog();
    AppendEntriesResponse res;
    if (currentTerm < appendEntry.term) {
        //stepdown
        currentTerm = untaint appendEntry.term;
        state = "Follower";
        votedFor = "None";

    }
    if (currentTerm > appendEntry.term) {
        res = { term: currentTerm, sucess: false };
    } else {
        resetElectionTimer();
        leader = untaint appendEntry.leaderID;
        state = "Follower";
        boolean sucess = appendEntry.prevLogTerm == 0 || (appendEntry.prevLogIndex < lengthof log && log[appendEntry.
                    prevLogIndex].term == appendEntry.prevLogTerm);
        //can parse entries
        int index = 0;
        if (sucess) {
            index = appendEntry.prevLogIndex;
            foreach i in appendEntry.entries{
                index = index +1;
                if (getTerm(index) != i.term) {
                    log[index - 1] = i;//not sure
                }
            }
            index = index - 1;
            commitIndex = untaint min(appendEntry.leaderCommit, index);
        } else {
            index = 0;
        }
        res = { term: currentTerm, sucess: sucess, followerMatchIndex: index };

    }
    if (commitIndex > lastApplied) {
        foreach i in lastApplied + 1...commitIndex {
            apply(log[i].command);
            lastApplied = i;
        }
    }
    true -> raftReadyChan;
    return res;
}



//function voteLogInit() returns boolean {
//    foreach node in nodeList {
//        voteLog[node] = false;
//    }
//    return true;
//}

function voteResponseHandle(VoteRequest voteReq) returns boolean {
    boolean granted;
    int term = voteReq.term;
    if (term > currentTerm) {
        currentTerm = untaint term;
        state = "Follower";
        votedFor = "None";
        //resetElectionTimer();
        //startElectionTimer();//maybe move this down
        //Leader variable init
    }

    //if (term == currentTerm){
    //    if (votedFor == voteReq.candidateID){
    //        return true;
    //    }
    //    else {
    //        return false;
    //    }
    //}

    if (term < currentTerm) {//<=??
        return (false);
    }
    if votedFor != "None" && votedFor != voteReq.candidateID {
        return (false);
    }
    int ourLastLogIndex = (lengthof log) - 1;
    int ourLastLogTerm = -1;
    if (lengthof log != 0) {
        ourLastLogTerm = log[ourLastLogIndex].term;
    }

    if (voteReq.lastLogTerm < ourLastLogTerm) {
        return (false);
    }

    if (voteReq.lastLogTerm == ourLastLogTerm && voteReq.lastLogIndex < ourLastLogIndex) { //checkk
        return (false);
    }

    resetElectionTimer();
    votedFor = untaint voteReq.candidateID;

    return true;

    //
    //VoteRequest m = voteReq;
    //if (currentTerm == m.term && votedFor in [None, peer] &&(m.lastLogTerm > logTerm(len(log)) ||(m.lastLogTerm == logTerm(len(log)) &&m.lastLogIndex >= len(log)))):
}

function getTerm(int index) returns int {
    if (index < 1 || index >= lengthof log) {
        return 0;
    }
    else {
        return log[index].term;
    }
}



//service raft bind listener {
//    //Internal
//    voteResponseRPC(endpoint caller, VoteRequest voteReq, grpc:Headers headers) {
//        log:printInfo("Vote request came from " + voteReq.candidateID);
//        boolean granted = voteResponseHandle(voteReq);
//        VoteResponse res = { granted: granted, term: currentTerm };
//        error? err = caller->send(res);
//        log:printInfo(err.message but { () => "vote response " +
//                res.term + " " + res.granted });
//
//        _ = caller->complete();
//    }
//    appendEntriesRPC(endpoint caller, AppendEntries appendEntry, grpc:Headers headers) {
//        log:printInfo("AppendRPC request came from " + appendEntry.leaderID);
//        AppendEntriesResponse res = heartbeatHandle(appendEntry);
//        error? err = caller->send(res);
//        log:printInfo(err.message but { () => "Append RPC response " +
//                res.term + " " + res.sucess });
//        _ = caller->complete();
//    }
//
//    //External
//    addServerRPC(endpoint caller, string ip, grpc:Headers headers) {
//        ConfigChangeResponse res = addNode(ip);
//        error? err = caller->send(res);
//        log:printInfo(err.message but { () => "Add server response : " +
//                res.sucess + " " + res.leaderHint });
//
//        _ = caller->complete();
//    }
//
//    clientRequestRPC(endpoint caller, string command, grpc:Headers headers) {
//        boolean sucess = clientRequest(command);
//        ConfigChangeResponse res = { sucess: sucess, leaderHint: leader };
//        error? err = caller->send(res);
//        log:printInfo(err.message but { () => "Client RPC Response : " +
//                res.sucess + " " + res.leaderHint });
//
//        _ = caller->complete();
//    }
//    failCheckRPC(endpoint caller, string command, grpc:Headers headers) {
//        boolean sucess = clientRequest(command);
//        ConfigChangeResponse res = { sucess: sucess, leaderHint: leader };
//        error? err = caller->send(res);
//        log:printInfo(err.message but { () => "Client RPC Response : " +
//                res.sucess + " " + res.leaderHint });
//
//        _ = caller->complete();
//    }
//}
