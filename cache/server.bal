import ballerina/grpc;
import ballerina/io;
import ballerina/config;
import ballerina/log;
import ballerina/http;

endpoint http:Listener listener {
    port: config:getAsInt("raft.port", default = 7000)
};

# An entry of the replicated log
# + term- Term of the entry
# + command - Command to be executed once the entry is commited
type LogEntry record {
    int term;
    string command;
};

# Incoming Vote Request from the candidate
# + term - Candidate's term
# + candidateID - Candidate's identifier
# + lastLogIndex - Index of the last log entry of the candidate
# + lastLogTerm - Term of the last log entry of the candidate
type VoteRequest record {
    int term;
    string candidateID;
    int lastLogIndex;
    int lastLogTerm;
};

# Response for a candidate vote request
# + granted - Vote granted status
# + term - Current term of the reciever's node
type VoteResponse record {
    boolean granted;
    int term;
};

# Heartbeat or new Entry request from the leader
# + term- Leader's term
# + leaderID - Leader's identifier
# + prevLogIndex - Previous log index of the leader
# + prevLogTerm - Previous log term of the leader
# + entries - Contains non replicated entries from the leader. Empty for a hearbeat
# + leaderCommit - Highest commit index of the leader
type AppendEntries record {
    int term;
    string leaderID;
    int prevLogIndex;
    int prevLogTerm;
    LogEntry[] entries;
    int leaderCommit;
};

# Response to a new entry request or heartbeat
# + term- term of the reciever's node
# + sucess - notifies if the new entries has been replicated
# + followerMatchIndex - last commited index of the follower
type AppendEntriesResponse record {
    int term;
    boolean sucess;
    int followerMatchIndex;
};

# The reponse for a client intraction. This includes Cluster changes aswell
# + sucess - status of the client request
# + leaderHint - last known leader of the cluster
type ClientResponse record {
    boolean sucess;
    string leaderHint;
};

@http:ServiceConfig { basePath: "/raft" }
service<http:Service> raft bind listener {
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/vote"
    }
    voteResponseRPC(endpoint client, http:Request request) {
        json jsonPayload = check request.getJsonPayload();
        VoteRequest voteReq = check <VoteRequest>jsonPayload;
        log:printInfo("Vote request came from " + voteReq.candidateID);
        boolean granted = voteResponseHandle(voteReq);
        VoteResponse voteResponse = { granted: granted, term: currentTerm };
        log:printInfo("Vote status for " + voteReq.candidateID + " is " + voteResponse.granted);
        http:Response response;
        response.setJsonPayload(check <json>voteResponse);
        client->respond(response) but {
            error e => log:printError("Error in responding to vote request", err = e)
        };
    }

    @http:ResourceConfig {
        methods: ["POST"],
        path: "/append"
    }
    appendEntriesRPC(endpoint client, http:Request request) {
        json jsonPayload = check request.getJsonPayload();
        AppendEntries appendEntry = check <AppendEntries>jsonPayload;
        AppendEntriesResponse appendEntriesResponse = heartbeatHandle(appendEntry);
        http:Response response;
        response.setJsonPayload(untaint check <json>appendEntriesResponse);
        client->respond(response) but {
            error e => log:printError("Error in responding to append request", err = e)
        };
    }

    @http:ResourceConfig {
        methods: ["POST"],
        path: "/server"
    }
    addServerRPC(endpoint client, http:Request request) {
        string ip = check request.getTextPayload();
        ClientResponse configChangeResponse = addNode(ip);
        http:Response response;
        response.setJsonPayload(check <json>configChangeResponse);
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
        ClientResponse res = { sucess: sucess, leaderHint: leader };
        http:Response response;
        response.setJsonPayload(check <json>res);
        client->respond(response) but {
            error e => log:printError("Error in responding to client request", err = e)
        };
    }

    @http:ResourceConfig {
        methods: ["POST"],
        path: "/indirect"
    }
    indirectRPC(endpoint client, http:Request request) {
        json reqq = check request.getJsonPayload();
        string targetIP = check <string>reqq.ip;
        Node targetNode;
        foreach i in raftClientMap {
            if (i.ip == targetIP) {
                raftEndpoint = i.nodeEndpoint;
                targetNode = i;
                break;
            }
        }
        var resp = raftEndpoint->get("/raft/fail/check/");
        json responsePayload;
        match resp {
            http:Response payload => {
                string result = check payload.getTextPayload();
                boolean relocate;
                if (result == "true") {
                    relocate = true;
                } else {
                    relocate = false;
                }
                responsePayload = { "status": true, "relocate": relocate };
            }
            error err => {
                responsePayload = { "status": false, "relocate": false };
            }
        }
        log:printInfo("Indirect ping for " + targetNode.ip + " Server Status : " + responsePayload["status"].
                toString());
        http:Response response;
        response.setJsonPayload(responsePayload);
        client->respond(response) but {
            error e => log:printError("Error in responding to indirect ping", err = e)
        };
    }

    @http:ResourceConfig {
        methods: ["GET"],
        path: "/fail/check"
    }
    failCheckRPC(endpoint client, http:Request request) {
        string res;
        if (isRelocationOrEvictionRunning) {
            res = "true";
        } else {
            res = "false";
        }
        http:Response response;
        response.setTextPayload(res);
        client->respond(response) but {
            error e => log:printError("Error in responding to fail check RPC", err = e)
        };
    }
}

function heartbeatHandle(AppendEntries appendEntry) returns AppendEntriesResponse {
    AppendEntriesResponse res;
    // step down before handling RPC if need be
    if (currentTerm < appendEntry.term) {
        //stepdown
        currentTerm = untaint appendEntry.term;
        state = "Follower";
        votedFor = "None";

    }
    // outdated leader request
    if (currentTerm > appendEntry.term) {
        res = { term: currentTerm, sucess: false };
    } else {
        resetElectionTimer();
        leader = untaint appendEntry.leaderID;
        state = "Follower";
        boolean sucess = appendEntry.prevLogTerm == 0 || (appendEntry.prevLogIndex < lengthof log && log[appendEntry.
                    prevLogIndex].term == appendEntry.prevLogTerm);
        int index = 0;
        //can parse entries in appendRPC
        if (sucess) {
            index = appendEntry.prevLogIndex;
            //make the log same as leaders log
            foreach i in appendEntry.entries{
                index = index + 1;
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
    //commit entries for the follower
    if (commitIndex > lastApplied) {
        boolean isNodeChanged = false;
        foreach i in lastApplied + 1...commitIndex {
            //To Reduce multiple relocation need better fix
            if (log[i].command.substring(0, 2) == "NA" || log[i].command.substring(0, 2) == "NR") {
                isNodeChanged = true;
            }
            apply(log[i].command);
            lastApplied = i;
        }
        //to avoid too much data relocation
        if (isNodeChanged) {
            relocateData();
        }
    }
    //signal raft is ready.
    true -> raftReadyChan;
    return res;
}

function voteResponseHandle(VoteRequest voteReq) returns boolean {
    boolean granted;
    int term = voteReq.term;
    // step down before handling RPC if need be
    if (term > currentTerm) {
        currentTerm = untaint term;
        state = "Follower";
        votedFor = "None";
        //resetElectionTimer();
        //startElectionTimer();//maybe move this down
        //Leader variable init
    }
    // don't vote for out-of-date candidates
    if (term < currentTerm) {
        return false;
    }
    // don't double vote
    if votedFor != "None" && votedFor != voteReq.candidateID {
        return false;
    }
    // check how up-to-date our log is
    int ourLastLogIndex = (lengthof log) - 1;
    int ourLastLogTerm = -1;
    if (lengthof log != 0) {
        ourLastLogTerm = log[ourLastLogIndex].term;
    }
    // reject leaders with old logs
    if (voteReq.lastLogTerm < ourLastLogTerm) {
        return false;
    }
    // reject leaders with short logs
    if (voteReq.lastLogTerm == ourLastLogTerm && voteReq.lastLogIndex < ourLastLogIndex) {
        return false;
    }
    resetElectionTimer();
    votedFor = untaint voteReq.candidateID;
    return true;
}

# Gets term number of a given index
# + index - Index of the entry
# + return - term of the entry
function getTerm(int index) returns int {
    if (index < 1 || index >= lengthof log) {
        return 0;
    }
    else {
        return log[index].term;
    }
}
