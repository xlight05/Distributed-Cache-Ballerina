import ballerina/grpc;
import ballerina/io;
import ballerina/config;
import ballerina/log;
import ballerina/http;

listener http:Listener cacheListner = new(config:getAsInt("cache.port", defaultValue = 7000));

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

type IndirectResponse record {
    boolean status;
    boolean relocate;
};

@http:ServiceConfig { basePath: "/raft" }
service raft on cacheListner {
    @http:ResourceConfig {
        methods: ["POST"],
        path: "/vote"
    }
    resource function voteResponseRPC(http:Caller caller, http:Request request) {
        json|error jsonPayload = request.getJsonPayload();
        http:Response response = new;
        if (jsonPayload is json){
            VoteRequest|error voteReq = VoteRequest.convert(jsonPayload);
            if (voteReq is VoteRequest){
                log:printInfo("Vote request came from " + voteReq.candidateID);
                boolean granted = voteResponseHandle(voteReq);
                VoteResponse voteResponse = { granted: granted, term: currentTerm };
                log:printInfo("Vote status for " + voteReq.candidateID + " is " + voteResponse.granted);
                json|error responseJSON = json.convert(voteResponse);
                if (responseJSON is json){
                    response.setJsonPayload(responseJSON);
                }else {
                    log:printError("Error converting vote to json",err=responseJSON);
                    response.setJsonPayload ({ granted: false, term: currentTerm });   
                }
            }else {
                log:printError("Error converting json to vote request",err=voteReq);
                response.setJsonPayload ({ granted: false, term: currentTerm });
            }
        }else {
            log:printError("Error parsing JSON ",err=jsonPayload);
            response.setJsonPayload ({ granted: false, term: currentTerm });
        }
        var result =caller->respond(response);
        if (result is error){
            log:printError("Error in responding to vote request",err=result);
        }
    }

    @http:ResourceConfig {
        methods: ["POST"],
        path: "/append"
    }
    resource function appendEntriesRPC(http:Caller caller, http:Request request) {
        json|error jsonPayload = json.convert(request.getJsonPayload());
        http:Response response = new;
        if (jsonPayload is json){
            AppendEntries|error appendEntry = AppendEntries.convert(jsonPayload);
            if (appendEntry is AppendEntries){
                AppendEntriesResponse appendEntriesResponse = heartbeatHandle(appendEntry);
                json|error responseJSON = json.convert(appendEntriesResponse);
                if (responseJSON is json){
                    response.setJsonPayload(untaint responseJSON);
                }
            }else {
                response.setJsonPayload({ term: currentTerm, sucess: false,followerMatchIndex:0 });
            }
        }else {
            response.setJsonPayload({ term: currentTerm, sucess: false,followerMatchIndex:0 });
        }
        var result =caller->respond(response);
        if (result is error){
            log:printError("Error in responding to append request", err = result);
        }
    }

    @http:ResourceConfig {
        methods: ["POST"],
        path: "/server"
    }
    resource function addServerRPC(http:Caller caller, http:Request request) {
        string|error ip = request.getTextPayload();
        http:Response response = new;
        if (ip is string){
             ClientResponse configChangeResponse = addNode(ip);
             json|error responseJSON = json.convert(configChangeResponse);
             if (responseJSON is json){
                 response.setJsonPayload(responseJSON);
             }
        }else {
            response.setJsonPayload ({ sucess: false, leaderHint: leader });
        }
        var result =caller->respond(response);
        if (result is error){
            log:printError("Error in responding to add server", err = result);
        }
    }

    @http:ResourceConfig {
        methods: ["POST"],
        path: "/client"
    }
    resource function clientRequestRPC(http:Caller caller, http:Request request) {
        string|error command = request.getTextPayload();
        http:Response response = new;
        if (command is string){
            boolean sucess = clientRequest(command);
            response.setJsonPayload({ sucess: sucess, leaderHint: leader });
        }else {
            response.setJsonPayload({ sucess: false, leaderHint: leader });
        }
        var result =caller->respond(response);
        if (result is error){
            log:printError("Error in responding to client request", err = result);
        }
    }

    @http:ResourceConfig {
        methods: ["POST"],
        path: "/indirect"
    }
    resource function indirectRPC(http:Caller caller, http:Request request) {
        json|error reqq = request.getJsonPayload();
        json responsePayload={};
        if (reqq is json){
            string targetIP = reqq.ip.toString();
            Node? targetNode = raftClientMap[targetIP];
            if (targetNode is Node){
                raftEndpoint = targetNode.nodeEndpoint;
                var resp = raftEndpoint->get("/raft/fail/check/");
                if (resp is http:Response){
                    string|error result = resp.getTextPayload();
                    if (result is string){
                        boolean relocate;
                        if (result == "true") {
                            relocate = true;
                        } else {
                            relocate = false;
                        }
                        responsePayload = { "status": true, "relocate": relocate };
                    }else {
                        responsePayload = { "status": false, "relocate": false };
                    }
                }else {
                    responsePayload = { "status": false, "relocate": false };
                }
                log:printInfo("Indirect ping for " + targetNode.ip + " Server Status : " + responsePayload["status"].
                    toString());
            }else {
                responsePayload = { "status": false, "relocate": false };
            }
        }else {
            responsePayload = { "status": false, "relocate": false };
        }
        http:Response response = new;
        response.setJsonPayload(responsePayload);
        var result =caller->respond(response);
        if (result is error){
            log:printError("Error in responding to indirect ping", err = result);
        }
    }

    @http:ResourceConfig {
        methods: ["GET"],
        path: "/fail/check"
    }
    resource function failCheckRPC(http:Caller caller, http:Request request) {
        string res;
        if (isRelocationOrEvictionRunning) {
            res = "true";
        } else {
            res = "false";
        }
        http:Response response = new;
        response.setTextPayload(res);
        var result =caller->respond(response);
        if (result is error){
            log:printError("Error in responding to fail check RPC", err = result);
        }
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
        res = { term: currentTerm, sucess: false,followerMatchIndex:0 };
    } else {
        resetElectionTimer();
        leader = untaint appendEntry.leaderID;
        state = "Follower";
        boolean sucess = appendEntry.prevLogTerm == 0 || (appendEntry.prevLogIndex < log.length() && log[appendEntry.
                    prevLogIndex].term == appendEntry.prevLogTerm);
        int index = 0;
        //can parse entries in appendRPC
        if (sucess) {
            index = appendEntry.prevLogIndex;
            //make the log same as leaders log
            foreach var i in appendEntry.entries{
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
        foreach var i in lastApplied + 1...commitIndex {
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
    int ourLastLogIndex = (log.length()) - 1;
    int ourLastLogTerm = -1;
    if (log.length() != 0) {
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
    if (index < 1 || index >= log.length()) {
        return 0;
    }
    else {
        return log[index].term;
    }
}
