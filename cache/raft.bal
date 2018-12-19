import ballerina/io;
import ballerina/runtime;
import ballerina/task;
import ballerina/math;
import ballerina/grpc;
import ballerina/log;
import ballerina/config;
import ballerina/http;

# Current node IP
string currentNode = config:getAsString("raft.ip") + ":" + config:getAsString("raft.port");

http:Client raftEndpoint = new (currentNode);

# Contains http clients raft uses
map<Node> raftClientMap={};

int MIN_ELECTION_TIMEOUT = config:getAsInt("raft.min.election.timeout", default = 2000);
int MAX_ELECTION_TIMEOUT = config:getAsInt("raft.max.election.timeout", default = 2500);
int HEARTBEAT_TIMEOUT = config:getAsInt("raft.heartbeat.timeout", default = 1000);

# Ones heartbeat is recieved each nodes keeps last known leader in this variable
string leader="";

# The state this server is currently in, can be FOLLOWER, CANDIDATE, or LEADER
string state = "Follower";

# This is the term this Raft server is currently in
int currentTerm=0;

# The log is a list of {term, command}, where the command is an opaque
# value which only holds meaning to the replicated state machine running on
# top of Raft.
LogEntry[] log = [{term:0,command:""}];

# This is the Raft peer that this server has voted for in *this* term (if any)
string votedFor = "None";

# The Raft entries up to and including this index are considered committed by
# Raft, meaning they will not change, and can safely be applied to the state
# machine.
int commitIndex = 0;

# The last command in the log to be applied to the state machine.
int lastApplied = 0;
task:Timer? electionTimer;
task:Timer? heartbeatTimer;
map<int> candVoteLog={};

# nextIndex is a guess as to how much of our log (as leader) matches that of
# each other peer. This is used to determine what entries to send to each peer
# next.
map<int> nextIndex={};

# matchIndex is a measurement of how much of our log (as leader) we know to be
# replicated at each other server.
map<int> matchIndex={};

# raftReadyChannel helps to notify the application ones raft is ready
channel<boolean> raftReadyChan = new;

# Suspect node
# +clientEndpoint - Http client of the suspected node
# +ip - ip of the client node
# +suspectRate - Suspect rate of the node. -50 = Recovered , 100 = Dead
type SuspectNode record {
    http:Client clientEndpoint;
    string ip;
    int suspectRate;
};

# Suspect weight
int SUSPECT_VALUE = config:getAsInt("failure.detector.suspect.value", default = 10);

# Timeout for failure detector
int FAILURE_TIMEOUT_MILS = config:getAsInt("failure.detector.timeout.millis", default = 1000);

# Suspect node list
map<SuspectNode> suspectNodes={};

public function startRaft() {
    //add current node in to log
    log[log.length()] = { term: 1, command: "NA " + currentNode };
    apply("NA " + currentNode);
    commitIndex = commitIndex + 1;
    lastApplied = lastApplied + 1;
    nextIndex[currentNode] = 1;
    matchIndex[currentNode] = 0;
    int interval = math:randomInRange(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT); //random election timeouts to prevent split vote
    (function () returns error?) onTriggerFunction = electLeader; //election timer trigger
    function (error) onErrorFunction = timerError;
    electionTimer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval);
    electionTimer.start();

    boolean ready;
    ready =<- raftReadyChan; //signals raft is ready
}

public function joinRaft() {
    nextIndex[currentNode] = 1;
    matchIndex[currentNode] = 0;
    int interval = math:randomInRange(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT); //random election timeouts to prevent split vote
    (function () returns error?) onTriggerFunction = electLeader;
    function (error) onErrorFunction = timerError;
    electionTimer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval, delay = interval);
    electionTimer.start();
    boolean ready;
    ready =<- raftReadyChan; //signals raft is ready
}

function electLeader() returns error?{
    //if not leader return
    if (state == "Leader") {
        //timer.stop();
        return;
    }
    log:printInfo("Starting Leader Election by " + currentNode);
    currentTerm = currentTerm + 1;
    int electionTerm = currentTerm;
    votedFor = currentNode;
    state = "Candidate";
    VoteRequest req = { term: currentTerm, candidateID: currentNode, lastLogIndex: (log.length()) - 1, lastLogTerm: log[(
        log.length()) - 1].term };
    future<int> voteResp = start sendVoteRequests(untaint req);
    int voteCount = wait voteResp; //TODO sync this
    //check if another appendEntry came while waitting for vote responses
    if (currentTerm != electionTerm) {
        log:printInfo ("Term changed while waitting for vote responses. Returning");
        return;
    }
    int quoram = <int>math:ceil(raftClientMap.length() / 2.0);
    log:printInfo(voteCount + " out of " + raftClientMap.length());
    //0 for first node
    if (voteCount < quoram) {
        state = "Follower";
        votedFor = "None";
        //heartbeatTimer.stop();
    } else {
        state = "Leader";
        leader = currentNode;
        //timer.stop();
        foreach var (index,node) in raftClientMap {
            nextIndex[node.ip] = log.length();
        }
        startHeartbeatTimer();
        startProcessingSuspects();
    }
    resetElectionTimer();
    log:printInfo(currentNode + " is a " + state);
    //TODO revist channels
    true -> raftReadyChan; // signal raft is ready
    return ();
}

function sendVoteRequests(VoteRequest req) returns int {
    //votes for itself
    future<()>[] futureVotes=[];
    foreach var (index,node) in raftClientMap {
        if (node.ip == currentNode) {
            continue;
        }
        candVoteLog[node.ip] = -1;
        //sends async vote requests
        future<()> asyncRes = start sendVoteRequestToSeperateNode(node, req);
        futureVotes[futureVotes.length()] = asyncRes;
        //ignore current Node
    }
    foreach var i in futureVotes { //change this in to quoram
        //waits for vote requests
        _ = wait i;
    }
    int count = 1;
    foreach var (index,item) in candVoteLog {
        if (item == 1) {
            //increment votes
            count = count + 1;
        }
        if (item == -2) {
            //a node has higher term, stepdown
            candVoteLog.clear();
            return 0;
        }
    }
    candVoteLog.clear();
    return count;
}

function sendVoteRequestToSeperateNode(Node node, VoteRequest req) {
    json|error requestJSON = json.convert(req);
    if (requestJSON is json){
        raftEndpoint = node.nodeEndpoint;
        var unionResp = raftEndpoint->post("/raft/vote", requestJSON);
        if (unionResp is http:Response){
            VoteResponse|error result = VoteResponse.convert (unionResp.getJsonPayload());
            if (result is VoteResponse){
                if (result.term > currentTerm) {
                    //target node has higher term. stop election
                    candVoteLog[node.ip] = -2; // to signal //TODO const
                    return;
                    //stepdown
                }
                if (result.granted) {
                    //if vote granted
                    candVoteLog[node.ip] = 1;
                }
                else {
                    candVoteLog[node.ip] = 0;
                }
            }else {
                log:printError("Invalid JSON", err = result);
            }
        }else {
                log:printError("Voted Request failed: ",err=unionResp);
                candVoteLog[node.ip] = 0;
        }
    }else {

    }
}

function sendHeartbeats() returns error?{
    //return if node is not leader
    if (state != "Leader") {
        return;
    }
    future <()> [] heartbeatAsync=[];
    foreach var (index,node) in raftClientMap {
        if (node.ip == currentNode) {
            continue;
        }
        //sends heartbeats async
        future<()> asy = start heartbeatChannel(node);
        heartbeatAsync[heartbeatAsync.length()] = asy;
    }
    foreach var item in heartbeatAsync {
        //wait for heartbeat responses
        _ = wait item;
    }
    //start committing entries
    commitEntry();
    return ();
}

function heartbeatChannel(Node node) {
    if (state != "Leader") {
        return;
    }
    string peer = node.ip;
    int nextIndexOfPeer = nextIndex[peer] ?: 0; //Next index to be sent to the peer
    int prevLogIndex = nextIndexOfPeer - 1; //Last Index that needs to be sent to their peer
    int prevLogTerm = 0;
    if (prevLogIndex > 0) {
        prevLogTerm = log[prevLogIndex].term; //last term that needs to be sent to their peer
    }
    LogEntry[] entryList=[];
    foreach var i in prevLogIndex...log.length() - 1 {
        entryList[entryList.length()] = log[i]; //non replicated entry list empty in a healthy heartbeat
    }
    AppendEntries appendEntry = {
        term: currentTerm,
        leaderID: currentNode,
        prevLogIndex: prevLogIndex,
        prevLogTerm: log[prevLogIndex].term,
        entries: entryList,
        leaderCommit: commitIndex
    };
    json|error entryJSON = json.convert(appendEntry);
    if (entryJSON is json){
        raftEndpoint = untaint node.nodeEndpoint;
        var heartbeatResp = raftEndpoint->post("/raft/append", untaint entryJSON);
        if (heartbeatResp is http:Response){
            AppendEntriesResponse|error result = AppendEntriesResponse.convert(heartbeatResp.getJsonPayload());
            if (result is AppendEntriesResponse){
                if (result.sucess) { //if node's log is on par with leaders log
                    matchIndex[peer] = result.followerMatchIndex;
                    nextIndex[peer] = result.followerMatchIndex + 1; //atomicc
                } else {//if node log is behind with leaders log
                    nextIndex[peer] = max(1, nextIndexOfPeer - 1);
                    log:printInfo("Catching up the node with leader");
                    heartbeatChannel(node);
                }
            }
        }else {
            log:printError("Heartbeat failed: ", err = heartbeatResp);
            //begin to suspect
            boolean found = false;
            //check if already a suspect
            foreach var (index,suspect) in suspectNodes {
                if (suspect.ip == node.ip) {
                    found = true;
                }
            }
            if (!found) {
                http:ClientEndpointConfig cc = {timeoutMillis: 60000 };
                createSuspectNode(node,cc);
                boolean commited = clientRequest("NSA " + node.ip); //commit node as suspected
                // cant commit here, if doesnt hv majority wut to do
                log:printInfo(node.ip + " added to suspect list");
                //commited?
            }
        }
    }
}
function createSuspectNode(Node node,http:ClientEndpointConfig cc) {
    http:Client newClient= new (node.ip,config=cc);
    SuspectNode sNode = { ip: node.ip, clientEndpoint: newClient, suspectRate: 0 };
    suspectNodes[node.ip] = sNode;
}

# Starts processing existing suspects once a new leader is elected
function startProcessingSuspects() {
    foreach var (index,suspect) in suspectNodes {
        _ = start checkSuspectedNode(suspect);
    }
}

# Check a suspected node by sending indirect requests periodically.
# + node- http client of the suspected node
function checkSuspectedNode(SuspectNode node) {
    //TODO maybe backoff factor
    if (state != "Leader") {
        return;
    }
    Node healthyNode = getHealthyNode();
    log:printInfo("Healthy Node :" + healthyNode.ip);
    json req = { ip: node.ip };
    //change
    //increase timeout
    raftEndpoint = healthyNode.nodeEndpoint;
    var resp = raftEndpoint->post("/raft/indirect/", req);
    if (resp is http:Response){//TODO revisit
        var jsonPayload = IndirectResponse.convert(resp.getJsonPayload());
        if (jsonPayload is IndirectResponse){
            log:printInfo("Suspect rate of " + node.ip + " : " + node.suspectRate);
            boolean status = jsonPayload.status;
            if (status) {
                boolean relocate = jsonPayload.relocate;
                if (!relocate) {
                    //not relocating just slow lol or up noww !
                    node.suspectRate = node.suspectRate - SUSPECT_VALUE;
                    if (node.suspectRate <= -50) {
                        //commit remove from suspect
                        boolean commited = clientRequest("NSR " + node.ip);
                        log:printInfo(node.ip + " Recovred from suspection " + commited);
                        //??commited
                        return;
                    }
                }
            } else {
                //not responding
                node.suspectRate = node.suspectRate + SUSPECT_VALUE;
                if (node.suspectRate >= 100) {
                    //commit dead
                    boolean commited = clientRequest("NR " + node.ip);
                    log:printInfo(node.ip + " Removed from the cluster");
                    return;
                }
            }
            runtime:sleep(FAILURE_TIMEOUT_MILS);
            checkSuspectedNode(node);
        } else {
            log:printError("Invalid Response " ,err = jsonPayload);
        }
    }else {
            //if healthy node didnt respond //could be  be coz of packet loss
            log:printError("Healthy Node didn't respond: " ,err = resp );
            //this codeblock should be removed after increasing timeouts. for now since both timeouts in current n healthy nodes r same failed request will timeout
            //
            node.suspectRate = node.suspectRate + SUSPECT_VALUE;
            if (node.suspectRate >= 100) {
                //commit dead
                boolean commited = clientRequest("NR " + node.ip);
                log:printInfo(node.ip + " Removed from the cluster");
                return;
            }
            //
            runtime:sleep(FAILURE_TIMEOUT_MILS);
            checkSuspectedNode(node);
    }
}

# Gives a healthy ndoe in the cluster for indirect RPCs
# +return - Retruns a healthy node or current node if no other healthy nodes available
function getHealthyNode() returns Node {
    Node healthyNode={ip:currentNode,nodeEndpoint:raftEndpoint}; //TODO Revisit
    foreach var (index,node) in raftClientMap {
        //skip current node first
        if (node.ip == currentNode) {
            continue;
        }
        boolean inSuspect = false;
        foreach var (suspectIndex,suspectNode) in suspectNodes {
            if (node.ip == suspectNode.ip) {
                inSuspect = true;
            }
        }
        if (!inSuspect) {
            healthyNode = node;
        }
    }
    //if none return current Node
    foreach var (index,node) in raftClientMap {
        if (node.ip == currentNode) {
            healthyNode = node;
        }
    }
    return healthyNode;
}

# Commits entries of leaders log
function commitEntry() {
    if (state != "Leader") {
        return;
    }
    int item = log.length() - 1;
    while (item > commitIndex) {
        int replicatedCount = 1; //replicated node count
        foreach var (index,server) in raftClientMap {
            if (server.ip == currentNode) {
                continue;
            }
            if (matchIndex[server.ip] == item) {
                replicatedCount = replicatedCount + 1;
            }
        }
        //if entry is replicated to a majority of nodes commit
        if (replicatedCount >= math:ceil(raftClientMap.length() / 2.0)) {
            commitIndex = item;
            apply(log[item].command);
            //To Reduce multiple relocation
            if (log[item].command.substring(0, 2) == "NA" || log[item].command.substring(0, 2) == "NR") {
                relocateData();
            }
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

function resetElectionTimer() {
    int interval = math:randomInRange(MIN_ELECTION_TIMEOUT, MAX_ELECTION_TIMEOUT);
    lock {
        electionTimer.stop();
        (function () returns error?) onTriggerFunction = electLeader;

        function (error) onErrorFunction = timerError;
        electionTimer = new task:Timer(onTriggerFunction, onErrorFunction,
            interval);
        electionTimer.start();
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
    electionTimer = new task:Timer(onTriggerFunction, onErrorFunction,
        interval, delay = interval);
    electionTimer.start();
}

//TODO linearizable semantics
//TODO Timeout if not committed
function clientRequest(string command) returns boolean {
    if (state == "Leader") {
        int entryIndex = log.length();
        log[entryIndex] = { term: currentTerm, command: command };
        future<error?> ee = start sendHeartbeats();
        _ = wait ee;
        //without majority no nop :S
        //check if commited
        if (commitIndex >= entryIndex) {
            return true;
        } else {
            return false;
        }
    } else {
        return false;
    }
}

# Requests to add a node to the cluster
# +ip - IP of the new node
# +return - member join status (sucess or not) , last known leader
function addNode(string ip) returns ClientResponse {
    if (state != "Leader") {
        return { sucess: false, leaderHint: leader };
    } else {
        foreach var (index,item) in raftClientMap { // temp. check heartbeat commiting agian
            if (item.ip == ip) {
                return { sucess: false, leaderHint: leader };
            }
        }
        string command = "NA " + ip;
        //or commit
        boolean sucess = clientRequest(command);
        return { sucess: sucess, leaderHint: leader };
    }
}

# Applies a certain command to the state machine
function apply(string command) {
    if (command.substring(0, 2) == "NA") { //NODE ADD
        string ip = command.split(" ")[1];
        foreach var (index,item) in raftClientMap { // temp. check heartbeat commiting agian
            if (item.ip == ip) {
                return;
            }
        }
        http:ClientEndpointConfig cc = {
            timeoutMillis: MIN_ELECTION_TIMEOUT / 3,
            retryConfig: {
                interval: 20,
                count: 1,
                backOffFactor: 1.0,
                maxWaitInterval: HEARTBEAT_TIMEOUT / 3
            }
        };
        //http:Client raftClient= new (ip,config=cc);
        Node raftNode = {ip:ip,nodeEndpoint:createHttpClient(ip,cc)};
        raftClientMap[ip] = raftNode;

        http:ClientEndpointConfig cacheClientCfg = {
            timeoutMillis: config:getAsInt("cache.request.timeout", default = 2000),
            retryConfig: {
                interval: config:getAsInt("cache.request.timeout", default = 2000)/2,
                count: 1,
                backOffFactor: 1.0,
                maxWaitInterval: 5000
            }
        };
        //http:Client cacheClient =new (ip,config=cacheClientCfg);
        Node cacheNode = {ip:ip,nodeEndpoint:createHttpClient(ip,cacheClientCfg)};
        cacheClientMap[ip]=cacheNode;

        http:ClientEndpointConfig relocationConfig = {
            timeoutMillis: config:getAsInt("cache.relocation.timeout", default = 10000)
        };
        //http:Client relocationClient = new (ip,config=relocationConfig);
        Node relocationNode = {ip:ip,nodeEndpoint:createHttpClient(ip,relocationConfig)};
        relocationClientMap[ip] = relocationNode;

        nextIndex[ip] = 1;
        matchIndex[ip] = 0;
        hashRing.add(ip);
    }

    if (command.substring(0, 3) == "NSA") { //NODE SUSPECT Add
        string ip = command.split(" ")[1];
        http:ClientEndpointConfig cc = { timeoutMillis: 60000 };
        SuspectNode node = { ip: ip, clientEndpoint: createHttpClient(ip,cc), suspectRate: 0 };
        suspectNodes[ip] = node;
        _ = start checkSuspectedNode(node);
        printSuspectedNodes();
    }

    if (command.substring(0, 3) == "NSR") { //NODE SUSPECT Remove
        string ip = command.split(" ")[1];
        _ = suspectNodes.remove(ip);
        printSuspectedNodes();
    }

    if (command.substring(0, 2) == "NR") { //NODE Remove
        string ip = command.split(" ")[1];
        boolean sucess = clientRequest("NSR " + ip);
        if (sucess) {
            _ = raftClientMap.remove(ip);
            _ = cacheClientMap.remove(ip);
            _ = relocationClientMap.remove(ip);
            hashRing.removeNode(ip); //Todo Check remove nodes
            printSuspectedNodes();
            printClientNodes();
        }
    }
    log:printInfo(command + " Applied!!");
}

function createHttpClient(string ip,http:ClientEndpointConfig config) returns http:Client{
    http:Client newRaftClient= new (ip,config=config);
    return newRaftClient;
}

# Prints all the nodes in raft
# Debug only
function printClientNodes() {
    io:println("Client map list");
    foreach var (index,node) in raftClientMap {
        io:println(node.ip);
    }
}

# Prints all the suspected nodes
# Debug only
function printSuspectedNodes() {
    io:println("Suspected node list");
    foreach var (index,node) in suspectNodes {
        io:println(node.ip);
    }
}
