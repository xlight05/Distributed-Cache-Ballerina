import ballerina/time;
import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;

//TODO Ensure node wont go out of memory.
//TODO Healthchecks for accurate config values  | Packet loss -> higher retries , higher timeout |
//TODO Better service discovery
//TODO Choose a better hashing algo. less collutions, high speed 
http:Client nodeClientEndpoint = new ("http://localhost:" + config:getAsString("cache.port", default = "7000"));

# Represents a node in the cluster
# + ip - IP of the node
# + nodeEndpoint - Http client of the node
type Node record {
    string ip;
    http:Client nodeEndpoint;
    !...
};

# Represents a cache entry.
# + value - cache value
# + key - key of the entry
# + lastAccessedTime - last accessed time in ms of this value which is used to remove LRU cached values
# + replica - checks if a record is a replica
# + cacheName - Cache
# + expiryTimeMillis - Max time limit of the entry
type CacheEntry record {
    string cacheName;
    anydata value;
    string key;
    int lastAccessedTime;
    boolean replica;
    int expiryTimeMillis;
    !...
};

# Map which stores all of the caches.
map<Cache> cacheMap={};
string currentIP = config:getAsString("cache.ip", default = "http://localhost");
int currentPort = config:getAsInt("cache.port", default = 7000);
int replicationFact = config:getAsInt("cache.replication.fact", default = 1);
float cacheEvictionFactor = config:getAsFloat("cache.eviction.factor", default = 0.25);
int cacheCapacity = config:getAsInt("cache.capacity", default = 100000);
boolean isLocalCacheEnabled = config:getAsBoolean("cache.local.cache", default = false);
boolean isRelocationOrEvictionRunning = false;
map <Node> cacheClientMap={};
map <Node> relocationClientMap={};

public function initNodeConfig(){
    //Suggestion rest para array suppot for config API
    string hosts = config:getAsString("cache.hosts");
    string[] configNodeList = hosts.split(",");
    if (configNodeList[0] == "") {
        createCluster();
    } else {
        joinCluster(configNodeList);
    }
}

# Allows uesrs to create the cluster
function createCluster() {
    startRaft();
}

//# Allows uesrs to join the cluster
//# + nodesInCfg - ips of the nodes in the cluster
//public function joinCluster(string[] nodesInCfg) {
//    //TODO parral
//    //sends join request to all nodes specified in config
//    foreach node in nodesInCfg {
//        http:Client client;
//        http:ClientEndpointConfig cfg = { url: node };
//        client.init(cfg);
//        nodeEndpoint = client;
//        var serverResponse = nodeEndpoint->post("/raft/server", currentNode);
//        match serverResponse {
//            http:Response payload => {
//                ClientResponse result = check <ClientResponse>check payload.getJsonPayload();
//                //if node is the leader join the cluster
//                if (result.sucess) {
//                    joinRaft();
//                    return;
//                } else {
//                    log:printInfo("No " + node + " is not the leader");
//                    string[] leaderIP;
//                    //if node is not the leader it will send last known leader as the hint
//                    leaderIP[0] = result.leaderHint;
//                    if (leaderIP[0] == "") {
//                        continue;
//                    }
//                    joinCluster(leaderIP);
//                }
//            }
//            error err => {
//                log:printInfo("Node didn't Respond");
//                continue;
//            }
//        }
//    }
//    //wait few seconds and retry
//    runtime:sleep(1000);
//    joinCluster(nodesInCfg);
//}

# Allows uesrs to join the cluster
# + nodesInCfg - ips of the nodes in the cluster
public function joinCluster(string[] nodesInCfg) {
    //TODO parral
    //sends join request to all nodes specified in config
    foreach var node in nodesInCfg {
        nodeClientEndpoint.__init(node);
        var serverResponse = nodeClientEndpoint->post("/raft/server", currentNode);
        if (serverResponse is http:Response){
            ClientResponse|error result = ClientResponse.convert(serverResponse.getJsonPayload());
            if (result is ClientResponse){
                //if node is the leader join the cluster
                if (result.sucess) {
                    joinRaft();
                    return;
                } else {
                    log:printInfo("No " + node + " is not the leader");
                    if (result.leaderHint != ""){
                        callLeaderHint(result.leaderHint);
                    }
                }
            }else{
                log:printError("Invalid JSON ",err=result);
            }
        }else {
            log:printError("Node didn't Respond",err=serverResponse);
            continue;
        }
    }
    //wait few seconds and retry
    runtime:sleep(1000);
    joinCluster(nodesInCfg);
}

function callLeaderHint (string leaderHint) {
    nodeClientEndpoint.__init(leaderHint);
    var serverResponse = nodeClientEndpoint->post("/raft/server", currentNode);
    if (serverResponse is http:Response){
        ClientResponse|error result = ClientResponse.convert(serverResponse.getJsonPayload());
            if (result is ClientResponse){
                //if node is the leader join the cluster
            if (result.sucess) {
                joinRaft();
                return;
            } else {
                log:printInfo("No " + leaderHint + " is not the leader");
                if (result.leaderHint == "") {
                    return;
                }
                callLeaderHint(result.leaderHint);
            }
        }else {
            log:printError("Invalid JSON ",err=result);
        }
    }else {
        log:printError("Node didn't Respond",err=serverResponse);
        return;
    }
}


# Allows users to create a cache object
# + name - name of the cache object
public function createCache(string name) {
    cacheMap[name] = new Cache(name);
    log:printInfo("Cache Created " + name);
}

# Allows users to create a cache object
# + name - name of the cache object
# + return - Cache object associated with the given name
public function getCache(string name) returns Cache? {
    foreach var (ip,node) in cacheClientMap {
        //changing the url of client endpoint
        nodeClientEndpoint =untaint node.nodeEndpoint;
        var response = nodeClientEndpoint->get("/cache/" + name);
        if (response is http:Response){
            var cacheJson = response.getJsonPayload();
            if (cacheJson is json){
                //if Cache object found in the node
                if (response.statusCode!=204) {
                    Cache cacheObj = new("temp"); //TODO Fix casting
                    cacheObj.name = cacheJson.name.toString();
                    cacheObj.expiryTimeMillis = <int>cacheJson.expiryTimeMillis;
                    LocalCacheConfig|error cfg = LocalCacheConfig.convert(cacheJson.LocalCacheConfig);
                    if (cfg is LocalCacheConfig){
                        cacheObj.nearCache.config = cfg;
                    }
                    //store object locally
                    return cacheObj;
                } else {
                    log:printWarn(name + " Cache not found- in " + node.ip);
                }
            }else if (cacheJson is error){
                log:printError("Error parsing JSON", err = cacheJson);
            }
        }else {
            log:printError("Server response not recieved", err = response);
        }
    }
    return ();
}

type CacheConfig record {
    int replicationFactor;
    int capacity;
    int maxEntrySize;
};

# Represents a cache.
public type Cache object {
    string name="";
    int expiryTimeMillis=60000;
    //TODO add time eviction support
    LocalCache nearCache = new (expiryTimeMillis,config:getAsInt("local.cache.capacity"),config:getAsFloat("local.cache.eviction.factor")); 

    public function __init (string name, int expiryTimeMillis = 60000) {
        Cache? cache = cacheMap[name];
        if (cache is Cache){
            self = cache;
            log:printInfo("Cache Found- " + self.name);
            return;
        }else {
            Cache? remoteCache = getCache(name);
            if (remoteCache is Cache){
                    self = remoteCache;
                    cacheMap[name] = remoteCache;
                    log:printInfo("Cache Found- " + self.name);
            }else {
                    cacheMap[name] = self;
                    self.name=name;
                    self.expiryTimeMillis=expiryTimeMillis;
                    //TODO local cache config
                    log:printInfo("Cache Created " + self.name);
                    return;
            }
        }
    }

    # Adds the given key, value pair to the provided cache.It will be stored in a appropirate node in the cluster
    # + key - value which should be used as the key
    # + value - value to be cached
    public function put(string key, anydata value) {
        //Adding in to nearCache for quick retrival
        if (isLocalCacheEnabled) {
            self.nearCache.put(key, value);
        }
        //gets the assigned node ip from the ring
        string originalEntryNode = hashRing.get(key);
        int currentTime = time:currentTime().time;
        CacheEntry entry = { value: value, key: key, lastAccessedTime: currentTime, cacheName:
        self.name, replica: false, expiryTimeMillis: self.expiryTimeMillis };
        json|error entryJSON = json.convert(entry);
        if (entryJSON is json){
            Node? clientNode = cacheClientMap[originalEntryNode];
            if (clientNode is Node){
                nodeClientEndpoint = clientNode.nodeEndpoint;
                var res = nodeClientEndpoint->post("/cache/entries/"+key, entryJSON);
                if (res is http:Response){
                    var msg = res.getJsonPayload();
                    if (msg is json){
                        log:printInfo("'" + msg["key"].toString() + "' added");
                    }else {
                        log:printError("error json convert", err = msg);
                    }
                }else {
                    log:printError("Put request failed", err = res);
                }
                _ = start putEntriesInToReplicas(entryJSON, key, originalEntryNode);
        }else {
            log:printError("Client not found");
        }
        }else {
            log:printError("Error parsing JSON", err = entryJSON);
        }
    }

    # Returns the cached value associated with the given key. If the provided cache key is not found in the cluster, () will be returned.
    # + key - key which is used to retrieve the cached value
    # + return  -The cached value associated with the given key
    public function get(string key) returns anydata? {
        if (isLocalCacheEnabled) {
            if (self.nearCache.hasKey(key)) {
                log:printInfo(key + " retrived by local Cache");
                //sends async request to update last accessed time of distributed cache
                _ = start updateLastAccessedTime(key);
                //TODO update local cahe maybe
                return self.nearCache.get(key);
            }
        }
        string nodeIP = hashRing.get(key);
        //Cache key  made from replica status,cache name and actual key
        //O - Original R - Replica
        string originalKey = "O:" + self.name + ":" + key;
        var remoteEntry = getEntryFromServer(nodeIP, originalKey);
        if (remoteEntry is json){
            if (remoteEntry.value != null) {
                //CacheEntry entry = CacheEntry.convert(remoteEntry);
                CacheEntry|error entry = CacheEntry.convert(remoteEntry);
                if (entry is CacheEntry){
                    self.nearCache.put(key, entry.value); //add to near cache for quick retrival
                    //log:printInfo("Entry found '" + key + "'");
                return entry.value;
                }else {
                    return ();
                }
            }
            else {
                log:printWarn("Entry not found '" + key + "'");
                //returning is importent because  replicas might have consitency issues.
                return ();
            }
        }else {
            log:printError("Original Server couldn't connect", err = remoteEntry);
            string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
            string replicaKey = "R:" + self.name + ":" + key;
            future<json|error>[] replicaNodeFutures=[];
            //sends to both replicas async. this should be updated in to first come basis.
            foreach var node in replicaNodes {
                if (node == nodeIP) {
                    continue;
                }
                replicaNodeFutures[replicaNodeFutures.length()] = start getEntryFromServer(node, replicaKey);
            }
            foreach var replicaFuture in replicaNodeFutures {
                json|error response = wait replicaFuture;
                if (response is json){
                    if (response.value != null) {
                        CacheEntry|error replicaEntry = CacheEntry.convert(response);
                        if (replicaEntry is CacheEntry){
                            //log:printInfo("Entry found in replica '" + key + "'");
                            return replicaEntry.value;
                        }else {
                            log:printWarn("Entry not valid '" + key + "'");
                        }
                    }
                    else {
                        log:printWarn("Entry not found '" + key + "'");
                    }
                }else {
                        log:printError("GET replica failed.", err = response);
                }
            }
        }
        log:printWarn("Entry not found '" + key + "'");
        return ();
    }

    # Returns the cached value associated with the given key. If the provided cache key is not found in the cluster, () will be returned.
    # + key - key which is used to remove the entry
    //TODO Fix remove
    public function remove(string key) {
        //Adding in to nearCache for quick retrival
        if (isLocalCacheEnabled) {
            self.nearCache.remove(key);
        }
        string nodeIP = hashRing.get(key);
        json entryJSON = { "key": key };
        Node? clientNode = cacheClientMap[nodeIP];
        if (clientNode is Node){
            nodeClientEndpoint = clientNode.nodeEndpoint;
            var res = nodeClientEndpoint->delete("/cache/entries"+key, entryJSON);
            if (res is http:Response){
                json|error msg = res.getJsonPayload();
                if (msg is json){
                    log:printInfo("Cache entry remove " + msg["status"].toString());
                }else {
                    log:printError("error json convert", err = msg);
                }
            }else {
                //TODO Queue here
                log:printError("error removing from node", err = res);
            }
            //remove entries in replicas asnyc
            _ = start removeReplicas(key, nodeIP);
        }else {
            log:printError("Client not found");
        }
    }
};

function putEntriesInToReplicas(json entryJSON, string key, string originalTarget) {
    entryJSON["replica"] = true;
    //gets replica nodes
    string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
    foreach var node in replicaNodes {
        Node? replicaNode = cacheClientMap[node];
        if (replicaNode is Node){
            nodeClientEndpoint = replicaNode.nodeEndpoint;
                var response = nodeClientEndpoint->post("/cache/entries"+key, entryJSON);
                if (response is http:Response){
                    var msg = response.getJsonPayload();
                    if (msg is json){
                                                        //silent in sucess case
                            //log:printInfo("'" + jsonPayload["key"].toString() + "' replica added to node " + node);
                    }else {
                            log:printError("Not a valid JSON", err = msg);
                    }
                }else {
                    log:printError("Put replica request failed", err = response);
                }
        }else {
                log:printError("Client not found");
        }
    }
}

# Updates last accessed time of certain entry
# + key - key of the cache entry that needs to be updated
function updateLastAccessedTime(string key) {
    //Sending a get to replicas updates their last accessed time.
    string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
    replicaNodes[replicaNodes.length()] = hashRing.get(key);
    foreach var node in replicaNodes {
        _ = start getEntryFromServer(node, key);
    }
}

# Gets cache entries from target nodes.
# + ip - target Node ip
# + key - key of cache entry
# + return - entry if request succeed, error if failed
function getEntryFromServer(string ip, string key) returns json|error {
    Node? replicaNode = cacheClientMap[ip];
    if (replicaNode is Node){
        nodeClientEndpoint = replicaNode.nodeEndpoint;
        var res = nodeClientEndpoint->get("/cache/entries/" + key);
        if (res is http:Response){
            var msg = res.getJsonPayload();
            if (msg is json){
                return msg;
            }else {
                log:printError("Invalid JSON ", err = msg);
                return msg;
            }
        }else {
            log:printError("Server did not respond", err = res);
            return res;
        }
    }else {
        log:printError("Client not found");
        error err = error("Client not found");
        return err;
    }
}
# Debug functions to see which node a cerain key is located in
# + key - key of the entry that needs to be located
public function locateNode(string key) {
    io:println(key + " located in ");
    io:println(hashRing.get(key));
}
# Debug functions to see which nodes a cerain replica key is located in
# + key - key of the entry that needs to be located
public function locateReplicas(string key) {
    io:println(key + " replica located in ");
    string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
    foreach var item in replicaNodes {
        io:println(item);
    }
    io:println();
}


