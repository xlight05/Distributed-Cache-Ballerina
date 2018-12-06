import ballerina/time;
import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;

//TODO Ensure node wont go out of memory.
//TODO Healthchecks for accurate config values  | Packet loss -> higher retries , higher timeout |
//TODO Better service discovery
//TODO Choose a better hashing algo. less collutions, high speed 
endpoint http:Client nodeEndpoint {
    url: "http://localhost:" + config:getAsString("cache.port", default = "7000")
};

# Represents a node in the cluster
# + ip - IP of the node
# + nodeEndpoint - Http client of the node
type Node record {
    string ip;
    http:Client nodeEndpoint;
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
    any value;
    string key;
    int lastAccessedTime;
    boolean replica;
    int expiryTimeMillis;
};

# Map which stores all of the caches.
map<Cache> cacheMap;
string currentIP = config:getAsString("cache.ip", default = "http://localhost");
int currentPort = config:getAsInt("cache.port", default = 7000);
int replicationFact = config:getAsInt("cache.replication.fact", default = 1);
float cacheEvictionFactor = config:getAsFloat("cache.eviction.factor", default = 0.25);
int cacheCapacity = config:getAsInt("cache.capacity", default = 100000);
boolean isLocalCacheEnabled = config:getAsBoolean("cache.local.cache", default = false);
boolean isRelocationOrEvictionRunning = false;
map <Node> cacheClientMap;
map <Node> relocationClientMap;

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
    foreach node in nodesInCfg {
        http:Client client;
        http:ClientEndpointConfig cfg = { url: node };
        client.init(cfg);
        nodeEndpoint = client;
        var serverResponse = nodeEndpoint->post("/raft/server", currentNode);
        match serverResponse {
            http:Response payload => {
                ClientResponse result = check <ClientResponse>check payload.getJsonPayload();
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
            }
            error err => {
                log:printInfo("Node didn't Respond");
                continue;
            }
        }
    }
    //wait few seconds and retry
    runtime:sleep(1000);
    joinCluster(nodesInCfg);
}

function callLeaderHint (string leaderHint) {
    http:Client client;
    http:ClientEndpointConfig cfg = { url: leaderHint };
    client.init(cfg);
    nodeEndpoint = client;
    var serverResponse = nodeEndpoint->post("/raft/server", currentNode);
    match serverResponse {
        http:Response payload => {
            ClientResponse result = check <ClientResponse>check payload.getJsonPayload();
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
        }
        error err => {
            log:printInfo("Node didn't Respond");
            return;
        }
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
    foreach node in cacheClientMap {
        nodeEndpoint = node.nodeEndpoint;
        //changing the url of client endpoint
        var response = nodeEndpoint->get("/cache/" + name);
        match response {
            http:Response resp => {
                var msg = resp.getJsonPayload();
                match msg {
                    json cacheJson => {
                        //if Cache object found in the node
                        if (resp.statusCode!=204) {
                            Cache cacheObj = check <Cache>cacheJson;
                            cacheMap[name] = cacheObj;
                            //store object locally
                            return cacheObj;
                        } else {
                            log:printWarn(name + " Cache not found- in " + node.ip);
                        }
                    }
                    error err => {
                        log:printError(err.message, err = err);
                    }
                }
            }
            error err => {
                log:printError(err.message, err = err);
            }
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
    string name;
    int expiryTimeMillis;
    //TODO add time eviction support
    LocalCache nearCache = new(name,capacity = config:getAsInt("local.cache.capacity", default = 100), evictionFactor =
        config:getAsFloat("local.cache.eviction.factor", default = 0.25),expiryTimeMillis = expiryTimeMillis); //maybe move from the object?

    public new(name, expiryTimeMillis = 60000) {
        var cacheObj = cacheMap[name];
        match cacheObj {
            Cache cache => {
                self = cache;
                log:printInfo("Cache Found- " + self.name);
                return;
            }
            () => {
                var cacheVar = getCache(name);
                match cacheVar {
                    Cache cache => {
                        self = cache;
                        log:printInfo("Cache Found- " + self.name);
                    }
                    () => {
                        cacheMap[name] = self;
                        log:printInfo("Cache Created " + name);
                        return;
                    }
                }
            }
        }
    }

    # Adds the given key, value pair to the provided cache.It will be stored in a appropirate node in the cluster
    # + key - value which should be used as the key
    # + value - value to be cached
    public function put(string key, any value) {
        //Adding in to nearCache for quick retrival
        if (isLocalCacheEnabled) {
            nearCache.put(key, value);
        }
        //gets the assigned node ip from the ring
        string originalEntryNode = hashRing.get(key);
        int currentTime = time:currentTime().time;
        CacheEntry entry = { value: value, key: key, lastAccessedTime: currentTime, createdTime: currentTime, cacheName:
        self.name, replica: false, expiryTimeMillis: self.expiryTimeMillis };
        json entryJSON = check <json>entry;
        Node? clientNode = cacheClientMap[originalEntryNode];
        match clientNode {
            Node client => {
                nodeEndpoint = client.nodeEndpoint;
                var res = nodeEndpoint->post("/cache/entries/"+key, entryJSON);
                match res {
                    http:Response resp => {
                        var msg = resp.getJsonPayload();
                        match msg {
                            json jsonPayload => {
                                log:printInfo("'" + jsonPayload["key"].toString() + "' added");
                            }
                            error err => {
                                log:printError("error json convert", err = err);
                            }
                        }
                    }
                    error err => {
                        log:printError("Put request failed", err = err);
                    }
                }
                _ = start putEntriesInToReplicas(entryJSON, key, originalEntryNode);
            }
            () => {
                log:printError("Client not found");
            }
        }
    }

    # Returns the cached value associated with the given key. If the provided cache key is not found in the cluster, () will be returned.
    # + key - key which is used to retrieve the cached value
    # + return  -The cached value associated with the given key
    public function get(string key) returns any? {
        if (isLocalCacheEnabled) {
            if (nearCache.hasKey(key)) {
                log:printInfo(key + " retrived by local Cache");
                //sends async request to update last accessed time of distributed cache
                _ = start updateLastAccessedTime(key);
                //TODO update local cahe maybe
                return nearCache.get(key);
            }
        }
        string nodeIP = hashRing.get(key);
        //Cache key  made from replica status,cache name and actual key
        //O - Original R - Replica
        string originalKey = "O:" + name + ":" + key;
        var msg = getEntryFromServer(nodeIP, originalKey);
        match msg {
            json jsonPayload => {
                if (jsonPayload.value != null) {
                    CacheEntry entry = check <CacheEntry>jsonPayload;
                    nearCache.put(key, entry.value); //add to near cache for quick retrival
                    //log:printInfo("Entry found '" + key + "'");
                    return entry.value;
                }
                else {
                    log:printWarn("Entry not found '" + key + "'");
                    //returning is importent because  replicas might have consitency issues.
                    return ();
                }
            }
            //if main node didn't respond go for replica nodes
            error err => {
                log:printError(err.message, err = err);
                string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
                string replicaKey = "R:" + name + ":" + key;
                future<json|error>[] replicaNodeFutures;
                //sends to both replicas async. this should be updated in to first come basis.
                foreach node in replicaNodes {
                    if (node == nodeIP) {
                        continue;
                    }
                    replicaNodeFutures[lengthof replicaNodeFutures] = start getEntryFromServer(node, replicaKey);
                }
                foreach replicaFuture in replicaNodeFutures {
                    json|error response = await replicaFuture;
                    match response {
                        json replicaJSON => {
                            if (replicaJSON.value != null) {
                                CacheEntry replicaEntry = check <CacheEntry>replicaJSON;
                                //log:printInfo("Entry found in replica '" + key + "'");
                                return replicaEntry.value;
                            }
                            else {
                                log:printWarn("Entry not found '" + key + "'");
                            }
                        }
                        error jserror => {
                            log:printError("GET replica failed.", err = err);
                        }
                    }
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
            nearCache.remove(key);
        }
        string nodeIP = hashRing.get(key);
        json entryJSON = { "key": key };
        Node? clientNode = cacheClientMap[nodeIP];
        match clientNode {
            Node client => {
                nodeEndpoint = client.nodeEndpoint;
                var res = nodeEndpoint->delete("/cache/entries"+key, entryJSON);
                match res {
                    http:Response resp => {
                        json|error msg = resp.getJsonPayload();
                        match msg {
                            json jsonPayload => {
                                log:printInfo("Cache entry remove " + jsonPayload["status"].toString());
                            }
                            error err => {
                                log:printError("error json convert", err = err);
                            }
                        }
                    }
                    error err => {
                        //TODO Queue here
                        log:printError("error removing from node", err = err);
                    }
                }
                //remove entries in replicas asnyc
                _ = start removeReplicas(key, nodeIP);
            }
            () => {
                log:printError("Client not found");
            }
        }
    }
};

function putEntriesInToReplicas(json entryJSON, string key, string originalTarget) {
    entryJSON["replica"] = true;
    //gets replica nodes
    string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
    foreach node in replicaNodes {
        Node? replicaNode = cacheClientMap[node];
        match replicaNode {
            Node replica => {
                nodeEndpoint = replica.nodeEndpoint;
                var response = nodeEndpoint->post("/cache/entries"+key, entryJSON);
                match response {
                    http:Response resp => {
                        var msg = resp.getJsonPayload();
                        match msg {
                            json jsonPayload => {
                                //silent in sucess case
                                //log:printInfo("'" + jsonPayload["key"].toString() + "' replica added to node " + node);
                            }
                            error err => {
                                log:printError(err.message, err = err);
                            }
                        }
                    }
                    error err => {
                        log:printError("Put replica request failed", err = err);
                    }
                }
            }
            () => {
                log:printError("Client not found");
            }
        }
    }
}

# Updates last accessed time of certain entry
# + key - key of the cache entry that needs to be updated
function updateLastAccessedTime(string key) {
    //Sending a get to replicas updates their last accessed time.
    string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
    replicaNodes[lengthof replicaNodes] = hashRing.get(key);
    foreach node in replicaNodes {
        _ = start getEntryFromServer(node, key);
    }
}

# Gets cache entries from target nodes.
# + ip - target Node ip
# + key - key of cache entry
# + return - entry if request succeed, error if failed
function getEntryFromServer(string ip, string key) returns json|error {
    Node? replicaNode = cacheClientMap[ip];
    match replicaNode {
        Node replica => {
            nodeEndpoint = replica.nodeEndpoint;
            var res = nodeEndpoint->get("/cache/entries/" + key);
            match res {
                http:Response resp => {
                    var msg = resp.getJsonPayload();
                    match msg {
                        json jsonPayload => {
                            return jsonPayload;
                        }
                        error err => {
                            log:printError(err.message, err = err);
                            return err;
                        }
                    }
                }
                error err => {
                    log:printError(err.message, err = err);
                    return err;
                }
            }
        }
        () => {
            log:printError("Client not found");
            error err = { message: "Client not found" };
            return err;
        }
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
    foreach item in replicaNodes {
        io:println(item);
    }
    io:println();
}


