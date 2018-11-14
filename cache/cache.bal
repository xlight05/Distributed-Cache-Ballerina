import ballerina/time;
import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;

//TODO Ensure node wont go out of memory. Since we cant ensure max size per object I dont think capacity is a good choice.
//TODO Cache entry loses when replica and original both have the same id. TIP - put replica, original sign to the key.
endpoint http:Client nodeEndpoint {
    url: "http://localhost:" + config:getAsString("cache.port", default = "7000")
};

# Represents a node in the cluster
#
# + id - Node ID
# + ip - IP of the node

type Node record {
    string id;
    string ip;
};

#    Represents a cache entry.
#
# + value - cache value
# + key - key of the entry
# + lastAccessedTime - last accessed time in ms of this value which is used to remove LRU cached values
# + createdTime - records the created time of the entry 
# + replica - checks if a record is a replica

type CacheEntry record {
    any value;
    string key;
    int lastAccessedTime;
    int createdTime;
    boolean replica;
};
// Map which stores all of the caches.

map<Cache> cacheMap;
string currentIP = config:getAsString("cache.ip", default = "http://localhost");
int currentPort = config:getAsInt("cache.port", default = 7000);
int replicationFact = config:getAsInt("cache.replicationFact", default = 1);
float cacheEvictionFactor = config:getAsFloat("cache.evictionFactor", default = 0.25);
int cacheCapacity = config:getAsInt("cache.capacity", default = 100000);
boolean isLocalCacheEnabled = config:getAsBoolean("cache.localCache", default = false);
boolean isRelocationOrEvictionRunning = false;
//boolean init = initNodeConfig();
// Object contains details of the current Node
//channel<boolean> raftReady;
//channel<boolean> cacheReady;

public function initNodeConfig() {
    //Suggestion rest para array suppot for config API
    string hosts = config:getAsString("cache.hosts");
    string[] configNodeList = hosts.split(",");
    io:println(configNodeList);
    if (configNodeList[0] == "") {
        createCluster();
    } else {
        joinCluster(configNodeList);
    }
}

#Allows uesrs to create the cluster
public function createCluster() {
    //_ = addServer(currentNode);
    startRaft();

}


#    Allows uesrs to join the cluster
#    + nodeIPs - ips of the nodes in the cluster

public function joinCluster(string[] nodeIPs) {
    //improve resiliancy
    //when leader is not appointed
    //when target is not leader
    //fix recurse
    foreach node in nodeIPs {
        http:Client client;
        http:ClientEndpointConfig cfg = { url: node };
        client.init(cfg);
        nodeEndpoint = client;
        var ee = nodeEndpoint->post("/raft/server", currentNode);
        match ee {
            http:Response payload => {
                ConfigChangeResponse result = check <ConfigChangeResponse>check payload.getJsonPayload();
                io:println(result);
                if (result.sucess) {
                    joinRaft();
                    return;
                } else {
                    log:printInfo("No " + node + " is not the leader");
                    string[] leaderIP;
                    leaderIP[0] = result.leaderHint;

                    joinCluster(leaderIP);
                    return;
                }
            }
            error err => {
                log:printInfo("not recieved");
            }
        }
    }

    //string currentIpWithPort = currentNode.ip;
    ////server list json init
    ////json serverList = { "0": check <json>currentNode };
    //string [] serverListArr = [currentNode.ip];
    ////sending requests for existing nodes
    //foreach node in nodeIPs {
    //    //changing the url of client endpoint
    //    http:ClientEndpointConfig config = { url: node };
    //    nodeEndpoint.init(config);
    //
    //    json serverDetailsJSON = <json>currentNode;
    //    var response = nodeEndpoint->post("/node/add", untaint serverDetailsJSON);
    //
    //    match response {
    //        http:Response resp => {
    //            var msg = resp.getJsonPayload();
    //            match msg {
    //                json jsonPayload => {
    //                    serverListArr = check<string []>jsonPayload;
    //                }
    //                error err => {
    //                    log:printError(err.message, err = err);
    //                }
    //            }
    //        }
    //        error err => {
    //            log:printError(err.message, err = err);
    //        }
    //    }
    //}
    ////Setting local node list
    //foreach item in serverListArr{
    //    http:Client client;
    //    http:ClientEndpointConfig cc = { url: item };
    //    client.init(cc);
    //    clientMap[item] = client;
    //}
    //setServers();
}


# Allows users to create a cache object
# + name - name of the cache object

public function createCache(string name) {
    cacheMap[name] = new Cache(name);
    log:printInfo("Cache Created " + name);
}

# Allows users to create a cache object
#
# + name - name of the cache object
# + return - Cache object associated with the given name

public function getCache(string name) returns Cache? {

    foreach node in clientMap {
        nodeEndpoint = node;
        //changing the url of client endpoint

        var response = nodeEndpoint->get("/cache/get/" + name);

        match response {
            http:Response resp => {
                var msg = resp.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        if (!(jsonPayload["status"].toString() == "Not found")) {
                            Cache cacheObj = check <Cache>jsonPayload;
                            cacheMap[name] = cacheObj;
                            return cacheObj;
                        } else {
                            log:printWarn(name + " Cache not found- in " + node.config.url);
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
    //TODO local cache per node or cache?
    LocalCache nearCache = new(capacity = config:getAsInt("local.cache.capacity", default = 100), evictionFactor =
        config:getAsFloat("local.cache.evictionFactor", default = 0.25)); //maybe move from the object?
    //CacheConfig config;

    public new(name) {
        initNodeConfig(); // not the best choice -,- should init before this
        var cc = cacheMap[name];
        match cc {
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
    #
    # + key - value which should be used as the key
    # + value - value to be cached

    public function put(string key, any value) {
        //Adding in to nearCache for quick retrival
        if (isLocalCacheEnabled) {
            nearCache.put(key, value);
        }
        string nodeIP = hashRing.get(key);
        int currentTime = time:currentTime().time;
        CacheEntry entry = { value: value, key: key, lastAccessedTime: currentTime, createdTime: currentTime };
        json entryJSON = check <json>entry;
        entryJSON["key"] = key;
        entryJSON["replica"] = false;

        //http:ClientEndpointConfig config = { url: nodeIP };
        //nodeEndpoint.init(config);

        http:Client? clientNode = clientMap[nodeIP];
        http:Client client;
        match clientNode {
            http:Client c => {
                client = c;
            }
            () => {
                log:printError("Client not found");
            }
        }
        nodeEndpoint = client;
        var res = nodeEndpoint->post("/data/store/", entryJSON);
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
                log:printError("error zaxs", err = err);
            }
        }
        //TODO Not enoguh nodes for replica
        _ = start putEntriesInToReplicas(entryJSON,key,nodeIP);

    }
    function putEntriesInToReplicas(json entryJSON,string key,string originalTarget) {
        entryJSON["replica"] = true;
        string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
        foreach node in replicaNodes {
            if (node == originalTarget) {
                continue;
            }
            io:println("Replica : " + node);
            //http:ClientEndpointConfig cfg = { url: node };
            //nodeEndpoint.init(cfg);


            http:Client? replicaNode = clientMap[node];
            http:Client replica;
            match replicaNode {
                http:Client c => {
                    replica = c;
                }
                () => {
                    log:printError("Client not found");
                }
            }
            nodeEndpoint = replica;
            var resz = nodeEndpoint->post("/data/store/", entryJSON);
            match resz {
                http:Response resp => {
                    var msg = resp.getJsonPayload();
                    match msg {
                        json jsonPayload => {
                            log:printInfo("'" + jsonPayload["key"].toString() + "' replica added to node " + node);
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

}


    #Returns the cached value associated with the given key. If the provided cache key is not found in the cluster, () will be returned.
#
# + key - key which is used to retrieve the cached value
# + return  -The cached value associated with the given key
    public function get(string key) returns any? {
        if (isLocalCacheEnabled) {
            if (nearCache.hasKey(key)) {
                log:printInfo(key + " retrived by local Cache");
                _ = start updateLastAccessedTime(key);
                return nearCache.get(key);
            }
        }
        string nodeIP = hashRing.get(key);
        var msg = getEntryFromServer(nodeIP, key);
        match msg {
            json jsonPayload => {
                if (jsonPayload.value != null) {
                    CacheEntry entry = check <CacheEntry>jsonPayload;
                    log:printInfo("Entry found '" + key + "'");
                    return entry.value;
                }
                else {
                    log:printWarn("Entry not found '" + key + "'");
                    return ();
                }
            }
            error err => {
                log:printError(err.message, err = err);
                string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
                future <json|error> [] replicaNodeFutures;
                foreach node in replicaNodes {
                    if (node == nodeIP) {
                        continue;
                    }
                    replicaNodeFutures[lengthof replicaNodeFutures] =start getEntryFromServer(node, key);
                }
                foreach replicaFuture in replicaNodeFutures {
                    json|error response =await replicaFuture;
                    match response {
                        json resp => {
                            if (resp.value != null) {
                                CacheEntry entry = check <CacheEntry>resp;
                                log:printInfo("Entry found in replica '" + key + "'");
                                return entry.value;
                            }
                            else {
                                log:printWarn("Entry not found '" + key + "'");
                            }
                        }
                        error jserror => {
                            log:printError(err.message, err = err);
                        }
                    }
                }

            }
        }
        return ();
    }

    function updateLastAccessedTime(string key) {
        string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
        replicaNodes[lengthof replicaNodes] = hashRing.get(key);
        foreach node in replicaNodes {
            _ =start getEntryFromServer(node, key);
        }
    }

    function getEntryFromServer(string ip, string key) returns json|error {
        http:Client? replicaNode = clientMap[ip];
        http:Client replica;
        match replicaNode {
            http:Client c => {
                replica = c;
            }
            () => {
                log:printError("Client not found");
            }
        }
        nodeEndpoint = replica;
        var res = nodeEndpoint->get("/data/get/" + key);
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
                return err;
            }
        }
    }

    // public function size() returns int {
    //     return lengthof entries;
    // }
    // public function hasKey(string key) returns boolean {
    //     return entries.hasKey(key);
    // }



    public function locateNode(string key) {
        io:println(key + " located in ");
        io:println(hashRing.get(key));
    }
    public function locateReplicas(string key) {
        io:println(key + " replica located in ");
        string[] gg = hashRing.GetClosestN(key, replicationFact);
        foreach item in gg {
            io:println(item);
        }
        io:println();
    }
    public function clearAllEntries() {
        foreach node in clientMap {
            nodeEndpoint = node;
            json testJson = { "message": "Test JSON", "status": 200 };
            var response = nodeEndpoint->delete("/data/clear", testJson);

            match response {
                http:Response resp => {
                    var msg = resp.getJsonPayload();
                    match msg {
                        json jsonPayload => {

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
        log:printInfo("Nodes Cleared");
    }



    # Returns the cached value associated with the given key. If the provided cache key is not found in the cluster, () will be returned.
#
# + key - key which is used to remove the entry
public function remove(string key) {
        //Adding in to nearCache for quick retrival
        if (isLocalCacheEnabled) {
            nearCache.remove(key);
        }
        string nodeIP = hashRing.get(key);
        json entryJSON = { "key": key };


        http:Client? clientNode = clientMap[nodeIP];
        http:Client client;
        match clientNode {
            http:Client c => {
                client = c;
            }
            () => {
                log:printError("Client not found");
            }
        }
        nodeEndpoint = client;
        var res = nodeEndpoint->delete("/data/remove/", entryJSON);
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
                log:printError("error removing from node", err = err);
            }
        }
        //TODO Not enoguh nodes for replica
        string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
        foreach node in replicaNodes {
            if (node == nodeIP) {
                continue;
            }
            io:println("Replica : " + node);
            //http:ClientEndpointConfig cfg = { url: node };
            //nodeEndpoint.init(cfg);


            http:Client? replicaNode = clientMap[node];
            http:Client replica;
            match replicaNode {
                http:Client c => {
                    replica = c;
                }
                () => {
                    log:printError("Client not found");
                }
            }
            nodeEndpoint = replica;
            var resz = nodeEndpoint->delete("/data/store/", entryJSON);
            match resz {
                http:Response resp => {
                    var msg = resp.getJsonPayload();
                    match msg {
                        json jsonPayload => {
                            log:printInfo("Cache entry replica remove " + jsonPayload["status"].toString());
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
    }

};



