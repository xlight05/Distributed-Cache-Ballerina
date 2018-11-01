import ballerina/time;
import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;

endpoint http:Client nodeEndpoint {
    url: "http://localhost:" + config:getAsString("port", default = "7000")
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
# + timesAccessed - records the times entry retrived by the user
# + createdTime - records the created time of the entry 
# + replica - checks if a record is a replica

type CacheEntry record {
    any value;
    string key;
    int lastAccessedTime;
    int timesAccessed;
    int createdTime;
    boolean replica;
}; 
// Map which stores all of the caches.

map<Cache> cacheMap;
string currentIP = config:getAsString("ip", default = "http://localhost");
int currentPort = config:getAsInt("port", default = 7000);
int replicationFact = 1; // Per Node
float cacheEvictionFactor = 0.1; //Per Node
int CacheCapacity = 1000000; //Per Node
boolean isLocalCacheEnabled = false;
//boolean init = initNodeConfig();
// Object contains details of the current Node


public function initNodeConfig() returns boolean {
    //Suggestion rest para array suppot for config API
    string hosts = config:getAsString("hosts");
    string[] configNodeList = hosts.split(",");
    io:println(configNodeList);
    if (configNodeList[0] == ""){
        io:println("In Create");
        createCluster();
    } else {
        io:println("In Join");
        joinCluster(configNodeList);
    }
    return true;
}

#Allows uesrs to create the cluster
public function createCluster() {
    //_ = addServer(currentNode);
    startRaft();

}


#    Allows uesrs to join the cluster
#    + nodeIPs - ips of the nodes in the cluster

public function joinCluster(string[] nodeIPs) {
    foreach node in nodeIPs {
        http:Client client;
        http:ClientEndpointConfig cfg=  {url:node};
        client.init(cfg);
        nodeEndpoint = client;
        var ee = nodeEndpoint->post("/raft/server",currentNode);
        match ee {
            http:Response payload => {
                ConfigChangeResponse result = check <ConfigChangeResponse> check payload.getJsonPayload();
                if (result.sucess){
                    joinRaft();
                    break;
                }else {
                    log:printInfo("No "+node +" is not the leader");
                    string[] leaderIP;
                    leaderIP[0] = result.leaderHint;

                    joinCluster(leaderIP);
                }
            }
            error err => {
                log:printInfo ("not recieved");
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
        nodeEndpoint= node;
        //changing the url of client endpoint

        var response = nodeEndpoint->get("/cache/get/" + name);

        match response {
            http:Response resp => {
                var msg = resp.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        if (!(jsonPayload["status"].toString() == "Not found")){
                            Cache cacheObj = check <Cache>jsonPayload;
                            cacheMap[name] = cacheObj;
                            log:printInfo("Cache Found- " + name);
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


# Represents a cache.
public type Cache object {
    string name;
    LocalCache nearCache = new(capacity = 2, evictionFactor = 0.5);
    string cacheCfg;

    public new(name, cacheCfg = "none") {
        if (cacheMap.hasKey(name)){
            return;
        }
        var cacheVar = getCache(name);

        match cacheVar {
            Cache cache => {
                name = cache.name;
                cacheCfg = cache.cacheCfg;
                //nearCache cfg
            }
            () => {
                cacheMap[name] = self;
                log:printInfo("Cache Created " + name);
                return;
            }
        }
    }


    # Adds the given key, value pair to the provided cache.It will be stored in a appropirate node in the cluster
    #
    # + key - value which should be used as the key
    # + value - value to be cached

    public function put(string key, any value) {
        //Adding in to nearCache for quick retrival
        if (isLocalCacheEnabled){
            nearCache.put(key, value);
        }
        string nodeIP = hashRing.get(key);
        int currentTime = time:currentTime().time;
        CacheEntry entry = { value: value, key: key, lastAccessedTime: currentTime, timesAccessed: 0, createdTime:
        currentTime };
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
                        //TODO Key already exists
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
        entryJSON["replica"] = true;
        string[] replicaNodes = hashRing.GetClosestN(key, replicationFact);
        foreach node in replicaNodes {
            if (node == nodeIP){
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
# Returns the cached value associated with the given key. If the provided cache key is not found in the cluster, () will be returned.
#
# + key - key which is used to retrieve the cached value
# + return  -The cached value associated with the given key
    public function get(string key) returns any? {
        if (isLocalCacheEnabled){
            if (nearCache.hasKey(key)){
                log:printInfo(key + " retrived by local Cache");
                return nearCache.get(key);
            }
        }
        string nodeIP = hashRing.get(key);
        json requestedJSON;
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
        var res = nodeEndpoint->get("/data/get/" + key);
        match res {
            http:Response resp => {
                var msg = resp.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        if (jsonPayload.value != null){
                            requestedJSON = jsonPayload;
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
                    }
                }
            }
            error err => {
                log:printError(err.message, err = err);
            }
        }
        return ();
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

};



