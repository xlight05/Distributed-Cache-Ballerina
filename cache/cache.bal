import ballerina/time;
import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;        

endpoint http:Client nodeEndpoint {
    url: "http://localhost:" + config:getAsString("port", default = "7000")
};
endpoint http:Listener listner {
    port: config:getAsInt("port", default = 7000)
};

documentation {
    Represents a node in the cluster

    F{{id}} Node ID
    F{{ip}} IP of the node
}
type Node record {
    string id;
    string ip;
};
documentation {
    Represents a cache entry.

    F{{value}} cache value
    F{{lastAccessedTime}} last accessed time in ms of this value which is used to remove LRU cached values
    F{{timesAccessed}} records the times entry retrived by the user
    F{{createdTime}} records the created time of the entry
}
type CacheEntry record {
    any value;
    int lastAccessedTime;
    int timesAccessed;
    int createdTime;
    boolean replica;
};
//
documentation { Map which stores all of the caches. }
map<Cache> cacheMap;
string currentIP = config:getAsString("ip", default = "http://localhost");
int currentPort = config:getAsInt("port", default = 7000);
int replicationFact = 1;
boolean isLocalCacheEnabled = true;
//boolean init = initNodeConfig();

documentation { Object contains details of the current Node }
Node currentNode = {
    id: config:getAsString("id", default = "1"),
    ip: config:getAsString("ip", default = "http://localhost") + ":" + config:getAsInt("port", default = 7000)
};


public function initNodeConfig() returns boolean {
    //Suggestion rest para array suppot for config API
    string hosts = config:getAsString("hosts");
    string[] configNodeList = hosts.split(",");
    io:println(configNodeList);
    if (configNodeList[0]==""){
        io:println ("In Create");
        createCluster();

    }else {
        io:println ("In Join");
        joinCluster(configNodeList);

    }
    return true;
}

documentation { Allows uesrs to create the cluster }
public function createCluster() {
    json j = addServer(currentNode);
}

documentation {
    Allows uesrs to join the cluster
     P{{nodeIPs}} ips of the nodes in the cluster
}
public function joinCluster(string [] nodeIPs) {

    string currentIpWithPort = currentNode.ip;
    //server list json init
    json serverList = { "0": check <json>currentNode };
    //sending requests for existing nodes
    foreach node in nodeIPs {
        //changing the url of client endpoint
        http:ClientEndpointConfig config = { url: node };
        nodeEndpoint.init(config);

        json serverDetailsJSON = check <json>currentNode;
        var response = nodeEndpoint->post("/node/add", untaint serverDetailsJSON);

        match response {
            http:Response resp => {
                var msg = resp.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        serverList = jsonPayload;
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
    //Setting local node list
    nodeList = untaint check <Node[]>serverList;
    setServers();
    log:printInfo("Joined the cluster");

}

documentation {
    Allows users to create a cache object
    P{{name}} name of the cache object
}
public function createCache(string name) {
    cacheMap[name] = new Cache(name);
    log:printInfo("Cache Created " + name);
}

documentation {
    Allows users to create a cache object
    P{{name}} name of the cache object
    R{{}}Cache object associated with the given name
}
public function getCache(string name) returns Cache? {

    foreach node in nodeList {
        //changing the url of client endpoint
        http:ClientEndpointConfig cfg = { url: node.ip };
        nodeEndpoint.init(cfg);
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
                            log:printWarn(name + " Cache not found- in "+node.ip);
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
    

documentation { Represents a cache. }
public type Cache object {
    public string name;
    LocalCache nearCache = new(capacity = 2, evictionFactor = 0.5);
    string cacheCfg;

    public new(name,cacheCfg="none") {
        if (cacheMap.hasKey(name)){
            return;
        }
        var cacheVar = getCache(name);
        
        match cacheVar {
            Cache cache => {
                name = cache.name;
                cacheCfg=cache.cacheCfg;
                //nearCache cfg
            }
            () => {
                cacheMap[name]=self;
                log:printInfo("Cache Created " + name);
                return;
            }
        }
    }

    documentation {
        Adds the given key, value pair to the provided cache.It will be stored in a appropirate node in the cluster

        P{{key}} value which should be used as the key
        P{{value}} value to be cached
    }
    public function put(string key, any value) {
        //Adding in to nearCache for quick retrival
        if (isLocalCacheEnabled){
            nearCache.put (key,value);
        }
        string nodeIP = hashRing.get(key);
        int currentTime = time:currentTime().time;
        CacheEntry entry = { value: value, lastAccessedTime: currentTime, timesAccessed: 0, createdTime: currentTime };
        json entryJSON = check <json>entry;
        entryJSON["key"] = key;
        entryJSON["replica"]=false;
        http:ClientEndpointConfig config = { url: nodeIP };
        nodeEndpoint.init(config);

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
        entryJSON["replica"]=true;
        string[] replicaNodes = hashRing.GetClosestN(key,replicationFact);
        foreach node in replicaNodes {
            if (node==nodeIP){
                continue;
            }
            http:ClientEndpointConfig cfg = { url: node };
            nodeEndpoint.init(cfg);

            var resz = nodeEndpoint->post("/data/store/", entryJSON);
            match resz {
                http:Response resp => {
                    var msg = resp.getJsonPayload();
                    match msg {
                        json jsonPayload => {
                            log:printInfo("'" + jsonPayload["key"].toString() + "' replica added to node "+node);
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


    documentation {
        Returns the cached value associated with the given key. If the provided cache key is not found in the cluster, ()
        will be returned.

        R{{key}} key which is used to retrieve the cached value
        R{{}}The cached value associated with the given key
    }
    public function get(string key) returns any? {
        if (isLocalCacheEnabled){
            if (nearCache.hasKey(key)){
                log:printInfo(key+ " retrived by local Cache");
                return nearCache.get(key);
            }
        }
        string nodeIP = hashRing.get(key);
        json requestedJSON;
        http:ClientEndpointConfig config = { url: nodeIP };
        nodeEndpoint.init(config);

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
        io:println (hashRing.get(key));
    }
    public function locateReplicas(string key) {
        io:println(key + " replica located in ");
        string[] gg = hashRing.GetClosestN(key, replicationFact);
        foreach item in gg {
            io:println (item);
        }
        io:println();
    }

};



