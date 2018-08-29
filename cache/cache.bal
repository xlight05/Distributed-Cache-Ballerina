import ballerina/time;
import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;
import ballerina/cache as localCache;

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
};
//
documentation { Map which stores all of the caches. }
map<Cache> cacheMap;
string currentIP = config:getAsString("ip", default = "http://localhost");
int currentPort = config:getAsInt("port", default = 7000);

documentation { Object contains details of the current Node }
Node currentNode = {
    id: config:getAsString("id", default = "1"),
    ip: config:getAsString("ip", default = "http://localhost") + ":" + config:getAsInt("port", default = 7000)
};

documentation { Allows uesrs to create the cluster }
public function createCluster() {
    json j = addServer(currentNode);
}

documentation {
    Allows uesrs to join the cluster
     P{{nodeIPs}} ips of the nodes in the cluster
}
public function joinCluster(string... nodeIPs) {

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
                            log:printWarn("Cache not found- " + name);
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
    }
    return ();
}


documentation { Represents a cache. }
public type Cache object {
    string name;
    localCache:Cache nearCache = new(capacity = 100, expiryTimeMillis = 24 * 60000, evictionFactor = 0.2);


    public new(name) {

    }

    documentation {
        Adds the given key, value pair to the provided cache.It will be stored in a appropirate node in the cluster

        P{{key}} value which should be used as the key
        P{{value}} value to be cached
    }
    public function put(string key, any value) {
        //Adding in to nearCache for quick retrival
        // nearCache.put (key,value);
        string nodeIP = hashRing.get(key);
        int currentTime = time:currentTime().time;
        CacheEntry entry = { value: value, lastAccessedTime: currentTime, timesAccessed: 0, createdTime: currentTime };
        json entryJSON = check <json>entry;
        entryJSON["key"] = key;

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
                        log:printError(err.message, err = err);
                    }
                }
            }
            error err => {
                log:printError(err.message, err = err);
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

};



