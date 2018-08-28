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
type Node record {
    string id;
    string ip;
};
//Cache Entry record represents a single cache entry.
//This is how Cache entry saved in memory
type CacheEntry record {
    any value;
    int lastAccessedTime;
    int timesAccessed;
    int createdTime;
};
//
//This map holds Cache Maps
map<Cache> cacheMap;
string currentIP = config:getAsString("ip", default = "http://localhost");
int currentPort = config:getAsInt("port", default = 7000);

//Object contains details of the current Node
Node currentNode = {
    id: config:getAsString("id", default = "1"),
    ip: config:getAsString("ip", default = "http://localhost") + ":" + config:getAsInt("port", default = 7000)
};

public function createCluster() {
    json j = addServer(currentNode);
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

public function createCache(string name) {
    cacheMap[name] = new Cache(name);
    log:printInfo("Cache Created " + name);
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



public type Cache object {
    string name;
    localCache:Cache nearCache = new(capacity = 100, expiryTimeMillis = 24 * 60000, evictionFactor = 0.2);

    //To construct an Cache object you need two parameters. First one is your current IP in the node. 
    //Second one is a Rest parameter. you have to send ips of existing nodes in your cluster as string to this parameter.
    //In simple terms, current node sends broadcast to all the existing nodes so they can add the new node to their cluster node list.
    //then it takes the complete node list as the response and store it locally for further use.
    public new(name) {

    }

    //Put function allows users to store key and value in the cache.
    //In the current version put function, it sends key and value for loadbalacner located in node.bal to achieve round robin 
    //data distribute pattern
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


    //Get function allows you to retrieve data from the stores of all the nodes.
    //In this current version it checks each node if it has the given key or not (which is not very effecient.)
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



