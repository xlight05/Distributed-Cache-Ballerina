import ballerina/time;
import ballerina/io;
import ballerina/http;
import ballerina/log;


endpoint http:Client lbEndpoint {
    url: "http://localhost:9998"
};

endpoint http:Client nodeEndpoint {
    url: "http://localhost:7000"
};

endpoint http:Client storeEndpoint {
    url: "http://localhost:6969"
};


type CacheEntry record {
    any value;
    int lastAccessedTime;
    int timesAccessed;
    int createdTime;
};


public type Cache object {
    
    //To construct an Cache object you need two parameters. First one is your current IP in the node. 
    //Second one is a Rest parameter. you have to send ips of existing nodes in your cluster as string to this parameter.
    //In simple terms, current node sends broadcast to all the existing nodes so they can add the new node to their cluster node list.
    //then it takes the complete node list as the response and store it locally for further use.
    public new(string currentIP,string ... nodeIPs) {
        string nodePort = "7000"; //static ports (for now)
        string currentIpWithPort = currentIP + ":" + nodePort;
        int i = 0;
        int nodeLength = lengthof nodeIPs;
        io:println(nodeLength);
        //server list json init
        json serverList={"0":currentIP};
        //sending requests for existing nodes
        while (i < nodeLength) {
            //changing the url of client endpoint 
            http:ClientEndpointConfig config = { url: nodeIPs[i] + ":" + nodePort };
            nodeEndpoint.init(config);

            json serverDetailsJSON = { "ip": currentIP };
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
            i = i + 1;
        }

        //changing client endpoint url to current IP
        http:ClientEndpointConfig config = { url: currentIpWithPort };
        nodeEndpoint.init(config);
        io:println(serverList);
        //settting current node with server list.
        var response = nodeEndpoint->post("/node/set", untaint serverList);
        match response {
            http:Response resp => {
                var msg = resp.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        io:println (jsonPayload);
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

    //Put function allows users to store key and value in the cache.
    //In the current version put function, it sends key and value for loadbalacner located in node.bal to achieve round robin 
    //data distribute pattern
    public function put(string key, any value) {
        int currentTime = time:currentTime().time;
        CacheEntry entry = { value: value, lastAccessedTime: currentTime, timesAccessed: 0, createdTime: currentTime };
        json j = check <json>entry;
        j["key"] = key;

        //sends data to load balacner.
        var response = lbEndpoint->post("/lb", untaint j);
        match response {
            http:Response resp => {
                var msg = resp.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        io:println(jsonPayload);
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
        var response = nodeEndpoint->get("/node/list");
        json serverListJSON;
        string[] serverList;
        //getting serverlist
        match response {
            http:Response resp => {
                var msg = resp.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        serverListJSON = jsonPayload;
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
        //poulating server list with store port.
        int counter = 0;
        foreach item in serverListJSON {
            serverList[counter] = item.toString() + ":6969";
            counter++;
        }

        json requestedJSON;
        //checking all nodes and return the value of the entry if found.
        foreach item in serverList {
            http:ClientEndpointConfig config = { url: item };
            storeEndpoint.init(config);

            var res = storeEndpoint->get("/data/get/" + key);
            match res {
                http:Response resp => {
                    var msg = resp.getJsonPayload();
                    match msg {
                        json jsonPayload => {
                            if (jsonPayload.value != null){
                                requestedJSON = jsonPayload;
                                CacheEntry entry = check <CacheEntry>jsonPayload;
                                return entry.value;
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
        return requestedJSON;
    }

    // public function size() returns int {
    //     return lengthof entries;
    // }
    // public function hasKey(string key) returns boolean {
    //     return entries.hasKey(key);
    // }

};
