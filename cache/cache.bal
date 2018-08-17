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
endpoint http:Client initEndpoint {
    url: "http://localhost:9000"
};


type CacheEntry record {
    any value;
    int lastAccessedTime;
    int timesAccessed;
    int createdTime;
};


public type Cache object {
    

    public new(string currentIP,string ... nodeIPs) {
        json initJSON;
        initJSON["currentIP"]=currentIP;
        initJSON["nodeArr"]=[];
        foreach k,v in  nodeIPs{
            initJSON["nodeArr"][k]=v;
        }
        io:println(initJSON);
        var response = initEndpoint->put("/init/join",initJSON);
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

    public function put(string key, any value) {
        int currentTime = time:currentTime().time;
        CacheEntry entry = { value: value, lastAccessedTime: currentTime, timesAccessed: 0, createdTime: currentTime };
        json j = check <json>entry;
        j["key"] = key;


        //select node
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
        //send

    }

    public function get(string key) returns any? {
        var response = nodeEndpoint->get("/node/list");
        json serverListJSON;
        string[] serverList;
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
        int counter = 0;
        foreach item in serverListJSON {
            serverList[counter] = item.toString() + ":6969";
            counter++;
        }

        json requestedJSON;

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
