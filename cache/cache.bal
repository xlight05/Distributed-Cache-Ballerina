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

    private map<CacheEntry> entries;
    private float evictionFactor;

    public new(evictionFactor = 0.25) {

        // Cache eviction factor must be between 0.0 (exclusive) and 1.0 (inclusive).
        if (evictionFactor <= 0 || evictionFactor > 1) {
            error e = { message: "Cache eviction factor must be between 0.0 (exclusive) and 1.0 (inclusive)." };
            throw e;
        }

    }

    public function put(string key, any value) {
        int currentTime = time:currentTime().time;
        CacheEntry entry = { value: value, lastAccessedTime: currentTime, timesAccessed: 0, createdTime: currentTime };
        //entries[key]=entry;
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
            return ();

        }




        //if (!hasKey(key)){
        //    return  ();
        //}
        //map temp;
        //int currentTime = time:currentTime().time;
        //CacheEntry ent;
        //CacheEntry entry = entries[key] ?: ent;
        //entry.lastAccessedTime = currentTime;
        ////entries[key]= entry;
        //return entry.value;
    }

    public function size() returns int {
        return lengthof entries;
    }
    public function hasKey(string key) returns boolean {
        return entries.hasKey(key);
    }

};
