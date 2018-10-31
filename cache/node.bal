import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;
//import consistent;
import consistent_bound;

consistent_bound:Consistent hashRing = new();

Node[] nodeList;
map<http:Client> clientMap;

//returns node list as a json
function getNodeList() returns json {
    json jsonObj = check <json>nodeList;
    //foreach k, v in nodeList {
    //    jsonObj[k] = check v;
    //}
    return jsonObj;
}

function setReplicationFactor() {
    //better replication factor logic here
    replicationFact = 1;
}

function addServer(Node node) returns json {
    // Adds node to node array
    hashRing.add(node.ip);
    //Adds node ip to hash ringa
    setReplicationFactor();
    http:Client client;
    http:ClientEndpointConfig cc = {url: node.ip};
    client.init(cc);
    clientMap[node.ip] = client;
    string [] nodeIpArr;
    log:printInfo("New Node Added " + node.ip);
    //TODO change redistribution in to seperate method
    json changedJson = getChangedEntries();
    // Gets changed cache entries of the node
    foreach nodeItem in clientMap {
        string nodeIP = nodeItem.config.url;
        nodeIpArr[lengthof nodeIpArr]= nodeItem.config.url;
        if (nodeIP == currentNode.ip){ //Ignore if its the current node
            continue;
        }

        http:ClientEndpointConfig config = { url: nodeIP };
        nodeEndpoint.init(config);

        var res = nodeEndpoint->post("/data/multiple/store/", untaint changedJson[nodeIP]);
        //sends changed entries to correct node
        match res {
            http:Response resp => {
                var msg = resp.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        log:printInfo("Entries sent to " + nodeIP);
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
    json jsonNodeList = check <json>nodeIpArr;
    return jsonNodeList;
}

//Removes item from server
function removeServer(string ip) returns boolean {
    boolean found = false;
    foreach i in clientMap{
        if (i.config.url == ip){
            //remove node from array
            found = true;
        }
    }
    //Remove from ring
    //Reallocate Data
    return found;
}

//Adds servers in node list to hash ring
function setServers() {
    foreach item in clientMap {
        hashRing.add(item.config.url);
    }
}

