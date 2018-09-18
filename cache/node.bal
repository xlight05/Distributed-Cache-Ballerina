import ballerina/io;
import ballerina/http;
import ballerina/log;
import ballerina/config;
//import consistent;
import consistent_bound;

consistent_bound:Consistent hashRing = new();

Node[] nodeList;

//returns node list as a json
function getNodeList() returns json {
    json jsonObj = check <json>nodeList;
    //foreach k, v in nodeList {
    //    jsonObj[k] = check v;
    //}
    return jsonObj;
}

function setReplicationFactor (){
    //better replication factor logic here
    replicationFact = 1;
}

function addServer(Node node) returns json {
    nodeList[lengthof nodeList] = node;
    // Adds node to node array
    hashRing.add(node.ip);
    //Adds node ip to hash ringa
    setReplicationFactor();
    json jsonNodeList = check <json>nodeList;
    log:printInfo("New Node Added " + node.ip);
    //TODO change redistribution in to seperate method
    json changedJson = getChangedEntries();
    // Gets changed cache entries of the node
    foreach nodeItem in nodeList {
        if (nodeItem.ip == currentNode.ip){ //Ignore if its the current node
            continue;
        }

        http:ClientEndpointConfig config = { url: nodeItem.ip };
        nodeEndpoint.init(config);

        var res = nodeEndpoint->post("/data/multiple/store/", untaint changedJson[nodeItem.ip]);
        //sends changed entries to correct node
        match res {
            http:Response resp => {
                var msg = resp.getJsonPayload();
                match msg {
                    json jsonPayload => {
                        log:printInfo("Entries sent to " + nodeItem.ip);
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
    return jsonNodeList;
}

//Removes item from server
function removeServer(string ip) returns boolean {
    boolean found = false;
    foreach k, v in nodeList{
        if (v.ip == ip){
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
    foreach item in nodeList {
        hashRing.add(item.ip);
    }
}

