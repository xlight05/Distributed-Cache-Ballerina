import ballerina/io;
import ballerina/http;
import ballerina/log;
//import consistent;
import consistent_bound;

consistent_bound:Consistent hashRing = new();

//returns node list as a json
function getNodeList() returns json {
    string [] nodeArr;
    foreach item in clientMap {
        nodeArr[lengthof nodeArr]=item.config.url;
    }
    return check <json>nodeArr;
}

function setReplicationFactor() {
    //better replication factor logic here
    replicationFact = 1;
}

//function addServer(Node node) returns json {
//    // Adds node to node array
//    hashRing.add(node.ip);
//    //Adds node ip to hash ringa
//    setReplicationFactor();
//    http:Client client;
//    http:ClientEndpointConfig cc = {url: node.ip};
//    client.init(cc);
//    clientMap[node.ip] = client;
//    string [] nodeIpArr;
//    log:printInfo("New Node Added " + node.ip);
//    //TODO change redistribution in to seperate method
//    json changedJson = getChangedEntries();
//    // Gets changed cache entries of the node
//    foreach nodeItem in clientMap {
//        string nodeIP = nodeItem.config.url;
//        nodeIpArr[lengthof nodeIpArr]= nodeItem.config.url;
//        if (nodeIP == currentNode){ //Ignore if its the current node
//            continue;
//        }
//
//        http:ClientEndpointConfig config = { url: nodeIP };
//        nodeEndpoint.init(config);
//
//        var res = nodeEndpoint->post("/data/multiple/store/", untaint changedJson[nodeIP]);
//        //sends changed entries to correct node
//        match res {
//            http:Response resp => {
//                var msg = resp.getJsonPayload();
//                match msg {
//                    json jsonPayload => {
//                        log:printInfo("Entries sent to " + nodeIP);
//                    }
//                    error err => {
//                        log:printError(err.message, err = err);
//                    }
//                }
//            }
//            error err => {
//                log:printError(err.message, err = err);
//            }
//        }
//    }
//    json jsonNodeList = check <json>nodeIpArr;
//    return jsonNodeList;
//}

function relocateData() {
    json changedJson = getChangedEntries();
    foreach nodeItem in clientMap {
        string nodeIP = nodeItem.config.url;
        if (nodeIP == currentNode){ //Ignore if its the current node
            continue;
        }
        nodeEndpoint = nodeItem;

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
}


//Adds servers in node list to hash ring
function setServers() {
    foreach item in clientMap {
        hashRing.add(item.config.url);
    }
}

