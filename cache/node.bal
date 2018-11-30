import ballerina/io;
import ballerina/http;
import ballerina/log;
//import consistent;
import consistent_bound;

consistent_bound:Consistent hashRing = new();

//returns node list as a json
function getNodeList() returns json {
    string [] nodeArr;
    foreach item in cacheClientMap {
        nodeArr[lengthof nodeArr]=item.config.url;
    }
    return check <json>nodeArr;
}

//TODO maintain counter in both sender and reciver to ensure request is recieved. or MB
function relocateData() {
    //lock{
        json changedJson = getChangedEntries();
        foreach nodeItem in relocationClientMap {
            string nodeIP = nodeItem.config.url;
            nodeEndpoint = nodeItem;
            log:printInfo("Relocating data");
            var res = nodeEndpoint->post("/data/multiple/store/", untaint changedJson[nodeIP]);
            //sends changed entries to correct node
            match res {
                http:Response resp => {
                    var msg = resp.getJsonPayload();
                    match msg {
                        json jsonPayload => {
                            //TODO Remove after recieved response
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
    //}
}
