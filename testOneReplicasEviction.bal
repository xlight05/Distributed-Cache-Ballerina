import ballerina/test;
import ballerina/config;
import ballerina/io;
import distributed_cache as cache;

@test:Config
function testNoReplicasEviction() {
    cache:connectToCluster();
    cache:Cache oauthCache = new("oauthCache",expiryTimeMillis=30000);

    oauthCache.put("1", "1");
    oauthCache.put("2", "2");
    oauthCache.put("3", "3");
    oauthCache.put("4", "4");
    oauthCache.put("5", "5");
    oauthCache.put("6", "6");


    test:assertEquals(oauthCache.get("1"), (), msg = "Expects to be evicted");
    test:assertEquals(<string> oauthCache.get("2"), "2", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("3"), "3", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("4"), "4", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("5"), "5", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("6"), "6", msg = "Should be in the node");

}