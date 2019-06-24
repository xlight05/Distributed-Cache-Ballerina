import ballerina/test;
import ballerina/config;
import ballerina/io;
import cache;

@test:Config
function testNoReplicasEviction() {
    _ = cache:initNodeConfig();
    cache:Cache oauthCache = new("oauthCache",expiryTimeMillis=30000);

    oauthCache.put("1", "1");
    oauthCache.put("2", "2");
    oauthCache.put("3", "3");
    oauthCache.put("4", "4");
    oauthCache.put("5", "5");
    oauthCache.put("6", "6");
    oauthCache.put("7", "7");
    oauthCache.put("8", "8");
    oauthCache.put("9", "9");
    oauthCache.put("10", "10");
    oauthCache.put("11", "11");

    test:assertEquals(oauthCache.get("1"), (), msg = "Expects to be evicted");
    test:assertEquals(<string> oauthCache.get("2"), "2", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("3"), "3", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("4"), "4", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("5"), "5", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("6"), "6", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("7"), "7", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("8"), "8", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("9"), "9", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("10"), "10", msg = "Should be in the node");
    test:assertEquals(<string> oauthCache.get("11"), "11", msg = "Should be in the node");
}