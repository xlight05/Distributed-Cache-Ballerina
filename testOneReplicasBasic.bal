import ballerina/test;
import ballerina/config;
import ballerina/io;
import distributed_cache as cache;


@test:Config
function testOneReplicasBasic() {
    cache:connectToCluster();
    cache:Cache oauthCache = new("oauthCache",expiryTimeMillis=30000);

    oauthCache.put("1", "1");
    oauthCache.put("2", "2");
    oauthCache.put("3", "3");
    oauthCache.put("4", "4");
    oauthCache.put("5", "5");


    string[] expectedResults = ["1","2","3","4","5"];
    string[] actualResults = [];
    actualResults[0] = <string> oauthCache.get("1");
    actualResults[1] = <string> oauthCache.get("2");
    actualResults[2] = <string> oauthCache.get("3");
    actualResults[3] = <string> oauthCache.get("4");
    actualResults[4] = <string> oauthCache.get("5");


    test:assertEquals(actualResults, expectedResults, msg = "Single Node Non capacity exceed basic test fails");
}
