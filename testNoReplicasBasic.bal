import ballerina/test;
import ballerina/config;
import ballerina/io;
import cache;


@test:Config
function testNoReplicasBasic() {
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

    string[] expectedResults = ["1","2","3","4","5","6","7","8","9","10"];
    string[] actualResults = [];
    actualResults[0] = <string> oauthCache.get("1");
    actualResults[1] = <string> oauthCache.get("2");
    actualResults[2] = <string> oauthCache.get("3");
    actualResults[3] = <string> oauthCache.get("4");
    actualResults[4] = <string> oauthCache.get("5");
    actualResults[5] = <string> oauthCache.get("6");
    actualResults[6] = <string> oauthCache.get("7");
    actualResults[7] = <string> oauthCache.get("8");
    actualResults[8] = <string> oauthCache.get("9");
    actualResults[9] = <string> oauthCache.get("10");

    test:assertEquals(actualResults, expectedResults, msg = "Single Node Non capacity exceed basic test fails");
}
