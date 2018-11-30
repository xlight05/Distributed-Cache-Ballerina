import ballerina/io;
import ballerina/runtime;
import cache;
public function main(string... args) {
    _ = cache:initNodeConfig();
    cache:Cache oauthCache =  new("oauthCache",expiryTimeMillis=30000000);

    oauthCache.put("G", "G");
    oauthCache.put("H", "H");
    oauthCache.put("I", "I");
    
    any?[] results;
    results [lengthof results] = oauthCache.get("A");
    results [lengthof results] = oauthCache.get("B");
    results [lengthof results] = oauthCache.get("C");
    results [lengthof results] = oauthCache.get("D");
    results [lengthof results] = oauthCache.get("E");
    results [lengthof results] = oauthCache.get("F");
    results [lengthof results] = oauthCache.get("G");
    results [lengthof results] = oauthCache.get("H");
    results [lengthof results] = oauthCache.get("I");

    io:println (results);
    any?[] actualResults = ["A", "B", "C", "D", "E", "F", "G", "H", "I"];
    foreach index,value in results {
        if (results[index]==actualResults[index]){
            io:println ("Test #"+index+" Passed");
        }else {
            io:println ("Test #"+index+" Failed");
        }
    }
    runtime:sleep (5000);
}
