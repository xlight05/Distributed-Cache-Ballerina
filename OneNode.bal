
import ballerina/io;
import cache;
public function main(string... args) {
    _ = cache:initNodeConfig();
    cache:Cache oauthCache =  new("oauthCache",expiryTimeMillis=30000000);
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
    oauthCache.put("12", "12");

    any?[] results;
    results [lengthof results] = oauthCache.get("1");
    results [lengthof results] = oauthCache.get("2");
    results [lengthof results] = oauthCache.get("3");
    results [lengthof results] = oauthCache.get("4");
    results [lengthof results] = oauthCache.get("5");
    results [lengthof results] = oauthCache.get("6");
    results [lengthof results] = oauthCache.get("7");
    results [lengthof results] = oauthCache.get("8");
    results [lengthof results] = oauthCache.get("9");
    results [lengthof results] = oauthCache.get("10");
    results [lengthof results] = oauthCache.get("11");
    results [lengthof results] = oauthCache.get("12");

    any?[] actualResults = ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"];
    foreach index,value in results {
        if (results[index]==actualResults[index]){
            io:println ("Test #"+index+" Passed");
        }else {
            io:println ("Test #"+index+" Failed");
        }
    }
}
