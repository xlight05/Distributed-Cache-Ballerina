import ballerina/io;
import ballerina/runtime;
import cache;
public function main(string... args) {
    _ = cache:connectToCluster();
    cache:Cache oauthCache =  new("oauthCache",expiryTimeMillis=30000000);
    oauthCache.put("A", "A");
    oauthCache.put("B", "B");
    oauthCache.put("C", "C");
    runtime:sleep (500000);

}
