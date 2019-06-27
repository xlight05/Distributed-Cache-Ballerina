import ballerina/io;
import distributed_cache as cache;
import ballerina/runtime;
public function main(string... args) {
    _ = cache:connectToCluster();
    cache:Cache oauthCache =  new("oauthCache",expiryTimeMillis=30000000);
    oauthCache.put("D", "D");
    oauthCache.put("E", "E");
    oauthCache.put("F", "F");
    runtime:sleep (500000);

}
