import ballerina/io;
import cache;
import ballerina/runtime;
public function main(string... args) {
    _ = cache:initNodeConfig();
    cache:Cache oauthCache =  new("oauthCache",expiryTimeMillis=30000000);
    oauthCache.put("D", "D");
    oauthCache.put("E", "E");
    oauthCache.put("F", "F");
    runtime:sleep (5000);

}
