// import ballerina/io;
// import ballerina/runtime;
// import cache;
// //TODO FIX
// public function main(string... args) {
//     _ = cache:initNodeConfig();
//     cache:Cache oauthCache =  new("oauthCache",expiryTimeMillis=30000000);

//     oauthCache.put("G", "G");
//     oauthCache.put("H", "H");
//     oauthCache.put("I", "I");
    
//     any?[] results;
//     results [results.length()] = oauthCache.get("A");
//     results [results.length()] = oauthCache.get("B");
//     results [results.length()] = oauthCache.get("C");
//     results [results.length()] = oauthCache.get("D");
//     results [results.length()] = oauthCache.get("E");
//     results [results.length()] = oauthCache.get("F");
//     results [results.length()] = oauthCache.get("G");
//     results [results.length()] = oauthCache.get("H");
//     results [results.length()] = oauthCache.get("I");

//     io:println (results);
//     any?[] actualResults = ["A", "B", "C", "D", "E", "F", "G", "H", "I"];
//     foreach index,value in results {
//         if (results[index]==actualResults[index]){
//             io:println ("Test #"+index+" Passed");
//         }else {
//             io:println ("Test #"+index+" Failed");
//         }
//     }
//     runtime:sleep (5000);
// }
