import cache;
import ballerina/runtime;
// import ballerina/http;
// import ballerina/config;

 function main(string... args) {

     _ =cache:initNodeConfig();
     runtime:sleep(100000000000);
 }