import ballerina/io;



//
//////s2 sending to s3 - should pass
//public function main(string... args) {
//    initLog();
//    VoteRequest voteReq = {term:6+1,candidateID:"http://localhost:4000",lastLogIndex:4,lastLogTerm:6};
//    boolean x = voteResponseHandle(voteReq);
//    io:println(x);
//}
//
//function initLog() {
//     votedFor="None";
//     currentTerm=5;
//     commitIndex=3;
//     log = [{},{term:2},{term:2},{term:5},{term:5}];
// }


////s3 sending to s2 - should fail
//public function main(string... args) {
//    initLog();
//    VoteRequest voteReq = {term:5+1,candidateID:"http://localhost:4000",lastLogIndex:4,lastLogTerm:5};
//    boolean x = voteResponseHandle(voteReq);
//    io:println(x);
//}
//
// function initLog() {
//     votedFor="None";
//     currentTerm=6;
//     commitIndex=3;
//     log = [{},{term:2},{term:2},{term:5},{term:6}];
// }