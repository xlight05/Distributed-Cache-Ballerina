
// LogEntry[] leaderLog = [{},{term:1},{term:1},{term:1},{term:4},{term:4},{term:5},{term:5},{term:6},{term:6},{term:6}];
// int peerNextIndex=lengthof leaderLog;
// public function main(string... args) {
//     run();
//     run();
//     run();
//     run();
// }

// function run() {
//     AppendEntries appendEntry={
//         term:6,
//         leaderID:"http://localhost:4000",
//         prevLogIndex:peerNextIndex-1,
//         prevLogTerm:leaderLog[peerNextIndex-1].term,
//         //entries:
//         leaderCommit:
//     };
//     AppendEntriesResponse resz  = heartbeatHandle(appendEntry);

//     if (resz.sucess){
//         //matchindex
//         peerNextIndex = resz.followerMatchIndex+1;
//     }
//     else {
//         peerNextIndex = max(1,peerNextIndex-1);
//     }
// }

// function max(int x, int y)returns int {
//     if (x>y){
//         return x;
//     }
//     else {
//         return y;
//     }
// }

// function initLog() {
//     currentTerm=6;
//     log = [{},{term:1},{term:1},{term:1},{term:4},{term:4},{term:5},{term:5},{term:6},{term:6},{term:6},{term:6}];
// }






























//If follower has one entry missing

// int peerNextIndex=11;
// LogEntry[] leaderLog = [{},{term:1},{term:1},{term:1},{term:4},{term:4},{term:5},{term:5},{term:6},{term:6},{term:6}];
// public function main(string... args) {
//     initLog();
//     run();
//     run();
//     run();
//     run();
// }

// function run() {
//     LogEntry[] entryList;
//     foreach i in peerNextIndex...lengthof leaderLog-1 {
//         entryList[lengthof entryList]= leaderLog[i];
//     }
//     AppendEntries appendEntry={
//         term:6,
//         leaderID:"http://localhost:4000",
//         prevLogIndex:peerNextIndex-1,
//         prevLogTerm:leaderLog[peerNextIndex-1].term,
//         entries:entryList,
//         leaderCommit:9
//     };
//     AppendEntriesResponse resz  = heartbeatHandle(appendEntry);

//     if (resz.sucess){
//         //matchindex
//         peerNextIndex = resz.followerMatchIndex+1;
//     }
//     else {
//         peerNextIndex = max(1,peerNextIndex-1);
//     }
// }

// function max(int x, int y)returns int {
//     if (x>y){
//         return x;
//     }
//     else {
//         return y;
//     }
// }

// function initLog() {
//     currentTerm=6;
//     log = [{},{term:1},{term:1},{term:1},{term:4},{term:4},{term:5},{term:5},{term:6},{term:6}];
// }

// //Simple Heartbeat with upto date term and entries
// //
// //
// int peerNextIndex=11;
// public function main(string... args) {
//     run();
//     run();
//     run();
//     run();
// }

// function run() {
//     AppendEntries appendEntry={
//         term:6,
//         leaderID:"http://localhost:4000",
//         prevLogIndex:peerNextIndex-1,
//         prevLogTerm:getTerm(peerNextIndex-1),
//     //entries:
//     leaderCommit:10
//     };
//     AppendEntriesResponse resz  = heartbeatHandle(appendEntry);

//     if (resz.sucess){
//         //matchindex
//         peerNextIndex = resz.followerMatchIndex+1;
//     }
//     else {
//         peerNextIndex = max(1,peerNextIndex-1);
//     }
// }


// function initLog() {
//     currentTerm=6;
//     log = [{},{term:1},{term:1},{term:1},{term:4},{term:4},{term:5},{term:5},{term:6},{term:6},{term:6}];
// }