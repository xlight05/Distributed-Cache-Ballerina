import ballerina/io;
import ballerina/runtime;
import ballerina/math;
import ballerina/config;


//endpoint http:Client leaderEP {
//    url: config:getAsString("leaderIP", default = "3000")
//};
//
//public function main(string... args) {
//
//    //io:println ("Raft Starting");
//    //
//    //var ee = leaderEP->post("/server",<json>currentNode);
//    //match ee {
//    //    http:Response payload => {
//    //        ConfigChangeResponse result = check <ConfigChangeResponse> check payload.getJsonPayload();
//    //        grpc:Headers resHeaders;
//    //        if (result.sucess){
//    //            joinRaft();
//    //        }else {
//    //            io:println ("Failed to join :/");
//    //        }
//    //    }
//    //    error err => {
//    //
//    //    }
//    //}
//    io:println(<int>math:ceil(3 / 2.0));
//
//}

//public function main(string... args) {
//    io:println ("Raft Starting");
//    startRaft();
//}
// import ballerina/runtime;

// int voteCount =0;
// map <boolean> doneMap;
// public function main() {
//     future xx;
//     foreach i in 100...103 {
//         doneMap[<string>i]=false;
//          xx= start test(i);
//     }
//     while (true){
//         int count =0;
//         foreach item in doneMap {
//             if (item==true){
//                 count++;
//             }
//         }
//         if (count>=2){
//             io:println("Executed");
//             return;
//         }
//     }
//     //io:println(voteCount);
// }

// function test(int x){
//         int n = 1000;
//         foreach i in 1...n {
//             runtime:sleep(10);
//             io:println(x+" "+i);
//         }
//         doneMap[<string>x]=true;
// }






// public function main() {
//     fork {
//         worker name {
//             foreach i in 1...3{
            
//             }
//         }
//     } join (all) (map results) {
        
//     }
// }










// import ballerina/io;
// import ballerina/math;
// import ballerina/runtime;
// import ballerina/task;

// int count;
// task:Timer? timer;
// int interval = 1000;
// public function main() {
//     io:println("Timer task demo");

//     (function() returns error?) onTriggerFunction = cleanup;

//     function(error) onErrorFunction = cleanupError;

//     timer = new task:Timer(onTriggerFunction, onErrorFunction,
//                            interval);
    
    
//     timer.start();
    
//     runtime:sleep(30000); // Temp. workaround to stop the process from exiting.
// }

// function cleanup() returns error? {
//     interval = interval + 1000;
//     (function() returns error?) onTriggerFunction = cleanup;

//     function(error) onErrorFunction = cleanupError;
//     timer.stop();
//      timer = new task:Timer(onTriggerFunction, onErrorFunction,
//                            interval);
//     timer.start();
//     count = count + 1;
    
//     io:println(count);
    
//     if (count >= 10) {
        

//         timer.stop();
//         io:println("Stopped timer");
//     }
//     return ();
// }

// function cleanupError(error e) {
//     io:print("[ERROR] cleanup failed");
//     io:println(e);
// }


// public function main(string... args) {
//     foreach i in 100...103 {
//          _= start test(i);
//     }
// }

// function test(int x){
//     worker yay {
//         int n = 10;
//         foreach i in 1...n {
//             io:println(x+" "+i);
//         }
//         runtime:sleep(2000);
//         //test(x);
//     }
// }

// function name() {
    
// }





// import ballerina/io;
// import ballerina/math;
// import ballerina/runtime;
// import ballerina/task;

// int count;
// task:Timer? timer;

// public function main() {
//     io:println("Timer task demo");

//     (function() returns error?) onTriggerFunction = cleanup;

//     function(error) onErrorFunction = cleanupError;

//     timer = new task:Timer(onTriggerFunction, onErrorFunction,
//                            1000, delay = 500);
//     timer.start();

//     runtime:sleep(30000); // Temp. workaround to stop the process from exiting.
// }

// function cleanup() returns error? {
//     count = count + 1;
//     io:println("Cleaning up...");
//     io:println(count);

//     if (math:randomInRange(0, 10) == 5) {
//         error e = { message: "Cleanup error" };
//         return e;
//     }
    
//     if (count >= 10) {
        

//         timer.stop();
//         io:println("Stopped timer");
//     }
//     return ();
// }

// function cleanupError(error e) {
//     io:print("[ERROR] cleanup failed");
//     io:println(e);
// }



// future[] gg;
// public function main(string... args) {
//     future xx = start async1();
//     var yy = await xx;
//     io:println(xx.isDone());
// }


// function async1() {
//     foreach item in  100...105{
//         string x ="5";
//         future ee = start asyncRepeat(item);
//         gg[lengthof gg]=ee;
//     }
//     foreach item in gg {
//         var x = await item;
//     }
// }

// function asyncRepeat(int x) {
//     int n= 100;
//     foreach i in 1...n{
//         io:println(x+" "+i);
//     }
// }