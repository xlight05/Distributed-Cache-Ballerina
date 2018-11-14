import ballerina/io;
import ballerina/time;
public function main() {
    map <string> testmap1;
    foreach item in 0... 10000 {
        string item1 = <string> item;
	    testmap1[item1] = "yayy";
    }

    map <string> testmap2;
    testmap2["0"] = "yayy";

    int startingTime = time:nanoTime();
    string x =  testmap1["500"] ?: "not found";
    int endingTime = time:nanoTime();

    io:println(endingTime-startingTime);

    int startingTime1 = time:nanoTime();
    string y =  testmap2["0"] ?: "not found";
    int endingTime1 = time:nanoTime();

    io:println(endingTime1-startingTime1);
}
