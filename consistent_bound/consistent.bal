import ballerina/io;
import ballerina/math;
import ballerina/crypto;
import sort;
import ballerina/log;


//TODO Make this thread safe
type Config record {
    int partitionCount;
    int replicationFactor;
    float load;
};


public type Consistent object {
    int[] sortedSet;
    int partitionCount;
    int replicationFactor;
    map<float> loads;
    map<string> members;
    map<string> partitions;
    map<string> ring;
    float load;

    public new(partitionCount = 7, replicationFactor = 20, load = 1.25) {

    }

    public function add(string member) {
        //TODO check if member exists)
        //Add
        foreach i in 0...replicationFactor - 1 {
            string key = member + i;
            int hash = getCrc32HashDecimal(key);
            ring[<string>hash] = member;
            sortedSet[lengthof sortedSet] = hash;
            sortedSet = sort:mergeSort(sortedSet);
            members[member] = member;
        }
        distributePartitions();
        //Distribute partitions
    }

    function distributePartitions() {
        map<float> loadsTemp;
        map<string> partitionsTemp;
        foreach partID in 0...partitionCount - 1  {
            int key = getCrc32HashDecimal(<string>partID);
            int i = 0;
            int j = lengthof sortedSet;
            while (i < j) {

                int h = <int>math:floor((i + j) / 2);
                //binary search

                if !(sortedSet[h] >= key) {
                    i = h + 1; // preserves f(i-1) == false
                } else {
                    j = h; // preserves f(j) == true
                }
            }
            if (i >= lengthof sortedSet){
                i = 0;
            }
            //io:println(loadsTemp);
            //distribute with load
            distributeWithLoad(partID, i, loadsTemp, partitionsTemp);
        }
        loads = loadsTemp;
        partitions = partitionsTemp;
    }

    function distributeWithLoad(int partID, int id, map<float> loadsTemp, map<string> partitionsTemp) {
        int idx = id;
        float avgLoad = averageLoad();
        // Complete
        int count = 0;
        while (true) {
            count++;
            if (count >= lengthof sortedSet){
                log:printError("not enough room to distribute partitions");
            }
            int i = sortedSet[idx];
            var memberVar = ring[<string>i];
            //match
            string member;
            match memberVar {
                string x => {
                    member = x;
                }
                () => {
                    io:println("Fked");
                }
            }
            float loadT = 0;
            var loadVar = loadsTemp[member];
            match loadVar {
                float x => {
                    loadT = x;
                }
                () => {
                    loadsTemp[member] = 0;
                    loadT = 0;
                }
            }
            if (loadT + 1 <= avgLoad){
                partitionsTemp[<string>partID] = member;
                var loadz = loadsTemp[member];
                match loadz {
                    float x => {
                        loadsTemp[member] = x + 1;
                    }
                    () => {
                        //io:println("Fucked");
                    }
                }
                return;
            }
            idx++;
            if (idx >= lengthof sortedSet){
                idx = 0;
            }
        }
    }
    public function averageLoad() returns float {
        float avgLoad = (<float>(partitionCount / lengthof members)) * load;
        return math:ceil(avgLoad);
    }

    public function get(string key) returns string {
        int partID = findPartID(key);
        return getPartitionOwner(partID);
    }

    function findPartID(string key) returns int {
        int hkey = getCrc32HashDecimal(key);
        return (hkey % partitionCount);
    }

    function getPartitionOwner(int partnerID) returns string {
        var x = partitions[<string>partnerID];
        match x {
            string y => {
                return y;
            }
            () => {
                return "Not found";
            }
        }
    }

    //Converts Hex in to Decimal vakye
    function hexToDecimal(string hex) returns int {
        string hexRep = "0123456789abcdef";
        int counter = (hex.length()) - 1;
        int sum = 0;
        int i = 0;
        while (i < hex.length()) {
            string c = hex.substring(i, i + 1);
            int j = hexRep.indexOf(c);
            sum = <int>(sum + (math:pow(16, counter)) * j);
            counter--;
            i++;
        }
        return sum;
    }

    //Converts key into the Decimal value of crc32 hash
    function getCrc32HashDecimal(string key) returns int {
        string crc32HashHex = crypto:crc32(key);
        int crc32HashDecimal = hexToDecimal(crc32HashHex);
        return crc32HashDecimal;
    }

    function getClosestN(int partID, int count) returns string[] {
        string owner = getPartitionOwner(partID);
        string[] res;
        int[] keys;
        map<string> kmems;
        int ownerKey;
        foreach key, value in members {
            int nodeKey = getCrc32HashDecimal(key);
            if (key == owner){
                ownerKey = nodeKey;
            }
            keys[lengthof keys] = nodeKey;
            kmems[<string>nodeKey] = value;
        }
        keys = sort:mergeSort(keys);

        int idx;
        while (idx < lengthof keys) {
            if (keys[idx] == ownerKey){
                break;
            }
            idx++;
        }

        while (lengthof res < count) {
            idx++;
            if (idx >= lengthof keys){
                idx = 0;
            }
            int key = keys[idx];
            var kee = kmems[<string>key];
            match kee {
                string str => {
                    res[lengthof res] = str;
                }
                () => {
                    io:println("fuckeeedd");
                }
            }
        }
        return res;
    }


    public function GetClosestN(string key, int count) returns string[] {
        int partID = findPartID(key);
        return getClosestN(partID, count);
    }

    public function removeNode(string nodeKey) {
        //io:println(members);
        if (!members.hasKey(nodeKey)){
            return;
        }
        foreach i in 0...replicationFactor - 1 {
            string key = nodeKey + i;
            int hash = getCrc32HashDecimal(key);
            _ = ring.remove(<string>hash);

            foreach item in sortedSet {
                if (hash == item){
                    sortedSet = removeItemFromArray(sortedSet, hash);
                }
            }
        }
        _ = members.remove(nodeKey);
        if (lengthof members == 0){
            map<string> part;
            partitions = part; //check again
        }
        distributePartitions();
    }

    function removeItemFromArray(int[] originalArr, int item) returns int[] {
        int[] newArr;
        foreach i, value in originalArr{
            if (originalArr[i] == item){

                foreach index in 0...i - 1{
                    newArr[index] = originalArr[index];
                }
                foreach j in i ... (lengthof originalArr) - 2 {
                    newArr[j] = originalArr[j + 1];
                }
                return newArr;
            }
        }
        return originalArr;
    }


};

// function main(string... args) {
//     Consistent c = new();
//     c.add("test");
//     c.add("test2");
//     c.add("test3");


//     string x = c.locateKey("key1");
//     io:println(x);
//     io:println(c.locateKey("key2"));
//     io:println(c.locateKey("key3"));
//     io:println(c.locateKey("key4"));
//     io:println(c.locateKey("key5"));


//     //io:println(c.GetClosestN("key1",2));
//     //io:println(c.removeNode("key1"));
//     c.removeNode("test");

//     io:println();

//     io:println(c.locateKey("key1"));
//     io:println(c.locateKey("key2"));
//     io:println(c.locateKey("key3"));
//     io:println(c.locateKey("key4"));
//     io:println(c.locateKey("key5"));
// }