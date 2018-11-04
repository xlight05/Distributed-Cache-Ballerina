import ballerina/io;
import ballerina/crypto;
import ballerina/math;
import sort;
import ballerina/log;

public type ConsistentHash object {
    map<string> circle;
    map<boolean> members;
    public int[] sortedHashes;
    int numberOfReplicas;
    int count;

    //User can specify number of replicas when object is being created
    public new(numberOfReplicas = 20) {

    }

    //documentation {
    //Adds a node to the hash ring
    //P{{nodeName}} name of the node (identifier)
    //}
    public function add(string nodeName) {
        int i = 0;
        //creates replica nodes (virtual nodes)
        while (i < numberOfReplicas) {
            string nodeReplicaHash = <string>getCrc32HashDecimal(i + nodeName);
            circle[nodeReplicaHash] = nodeName;
            i = i+1;
        }
        updateSortedHashes();
        count = count +1;
    }

    //Uodate sortedHashes array with the key of circle map
    function updateSortedHashes() {
        int[] sortArr;
        //check 1/4

        foreach key, value in circle {
            sortArr[lengthof sortArr] = check <int>key;
        }
        sortArr = sort:mergeSort(sortArr);

        sortedHashes = sortArr;
    }

    //documentation {
    //Used to identify which node is repnsible for handling the data for the key
    //P{{key}} Key of the data or node identifier
    //R{{}} Responsible node for the given key
    //}
    public function get(string key) returns string {
        int enteredKeyHash = getCrc32HashDecimal(key);
        int ind = 0;

        int i = 0;
        int j = lengthof sortedHashes;
        while (i < j) {
            int h = <int>math:floor((<float>(i + j) / 2)); //binary search

            if !(sortedHashes[h] > enteredKeyHash) {
                i = h + 1; // preserves f(i-1) == false
            } else {
                j = h; // preserves f(j) == true
            }
        }
        if (i >= lengthof sortedHashes){
            i = 0;
        }

        var cir = circle[<string>sortedHashes[i]];
        match cir {
            string str => {
                return str;
            }
            () => {
                log:printWarn("Not found");
            }
        }

        return "Not found";

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
            sum = <int>(sum + (math:pow(16.0, <float>counter)) * j);
            counter = count -1;
            i = i +1;
        }
        return sum;
    }

    //Converts key into the Decimal value of crc32 hash
    function getCrc32HashDecimal(string key) returns int {
        string crc32HashHex = crypto:crc32(key);
        int crc32HashDecimal = hexToDecimal(crc32HashHex);
        return crc32HashDecimal;
    }

};


