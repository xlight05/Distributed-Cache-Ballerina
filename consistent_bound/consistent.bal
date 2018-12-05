import ballerina/io;
import ballerina/math;
import ballerina/crypto;
import sort;
import ballerina/log;

public type Consistent object {
    int[] sortedSet; //Sorted hash values of nodes
    int partitionCount; //Number of partitions in the ring. Keys are distributed among partitions
    int replicationFactor; //Number of replications of each member in te ring
    map<float> loads; // Load for each node
    map<string> members; //Original members map
    map<string> partitions; //Partition map
    map<string> ring; //hash ring
    float load; // Maxiumum relative load allowed per node

    public new(partitionCount = 7, replicationFactor = 20, load = 1.25) {

    }

    # Adds a Node to the hash ring
    # +member - identifer of the member
    public function add(string member) {
        foreach i in 0...replicationFactor - 1 {
            string key = member + i;
            int hash = getCrc32HashDecimal(key);
            ring[<string>hash] = member;
            sortedSet[lengthof sortedSet] = hash;
            sortedSet = sort:mergeSort(sortedSet);
            members[member] = member;
        }
        distributePartitions();
    }

    function distributePartitions() {
        map<float> loadsTemp;
        map<string> partitionsTemp;
        foreach partID in 0...partitionCount - 1  {
            int key = getCrc32HashDecimal(<string>partID);
            int i = 0;
            int j = lengthof sortedSet;
            while (i < j) {
                int h = <int>math:floor(<float>((i + j) / 2));
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
            count = count +1;
            if (count >= lengthof sortedSet){
                // User needs to decrease partition count, increase member count or increase load factor.
                error err = { message: "not enough room to distribute partitions" };
                throw err;
            }
            int i = sortedSet[idx];
            var member = ring[<string>i]?:"";
            float loadT = loadsTemp[member]?:0.0;
            if (loadT + 1 <= avgLoad){
                partitionsTemp[<string>partID] = member;
                loadsTemp[member]=(loadsTemp[member]?:0.0)+1;
                return;
            }
            idx = idx +1;
            if (idx >= lengthof sortedSet){
                idx = 0;
            }
        }
    }
    # AverageLoad exposes the current average load.
    # +return - current average load
    function averageLoad() returns float {
        float avgLoad = (<float>(partitionCount / lengthof members)) * load;
        return math:ceil(avgLoad);
    }

    # Locates the node for a given key
    #+key - key item to be located
    #+return - Node identifier for the key
    public function get(string key) returns string {
        int partID = findPartID(key);
        return getPartitionOwner(partID);
    }

    # Gives partition ID for a given key
    #+ key- key
    #+ return - partion id of the key
    function findPartID(string key) returns int {
        int hkey = getCrc32HashDecimal(key);
        return (hkey % partitionCount);
    }

    # GetPartitionOwner returns the owner of the given partition
    #+ partID - id of the partition
    #+ return - owner node of the partition
    function getPartitionOwner(int partID) returns string {
        return partitions[<string>partID]?:"";
    }

    # Converts Hex in to Decimal vakye
    #+hex - Hex string
    #+return - Decimal int
    function hexToDecimal(string hex) returns int {
        //hex chars
        string hexRep = "0123456789abcdef";
        int counter = (hex.length()) - 1;
        int sum = 0;
        int i = 0;
        while (i < hex.length()) {
            string c = hex.substring(i, i + 1);
            int j = hexRep.indexOf(c);
            sum = <int>(sum + (math:pow(16.0, <float>counter)) * j);
            counter = counter -1;
            i = i +1;
        }
        return sum;
    }

    # Converts key into the Decimal value of crc32 hash
    #+key- key that needs to be hashed
    #+return - decimal of the crc32 hash
    function getCrc32HashDecimal(string key) returns int {
        string crc32HashHex = crypto:crc32(key);
        int crc32HashDecimal = hexToDecimal(crc32HashHex);
        return crc32HashDecimal;
    }

    # GetClosestN returns the closest N member to a key in the hash ring. This may be useful to find members for replication.
    #+key- target key
    #+count - closest N number
    #+return - closest node identifier array
    public function GetClosestN(string key, int count) returns string[] {
        int partID = findPartID(key);
        string[] res;
        string owner = getPartitionOwner(partID);
        int[] keys;
        map<string> kmems;
        int ownerKey;
        //hash members
        foreach k, v in members {
            int nodeKey = getCrc32HashDecimal(k);
            if (k == owner){
                ownerKey = nodeKey;
            }
            keys[lengthof keys] = nodeKey;
            kmems[<string>nodeKey] = v;
        }
        //sort members
        keys = sort:mergeSort(keys);
        //find member
        int idx;
        while (idx < lengthof keys) {
            if (keys[idx] == ownerKey){
                break;
            }
            idx = idx +1;
        }
        //find the closest members
        while (lengthof res < count) {
            idx =idx +1;
            if (idx >= lengthof keys){
                idx = 0;
            }
            int keyEntry = keys[idx];
            res[lengthof res]= kmems[<string>keyEntry]?:"";
        }
        return res;
    }

    # Removes a node from the Hash Ring
    #+nodeKey - Node identifier
    public function removeNode(string nodeKey) {
        if (!members.hasKey(nodeKey)){
            // There is no member with that name. Quit immediately.
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
            // consistent hash ring is empty now. Reset the partition table.
            partitions = part;
            return;
        }
        distributePartitions();
    }
    # Removes item from a given array
    #+ originalArr - main array
    #+ item - item that should be removed from array
    #+ return - new array after removing the elemnt
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
