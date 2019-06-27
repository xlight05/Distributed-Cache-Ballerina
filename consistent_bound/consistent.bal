import ballerina/io;
import ballerina/math;
import ballerina/crypto;
import sort;
import ballerina/log;

public type Consistent object {
    int[] sortedSet=[]; //Sorted hash values of nodes
    int partitionCount=7; //Number of partitions in the ring. Keys are distributed among partitions
    int replicationFactor=20; //Number of replications of each member in te ring
    map<float> loads={}; // Load for each node
    map<string> members={}; //Original members map
    map<string> partitions={}; //Partition map
    map<string> ring={}; //hash ring
    float load=1.25; // Maxiumum relative load allowed per node

    public function __init(int partitionCount = 7,int replicationFactor = 20,float load = 1.25) {
        
    }

    # Adds a Node to the hash ring
    # +member - identifer of the member
    public function add(string member) {
        foreach var i in 0...self.replicationFactor - 1 {
            string key = member + i;
            int hash = getCrc32HashDecimal(key);
            self.ring[string.convert(hash)] = member;
            self.sortedSet[self.sortedSet.length()] = hash;
            self.sortedSet = sort:mergeSort(self.sortedSet);
            self.members[member] = member;
        }
        self.distributePartitions();
    }

    function distributePartitions() {
        map<float> loadsTemp={};
        map<string> partitionsTemp={};
        foreach var partID in 0...self.partitionCount - 1  {
            int key = getCrc32HashDecimal(string.convert(partID));
            int i = 0;
            int j = self.sortedSet.length();
            while (i < j) {
                int h = <int>math:floor(<float>((i + j) / 2));
                //binary search
                if !(self.sortedSet[h] >= key) {
                    i = h + 1; // preserves f(i-1) == false
                } else {
                    j = h; // preserves f(j) == true
                }
            }
            if (i >= self.sortedSet.length()){
                i = 0;
            }
            self.distributeWithLoad(partID, i, loadsTemp, partitionsTemp);
        }
        self.loads = loadsTemp;
        self.partitions = partitionsTemp;
    }

    function distributeWithLoad(int partID, int id, map<float> loadsTemp, map<string> partitionsTemp) {
        int idx = id;
        float avgLoad = self.averageLoad();
        // Complete
        int count = 0;
        while (true) {
            count = count +1;
            if (count >=self.sortedSet.length()){
                // User needs to decrease partition count, increase member count or increase load factor.
                error err = error("not enough room to distribute partitions" );
                panic err;
            }
            int i = self.sortedSet[idx];
            var member = self.ring[string.convert(i)]?:"";
            float loadT = loadsTemp[member]?:0.0;
            if (loadT + 1 <= avgLoad){
                partitionsTemp[string.convert(partID)] = member;
                loadsTemp[member]=(loadsTemp[member]?:0.0)+1;
                return;
            }
            idx = idx +1;
            if (idx >= self.sortedSet.length()){
                idx = 0;
            }
        }
    }
    # AverageLoad exposes the current average load.
    # +return - current average load
    function averageLoad() returns float {
        float avgLoad = (<float>(self.partitionCount /self.members.length())) * self.load;
        return math:ceil(avgLoad);
    }

    # Locates the node for a given key
    #+key - key item to be located
    #+return - Node identifier for the key
    public function get(string key) returns string {
        int partID = self.findPartID(key);
        return self.getPartitionOwner(partID);
    }

    # Gives partition ID for a given key
    #+ key- key
    #+ return - partion id of the key
    function findPartID(string key) returns int {
        int hkey = getCrc32HashDecimal(key);
        return (hkey % self.partitionCount);
    }

    # GetPartitionOwner returns the owner of the given partition
    #+ partID - id of the partition
    #+ return - owner node of the partition
    function getPartitionOwner(int partID) returns string {
        return self.partitions[string.convert(partID)]?:"";
    }

    # GetClosestN returns the closest N member to a key in the hash ring. This may be useful to find members for replication.
    #+key- target key
    #+count - closest N number
    #+return - closest node identifier array
    public function GetClosestN(string key, int count) returns string[] {
        int partID = self.findPartID(key);
        string[] res=[];
        string owner = self.getPartitionOwner(partID);
        int[] keys=[];
        map<string> kmems={};
        int ownerKey=0;
        //hash members
        foreach var (k, v) in self.members {
            int nodeKey = getCrc32HashDecimal(k);
            if (k == owner){
                ownerKey = nodeKey;
            }
            keys[keys.length()] = nodeKey;
            kmems[string.convert(nodeKey)] = v;
        }
        //sort members
        keys = sort:mergeSort(keys);
        //find member
        int idx=0;
        while (idx < keys.length()) {
            if (keys[idx] == ownerKey){
                break;
            }
            idx = idx +1;
        }
        //find the closest members
        while (res.length() < count) {
            idx =idx +1;
            if (idx >= keys.length()){
                idx = 0;
            }
            int keyEntry = keys[idx];
            res[res.length()]= kmems[string.convert(keyEntry)]?:"";
        }
        return res;
    }

    # Removes a node from the Hash Ring
    #+nodeKey - Node identifier
    public function removeNode(string nodeKey) {
        if (!self.members.hasKey(nodeKey)){
            // There is no member with that name. Quit immediately.
            return;
        }
        foreach var i in 0...self.replicationFactor - 1 {
            string key = nodeKey + i;
            int hash = getCrc32HashDecimal(key);
            _ = self.ring.remove(string.convert(hash));

            foreach var item in self.sortedSet {
                if (hash == item){
                    self.sortedSet = removeItemFromArray(self.sortedSet, hash);
                }
            }
        }
        _ = self.members.remove(nodeKey);
        if (self.members.length() == 0){
            map<string> part={};
            // consistent hash ring is empty now. Reset the partition table.
            self.partitions = part;
            return;
        }
        self.distributePartitions();
    }
};
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
        string crc32HashHex = crypto:crc32b(key);
        int crc32HashDecimal = hexToDecimal(crc32HashHex);
        return crc32HashDecimal;
    }

    # Removes item from a given array
    #+ originalArr - main array
    #+ item - item that should be removed from array
    #+ return - new array after removing the elemnt
    function removeItemFromArray(int[] originalArr, int item) returns int[] {
        int[] newArr=[];
        foreach var i in 0...(originalArr.length()-1) {
            if (originalArr[i] == item){
                foreach var index in 0...i - 1{
                    newArr[index] = originalArr[index];
                }
                foreach var j in i ... (originalArr.length()) - 2 {
                    newArr[j] = originalArr[j + 1];
                }
                return newArr;
            }
        }
        return originalArr;
    }