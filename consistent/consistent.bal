import ballerina/io;
import ballerina/crypto;
import ballerina/math;
import sort;

public type ConsistentHash object {
    map<string> circle;
    map<boolean>members;
    public int [] sortedHashes;
    int numberOfReplicas;
    int count;

    public new (numberOfReplicas = 20){

    }

    public function add(string nodeName) {
        int i=0;
        while (i<numberOfReplicas){
            string nodeReplicaHash = <string>getCrc32HashDecimal(i+nodeName);
            circle[nodeReplicaHash]=nodeName;
            i++;
        }
        updateSortedHashes();
        count++;
    }



    function updateSortedHashes (){
        int [] sortArr ;
        //check 1/4

        foreach key,value in circle {
            sortArr[lengthof sortArr]=check <int>key;
        }
        sortArr =sort:mergeSort(sortArr);

        sortedHashes = sortArr;
    }

    public function get (string key)returns string{
        int enteredKeyHash = getCrc32HashDecimal (key);
        int ind =0;
        // foreach index,hash in sortedHashes {
        //     if (sortedHashes[index]>enteredKeyHash){
        //         ind = index;
        //         break;
        //     }
        // }

        int i =0;
		int j =lengthof sortedHashes;
	while (i < j) {

		int h = <int>math:floor((i+j)/2);
		// io:println("i=",i);
		// io:println("j=",j);
		// io:println("h=",h);
		// io:println();

		if !(sortedHashes[h]>enteredKeyHash) {
			i = h + 1; // preserves f(i-1) == false
		} else{
			j = h; // preserves f(j) == true
		}
	}
        if (i>= lengthof sortedHashes){
            i=0;
        }

        var cir = circle[<string>sortedHashes[i]];
        match cir {
            string str=> {
                return str;
            }
            () => {
                io:println("Not found");
            }
        }
        return "Not found";

    }


    function hexToDecimal(string hex) returns int{
        string hexRep = "0123456789abcdef";
        int counter =(hex.length())-1;
        int sum = 0;
        int i =0;
        while (i <hex.length()){
            string c =hex.substring (i,i+1);
            int j = hexRep.indexOf(c);
            sum =  <int>(sum + (math:pow(16,counter))*j);
            counter--;
            i++;
        }
    return sum;
    }

    function getCrc32HashDecimal (string key) returns int{
        string crc32HashHex = crypto:crc32 (key);
        int crc32HashDecimal = hexToDecimal (crc32HashHex);
        return crc32HashDecimal;
    }

};


