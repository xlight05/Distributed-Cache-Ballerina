An optimized version of consistent hashing algorithm

# Package Overview

Consistent Hashing is a distributed hashing scheme that operates independently of the number of servers or objects in
 a distributed hash table by assigning them a position on an abstract circle, or hash ring. This implementation uses 
 crc32b as the hashing algorithm

## Compatibility

|                                 |       Version                  |
|  :---------------------------:  |  :---------------------------: |
|  Ballerina Language             |   0.991.0                      |

## Sample


```ballerina
    import anjanas/consistent_bound;
    
    Consistent hashRing = new ();
    hashRing.add("http://localhost:3000");
    hashRing.add("http://localhost:4000");
    hashRing.add("http://localhost:5000");
    string key = "oauth"; //Key here
    string node = hashRing.get(key);
    string actualNode = "http://localhost:5000";
```
