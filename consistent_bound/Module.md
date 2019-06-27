Implementation of Consistent Hashing algorithm
# Module Overview
Consistent Hashing is a distributed hashing scheme that operates independently 
of the number of servers or objects by assigning them a position on a hash ring. 
This implementation uses crc32b as the hashing algorithm

## Compatibility

|                                 |       Version                  |
|  :---------------------------:  |  :---------------------------: |
|  Ballerina Language             |   0.991.0                      |

## Sample

```ballerina
    import anjanas/consistent_bound;
    
    Consistent hashRing = new ();
    hashRing.add("http://localhost:3000");
    string key = "oauth"; //Key here
    string node = hashRing.get(key);
```
