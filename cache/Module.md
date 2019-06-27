# Package Overview
Distributed Cache for ballerina

## Compatibility

| Ballerina Version  |
|:------------------:|
| 0.991.0           |

### Features

- Core Distributed in memory storage using consistent hashing
- Node Add/Remove in runtime with data reallocation
- Data Replication with Dynamic/Adaptive Replication
- Failure detection
- Cluster Discovery
- Consensus protocol


### Todos

- Performance analysis
- Make Raft reusable with objects

### Get started
```ballerina
    import anjanas/cache;
    
    cache:connectToCluster();
    cache:Cache oauthCache = new("oauthCache");
    oauthCache.put("Name", "Ballerina");
    io:println (<string>oauthCache.get("Name"));
```
