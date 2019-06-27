# Package Overview
Distributed Cache for ballerina using Raft

## Compatibility

|                                 |       Version                  |
|  :---------------------------:  |  :---------------------------: |
|  Ballerina Language             |   0.991.0                      |

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
    import anjanas/distributed_cache as cache;
    
    cache:connectToCluster();
    cache:Cache oauthCache = new("oauthCache");
    oauthCache.put("Name", "Ballerina");
    io:println (<string>oauthCache.get("Name"));
```

### Configuration documentation
```toml
[cache]
ip="http://localhost" #IP of the Node
port=7000 #Port for cache communication
hosts="" #IP and ports of the known nodes in the cluster
capacity=10 #Entry capacity for the node
request.timeout=2000   #Timeout of remote calls
relocation.timeout=60000 #Timeout for a relocation request
eviction.factor=0.1 #Amount of entries will be cleaned in a cleanup
replication.fact=1 #Replicas available of one entry

[local.cache]
enabled=false #Local cache availability
capacity=100 #Capacity of the local cache
eviction.factor=0.25 #Eviction factor of the local cache

[raft] #Keep these at default values if you don't know about Raft
min.election.timeout=2000 #Maximum election timeout for raft
max.election.timeout=3000 #Minimum election timeout for raft
heartbeat.timeout=1000 #Heartbeat timeout for raft

[failure.detector]
suspect.value=10 # Suspect points increased for each connection failure
timeout.millis=1000 # Timeout for failure detector
backoff.fact=1 # Backoff factor for failure detector

[consistent.hashing]
partitions=7 # Number of partitions in the hash ring
```

