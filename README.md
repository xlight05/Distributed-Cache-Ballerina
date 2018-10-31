# Distributed-Cache-Ballerina

### Planned Features

- Core Distributed in memory storage using consistant hashing
- Node Add/Remove in runtime with data realocation
- Data Replication with Dynamic/Adaptive Replication
- Failure detection
- Cluster Discovery
- Consensus protocol


### Todos

- Refactor code,Documentation
- Merge clientMap in node.bal with raft.
- Node add / remove function
- Performance analysis


### Get started

	_ = cache:initNodeConfig();
    cache:Cache oauthCache = new("oauthCache");
    oauthCache.put("Name", "Ballerina");
    io:println (<string>oauthCache.get("Name"));
