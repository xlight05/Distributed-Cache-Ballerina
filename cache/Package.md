# Package Overview
Distributed Cache for ballerina

## Compatibility

| Ballerina Version  |
|:------------------:|
| 0.983.0            |

### Features

- Core Distributed in memory storage using consistant hashing
- Node Add/Remove in runtime with data realocation
- Data Replication with Dynamic/Adaptive Replication
- Failure detection
- Cluster Discovery
- Consensus protocol


### Todos

- Refactor code,Documentation
- Performance analysis
- Healthcheck feature

### Get started

	_ = cache:initNodeConfig();
    cache:Cache oauthCache = new("oauthCache");
    oauthCache.put("Name", "Ballerina");
    io:println (<string>oauthCache.get("Name"));
