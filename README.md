# Distributed-Cache-Ballerina

### Features

- Multiple Node Support ( Add / Remove Node in runtime)
- Supports put and get methods.
- Near Cache
- Data distribution based on consitent hashing
- 

### Todos
- ~~Reduce Port usage and avoid port clashing~~ - Done
- Add code comments, handle edge cases, improve readability , refactor according to ballerina spec - In progress
- Implement consistent hashing. - Done
	Add node and Key location is implemented sucessfully in the application.
	After new node has been added, we need to relocate the data that has changed. - In Progress

	Implement Consistent hashing with bounded values help data distribute equally in the cluster. - Next

- Implement Raft protocol. - Not started
	Well,all distributed systems can fail at any given time. Raft is a consensus protocol that helps you to manage  nodes  even when there is a node failure. Implementating raft in to cache will drastiacally increase reliability of the distributed system.



### Get started

    cache:Cache oauthCache = new("oauthCache");
    oauthCache.put("Name", "Ballerina");
    io:println (<string>oauthCache.get("Name"));
