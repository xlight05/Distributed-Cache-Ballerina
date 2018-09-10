# Package Overview
Distributed Cache for ballerina

## Compatibility

| Ballerina Version  |
|:------------------:|
| 0.981.1            |

### Features

- Multiple Node Support ( Add / Remove Node in runtime)
- Supports put and get methods.
- Near Cache
- Data distribution based on consitent hashing

### Todos
- ~~Reduce Port usage and avoid port clashing~~ - Done
- Add code comments, handle edge cases, improve readability , refactor according to ballerina spec - In progress
- Implement consistent hashing. - In Progress
	Initially, 
	Put method uses round robin mechanism with ballerina load balancer(lol).
	get method checks all the nodes until you find the value for the given key.(lol)
	Consistent hashing will drastically improve performance on both reads and writes also it will change data distribution among nodes,changes node add and remove process too.
	Currently,
	Add node and Key location is implemented sucessfully in the application.
	After new node has been added, we need to relocate the data that has changed. - In Progress

	Implement Consistent hashing with bounded values help data distribute equally in the cluster. - Next

- Implement Raft protocol. - Not started
	Well,all distributed systems can fail at any given time. Raft is a consensus protocol that helps you to manage  nodes  even when there is a node failure. Implementating raft in to cache will drastiacally increase reliability of the distributed system.

### How it works (In initial version)
- Constructor has one required parameter and string rest parameter.
First parameter is your ip address. Rest parameter is the existing nodes in your cluster.
- Once you run the constructor it sends a broadcast to all existing nodes in your cluster saying a new node has joined.(static :/)
- Put method
	Once you execute put method it will send the key and value to load balancer service and it will distribute those data using round robin pattern. (This will be changed in next version)
- Get method
	It gets node list from node service and checks if the key exist in each node.



### Get started


###### Node 1
`cache:Cache cache = new ("http://<ip of the current node>");`

###### Node 2
`cache:Cache cache = new ("http://<ip of the current node>","http://<ip of node 1>");`

###### Node 3
`cache:Cache cache = new ("http://<ip of the current node>","http://<ip of node 1>","http://<ip of node 2>");`

