#!/bin/bash
port=2000
api=7000
counter=1
lastHost=""
while [ $counter -le 2 ]
do
url="http://localhost"
urlWithPort=$url":"$port
firstChar=${lastHost:0:1}
if [ "$firstChar" = "," ]
then
     lastHost="${lastHost:1}"
fi
##Open in a new window
gnome-terminal -e "ballerina run -e cache.port=$port -e cache.ip="http://localhost" -e cache.id=$counter -e cache.api=$api -e cache.hosts=$lastHost -e cache.capacity=10000000 -e cache.eviction.factor=0.25 -e cache.replication.fact=1 -e cache.local.cache=false -e consistent.hashing.partitions=7 -e benchmark.iteration=1 -e benchmark.nodes=3 -e raft.ip="http://localhost" -e raft.port=$port -e raft.min.election.timeout=2000 -e raft.max.election.timeout=3000 -e raft.heartbeat.timeout=1000 member.bal "
lastHost=$lastHost","$urlWithPort
    ((port++))
    ((counter++))
    ((api++))
done
gnome-terminal -e "ballerina run -e cache.port=$port -e cache.ip="http://localhost" -e cache.id=$counter -e cache.api=$api -e cache.hosts=$lastHost -e cache.capacity=10000000 -e cache.eviction.factor=0.25 -e cache.replication.fact=1 -e cache.local.cache=false -e consistent.hashing.partitions=7 -e benchmark.iteration=1 -e benchmark.nodes=3 -e raft.ip="http://localhost" -e raft.port=$port -e raft.min.election.timeout=2000 -e raft.max.election.timeout=3000 -e raft.heartbeat.timeout=1000 get_bench.bal"
