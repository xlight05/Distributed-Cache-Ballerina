#!/bin/bash
# Basic while loop
# counter=1
# while [ $counter -le 10 ]
# do
#     echo $counter
#     ((counter++))
# done

port=2000
api=7000
counter=1
lastHost=""
#echo "Total Nodes?"
#totalNodes=read
while [ $counter -le 2 ]
do
url="http://localhost"
urlWithPort=$url":"$port
firstChar=${lastHost:0:1}
if [ "$firstChar" = "," ]
then
     lastHost="${lastHost:1}"
fi
#echo $lastHost

##Open in a new window
#ballerina run Client.bal -e port=$port -e ip="http://localhost" -e id=$id -e api=$api -e hosts=$lastHost
gnome-terminal -e "ballerina run -e cache.port=$port -e cache.ip="http://localhost" -e cache.id=$counter -e cache.api=$api -e cache.hosts=$lastHost -e cache.capacity=10000000 -e cache.evictionFactor=0.25 -e cache.replicationFact=1 -e cache.localCache=false -e consistent.hashing.partitions=7 -e benchmark.iteration=1 -e benchmark.nodes=3 -e raft.ip="http://localhost" -e raft.port=$port -e raft.min.election.timeout=2000 -e raft.max.election.timeout=3000 -e raft.heartbeat.timeout=1000 member.bal "
lastHost=$lastHost","$urlWithPort
    ((port++))
    ((counter++))
    ((api++))
done
gnome-terminal -e "ballerina run -e cache.port=$port -e cache.ip="http://localhost" -e cache.id=$counter -e cache.api=$api -e cache.hosts=$lastHost -e cache.capacity=10000000 -e cache.evictionFactor=0.25 -e cache.replicationFact=1 -e cache.localCache=false -e consistent.hashing.partitions=7 -e benchmark.iteration=1 -e benchmark.nodes=3 -e raft.ip="http://localhost" -e raft.port=$port -e raft.min.election.timeout=2000 -e raft.max.election.timeout=3000 -e raft.heartbeat.timeout=1000 put_bench.bal"

# myString="abcd"
# myString="${myString:1}"
# echo $myString