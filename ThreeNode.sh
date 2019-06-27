#!/bin/bash
port=2000
api=7000
counter=1

gnome-terminal -e "ballerina run --experimental -e cache.port=2000 -e cache.ip="http://localhost" -e cache.id=1 -e
cache.api=7000 -e cache.hosts="" -e cache.capacity=10000000 -e cache.eviction.factor=0.25 -e cache.replication.fact=1 -e cache.local.cache=false -e consistent.hashing.partitions=7 -e benchmark.iteration=1 -e benchmark.nodes=3 -e raft.ip="http://localhost" -e raft.port=2000 -e raft.min.election.timeout=2000 -e raft.max.election.timeout=3000 -e raft.heartbeat.timeout=1000 ThreeNode-1.bal"

gnome-terminal -e "ballerina run --experimental -e cache.port=2001 -e cache.ip="http://localhost" -e cache.id=2 -e
cache.api=7001 -e cache.hosts="http://localhost:2000" -e cache.capacity=10000000 -e cache.eviction.factor=0.25 -e cache.replication.fact=1 -e cache.local.cache=false -e consistent.hashing.partitions=7 -e benchmark.iteration=1 -e benchmark.nodes=3 -e raft.ip="http://localhost" -e raft.port=2001 -e raft.min.election.timeout=2000 -e raft.max.election.timeout=3000 -e raft.heartbeat.timeout=1000 ThreeNode-2.bal"

gnome-terminal -e "ballerina run --experimental -e cache.port=2002 -e cache.ip="http://localhost" -e cache.id=3 -e
cache.api=7002 -e cache.hosts="http://localhost:2000,http://localhost:2001" -e cache.capacity=10000000 -e cache.eviction.factor=0.25 -e cache.replication.fact=1 -e cache.local.cache=false -e consistent.hashing.partitions=7 -e benchmark.iteration=1 -e benchmark.nodes=3 -e raft.ip="http://localhost" -e raft.port=2002 -e raft.min.election.timeout=2000 -e raft.max.election.timeout=3000 -e raft.heartbeat.timeout=1000 ThreeNode-3.bal"
