#!/bin/bash
port=2000
api=7000
counter=1

gnome-terminal -e "ballerina run -e cache.port=2000 -e cache.ip="http://localhost" -e cache.id=1 -e cache.api=7000 -e cache.hosts="" -e cache.capacity=10000 -e cache.eviction.factor=0.25 -e cache.replication.fact=1 -e cache.local.cache=false -e consistent.hashing.partitions=7 -e benchmark.iteration=1 -e benchmark.nodes=3 -e raft.ip="http://localhost" -e raft.port=2000 -e raft.min.election.timeout=2000 -e raft.max.election.timeout=3000 -e raft.heartbeat.timeout=1000 ThreeNode-1.bal"

