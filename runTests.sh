#!/usr/bin/env bash

ballerina test --experimental -e cache.replication.fact=0 testNoReplicasBasic.bal
ballerina test --experimental -e cache.replication.fact=0 testNoReplicasEviction.bal

ballerina test --experimental -e cache.replication.fact=1 testOneReplicasBasic.bal
ballerina test --experimental -e cache.replication.fact=1 testOneReplicasEviction.bal
