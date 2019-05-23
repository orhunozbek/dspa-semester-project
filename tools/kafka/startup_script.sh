#!/usr/bin/env bash

bin/zookeeper-server-start.sh config/zookeeper.properties &

sleep 5

bin/kafka-server-start.sh config/server.properties &

sleep 2
