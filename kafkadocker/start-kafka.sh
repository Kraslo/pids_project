#!/bin/bash


/opt/bitnami/kafka/bin/zookeeper-server-start.sh /opt/bitnami/kafka/config/zookeeper.properties &


/opt/bitnami/kafka/bin/kafka-server-start.sh /opt/bitnami/kafka/config/server.properties.original &

sleep 1

/opt/bitnami/kafka/bin/kafka-topics.sh --create --bootstrap-server kafka:9092 --replication-factor 1 --partitions 1 --topic spark_input_topic

tail -f /dev/null
