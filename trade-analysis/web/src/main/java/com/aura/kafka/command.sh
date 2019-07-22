#!/usr/bin/env bash
#Start Zookeeper
bin/zkServer.sh start

# Start Kafka
bin/kafka-server-start.sh -daemon config/server.properties

# 创建aura kafka topic, 一个分区一个副本
bin/kafka-topics.sh --create --zookeeper master:2181 --replication-factor 1 --partitions 1 --topic user_pay

# 查看创建好的topic
bin/kafka-topics.sh --list --zookeeper master:2181

# Consumer
bin/kafka-console-consumer.sh --zookeeper master:2181 --from-beginning --topic user_pay --max-messages 10