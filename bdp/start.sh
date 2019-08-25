#!/usr/bin/env bash
spark-submit \
--conf spark.yarn.executor.memoryOverhead=4096 \
--master local \
--executor-memory 1G \
--driver-memory 1G \
--driver-cores 2 \
--executor-cores  2 \
--num-executors 3 \
label2hbase.py