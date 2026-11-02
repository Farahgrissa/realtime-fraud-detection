#!/usr/bin/env bash
set -e
docker exec hadoop-master hdfs dfs -mkdir -p /data/paysim /stream/in /stream/tmp /models /checkpoints
docker exec hadoop-master hdfs dfs -put -f /data/paysim_train.csv /data/paysim/
docker exec hadoop-master hdfs dfs -ls -h /data/paysim