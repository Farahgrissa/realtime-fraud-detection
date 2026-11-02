#!/usr/bin/env bash
# Couche batch : entraîne le modèle Spark MLlib et le sauvegarde dans HDFS.
set -e
export MSYS_NO_PATHCONV=1
C=fraud-hadoop-master

docker exec $C bash -c "export PYSPARK_PYTHON=python3 PYTHONIOENCODING=utf-8; cd /root/processing && \
  spark-submit --master local[2] --py-files common.py train.py \
  --data hdfs:///data/paysim_train.csv --model hdfs:///models/fraud_model $* 2>&1 | grep -v -E ' (INFO|WARN) '"
