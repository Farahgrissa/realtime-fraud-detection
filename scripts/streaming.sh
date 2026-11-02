#!/usr/bin/env bash
# Couche temps réel : note les nouveaux fichiers de /stream/in et écrit dans MongoDB.
# Reste au premier plan pour la démo. Pour l'arrêter : bash scripts/stop_streaming.sh
set -e
export MSYS_NO_PATHCONV=1
C=fraud-hadoop-master

docker exec $C bash -c "export PYSPARK_PYTHON=python3 PYTHONIOENCODING=utf-8; cd /root/processing && \
  spark-submit --master local[2] --py-files common.py streaming.py \
  --input hdfs:///stream/in --model hdfs:///models/fraud_model --mongo mongodb://mongo:27017 \
  2>&1 | grep --line-buffered -v -E ' (INFO|WARN) '"
