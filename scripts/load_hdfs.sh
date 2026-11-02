#!/usr/bin/env bash
# Charge les données d'entraînement dans HDFS et crée les dossiers du flux.
# Le dossier data/ du projet est monté dans le conteneur à /data (voir docker-compose.yml).
set -e
export MSYS_NO_PATHCONV=1
C=fraud-hadoop-master

docker exec $C bash -c "hdfs dfs -mkdir -p /data /models /stream/in /stream/tmp"
docker exec $C bash -c "hdfs dfs -put -f /data/paysim_train.csv /data/paysim_train.csv"
docker exec $C bash -c "hdfs dfs -ls -h /data"
echo "OK : données d'entraînement dans HDFS"
