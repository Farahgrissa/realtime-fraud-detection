#!/usr/bin/env bash
# Repartir de zéro avant une nouvelle démo : vide le flux HDFS et la collection MongoDB.
export MSYS_NO_PATHCONV=1
docker exec fraud-hadoop-master bash -c "hdfs dfs -rm -r -f -skipTrash /stream/in/* /stream/tmp/*" || true
docker exec fraud-mongo mongo fraud --quiet --eval "db.transactions.drop()"
echo "OK : flux et MongoDB vidés"
