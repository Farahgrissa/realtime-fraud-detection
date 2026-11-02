#!/usr/bin/env bash
# À lancer une fois après "docker compose up -d" (et après chaque redémarrage des conteneurs).
# Installe ce qui manque à l'image pour le code Python, puis démarre HDFS et YARN.
set -e
export MSYS_NO_PATHCONV=1          # Git Bash : ne pas transformer les chemins /root/... en chemins Windows
C=fraud-hadoop-master

if docker exec $C bash -c "python3 -c 'import numpy, pymongo'" 2>/dev/null; then
  echo "numpy et pymongo déjà installés"
else
  echo "Installation de pip, numpy et pymongo (quelques minutes)..."
  docker exec $C bash -c "apt-get update -qq && apt-get install -y -qq python3-pip python3-numpy && pip3 install -q 'pymongo==3.12.3'"
fi

docker exec $C bash -c "grep -q PYSPARK_PYTHON ~/.bashrc || echo 'export PYSPARK_PYTHON=python3' >> ~/.bashrc"
docker exec $C bash -c "grep -q PYTHONIOENCODING ~/.bashrc || echo 'export PYTHONIOENCODING=utf-8' >> ~/.bashrc"

echo "Démarrage de Hadoop..."
docker exec $C bash -c "cd /root && ./start-hadoop.sh"
sleep 10
docker exec $C bash -c "hdfs dfsadmin -report | grep -E 'Live datanodes'"
echo "OK : environnement prêt"
