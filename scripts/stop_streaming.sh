#!/usr/bin/env bash
export MSYS_NO_PATHCONV=1
docker exec fraud-hadoop-master bash -c "pkill -f streaming.py && echo 'Streaming arrêté' || echo 'Aucun streaming en cours'"
