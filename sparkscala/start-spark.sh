#!/bin/bash

/opt/spark/sbin/stop-master.sh
/opt/spark/sbin/stop-worker.sh
/opt/spark/sbin/stop-history-server.sh

ROLE=${ROLE:-master}
MASTER_NAME=${MASTER_NAME:-localhost}

if [ "$ROLE" = "master" ]; then
    /opt/spark/sbin/start-master.sh
    /opt/spark/bin/spark-submit script1.py
    service ssh start
elif [ "$ROLE" = "worker" ]; then
    WORKER_COUNT=${WORKER_COUNT:-1}
    for i in $(seq 1 $WORKER_COUNT); do
        /opt/spark/sbin/start-worker.sh spark://$MASTER_NAME:7077
        sleep 5
    done
else
    echo "ERROR: ROLE debe ser 'master' o 'worker'"
    exit 1
fi
# Mantener el contenedor en ejecución
tail -f /dev/null
