#!/usr/bin/env bash
set -e

echo "====================================================="
echo " Starting Spark Standalone Cluster"
echo "====================================================="

export SPARK_LOCAL_IP=127.0.0.1
export SPARK_MASTER_HOST=127.0.0.1

# Stop any leftover processes before starting fresh
$SPARK_HOME/sbin/stop-worker.sh 2>/dev/null || true
$SPARK_HOME/sbin/stop-master.sh 2>/dev/null || true
sleep 2

$SPARK_HOME/sbin/start-master.sh
sleep 5
$SPARK_HOME/sbin/start-worker.sh spark://127.0.0.1:7077
sleep 3

echo "Spark master UI: http://localhost:8080"
echo ""
