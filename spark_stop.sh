#!/usr/bin/env bash
set -e

echo "====================================================="
echo " Stopping Spark Standalone Cluster"
echo "====================================================="

$SPARK_HOME/sbin/stop-worker.sh
$SPARK_HOME/sbin/stop-master.sh

echo "Spark cluster stopped."
echo ""
