set -e

# Build-time classpaths derived from env vars set in ~/.zshrc
HADOOP_CP="$HADOOP_HOME/share/hadoop/common/hadoop-common-3.4.0.jar:$HADOOP_HOME/share/hadoop/mapreduce/hadoop-mapreduce-client-core-3.4.0.jar"
SPARK_CP=$(ls "$SPARK_HOME/jars/"*.jar | tr '\n' ':')

BUILD_DIR="build"
OUT_DIR="output"

mkdir -p "$BUILD_DIR" "$OUT_DIR"

echo "======================================================"
echo " TASK 1: Preprocess USvideos.csv (Hadoop MapReduce)"
echo "======================================================"

echo "[1/4] Compiling Task1PreprocessData.java..."
rm -rf "$BUILD_DIR/task1_classes"
mkdir -p "$BUILD_DIR/task1_classes"
javac -classpath "$HADOOP_CP" \
      -d "$BUILD_DIR/task1_classes" \
      Task1PreprocessData.java

echo "[2/4] Packaging Task1PreprocessData.jar..."
jar -cf "$BUILD_DIR/Task1PreprocessData.jar" -C "$BUILD_DIR/task1_classes" .

echo "[3/4] Uploading input to HDFS and running MapReduce job..."
$HADOOP_HOME/bin/hdfs dfs -mkdir -p input
$HADOOP_HOME/bin/hdfs dfs -put -f USvideos.csv input/USvideos.csv
$HADOOP_HOME/bin/hdfs dfs -rm -r -f output/task1_hdfs
$HADOOP_HOME/bin/hadoop jar "$BUILD_DIR/Task1PreprocessData.jar" Task1PreprocessData \
  input/USvideos.csv output/task1_hdfs

echo "[4/4] Downloading preprocessed result..."
$HADOOP_HOME/bin/hdfs dfs -getmerge output/task1_hdfs "$OUT_DIR/task1_preprocessed.csv"

echo ""
echo "Task 1 complete → $OUT_DIR/task1_preprocessed.csv"
echo ""

# ─── Start Spark Standalone Cluster ──────────────────────────────────────────
echo "====================================================="
echo " Starting Spark Standalone Cluster"
echo "====================================================="
export SPARK_LOCAL_IP=127.0.0.1
export SPARK_MASTER_HOST=127.0.0.1
$SPARK_HOME/sbin/stop-worker.sh 2>/dev/null || true
$SPARK_HOME/sbin/stop-master.sh 2>/dev/null || true
sleep 2
$SPARK_HOME/sbin/start-master.sh
sleep 5
$SPARK_HOME/sbin/start-worker.sh spark://127.0.0.1:7077
sleep 3
echo "Spark master UI: http://localhost:8080"
echo ""

echo "======================================================"
echo " TASK 2: Rank channels by total views (Spark)"
echo "======================================================"

echo "[1/3] Compiling Task2TopChannels.java..."
rm -rf "$BUILD_DIR/task2_classes"
mkdir -p "$BUILD_DIR/task2_classes"
javac -classpath "$SPARK_CP" \
      -d "$BUILD_DIR/task2_classes" \
      Task2TopChannels.java

echo "[2/3] Packaging Task2TopChannels.jar..."
jar -cf "$BUILD_DIR/Task2TopChannels.jar" -C "$BUILD_DIR/task2_classes" .

echo "[3/3] Running Spark job..."
rm -rf "$OUT_DIR/task2_top_channels"
$SPARK_HOME/bin/spark-submit \
  --class Task2TopChannels \
  --master spark://127.0.0.1:7077 \
  "$BUILD_DIR/Task2TopChannels.jar" \
  "file://$PWD/$OUT_DIR/task1_preprocessed.csv" \
  "$OUT_DIR/task2_top_channels"

echo ""
echo "Task 2 complete → $OUT_DIR/task2_top_channels/"

echo "======================================================"
echo " TASK 3: Rank publish dates by video count (Spark)"
echo "======================================================"

echo "[1/3] Compiling Task3VideosByDate.java..."
rm -rf "$BUILD_DIR/task3_classes"
mkdir -p "$BUILD_DIR/task3_classes"
javac -classpath "$SPARK_CP" \
      -d "$BUILD_DIR/task3_classes" \
      Task3VideosByDate.java

echo "[2/3] Packaging Task3VideosByDate.jar..."
jar -cf "$BUILD_DIR/Task3VideosByDate.jar" -C "$BUILD_DIR/task3_classes" .

echo "[3/3] Running Spark job..."
rm -rf "$OUT_DIR/task3_videos_by_date"
$SPARK_HOME/bin/spark-submit \
  --class Task3VideosByDate \
  --master spark://127.0.0.1:7077 \
  "$BUILD_DIR/Task3VideosByDate.jar" \
  "file://$PWD/$OUT_DIR/task1_preprocessed.csv" \
  "$OUT_DIR/task3_videos_by_date"

echo ""
echo "Task 3 complete → $OUT_DIR/task3_videos_by_date/"

# ─── Stop Spark Standalone Cluster ───────────────────────────────────────────
echo ""
echo "======================================================"
echo " Stopping Spark Standalone Cluster"
echo "======================================================"
$SPARK_HOME/sbin/stop-worker.sh
$SPARK_HOME/sbin/stop-master.sh

echo ""
echo "======================================================"
echo " All tasks complete."
echo " Artifacts:  build/    (classes + JARs)"
echo " Results:    output/   (preprocessed CSV + Spark output)"
echo " To clean:   rm -rf build/ output/"
echo "======================================================"