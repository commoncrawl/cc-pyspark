#!/bin/sh

# example shell script to run a cc-pyspark job on a Hadoop cluster (Spark on YARN)

SCRIPT="$1"
WAREHOUSE="$2"

if [ -z "$SCRIPT" ] || [ -z "$WAREHOUSE" ]; then
    echo "Usage: $0 <script> <warehouse> <args>..."
    echo "  Run a cc-pyspark job in Spark/Hadoop cluster"
    echo
    echo "Arguments:"
    echo "  <script>     cc-pyspark job implementation"
    echo "  <warehouse>  Spark SQL warehouse directory"
    echo "  <args>...    remaining args are passed to the job"
    echo
    echo "Example:"
    echo "  $0 server_count.py hdfs:///user/max/counts \\"
    echo "        wat_sample.paths servers"
    echo
    echo "Note: don't forget to adapt the number of executors,"
    echo "      input/output partitions, the memory requirements"
    echo "      and other parameters at your need!"
    echo "      Some params can be set per environment variable."
    exit 1
fi

# strip SCRIPT and WAREHOUSE from argument list
shift 2

SPARK_ON_YARN="--master yarn"
SPARK_HADOOP_OPTS=""
SPARK_EXTRA_OPTS=""

# defines SPARK_HOME, SPARK_HADOOP_OPTS and HADOOP_CONF_DIR
. $HOME/workspace/spark/spark_env.sh

NUM_EXECUTORS=${NUM_EXECUTORS:-1}
EXECUTOR_MEM=${EXECUTOR_MEM:-4g}
EXECUTOR_CORES=${EXECUTOR_CORES:-2}

# access data via S3
INPUT_BASE_URL="s3://commoncrawl/"

# temporary directory
# - must exist on task/compute nodes for buffering data
# - should provide several GBs of free space to hold temporarily
#   the downloaded data (WARC, WAT, WET files)
TMPDIR=/data/0/tmp

export PYSPARK_PYTHON="python"  # or "python3"

# Python dependencies (for simplicity, include all Python files: cc-pyspark/*.py)
PYFILES=$(ls sparkcc.py sparkcc_fastwarc.py *.py | sort -u | tr '\n' ',')



set -xe

$SPARK_HOME/bin/spark-submit \
              $SPARK_ON_YARN \
              $SPARK_HADOOP_OPTS \
              --conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
              --conf spark.task.maxFailures=5 \
              --conf spark.executor.memory=$EXECUTOR_MEM \
              --conf spark.driver.memory=3g \
              --conf spark.core.connection.ack.wait.timeout=600s \
              --conf spark.network.timeout=300s \
              --conf spark.shuffle.io.maxRetries=50 \
              --conf spark.shuffle.io.retryWait=600s \
              --conf spark.locality.wait=1s \
              --conf spark.io.compression.codec=zstd \
              --conf spark.checkpoint.compress=true \
              --conf spark.executorEnv.LD_LIBRARY_PATH=/usr/lib/hadoop/lib/native \
              --num-executors $NUM_EXECUTORS \
              --executor-cores $EXECUTOR_CORES \
              --executor-memory $EXECUTOR_MEM \
              --conf spark.sql.warehouse.dir=$WAREHOUSE \
              --conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MILLIS \
              --py-files $PYFILES \
              $SCRIPT \
              --input_base_url $INPUT_BASE_URL \
              --local_temp_dir $TMPDIR \
              "$@"
