#!/bin/sh

# Run a cc-pyspark job on AWS EMR Serverless.
#
# Modelled after run_ccpyspark_job_hadoop.sh (Spark on YARN) but targets
# the EMR Serverless API instead of spark-submit.
#
# Usage:
#   EMR_APPLICATION_ID=<id> JOB_ROLE_ARN=<arn> \
#     bash run_ccpyspark_job_aws_emr.sh <script-s3-uri> <warehouse-s3-uri> \
#       <job-args>...
#
# Arguments:
#   <script-s3-uri>    S3 URI of the cc-pyspark job script to run
#   <warehouse-s3-uri> S3 URI for spark.sql.warehouse.dir
#   <job-args>...      Remaining arguments are forwarded to the job
#
# Required environment variables:
#   EMR_APPLICATION_ID   EMR Serverless application ID
#   JOB_ROLE_ARN         IAM execution role ARN
#
# Optional environment variables:
#   PYFILES_S3        S3 URI of cc_pyspark.zip  (default: derived from SCRIPT_S3 dir)
#   VENV_S3           S3 URI of pyspark_venv.tar.gz for Python dependencies
#   EXECUTOR_MEM      Executor memory (default: 16g)
#   EXECUTOR_CORES    Executor cores  (default: 4)
#   DRIVER_MEM        Driver memory   (default: 8g)
#   DRIVER_CORES      Driver cores    (default: 4)
#   JOB_NAME          EMR job name    (default: basename of script)
#   EXTRA_SPARK_CONF  Additional --conf flags, e.g. "--conf spark.sql.broadcastTimeout=3600"
#
# Note: don't forget to adapt executor count, input/output partitions,
#       and memory requirements to your data volume.
#
# Example (full-pipeline mode):
#   EMR_APPLICATION_ID=00abc123 \
#   JOB_ROLE_ARN=arn:aws:iam::123456789012:role/EMRRole \
#   VENV_S3=s3://my-bucket/emr/pyspark_venv.tar.gz \
#     bash run_ccpyspark_job_aws_emr.sh \
#       s3://my-bucket/code/count_domain_links.py \
#       s3://my-bucket/warehouse \
#       s3://commoncrawl/cc-index/table/cc-main/warc/ \
#       domain_links \
#       --input_base_url s3://commoncrawl/ \
#       --target_domains s3://my-bucket/domains.csv \
#       --webgraph_prefix s3://commoncrawl/projects/hyperlinkgraph/cc-main-2024-aug-sep-oct/host \
#       --crawl CC-MAIN-2024-38 \
#       --num_input_partitions 500 \
#       --num_output_partitions 20

SCRIPT_S3="$1"
WAREHOUSE="$2"

if [ -z "$SCRIPT_S3" ] || [ -z "$WAREHOUSE" ]; then
    echo "Usage: $0 <script-s3-uri> <warehouse-s3-uri> <job-args>..."
    echo "  Run a cc-pyspark job on AWS EMR Serverless"
    echo
    echo "Required env vars: EMR_APPLICATION_ID, JOB_ROLE_ARN"
    echo "Optional env vars: PYFILES_S3, VENV_S3, EXECUTOR_MEM, EXECUTOR_CORES,"
    echo "                   DRIVER_MEM, DRIVER_CORES, JOB_NAME"
    exit 1
fi

if [ -z "$EMR_APPLICATION_ID" ] || [ -z "$JOB_ROLE_ARN" ]; then
    echo "ERROR: EMR_APPLICATION_ID and JOB_ROLE_ARN must be set" >&2
    exit 1
fi

# Strip script and warehouse from the argument list; remainder goes to the job
shift 2

# ── Resource sizing ────────────────────────────────────────────────────────────
EXECUTOR_MEM="${EXECUTOR_MEM:-16g}"
EXECUTOR_CORES="${EXECUTOR_CORES:-4}"
DRIVER_MEM="${DRIVER_MEM:-8g}"
DRIVER_CORES="${DRIVER_CORES:-4}"

# ── Derive PYFILES_S3 from script location if not set ─────────────────────────
SCRIPT_DIR_S3="$(dirname "$SCRIPT_S3")"
PYFILES_S3="${PYFILES_S3:-${SCRIPT_DIR_S3}/cc_pyspark.zip}"

# ── Job name ───────────────────────────────────────────────────────────────────
SCRIPT_BASE="$(basename "$SCRIPT_S3" .py)"
JOB_NAME="${JOB_NAME:-${SCRIPT_BASE}}"

# ── Spark configuration ────────────────────────────────────────────────────────
SPARK_PARAMS="\
--conf spark.driver.cores=${DRIVER_CORES} \
--conf spark.driver.memory=${DRIVER_MEM} \
--conf spark.executor.cores=${EXECUTOR_CORES} \
--conf spark.executor.memory=${EXECUTOR_MEM} \
--conf spark.sql.warehouse.dir=${WAREHOUSE} \
--conf spark.sql.parquet.outputTimestampType=TIMESTAMP_MILLIS \
--conf spark.serializer=org.apache.spark.serializer.KryoSerializer \
--conf spark.task.maxFailures=5 \
--conf spark.network.timeout=300s \
--conf spark.shuffle.io.maxRetries=10 \
--conf spark.shuffle.io.retryWait=60s \
--conf spark.io.compression.codec=zstd \
--py-files ${PYFILES_S3}"

# Add venv if provided
if [ -n "$VENV_S3" ]; then
    SPARK_PARAMS="${SPARK_PARAMS} \
--conf spark.archives=${VENV_S3}#environment \
--conf spark.emr-serverless.driverEnv.PYSPARK_DRIVER_PYTHON=./environment/bin/python \
--conf spark.emr-serverless.driverEnv.PYSPARK_PYTHON=./environment/bin/python \
--conf spark.emr-serverless.executorEnv.PYSPARK_PYTHON=./environment/bin/python"
fi

# Append any extra Spark conf flags (e.g. EXTRA_SPARK_CONF="--conf foo=bar --conf baz=qux")
if [ -n "${EXTRA_SPARK_CONF:-}" ]; then
    SPARK_PARAMS="${SPARK_PARAMS} ${EXTRA_SPARK_CONF}"
fi

# ── Build entryPointArguments JSON array from remaining shell args ─────────────
# Uses Python to handle quoting correctly for all argument values.
ARGS_JSON=$(python3 -c "
import json, sys
print(json.dumps(sys.argv[1:]))
" "$@")

# ── Submit job ─────────────────────────────────────────────────────────────────
set -e

echo "Submitting: ${JOB_NAME}"
echo "  Script:    ${SCRIPT_S3}"
echo "  Warehouse: ${WAREHOUSE}"
echo "  py-files:  ${PYFILES_S3}"

JOB_RUN_ID=$(aws emr-serverless start-job-run \
    --application-id "${EMR_APPLICATION_ID}" \
    --execution-role-arn "${JOB_ROLE_ARN}" \
    --name "${JOB_NAME}" \
    --job-driver "{
        \"sparkSubmit\": {
            \"entryPoint\": \"${SCRIPT_S3}\",
            \"entryPointArguments\": ${ARGS_JSON},
            \"sparkSubmitParameters\": \"${SPARK_PARAMS}\"
        }
    }" \
    --query 'jobRunId' --output text)

echo "Job submitted: ${JOB_RUN_ID}"

# ── Poll until terminal state ──────────────────────────────────────────────────
echo "Waiting for ${JOB_RUN_ID}..."
while true; do
    STATE=$(aws emr-serverless get-job-run \
        --application-id "${EMR_APPLICATION_ID}" \
        --job-run-id "${JOB_RUN_ID}" \
        --query 'jobRun.state' --output text)
    printf '  [%s] %s\n' "$(date -u +%H:%M:%S)" "${STATE}"
    case "${STATE}" in
        SUCCESS)
            echo "Done."
            exit 0
            ;;
        FAILED|CANCELLED|CANCELLING)
            echo "ERROR: job ${JOB_RUN_ID} ended with state ${STATE}" >&2
            aws emr-serverless get-job-run \
                --application-id "${EMR_APPLICATION_ID}" \
                --job-run-id "${JOB_RUN_ID}" \
                --query 'jobRun.stateDetails' --output text >&2
            exit 1
            ;;
    esac
    sleep 30
done
