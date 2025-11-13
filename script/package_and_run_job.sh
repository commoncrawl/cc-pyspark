
DIR=$(dirname "$0")

function usage() {
    echo "Usage: $0 --job_module <job_module> [--master <master_url>] [job_args...]"
    echo ""
    echo "Options:"
    echo "  --job_module <job_module>   The job module to run (e.g., cc_pyspark.jobs.server_count)"
    echo "  --master <master_url>       The Spark master URL (optional)"
    echo "  -h, --help                  Show this help message"
    echo ""
    echo "Positional Arguments:"
    echo "  job_args...                 Additional arguments to pass to the job"
}

MASTER_URL=
JOB_MODULE=
while [ "$#" -gt 0 ]; do
    case $1 in
        --job_module)
            JOB_MODULE=$2
            shift 2
            ;;
        --master)
            MASTER_URL=$2
            shift 2
            ;;
        -h|--help|-?)
            usage
            exit 0
            ;;
        *)
            # remaining args are job args
            break
            ;;
    esac
done

if [ -z "$JOB_MODULE" ]; then
    echo "Error: --job_module is required"
    usage
    exit 1
fi
if [ -z "$MASTER_URL" ]; then
    MASTER_URL="local[*]"
fi

VENV_DIR=$(mktemp -d)
VENV_PACK_FILE=$(mktemp -u).tar.gz

cleanup() {
    rm -rf "$VENV_DIR"
    echo rm -f "$VENV_PACK_FILE"
    echo NOT DELETING "$VENV_PACK_FILE" "for debugging purposes"
}
trap cleanup EXIT

set -e
set -x

python -m venv $VENV_DIR
source $VENV_DIR/bin/activate
#pip install cc-pyspark
pip install .
pip install venv-pack
venv-pack -o $VENV_PACK_FILE
deactivate

# if SPARK_HOME is not set, use `spark-submit` in path, otherwise use $SPARK_HOME/bin/spark-submit
if [ -z "$SPARK_HOME" ]; then
    SPARK_SUBMIT="spark-submit"
else
    SPARK_SUBMIT="$SPARK_HOME/bin/spark-submit"
fi

export PYSPARK_PYTHON=./environment/bin/python
$SPARK_SUBMIT \
  --master $MASTER_URL \
  --archives "$VENV_PACK_FILE#environment" \
  "$DIR"/run_job.py \
  --job_module $JOB_MODULE \
  "$@"

