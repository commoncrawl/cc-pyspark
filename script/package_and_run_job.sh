
DIR=$(dirname "$0")
# if "-h" or "--help" is passed, or no args, show usage
if [ "$#" -eq 0 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    echo "Submits a Spark job using cc-pyspark packaged in a virtual environment."
    echo "Usage: $0 <job_module> [job_args...]"
    echo "  <job_module>: The job module to run (e.g., cc_pyspark.jobs.server_count)"
    echo "  [job_args...]: Additional arguments to pass to the job"
    exit 1
fi

JOB_MODULE=$1
shift 1

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

export PYSPARK_PYTHON=./environment/bin/python
spark-submit --archives "$VENV_PACK_FILE#environment" "$DIR"/run_job.py --job_module $JOB_MODULE "$@"

