#!/bin/bash

# Spark Job Submission Script for Billups Merchant Data Analysis

# Default values
SPARK_MASTER="local[4]"
EXECUTOR_MEMORY="2g"
DRIVER_MEMORY="2g"
EXECUTOR_CORES="2"
NUM_EXECUTORS="2"

# Parse command line arguments
TRANSACTIONS_PATH=""
MERCHANTS_PATH=""
TASK="all"
OUTPUT_DIR="reports"
USE_HIVE="--use-hive"

usage() {
    echo "Usage: $0 -t <transactions_path> -m <merchants_path> [options]"
    echo ""
    echo "Required:"
    echo "  -t, --transactions    Path to transactions parquet file"
    echo "  -m, --merchants       Path to merchants CSV file"
    echo ""
    echo "Options:"
    echo "  --task                Task to run (1-5 or all, default: all)"
    echo "  -o, --output          Output directory (default: reports/)"
    echo "  --no-hive             Disable Hive support"
    echo "  --master              Spark master URL (default: local[4])"
    echo "  --executor-memory     Executor memory (default: 2g)"
    echo "  --driver-memory       Driver memory (default: 2g)"
    echo "  --executor-cores      Executor cores (default: 2)"
    echo "  --num-executors       Number of executors (default: 2)"
    echo ""
    echo "Examples:"
    echo "  # Run all tasks locally"
    echo "  $0 -t data/transactions.parquet -m data/merchants.csv"
    echo ""
    echo "  # Run task 1 on cluster"
    echo "  $0 -t hdfs://data/transactions.parquet -m hdfs://data/merchants.csv \\
             --task 1 --master spark://master:7077"
    exit 1
}

# Parse arguments
while [[ $# -gt 0 ]]; do
    case $1 in
        -t|--transactions)
            TRANSACTIONS_PATH="$2"
            shift 2
            ;;
        -m|--merchants)
            MERCHANTS_PATH="$2"
            shift 2
            ;;
        --task)
            TASK="$2"
            shift 2
            ;;
        -o|--output)
            OUTPUT_DIR="$2"
            shift 2
            ;;
        --no-hive)
            USE_HIVE="--no-hive"
            shift
            ;;
        --master)
            SPARK_MASTER="$2"
            shift 2
            ;;
        --executor-memory)
            EXECUTOR_MEMORY="$2"
            shift 2
            ;;
        --driver-memory)
            DRIVER_MEMORY="$2"
            shift 2
            ;;
        --executor-cores)
            EXECUTOR_CORES="$2"
            shift 2
            ;;
        --num-executors)
            NUM_EXECUTORS="$2"
            shift 2
            ;;
        -h|--help)
            usage
            ;;
        *)
            echo "Unknown option: $1"
            usage
            ;;
    esac
done

# Validate required arguments
if [ -z "$TRANSACTIONS_PATH" ] || [ -z "$MERCHANTS_PATH" ]; then
    echo "Error: Both transactions and merchants paths are required"
    usage
fi

# Set Spark submit options
SPARK_SUBMIT_OPTS=(
    --master "$SPARK_MASTER"
    --driver-memory "$DRIVER_MEMORY"
    --executor-memory "$EXECUTOR_MEMORY"
    --executor-cores "$EXECUTOR_CORES"
    --num-executors "$NUM_EXECUTORS"
    --conf spark.sql.adaptive.enabled=true
    --conf spark.sql.adaptive.coalescePartitions.enabled=true
    --conf spark.sql.shuffle.partitions=200
    --conf spark.sql.execution.arrow.pyspark.enabled=true
    --conf spark.serializer=org.apache.spark.serializer.KryoSerializer
    --conf spark.sql.hive.convertMetastoreParquet=false
    --conf spark.sql.parquet.compression.codec=snappy
)

# Add additional configurations for cluster mode
if [[ "$SPARK_MASTER" != local* ]]; then
    SPARK_SUBMIT_OPTS+=(
        --deploy-mode client
        --conf spark.dynamicAllocation.enabled=true
        --conf spark.dynamicAllocation.minExecutors=1
        --conf spark.dynamicAllocation.maxExecutors=10
        --conf spark.shuffle.service.enabled=true
    )
fi

# Build the command
echo "Starting Spark job..."
echo "Transactions: $TRANSACTIONS_PATH"
echo "Merchants: $MERCHANTS_PATH"
echo "Task: $TASK"
echo "Output: $OUTPUT_DIR"
echo ""

# Submit the job
spark-submit \
    "${SPARK_SUBMIT_OPTS[@]}" \
    src/spark_job.py \
    -t "$TRANSACTIONS_PATH" \
    -m "$MERCHANTS_PATH" \
    --task "$TASK" \
    -o "$OUTPUT_DIR" \
    $USE_HIVE