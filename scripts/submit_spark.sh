#!/usr/bin/env bash
# Submit a medallion Spark job into the Spark cluster with the required jars.
set -euo pipefail

LAYER="${1:?usage: submit_spark.sh <bronze|silver|gold> [table]}"
TABLE="${2:-}"

SPARK_MASTER="${SPARK_MASTER_URL:-spark://localhost:7077}"
ICEBERG_VER="1.5.2"
SCALA_VER="2.12"
SPARK_VER="3.5"

PACKAGES="org.apache.iceberg:iceberg-spark-runtime-${SPARK_VER}_${SCALA_VER}:${ICEBERG_VER}"
PACKAGES+=",org.apache.iceberg:iceberg-aws-bundle:${ICEBERG_VER}"
PACKAGES+=",org.apache.spark:spark-sql-kafka-0-10_${SCALA_VER}:3.5.1"
PACKAGES+=",org.apache.spark:spark-avro_${SCALA_VER}:3.5.1"
PACKAGES+=",org.apache.hadoop:hadoop-aws:3.3.4"

ARGS=("${LAYER}")
[[ -n "${TABLE}" ]] && ARGS+=(--table "${TABLE}")

exec spark-submit \
  --master "${SPARK_MASTER}" \
  --packages "${PACKAGES}" \
  --conf spark.sql.streaming.checkpointLocation.deleteOnStop=false \
  --name "cdc-${LAYER}-${TABLE:-all}" \
  -m cdc_platform.streaming "${ARGS[@]}"
