#!/usr/bin/env bash
# Submit a medallion Spark job INSIDE the spark-master container.
# Iceberg/S3 jars are baked into the image. The Kafka + Avro connectors and all
# their transitive deps are resolved together at submit time via --packages, so
# they share one class loader (avoids NoClassDefFoundError: ByteArraySerializer).
#
# spark.cores.max caps each app to 1 core so bronze + silver + gold can run
# concurrently on the single 4-core standalone worker (default grabs them all).
set -euo pipefail

LAYER="${1:?usage: submit_spark.sh <bronze|silver|gold> [table]}"
TABLE="${2:-}"

APP=/opt/app/src/cdc_platform/streaming/__main__.py
PACKAGES="org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,org.apache.spark:spark-avro_2.12:3.5.1"

ARGS=("${LAYER}")
[[ -n "${TABLE}" ]] && ARGS+=(--table "${TABLE}")

exec docker compose exec -w /opt/app spark-master \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --packages "${PACKAGES}" \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.cores.max=1 \
  --conf spark.executor.memory=1g \
  --name "cdc-${LAYER}-${TABLE:-all}" \
  "${APP}" "${ARGS[@]}"
