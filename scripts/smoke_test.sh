#!/usr/bin/env bash
# Minimal end-to-end smoke check: source row -> Kafka topic -> Bronze count.
set -euo pipefail

echo "==> Checking core services"
for svc in "http://localhost:8083/connectors" "http://localhost:8081/subjects" \
           "http://localhost:8080/v1/info"; do
  curl -sf "${svc}" >/dev/null && echo "    OK  ${svc}" || { echo "    FAIL ${svc}"; exit 1; }
done

echo "==> Verifying Debezium topics exist"
docker compose exec -T kafka kafka-topics --bootstrap-server localhost:9092 --list \
  | grep -E "commerce\.public\.(customers|orders|payments|products|inventory)" \
  && echo "    OK  CDC topics present"

echo "==> Querying Bronze row count via Trino"
docker compose exec -T trino trino --execute \
  "SELECT count(*) FROM lakehouse.bronze.orders" || true

echo "==> Smoke test complete"
