#!/usr/bin/env bash
# Register (idempotently) the Debezium connectors and wait for RUNNING state.
set -euo pipefail

CONNECT_URL="${KAFKA_CONNECT_URL:-http://localhost:8083}"
CONFIG="${1:-infra/debezium/postgres-connector.json}"
NAME="$(python3 -c "import json,sys;print(json.load(open('${CONFIG}'))['name'])")"

echo "==> Waiting for Kafka Connect at ${CONNECT_URL}"
until curl -sf "${CONNECT_URL}/connectors" >/dev/null; do sleep 3; done

echo "==> Upserting connector '${NAME}'"
RESP="$(curl -s -X PUT -H "Content-Type: application/json" \
  --data "$(python3 -c "import json;print(json.dumps(json.load(open('${CONFIG}'))['config']))")" \
  "${CONNECT_URL}/connectors/${NAME}/config")"
echo "${RESP}" | python3 -m json.tool 2>/dev/null || echo "${RESP}"

echo "==> Waiting for RUNNING"
for _ in $(seq 1 30); do
  STATE="$(curl -sf "${CONNECT_URL}/connectors/${NAME}/status" | python3 -c "import json,sys;print(json.load(sys.stdin)['connector']['state'])" 2>/dev/null || echo UNKNOWN)"
  echo "    state=${STATE}"
  [[ "${STATE}" == "RUNNING" ]] && { echo "==> Connector is RUNNING"; exit 0; }
  sleep 3
done
echo "!! Connector did not reach RUNNING" >&2
exit 1
