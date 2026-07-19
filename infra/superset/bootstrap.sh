#!/usr/bin/env bash
# Initialise Superset: admin user, DB migrations, register Trino datasource.
set -euo pipefail

superset db upgrade
superset fab create-admin \
  --username "${ADMIN_USER:-admin}" --password "${ADMIN_PASSWORD:-admin}" \
  --firstname Admin --lastname User --email admin@example.com || true
superset init

superset set-database-uri \
  --database-name "Lakehouse (Trino)" \
  --uri "${TRINO_SQLALCHEMY_URI:-trino://analytics@trino:8080/lakehouse}" || true

echo "Superset bootstrap complete. Import dashboards from infra/superset/assets/."
