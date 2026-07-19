"""Superset configuration for the CDC lakehouse."""

import os

SECRET_KEY = os.environ.get("SUPERSET_SECRET_KEY", "change_me_in_prod_please")
SQLALCHEMY_DATABASE_URI = os.environ.get(
    "SUPERSET_METADATA_DB_URI", "sqlite:////app/superset_home/superset.db"
)

# Trino connection used for all lakehouse datasets.
TRINO_SQLALCHEMY_URI = os.environ.get(
    "TRINO_SQLALCHEMY_URI", "trino://analytics@trino:8080/lakehouse"
)

FEATURE_FLAGS = {
    "DASHBOARD_RBAC": True,
    "EMBEDDED_SUPERSET": True,
    "ALERT_REPORTS": True,
}

ENABLE_PROXY_FIX = True
WEBDRIVER_BASEURL = "http://superset:8088/"
ROW_LIMIT = 50_000
SUPERSET_WEBSERVER_TIMEOUT = 120
