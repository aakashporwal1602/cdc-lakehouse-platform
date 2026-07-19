# Setup & Getting Started

Complete, tested, step-by-step guide to run the CDC Lakehouse Platform locally.
Follow the phases in order.

---

## 1. Prerequisites

| Tool | Version | Notes |
|------|---------|-------|
| Docker Desktop | 24+ | Give it **≥ 8 GB RAM** (Settings → Resources) — 15 containers run together |
| Docker Compose | v2 | Bundled with Docker Desktop (`docker compose version`) |
| Python | 3.10+ (3.11 recommended) | Only needed for host-side CLIs (`seed`, `simulate`, tests). macOS ships 3.9 — install 3.11 via `brew install python@3.11` |
| Git | any | To clone the repo |

Verify:

```bash
docker --version           # 24+
docker compose version     # v2.x
python3 --version          # 3.10+
```

> Everything else (Kafka, Spark, Trino, Postgres, Debezium, Superset, Airflow,
> Prometheus, Grafana, MinIO) runs **inside Docker** — you do not install them.

---

## 2. Clone & configure

```bash
git clone https://github.com/your-org/cdc-lakehouse-platform.git
cd cdc-lakehouse-platform
cp .env.example .env          # default values work out of the box
```

Create a Python 3.11 virtual env for the host-side CLIs:

```bash
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"
```

---

## 3. Bring up the platform

```bash
make up            # builds custom images + starts all services (~5-10 min first time)
docker compose ps  # wait until services show "running" / "healthy"
```

Give Kafka + Kafka Connect ~1-2 minutes to become healthy before the next step.

---

## 4. Register the Debezium connector

```bash
make register-connectors
```

Expect `Connector is RUNNING`. Verify the CDC topics get created after seeding.

---

## 5. Seed the source database

```bash
source .venv/bin/activate       # if not already active
make seed                       # 1000 customers, 300 products, 5000 orders
```

Verify CDC topics exist:

```bash
docker compose exec kafka kafka-topics --bootstrap-server localhost:9092 --list | grep commerce.public
```

---

## 6. Run the streaming pipeline

Bronze and Silver are long-running streaming jobs — run each in its **own terminal**
and leave it running. They execute inside the Spark container (no host Spark needed).

**Terminal A — Bronze** (Kafka → raw Iceberg):

```bash
make bronze          # defaults to TABLE=orders
```

**Terminal B — Silver** (dedup + MERGE → current state):

```bash
make silver
```

Each prints `batch_committed` then goes idle ("waiting for new data") — that is the
normal running state. Leave both running.

**Terminal C — live traffic** (optional but great for demos):

```bash
source .venv/bin/activate
make simulate        # continuous INSERT / UPDATE / DELETE
```

### Build the other tables (needed for the full Gold layer)

Silver reads Kafka directly, so you can build each table independently. Run each,
wait for `batch_committed`, then `Ctrl+C` and move on:

```bash
make silver TABLE=payments
make silver TABLE=products
make silver TABLE=inventory
make silver TABLE=customers
```

---

## 7. Build the Gold marts

```bash
make gold          # builds revenue_daily, inventory_health, order_metrics
# optional (dbt-based marts): make dbt-run
make gx            # optional: run Great Expectations data-quality gates
```

---

## 8. Verify end-to-end

```bash
docker compose exec trino trino --execute "SELECT count(*) FROM lakehouse.bronze.orders;"
docker compose exec trino trino --execute "SELECT count(*) FROM lakehouse.silver.orders;"
docker compose exec trino trino --execute "SELECT * FROM lakehouse.gold.order_metrics;"
```

**Exactly-once + dedup proof** — update a row at source, watch it collapse in Silver:

```bash
docker compose exec postgres psql -U cdc_admin -d commerce \
  -c "UPDATE orders SET status='delivered' WHERE order_id=1;"
# wait ~30s, then:
docker compose exec trino trino --execute \
  "SELECT order_id, status FROM lakehouse.silver.orders WHERE order_id=1;"
```

`order_id=1` appears exactly once, with the latest status.

---

## 9. UIs & credentials

| Service | URL | Login |
|---------|-----|-------|
| Superset (BI) | http://localhost:8088 | admin / admin |
| Grafana (monitoring) | http://localhost:3000 | admin / admin |
| Airflow (orchestration) | http://localhost:8085 | airflow / airflow |
| Trino | http://localhost:8080 | analytics |
| Spark master UI | http://localhost:8090 | — |
| MinIO console | http://localhost:9001 | minioadmin / minioadmin |
| Kafka Connect REST | http://localhost:8083 | — |

**Superset → Trino datasource:** Settings → Database Connections → + Database →
`trino://analytics@trino:8080/lakehouse`, then add datasets from the `gold` schema.

---

## 10. Shut down

```bash
make down     # stop containers, keep data volumes
make clean    # stop + delete volumes (full reset)
```

---

## 11. Troubleshooting

| Symptom | Fix |
|---------|-----|
| `zsh: command not found: python` | macOS uses `python3`; activate the venv (`source .venv/bin/activate`) |
| `requires a different Python: 3.9 not in '>=3.10'` | Install Python 3.11 (`brew install python@3.11`) and recreate the venv |
| `Cannot connect to the Docker daemon` | Start Docker Desktop (`open -a Docker`), wait for the whale icon |
| A container is `unhealthy` on `make up` | Give it more time / RAM; check `docker compose logs <service>` |
| Spark job: `has not accepted any resources` | Another job holds all worker cores; jobs are capped to 1 core each — restart them |
| Streaming job: source/checkpoint mismatch after code change | Delete the stale checkpoint: `docker compose exec spark-master rm -rf /tmp/cdc-checkpoints/<layer>/<table>` |
| Grafana panels show "No data" | Run `make simulate` so metrics flow; wait ~30s for Prometheus to scrape |
| Airflow login fails | Wait for the container to finish init (~1-2 min), then use `airflow` / `airflow` |

---

## Quick reference (happy path)

```bash
make up
make register-connectors
make seed
make bronze          # terminal A
make silver          # terminal B
make simulate        # terminal C (optional)
make gold
```
