# ============================================================================
# CDC Lakehouse Platform - Developer entrypoints
# ============================================================================
SHELL := /bin/bash
COMPOSE := docker compose -f docker-compose.yml
.DEFAULT_GOAL := help

.PHONY: help
help: ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | \
		awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-22s\033[0m %s\n", $$1, $$2}'

.PHONY: setup
setup: ## Install python deps + pre-commit hooks
	python -m pip install -e ".[dev,spark]"
	pre-commit install

.PHONY: up
up: ## Start the full platform
	cp -n .env.example .env || true
	$(COMPOSE) up -d --build

.PHONY: down
down: ## Stop and remove containers
	$(COMPOSE) down

.PHONY: clean
clean: ## Stop and remove containers + volumes (DESTRUCTIVE)
	$(COMPOSE) down -v --remove-orphans

.PHONY: register-connectors
register-connectors: ## Register Debezium connectors
	bash scripts/register_connectors.sh

.PHONY: seed
seed: ## Generate + load sample data into Postgres
	python -m cdc_platform.generators.seed_source

.PHONY: simulate
simulate: ## Emit a continuous stream of INSERT/UPDATE/DELETE
	python -m cdc_platform.generators.change_simulator

.PHONY: bronze silver gold
bronze: ## Submit the Bronze streaming job
	bash scripts/submit_spark.sh bronze
silver: ## Submit the Silver streaming job
	bash scripts/submit_spark.sh silver
gold: ## Submit the Gold batch job
	bash scripts/submit_spark.sh gold

.PHONY: dbt-run
dbt-run: ## Build dbt gold marts
	cd dbt && dbt deps && dbt build --profiles-dir .

.PHONY: gx
gx: ## Run Great Expectations checkpoints
	python -m cdc_platform.quality.run_checkpoints

.PHONY: lint
lint: ## Ruff + black + mypy
	ruff check src tests
	black --check src tests
	mypy src

.PHONY: fmt
fmt: ## Auto-format
	ruff check --fix src tests
	black src tests

.PHONY: test
test: ## Run unit tests with coverage
	pytest -m unit

.PHONY: test-integration
test-integration: ## Run integration tests (needs `make up`)
	pytest -m integration

.PHONY: smoke
smoke: ## End-to-end smoke check
	bash scripts/smoke_test.sh
