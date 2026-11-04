.DEFAULT_GOAL := help
VENV ?= .venv
PY := $(VENV)/bin/python
PG_PORT ?= 55433
TEST_PG_DSN ?= postgresql://postgres:postgres@localhost:$(PG_PORT)/postgres

.PHONY: help venv install test test-pg pg-up pg-down cov lint fmt bench site docker-up docker-down clean

help: ## Show targets
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-12s\033[0m %s\n", $$1, $$2}'

venv: ## Create virtualenv
	test -d $(VENV) || python3 -m venv $(VENV)

install: venv ## Install with dev extras
	$(VENV)/bin/pip install --upgrade pip
	$(VENV)/bin/pip install -e ".[dev]"

test: ## Unit tests and the captured-stream replay (no database needed)
	$(PY) -m pytest

pg-up: ## Start a throwaway PostgreSQL 16 with wal_level=logical on PG_PORT
	docker run -d --name cdc-relay-pg -p $(PG_PORT):5432 -e POSTGRES_PASSWORD=postgres \
		postgres:16-alpine -c wal_level=logical
	until docker exec cdc-relay-pg pg_isready -U postgres >/dev/null 2>&1; do sleep 1; done

pg-down: ## Remove the throwaway PostgreSQL
	docker rm -f cdc-relay-pg

test-pg: ## Full suite including end-to-end tests against TEST_PG_DSN
	TEST_PG_DSN=$(TEST_PG_DSN) $(PY) -m pytest -v

cov: ## Tests with coverage
	$(PY) -m pytest --cov=cdc_relay --cov-report=term-missing

lint: ## Ruff check + format check
	$(VENV)/bin/ruff check cdc_relay tests bench scripts
	$(VENV)/bin/ruff format --check cdc_relay tests bench scripts

fmt: ## Format + autofix
	$(VENV)/bin/ruff format cdc_relay tests bench scripts
	$(VENV)/bin/ruff check --fix cdc_relay tests bench scripts

bench: ## Decode/route throughput on the captured stream
	$(PY) bench/bench_decode.py

site: ## Build the protocol walkthrough page (what GitHub Pages serves) into _site/
	$(PY) scripts/build_site.py _site

docker-up: ## Postgres + Redis + relay + demo writer
	docker compose up --build -d

docker-down: ## Stop the stack and remove its volumes
	docker compose down -v

clean: ## Remove caches and local state
	rm -rf .pytest_cache .ruff_cache htmlcov .coverage _site state relay.checkpoint.json
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
