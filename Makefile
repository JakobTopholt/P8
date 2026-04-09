SHELL := /bin/bash

PYTHON ?= python
CSV ?= AISDATA/aisdk-2026-02-05.cleaned.csv
QUERY_ARGS ?= --help
FRONTEND_DIR ?= frontend

.PHONY: help setup install pipeline db-up db-down db-reset db-logs db-smoke db-import db-query db-api frontend-install frontend-dev frontend-build

help:
	@echo "Targets:"
	@echo "  setup            Create .venv and install Python dependencies"
	@echo "  install          Install Python dependencies into current interpreter"
	@echo "  pipeline         Run AIS cleaning pipeline"
	@echo "  db-up            Start PostGIS service"
	@echo "  db-down          Stop PostGIS service"
	@echo "  db-reset         Recreate PostGIS volume and schema"
	@echo "  db-logs          Tail PostGIS logs"
	@echo "  db-smoke         Run DB smoke test"
	@echo "  db-import        Import cleaned AIS CSV (override with CSV=...)"
	@echo "  db-query         Run range query script (override with QUERY_ARGS=...)"
	@echo "  db-api           Start DataPoints HTTP API"
	@echo "  frontend-install Install frontend dependencies"
	@echo "  frontend-dev     Start frontend dev server"
	@echo "  frontend-build   Build frontend"

setup:
	$(PYTHON) -m venv .venv
	. .venv/bin/activate && python -m pip install -r requirements.txt

install:
	$(PYTHON) -m pip install -r requirements.txt

pipeline:
	$(PYTHON) main.py

db-up:
	docker compose -f db/compose.yaml up -d

db-down:
	docker compose -f db/compose.yaml down

db-reset:
	docker compose -f db/compose.yaml down -v
	docker compose -f db/compose.yaml up -d

db-logs:
	docker compose -f db/compose.yaml logs -f postgis

db-smoke:
	$(PYTHON) db/smoke_test_db.py

db-import:
	$(PYTHON) db/import_ais_csv.py $(CSV)

db-query:
	$(PYTHON) db/run_range_query.py $(QUERY_ARGS)

db-api:
	$(PYTHON) db/api_server.py

frontend-install:
	cd $(FRONTEND_DIR) && npm install

frontend-dev:
	cd $(FRONTEND_DIR) && npm run dev

frontend-build:
	cd $(FRONTEND_DIR) && npm run build
