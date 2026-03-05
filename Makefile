VENV_DIR ?= .venv
PYTHON ?= $(VENV_DIR)/bin/python
HOST ?= 127.0.0.1
PORT ?= 8011
ARB_ALERTS_ARGS ?=

.PHONY: setup init-db run-web run-arb-alerts test check-pmxt

setup:
	python3 -m venv $(VENV_DIR)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

init-db:
	$(PYTHON) -m app.db

run-web:
	$(PYTHON) -m uvicorn app.main:app --host $(HOST) --port $(PORT) --reload

run-arb-alerts:
	$(PYTHON) scripts/ws_arb_alerts.py $(ARB_ALERTS_ARGS)

test:
	$(PYTHON) -m pytest -q

check-pmxt:
	$(PYTHON) scripts/check_pmxt_health.py
