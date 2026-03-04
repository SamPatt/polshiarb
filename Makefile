VENV_DIR ?= .venv
PYTHON ?= $(VENV_DIR)/bin/python
HOST ?= 127.0.0.1
PORT ?= 8011

.PHONY: setup init-db run-web check-pmxt

setup:
	python3 -m venv $(VENV_DIR)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

init-db:
	$(PYTHON) -m app.db

run-web:
	$(PYTHON) -m uvicorn app.main:app --host $(HOST) --port $(PORT) --reload

check-pmxt:
	$(PYTHON) scripts/check_pmxt_health.py
