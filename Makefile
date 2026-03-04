VENV_DIR ?= .venv
PYTHON ?= $(VENV_DIR)/bin/python

.PHONY: setup check-pmxt

setup:
	python3 -m venv $(VENV_DIR)
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt

check-pmxt:
	$(PYTHON) scripts/check_pmxt_health.py
