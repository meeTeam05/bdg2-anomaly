VENV    := dashboard/.venv
PY      := $(VENV)/bin/python
MOCK    := $(CURDIR)/dashboard/.mock_output/energy_results
COMPOSE := docker compose -f dashboard/docker-compose.yml

.DEFAULT_GOAL := help
.PHONY: help setup kafka kafka-down reset web mock demo test

help:  ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-11s %s\n", $$1, $$2}'

setup:  ## create dashboard venv (Python 3.11) and install deps
	uv venv --python 3.11 $(VENV)
	uv pip install --python $(PY) -r dashboard/requirements.txt

kafka:  ## start Kafka (localhost:9094)
	$(COMPOSE) up -d --wait

kafka-down:  ## stop Kafka
	$(COMPOSE) down

reset:  ## fresh demo run: stop Kafka, delete streaming/output, start Kafka
	dashboard/reset_demo.sh

web:  ## dashboard on real data, http://localhost:8501
	cd dashboard && .venv/bin/streamlit run dashboard.py

mock:  ## fake Task 3: write a batch to dashboard/.mock_output every 5 s
	$(PY) dashboard/mock_stream.py --reset

demo:  ## dashboard + mock data together (Ctrl+C stops both)
	ENERGY_RESULTS_PATH=$(MOCK) $(MAKE) -j2 mock web

test:  ## dashboard tests
	cd dashboard && .venv/bin/python -m pytest -q
