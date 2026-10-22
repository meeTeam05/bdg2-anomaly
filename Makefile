# Run from the repo root. `make` lists the targets.
COMPOSE := docker compose -f infra/docker-compose.yml
RAW_URL := https://media.githubusercontent.com/media/buds-lab/building-data-genome-project-2/master/data
METERS  := electricity chilledwater steam hotwater gas water irrigation solar
BIN     := .venv/bin
PY      := $(BIN)/python
# Spark driver memory for the full dataset; override: make task1 SPARK_DRIVER_MEMORY=8g
export SPARK_DRIVER_MEMORY ?= 6g

.DEFAULT_GOAL := help
.PHONY: help setup data lint format test infra-up infra-down reset task1 ingest preprocess validate \
	stream produce dashboard mock demo-mock

help:  ## list targets
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

setup:  ## create the bdg2 venv (.venv, Python 3.11) with all extras + git hooks; needs uv
	[ -d .venv ] || uv venv --python 3.11 --prompt bdg2 .venv
	uv pip install --python .venv/bin/python -e ".[spark,streaming,dashboard,notebook,dev]"
	$(BIN)/pre-commit install

data:  ## download the full BDG2 raw dataset into data/raw (~370 MB)
	mkdir -p data/raw
	@for m in $(METERS); do \
		[ -f data/raw/$$m.csv ] || wget -O data/raw/$$m.csv $(RAW_URL)/meters/raw/$$m.csv; \
	done
	@[ -f data/raw/weather.csv ] || wget -O data/raw/weather.csv $(RAW_URL)/weather/weather.csv
	@[ -f data/raw/metadata.csv ] || wget -O data/raw/metadata.csv $(RAW_URL)/metadata/metadata.csv
	@ls -lh data/raw

task1: ingest preprocess validate  ## Task 1 end to end: CSV -> Bronze -> Silver -> checks

ingest:  ## Task 1 step 1: raw CSV -> Bronze
	$(PY) data_pipeline/ingest.py

preprocess:  ## Task 1 step 3: Bronze -> Silver
	$(PY) data_pipeline/preprocess.py

validate:  ## Task 1 step 4: check Silver against the data contract
	$(PY) data_pipeline/validate_silver.py

lint:  ## check style
	$(BIN)/ruff check .
	$(BIN)/ruff format --check .

format:  ## auto-fix style
	$(BIN)/ruff check --fix .
	$(BIN)/ruff format .

test:  ## run all tests (exit code 5 = no tests collected yet)
	$(BIN)/pytest || [ $$? -eq 5 ]

infra-up:  ## start Kafka (localhost:9094) and create the topic
	$(COMPOSE) up -d --wait

infra-down:  ## stop Kafka
	$(COMPOSE) down

reset:  ## fresh demo run: Kafka, Gold table and checkpoint are reset together
	$(COMPOSE) down
	rm -rf data/gold data/checkpoints
	$(COMPOSE) up -d --wait

stream:  ## Task 3: streaming job, Kafka -> Gold (needs infra-up and the Task 2 outputs)
	$(PY) streaming/streaming_job.py

produce:  ## Task 3: replay 2017 into Kafka, e.g. make produce HOURS=48
	$(PY) streaming/kafka_producer.py $(if $(HOURS),--hours $(HOURS))

dashboard:  ## Task 4: dashboard on the Gold table, http://localhost:8501
	$(BIN)/streamlit run dashboard/app.py

mock:  ## Task 4: fake Gold table in dashboard/.mock_output, one batch every 5 s
	$(PY) dashboard/mock_stream.py --reset

demo-mock:  ## dashboard + mock data, no Kafka or Spark needed (Ctrl+C stops both)
	ENERGY_RESULTS_PATH=$(CURDIR)/dashboard/.mock_output/energy_results $(MAKE) -j2 mock dashboard
