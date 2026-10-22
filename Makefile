# Run from the repo root. `make` lists the targets.
COMPOSE := docker compose -f infra/docker-compose.yml
RAW_URL := https://media.githubusercontent.com/media/buds-lab/building-data-genome-project-2/master/data
METERS  := electricity chilledwater steam hotwater gas water irrigation solar
PY      := .venv/bin/python
# Spark driver memory for the full dataset; override: make task1 SPARK_DRIVER_MEMORY=8g
export SPARK_DRIVER_MEMORY ?= 6g

.DEFAULT_GOAL := help
.PHONY: help setup data lint format test infra-up infra-down task1 ingest preprocess validate

help:  ## list targets
	@grep -E '^[a-z0-9-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-12s %s\n", $$1, $$2}'

setup:  ## create the bdg2 venv (.venv, Python 3.11) with all extras + git hooks; needs uv
	[ -d .venv ] || uv venv --python 3.11 --prompt bdg2 .venv
	uv pip install --python .venv/bin/python -e ".[spark,dashboard,notebook,dev]"
	.venv/bin/pre-commit install

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
	ruff check .
	ruff format --check .

format:  ## auto-fix style
	ruff check --fix .
	ruff format .

test:  ## run all tests (exit code 5 = no tests collected yet)
	pytest || [ $$? -eq 5 ]

infra-up:  ## start shared infrastructure (Kafka)
	$(COMPOSE) up -d

infra-down:  ## stop shared infrastructure
	$(COMPOSE) down
