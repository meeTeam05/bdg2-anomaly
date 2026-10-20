# Run from the repo root. `make` lists the targets.
COMPOSE := docker compose -f infra/docker-compose.yml
RAW_URL := https://media.githubusercontent.com/media/buds-lab/building-data-genome-project-2/master/data
METERS  := electricity chilledwater steam hotwater gas water irrigation solar

.DEFAULT_GOAL := help
.PHONY: help setup data lint format test infra-up infra-down

help:  ## list targets
	@grep -E '^[a-z-]+:.*##' $(MAKEFILE_LIST) | awk -F':.*## ' '{printf "  %-11s %s\n", $$1, $$2}'

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
