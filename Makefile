# Plain `python3 -m ...` equivalents are listed in the README.
PY ?= python3

.PHONY: setup lint format test eval eval-regression secretscan demo bench

setup:
	$(PY) -m pip install -e ".[dev]"

lint:
	$(PY) -m ruff check src tests
	$(PY) -m ruff format --check src tests
	$(PY) -m mypy

format:
	$(PY) -m ruff format src tests
	$(PY) -m ruff check --fix src tests

test:
	$(PY) -m pytest

eval:
	$(PY) -m cdc_lab eval

eval-regression:
	$(PY) -m cdc_lab eval --regression

secretscan:
	$(PY) -m cdc_lab.secretscan

demo:
	$(PY) -m cdc_lab demo

bench:
	$(PY) -m cdc_lab bench --events 2000 --seed 7
