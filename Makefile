.PHONY: setup format format-check lint test test-cov clean

setup:
	uv sync --all-groups

format:
	uv run ruff format .
	uv run ruff check . --fix

format-check:
	uv run ruff check .

lint:
	uv run pylint src scripts tests

test:
	uv run pytest -m "not integration"

test-cov:
	uv run pytest -m "not integration" --cov --cov-report=term-missing --cov-report=html

clean:
	rm -rf .venv/ dist/ build/ work/ parquet/ sample-data/ tests/runs/
	rm -rf .pytest_cache/ .ruff_cache/ .mypy_cache/ htmlcov/ .coverage
	find . -type d -name '__pycache__' -exec rm -rf {} +
	find . -type d -name '*.egg-info' -exec rm -rf {} +
