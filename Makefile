.PHONY: setup format format-check lint clean

setup:
	uv sync --all-groups

format:
	uv run ruff format .
	uv run ruff check . --fix

format-check:
	uv run ruff check .

lint:
	uv run pylint src scripts tests

clean:
	rm -rf .venv/ dist/ build/ work/ parquet/ sample-data/ tests/runs/
	rm -rf .pytest_cache/ .ruff_cache/ .mypy_cache/
	find . -type d -name '__pycache__' -exec rm -rf {} +
	find . -type d -name '*.egg-info' -exec rm -rf {} +
