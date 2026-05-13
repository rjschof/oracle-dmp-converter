.PHONY: setup format format-check lint

setup:
	uv sync --all-groups

format:
	uv run ruff format .
	uv run ruff check . --fix

format-check:
	uv run ruff check .

lint:
	uv run pylint src scripts tests
