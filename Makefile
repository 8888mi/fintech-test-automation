.PHONY: install up down test test-api test-integration test-ui report lint clean

install:
	poetry install
	poetry run playwright install --with-deps chromium

up:
	docker compose up -d --wait

down:
	docker compose down -v

test:
	poetry run pytest -n auto --reruns 1 --reruns-delay 2

test-api:
	poetry run pytest tests/api -n auto

test-integration:
	poetry run pytest tests/integration

test-ui:
	poetry run pytest tests/ui

report:
	poetry run allure serve allure-results

lint:
	poetry run ruff check src tests demo_service
	poetry run mypy src

clean:
	rm -rf allure-results allure-report .pytest_cache
	find . -type d -name __pycache__ -exec rm -rf {} +
