.PHONY: dev docker docker-stop dagster-update
.DEFAULT_GOAL := help

help:
	@awk -F ':.*?## ' '/^[a-zA-Z]/ && NF==2 {printf "\033[36m  %-25s\033[0m %s\n", $$1, $$2}' $(MAKEFILE_LIST)

dev: ## Setup dev environment
	poetry env use python3.10
	poetry update
	poetry install
	poetry run pre-commit install

dagster_version=latest
dagster_libs_version=latest
dagster-update:
	poetry add dagster@${dagster_version} dagit@${dagster_version} dagster-postgres@${dagster_libs_version}

docker: ## Runs a local dockerized dagster environment
	docker compose  -f docker-compose.yaml up --build

docker-stop: ## Stops running docker process 
	docker compose  -f docker-compose.yaml down

run: dev ## Run the dagit service
	.venv/bin/dagit -p 3000

test: dev ## Runs pytest with test coverage information
	.venv/bin/pytest tests --cov=src -vv --cov-config=.coveragerc --cov-report=term-missing --cov-branch

format: dev ## Runs pre-commit run on all files
	.venv/bin/pre-commit run --all-files

autoupdate: dev ## Updates pre-commit to latest version
	poetry run pre-commit autoupdate
