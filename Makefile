.DEFAULT_GOAL := help
.PHONY: help deps build run dev dist show pyenv clean

# Makefile help for the project https://news.ycombinator.com/item?id=11939200
help: ## Show this help.
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | sort | awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-30s\033[0m %s\n", $$1, $$2}'

deps: ## Install deps via Poetry
	pip install -U pip poetry "packaging>=23.1"
	poetry install

build: ## Run black, isort, flake8 and pytest
	poetry run black .
	poetry run isort .
	poetry run flake8 .
	poetry run pytest --black --isort

run: build dev

dev: ## Run the project
	poetry run drugs_gen

dist: build ## Build the project distribution
	poetry build

show: ## Show the help of the project
	poetry run drugs_gen --help

pyenv: ## Install pyenv
	bash pyenv.sh

clean: ## Clear pyenv virtualenv
	pyenv uninstall -f pyspark-template-3.10.13

