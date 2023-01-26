deps:
	poetry install

build:
	poetry run black .
	poetry run isort .
	poetry run flake8 .
	poetry run pytest --black --isort

run: build dev

dev:
	poetry run drugs_gen

dist: build
	poetry build

help:
	poetry run drugs_gen --help
