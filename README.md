
# PySpark Template Project #

[![made-with-python](https://img.shields.io/badge/Made%20with-Python-1f425f.svg)](https://www.python.org/)
[![python-3.10](https://img.shields.io/badge/Python-%203.10%20-blue)](https://www.python.org/)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)
[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)
[![Checked with mypy](https://camo.githubusercontent.com/34b3a249cd6502d0a521ab2f42c8830b7cfd03fa/687474703a2f2f7777772e6d7970792d6c616e672e6f72672f7374617469632f6d7970795f62616467652e737667)](http://mypy-lang.org/)
[![made-with-Markdown](https://img.shields.io/badge/Made%20with-Markdown-1f425f.svg)](http://commonmark.org)

People has asked me several times how to setup a good/clean/code organization for Python project with PySpark. I didn't find a fully feature project, so this is my attempt for one. Moreover, have a simple integration with Jupyter Notebook inside the project too.

**Table of Contents**
- [PySpark Template Project](#pyspark-template-project)
  - [**Usage**](#usage)
  - [Inspiration](#inspiration)
  - [Development](#development)
    - [Prerequisites](#prerequisites)
    - [Pyenv Manual Install \[Optional\]](#pyenv-manual-install-optional)
    - [Add format, lint code tools](#add-format-lint-code-tools)
      - [Autolint/Format code with Black in IDE:](#autolintformat-code-with-black-in-ide)
      - [Checked optional type with Mypy PEP 484](#checked-optional-type-with-mypy-pep-484)
      - [Isort](#isort)
  - [Fix](#fix)
  - [Usage Local](#usage-local)
  - [Use with poetry](#use-with-poetry)
  - [Usage in distributed-mode depending on your cluster manager type](#usage-in-distributed-mode-depending-on-your-cluster-manager-type)

## [**Usage**](USAGE.md)

## Inspiration

* https://mungingdata.com/pyspark/chaining-dataframe-transformations/
* https://medium.com/albert-franzi/the-spark-job-pattern-862bc518632a
* https://pawamoy.github.io/copier-poetry/
* https://drivendata.github.io/cookiecutter-data-science/#why-use-this-project-structure

## Development

### Prerequisites
All you need is the following configuration already installed:

* [Git](https://git-scm.com/book/en/v2/Getting-Started-Installing-Git)
* The project was tested with **Python 3.10.13** managed by [pyenv](https://github.com/pyenv):
  * Use `make pyenv` goal to launch the automated install of pyenv
* `JAVA_HOME` environment variable configured with a Java `JDK11`
* `SPARK_HOME` environment variable configured with Spark version `spark-3.5.2-bin-hadoop3` package
* `PYSPARK_PYTHON` environment variable configured with `"python3.10"`
* `PYSPARK_DRIVER_PYTHON` environment variable configured with `"python3.10"`
* Install Make to run `Makefile` file
* Why `Python 3.10` because `PySpark 3.5.2` doesn't work with `Python 3.11` at the moment it seems (I haven't tried with Python 3.12)

### Pyenv Manual Install [Optional]

* [pyenv prerequisites for ubuntu](https://github.com/pyenv/pyenv/wiki#suggested-build-environment). Check the prerequisites for your OS.
  ```
  sudo apt-get update; sudo apt-get install make build-essential libssl-dev zlib1g-dev \
  libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm \
  libncursesw5-dev xz-utils tk-dev libxml2-dev libxmlsec1-dev libffi-dev liblzma-dev
  ```
* `pyenv` installed and available in path [pyenv installation](https://github.com/pyenv/pyenv#installation) with Prerequisites
* Install python 3.10 with pyenv on homebrew/linuxbrew
```shell
CONFIGURE_OPTS="--with-openssl=$(brew --prefix openssl)" pyenv install 3.10
```

### Add format, lint code tools

#### Autolint/Format code with Black in IDE:

[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/psf/black)

* Auto format via IDE https://github.com/psf/black#pycharmintellij-idea
* [Optional] You could setup a pre-commit to enforce Black format before commit https://github.com/psf/black#version-control-integration
* Or remember to type `black .` to apply the black rules formatting to all sources before commit
* Add integratin with Jenkins and it will complain and tests will fail if black format is not applied

* Add same mypy option for vscode in `Preferences: Open User Settings`
* Use the option to lint/format with black and flake8 on editor save in vscode

#### Checked optional type with Mypy [PEP 484](https://www.python.org/dev/peps/pep-0484/)

[![Checked with mypy](https://camo.githubusercontent.com/34b3a249cd6502d0a521ab2f42c8830b7cfd03fa/687474703a2f2f7777772e6d7970792d6c616e672e6f72672f7374617469632f6d7970795f62616467652e737667)](http://mypy-lang.org/)

Configure Mypy to help annotating/hinting type with Python Code. It's very useful for IDE and for catching errors/bugs early. 

* Install [mypy plugin for intellij](https://plugins.jetbrains.com/plugin/11086-mypy)
* Adjust the plugin with the following options:
    ```
    "--follow-imports=silent",
    "--show-column-numbers",
    "--ignore-missing-imports",
    "--disallow-untyped-defs",
    "--check-untyped-defs"
    ```  
* Documentation: [Type hints cheat sheet (Python 3)](https://mypy.readthedocs.io/en/stable/cheat_sheet_py3.html)
* Add same mypy option for vscode in `Preferences: Open User Settings` 

#### Isort

[![Imports: isort](https://img.shields.io/badge/%20imports-isort-%231674b1?style=flat&labelColor=ef8336)](https://pycqa.github.io/isort/)

* [isort is the default on pycharm](https://www.jetbrains.com/help/pycharm/code-style-python.html#imports_table)
* [isort with vscode](https://cereblanco.medium.com/setup-black-and-isort-in-vscode-514804590bf9)
* Lint/format/sort import on save with vscode in `Preferences: Open User Settings`:

```
{
    "editor.formatOnSave": true,
    "python.formatting.provider": "black",
    "[python]": {
        "editor.codeActionsOnSave": {
            "source.organizeImports": true
        }
    }
}
```

* isort configuration for pycharm. See [Set isort and black formatting code in pycharm](https://www.programmersought.com/article/23126972182/)
* You can use `make lint` command to check flake8/mypy rules & apply automatically format black and isort to the code with the previous configuration

```
isort .
```

## Fix

* Show a way to treat json erroneous file like `data/pubmed.json`

## Usage Local

* Create a poetry env with python 3.10
```shell
poetry env use 3.10
```
* Install [pyenv](https://github.com/pyenv) `make pyenv`
* Install dependencies in poetry env (virtualenv) `make deps`
* Lint & Test `make build`
* Lint,Test & Run `make run`
* Run dev `make dev`
* Build binary/python whell `make dist`

## Use with poetry

`poetry run drugs_gen --help` 

```
Usage: drugs_gen [OPTIONS]

Options:
  -d, --drugs TEXT             Path to drugs.csv
  -p, --pubmed TEXT            Path to pubmed.csv
  -c, --clinicals_trials TEXT  Path to clinical_trials.csv
  -o, --output TEXT            Output path to result.json (e.g
                               /path/to/result.json)
  --help                       Show this message and exit.
```

## Usage in distributed-mode depending on your cluster manager type

* Use `spark-submit` with the Python Wheel file built by `make dist` command in the `dist` folder.
