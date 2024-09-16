#!/bin/bash

set -ex

PYTHON_VERSION=3.10.13
PYENV_VENV=pyspark-template

# Check pyenv installed
if [ -d "${HOME}/.pyenv" ]; then
    echo "Pyenv already installed. If you want to reset the installation, please remove the directory: ${HOME}/.pyenv"
    echo "rm -fr ${HOME}/.pyenv"
else
    echo "Pyenv Installing..."
    sudo apt update -y
    sudo apt install -y make build-essential libssl-dev zlib1g-dev libbz2-dev libreadline-dev libsqlite3-dev wget curl llvm libncurses5-dev libncursesw5-dev xz-utils tk-dev libffi-dev liblzma-dev python3-openssl git
    curl https://pyenv.run | bash

    echo 'export PYENV_ROOT="$HOME/.pyenv"' >> ~/.bashrc
    echo '[[ -d $PYENV_ROOT/bin ]] && export PATH="$PYENV_ROOT/bin:$PATH"' >> ~/.bashrc
    echo 'eval "$(pyenv init -)"' >> ~/.bashrc
    echo 'eval "$(pyenv virtualenv-init -)"' >> ~/.bashrc
    
    export PYENV_ROOT="$HOME/.pyenv"
    echo "$PYENV_ROOT"
    export PATH="$PYENV_ROOT/bin:$PATH"
    echo "$PATH"
    eval "$(pyenv init -)"
    eval "$(pyenv virtualenv-init -)"
    pyenv --version
fi

echo "pyenv versions: "
pyenv versions

PYENV_DEBUG=1 pyenv install --skip-existing ${PYTHON_VERSION}

# Create pyenv virtualenv 
if [ ! -d "$(pyenv root)/versions/${PYENV_VENV}" ]; then
    pyenv virtualenv ${PYTHON_VERSION} --force ${PYENV_VENV}-${PYTHON_VERSION}
fi
pyenv local ${PYENV_VENV}-${PYTHON_VERSION}
pyenv versions