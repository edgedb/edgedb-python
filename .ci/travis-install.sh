#!/bin/bash

set -e -x

if [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    PYENV_ROOT="$HOME/.pyenv"
    PATH="$PYENV_ROOT/bin:$PATH"
    eval "$(pyenv init -)"
fi

git clone https://github.com/edgedb/edgedb.git edgedb-server
cd edgedb-server
git submodule update --init --depth 50
pip install --verbose -e .
cd ../

pip install --upgrade setuptools pip wheel
pip download --dest=/tmp/deps .[test]
pip install -U --no-index --find-links=/tmp/deps /tmp/deps/*
