#!/bin/bash

set -e -x

if [ "${TRAVIS_OS_NAME}" == "osx" ]; then
    brew update >/dev/null
    brew upgrade pyenv || true
    eval "$(pyenv init -)"

    if ! (pyenv versions | grep "${PYTHON_VERSION}$"); then
        pyenv install ${PYTHON_VERSION}
    fi
    pyenv global ${PYTHON_VERSION}
    pyenv rehash

    wget https://packages.edgedb.com/macos/edgedb-1-alpha2-latest.pkg \
        -O edgedb.pkg
    sudo env _EDGEDB_INSTALL_SKIP_BOOTSTRAP=1 installer -dumplog -verbose \
        -pkg "$(pwd)/edgedb.pkg" -target /
    rm edgedb.pkg
fi

if [ "${TRAVIS_OS_NAME}" == "linux" ]; then
    curl https://packages.edgedb.com/keys/edgedb.asc \
        | sudo apt-key add -

    dist=$(awk -F"=" '/VERSION_CODENAME=/ {print $2}' /etc/os-release)
    [ -n "${dist}" ] || \
        dist=$(awk -F"[)(]+" '/VERSION=/ {print $2}' /etc/os-release)
    echo deb https://packages.edgedb.com/apt ${dist}.nightly main \
        | sudo tee /etc/apt/sources.list.d/edgedb.list

    sudo apt-get update
    sudo env _EDGEDB_INSTALL_SKIP_BOOTSTRAP=1 apt-get install edgedb-1-alpha2
fi
