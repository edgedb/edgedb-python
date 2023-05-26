#!/bin/bash

set -Eexuo pipefail
shopt -s nullglob

srv="https://packages.edgedb.com"

curl -fL "${srv}/dist/$(uname -m)-unknown-linux-musl/edgedb-cli" \
    > "/usr/bin/edgedb"

chmod +x "/usr/bin/edgedb"

if command -v useradd >/dev/null 2>&1; then
    useradd --shell /bin/bash edgedb
else
    # musllinux/alpine doesn't have useradd
    adduser -s /bin/bash -D edgedb
fi

su -l edgedb -c "edgedb server install"
ln -s $(su -l edgedb -c "edgedb server info --latest --bin-path") \
    "/usr/bin/edgedb-server"

edgedb-server --version
