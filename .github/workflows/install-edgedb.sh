#!/bin/bash

set -Eexuo pipefail
shopt -s nullglob

srv="https://packages.edgedb.com"

curl -fL "${srv}/dist/x86_64-unknown-linux-musl/edgedb-cli" \
    > "/usr/local/bin/edgedb"

chmod +x "/usr/local/bin/edgedb"

useradd --shell /bin/bash edgedb

su -l edgedb -c "edgedb server install"
ln -s $(su -l edgedb -c "edgedb server info --latest --bin-path") \
    "/usr/local/bin/edgedb-server"

edgedb-server --version
