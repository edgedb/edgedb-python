#!/bin/bash

set -Eexuo pipefail
shopt -s nullglob

srv="https://packages.edgedb.com"

curl -fL "${srv}/dist/x86_64-unknown-linux-musl/edgedb-cli" \
    > "/usr/local/bin/edgedb"

chmod +x "/usr/local/bin/edgedb"

# XXX: replace this with edgedb server install once that supports
#      portable builds.
curl -fL "${srv}/archive/x86_64-unknown-linux-gnu/edgedb-server-1.0-rc.2%2Bc328744.tar.gz" \
    > "/tmp/edgedb-server.tar.gz"

mkdir -p "/opt/edgedb-server"
tar -xz --strip-components=1 -C "/opt/edgedb-server" -f "/tmp/edgedb-server.tar.gz"
ln -s "/opt/edgedb-server/bin/edgedb-server" "/usr/local/bin/edgedb-server"

useradd --shell /bin/bash edgedb
edgedb-server --version
