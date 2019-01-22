#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import socket

from . import base_con
from . import con_utils
from . import transaction

from .protocol import blocking_proto


class BlockingIOConnection(base_con.BaseConnection):

    def fetch(self, query, *args, **kwargs):
        return self._protocol.sync_execute_anonymous(
            False, False, self._codecs_registry, self._query_cache,
            query, args, kwargs)

    def fetch_value(self, query, *args, **kwargs):
        return self._protocol.sync_execute_anonymous(
            True, False, self._codecs_registry, self._query_cache,
            query, args, kwargs)

    def fetch_json(self, query, *args, **kwargs):
        return self._protocol.sync_execute_anonymous(
            False, True, self._codecs_registry, self._query_cache,
            query, args, kwargs)

    def execute(self, query):
        self._protocol.sync_simple_query(query)

    def transaction(self, *, isolation='read_committed', readonly=False,
                    deferrable=False):
        return transaction.Transaction(
            self, isolation, readonly, deferrable)

    def close(self):
        self._protocol.abort()


def _connect_addr(*, addr, params, config, connection_class):
    if isinstance(addr, str):
        # UNIX socket
        sock = socket.socket(socket.AF_UNIX)
        sock.connect(addr)
    else:
        sock = socket.socket(socket.AF_INET)
        sock.connect(addr)
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

    proto = blocking_proto.BlockingIOProtocol(params, sock)
    proto.sync_connect()

    con = connection_class(proto, addr, config, params)
    return con


def connect(dsn=None, *,
            host=None, port=None,
            user=None, password=None,
            database=None):

    addrs, params, config = con_utils.parse_connect_arguments(
        dsn=dsn, host=host, port=port, user=user, password=password,
        database=database,

        # ToDos
        timeout=None,
        command_timeout=None,
        server_settings=None)

    last_error = None
    addr = None
    for addr in addrs:
        try:
            con = _connect_addr(
                addr=addr,
                params=params, config=config,
                connection_class=BlockingIOConnection)
        except (OSError, TimeoutError, ConnectionError) as ex:
            last_error = ex
        else:
            return con

    raise last_error
