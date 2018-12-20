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


import asyncio
import collections

import getpass
import os
import time

from .errors import *  # NoQA
from . import transaction

from edgedb.protocol.aprotocol import CodecsRegistry as _CodecsRegistry
from edgedb.protocol.aprotocol import QueryCache as _QueryCache

from edgedb.protocol.aprotocol import Tuple, NamedTuple  # NoQA
from edgedb.protocol.aprotocol import Set, Object, Array  # NoQA
from edgedb.protocol.aprotocol import Protocol


__all__ = errors.__all__ + ('connect',)  # NoQA


_ConnectionParameters = collections.namedtuple(
    'ConnectionParameters',
    [
        'user',
        'password',
        'database',
        'ssl',
        'connect_timeout',
        'server_settings',
    ])


class Connection:

    def __init__(self, transport, protocol, loop):
        self._loop = loop
        self._transport = transport
        self._protocol = protocol

        self._codecs_registry = _CodecsRegistry()
        self._query_cache = _QueryCache()

        self._top_xact = None

    async def fetch(self, query, *args, **kwargs):
        return await self._protocol.execute_anonymous(
            self._codecs_registry, self._query_cache,
            query, args, kwargs)

    async def _legacy_execute(self, query, *, graphql=False):
        return await self._protocol.legacy(query, graphql)

    async def close(self):
        self._protocol.abort()

    def transaction(self):
        return transaction.Transaction(
            self, 'read_committed', False, False)


EDGEDB_PORT = 5656 + 1


async def connect(*,
                  host=None, port=None,
                  user=None, password=None,
                  database=None,
                  timeout=60,
                  retry_on_failure=False):

    loop = asyncio.get_event_loop()

    if host is None:
        host = os.getenv('EDGEDB_HOST')
        if not host:
            host = ['/tmp', '/private/tmp', '/run/edgedb', 'localhost']

    if not isinstance(host, list):
        host = [host]

    if port is None:
        port = os.getenv('EDGEDB_PORT')
        if not port:
            port = EDGEDB_PORT

    if user is None:
        user = os.getenv('EDGEDB_USER')
        if not user:
            user = getpass.getuser()

    if password is None:
        password = os.getenv('EDGEDB_PASSWORD')

    if database is None:
        database = os.getenv('EDGEDB_DATABASE', user)

    budget = timeout
    time_between_tries = 0.1

    con_param = _ConnectionParameters(user, password, database, False, 1, None)

    while budget >= 0:
        start = time.monotonic()

        last_ex = None
        for h in host:
            protocol_factory = lambda: Protocol((h, port), con_param, loop)

            if h.startswith('/'):
                # UNIX socket name
                sname = os.path.join(h, '.s.EDGEDB.{}'.format(port))
                conn = loop.create_unix_connection(protocol_factory, sname)
            else:
                conn = loop.create_connection(protocol_factory, h, port)

            try:
                tr, pr = await asyncio.wait_for(
                    conn, timeout=budget, loop=loop)
            except (OSError, asyncio.TimeoutError) as ex:
                last_ex = ex
            else:
                last_ex = None
                break

        if last_ex is None:
            try:
                await pr.connect()
            except BaseException as ex:
                tr.close()
                last_ex = ex
            else:
                break

        if last_ex is not None:
            if retry_on_failure:
                budget -= time.monotonic() - start + time_between_tries
                if budget > 0:
                    await asyncio.sleep(time_between_tries)
            else:
                raise last_ex

    return Connection(tr, pr, loop)
