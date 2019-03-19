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
import time

from . import base_con
from . import con_utils
from . import transaction

from .protocol import asyncio_proto


class AsyncIOConnection(base_con.BaseConnection):

    def __init__(self, transport, protocol, loop, addr, config, params):
        super().__init__(protocol, addr, config, params)
        self._transport = transport
        self._loop = loop

    async def fetchall(self, query, *args, **kwargs):
        return await self._protocol.execute_anonymous(
            False, False, self._codecs_registry, self._query_cache,
            query, args, kwargs)

    async def fetchone(self, query, *args, **kwargs):
        return await self._protocol.execute_anonymous(
            True, False, self._codecs_registry, self._query_cache,
            query, args, kwargs)

    async def fetchall_json(self, query, *args, **kwargs):
        return await self._protocol.execute_anonymous(
            False, True, self._codecs_registry, self._query_cache,
            query, args, kwargs)

    async def fetchone_json(self, query, *args, **kwargs):
        return await self._protocol.execute_anonymous(
            True, True, self._codecs_registry, self._query_cache,
            query, args, kwargs)

    async def execute(self, query):
        await self._protocol.simple_query(query)

    def transaction(self, *, isolation='read_committed', readonly=False,
                    deferrable=False):
        return transaction.AsyncTransaction(
            self, isolation, readonly, deferrable)

    async def close(self):
        self._protocol.abort()

    def is_closed(self):
        return self._transport.is_closing() or not self._protocol.connected


async def _connect_addr(*, addr, loop, timeout, params, config,
                        connection_class):
    assert loop is not None

    if timeout <= 0:
        raise asyncio.TimeoutError

    protocol_factory = lambda: asyncio_proto.AsyncIOProtocol(
        params, loop)

    if isinstance(addr, str):
        # UNIX socket
        connector = loop.create_unix_connection(protocol_factory, addr)
    else:
        connector = loop.create_connection(protocol_factory, *addr)

    before = time.monotonic()
    tr, pr = await asyncio.wait_for(
        connector, timeout=timeout, loop=loop)
    timeout -= time.monotonic() - before

    try:
        if timeout <= 0:
            raise asyncio.TimeoutError
        await asyncio.wait_for(pr.connect(), timeout=timeout)
    except Exception:
        tr.close()
        raise

    con = connection_class(tr, pr, loop, addr, config, params)
    return con


async def async_connect(dsn=None, *,
                        host=None, port=None,
                        user=None, password=None,
                        database=None,
                        timeout=60):

    loop = asyncio.get_event_loop()

    addrs, params, config = con_utils.parse_connect_arguments(
        dsn=dsn, host=host, port=port, user=user, password=password,
        database=database, timeout=timeout,

        # ToDos
        command_timeout=None,
        server_settings=None)

    last_error = None
    addr = None
    for addr in addrs:
        before = time.monotonic()
        try:
            con = await _connect_addr(
                addr=addr, loop=loop, timeout=timeout,
                params=params, config=config,
                connection_class=AsyncIOConnection)
        except (OSError, asyncio.TimeoutError, ConnectionError) as ex:
            last_error = ex
        else:
            return con
        finally:
            timeout -= time.monotonic() - before

    raise last_error
