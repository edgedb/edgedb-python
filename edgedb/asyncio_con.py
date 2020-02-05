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
import typing

from . import base_con
from . import con_utils
from . import errors
from . import transaction

from .datatypes import datatypes
from .protocol import asyncio_proto
from .protocol import protocol


class _ConnectionProxy:
    # Base class to enable `isinstance(AsyncIOConnection)` check.
    __slots__ = ()


class AsyncIOConnectionMeta(type):

    def __instancecheck__(cls, instance):
        mro = type(instance).__mro__
        return AsyncIOConnection in mro or _ConnectionProxy in mro


class AsyncIOConnection(base_con.BaseConnection,
                        metaclass=AsyncIOConnectionMeta):

    def __init__(self, transport, protocol, loop, addr, config, params, *,
                 codecs_registry=None, query_cache=None):
        super().__init__(protocol, addr, config, params,
                         codecs_registry=codecs_registry,
                         query_cache=query_cache)
        self._transport = transport
        self._loop = loop
        self._proxy = None
        # Incremented every time the connection is released back to a pool.
        # Used to catch invalid references to connection-related resources
        # post-release.
        self._pool_release_ctr = 0

    def _dispatch_log_message(self, msg):
        for cb in self._log_listeners:
            self._loop.call_soon(cb, self._ensure_proxied(), msg)

    async def _fetchall(
        self,
        query: str,
        *args,
        __limit__: int=0,
        **kwargs,
    ) -> datatypes.Set:
        return await self._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            implicit_limit=__limit__,
            io_format=protocol.IoFormat.BINARY,
        )

    async def _fetchall_json(
        self,
        query: str,
        *args,
        __limit__: int=0,
        **kwargs,
    ) -> datatypes.Set:
        return await self._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            implicit_limit=__limit__,
            io_format=protocol.IoFormat.JSON,
        )

    async def fetchall(self, query: str, *args, **kwargs) -> datatypes.Set:
        return await self._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.BINARY,
        )

    async def fetchone(self, query: str, *args, **kwargs) -> typing.Any:
        return await self._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.BINARY,
        )

    async def fetchall_json(self, query: str, *args, **kwargs) -> str:
        return await self._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.JSON,
        )

    async def _fetchall_json_elements(
            self, query: str, *args, **kwargs) -> typing.List[str]:
        return await self._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.JSON_ELEMENTS,
        )

    async def fetchone_json(self, query: str, *args, **kwargs) -> str:
        return await self._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.JSON,
        )

    async def execute(self, query: str) -> None:
        """Execute an EdgeQL command (or commands).

        Example:

        .. code-block:: pycon

            >>> await con.execute('''
            ...     CREATE TYPE MyType { CREATE PROPERTY a -> int64 };
            ...     FOR x IN {100, 200, 300} UNION INSERT MyType { a := x };
            ... ''')
        """
        await self._protocol.simple_query(query)

    def transaction(self, *, isolation: str = None, readonly: bool = None,
                    deferrable: bool = None) -> transaction.AsyncIOTransaction:
        return transaction.AsyncIOTransaction(
            self, isolation, readonly, deferrable)

    async def aclose(self) -> None:
        self.terminate()

    def terminate(self) -> None:
        if not self.is_closed():
            self._protocol.abort()
        self._cleanup()

    def _set_proxy(self, proxy):
        if self._proxy is not None and proxy is not None:
            # Should not happen unless there is a bug in `Pool`.
            raise errors.InterfaceError(
                'internal client error: connection is already proxied')

        self._proxy = proxy

    def _ensure_proxied(self):
        if self._proxy is None:
            con_ref = self
        else:
            # `_proxy` is not None when the connection is a member
            # of a connection pool.  Which means that the user is working
            # with a `PoolConnectionProxy` instance, and expects to see it
            # (and not the actual Connection) in their event callbacks.
            con_ref = self._proxy
        return con_ref

    def _on_release(self, stacklevel=1):
        # Invalidate external references to the connection.
        self._pool_release_ctr += 1

    def _cleanup(self):
        # Free the resources associated with this connection.
        # This must be called when a connection is terminated.

        if self._proxy is not None:
            # Connection is a member of a pool, so let the pool
            # know that this connection is dead.
            self._proxy._holder._release_on_close()

        super()._cleanup()

    def is_closed(self) -> bool:
        return self._transport.is_closing() or not self._protocol.connected


async def _connect_addr(*, addr, loop, timeout, params, config,
                        connection_class, codecs_registry=None,
                        query_cache=None):
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

    try:
        tr, pr = await asyncio.wait_for(
            connector, timeout=timeout)
    except (ConnectionError, FileNotFoundError, OSError) as e:
        msg = con_utils.render_client_no_connection_error(e, addr)
        raise errors.ClientConnectionError(msg) from e

    timeout -= time.monotonic() - before

    try:
        if timeout <= 0:
            raise asyncio.TimeoutError
        await asyncio.wait_for(pr.connect(), timeout=timeout)
    except (Exception, asyncio.CancelledError):
        tr.close()
        raise

    con = connection_class(tr, pr, loop, addr, config, params,
                           codecs_registry=codecs_registry,
                           query_cache=query_cache)
    return con


async def async_connect(dsn: str = None, *,
                        host: str = None, port: int = None,
                        user: str = None, password: str = None,
                        admin: str = None,
                        database: str = None,
                        connection_class=None,
                        timeout: int = 60) -> AsyncIOConnection:

    loop = asyncio.get_event_loop()

    if connection_class is None:
        connection_class = AsyncIOConnection

    addrs, params, config = con_utils.parse_connect_arguments(
        dsn=dsn, host=host, port=port, user=user, password=password,
        database=database, admin=admin, timeout=timeout,

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
                connection_class=connection_class)
        except (OSError, asyncio.TimeoutError, ConnectionError,
                errors.ClientConnectionError) as ex:
            last_error = ex
        else:
            return con
        finally:
            timeout -= time.monotonic() - before

    raise last_error
