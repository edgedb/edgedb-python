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
import errno
import random
import re
import socket
import time
import typing
import warnings

from . import abstract
from . import base_con
from . import con_utils
from . import errors
from . import transaction

from .datatypes import datatypes
from .protocol import asyncio_proto
from .protocol import protocol
from .protocol.protocol import CodecsRegistry as _CodecsRegistry
from .protocol.protocol import QueryCodecsCache as _QueryCodecsCache


ERRNO_RE = re.compile(r"\[Errno (\d+)\]")
TEMPORARY_ERRORS = frozenset({
    errno.ECONNREFUSED,
    errno.ECONNABORTED,
    errno.ECONNRESET,
    errno.ENOENT,
})


def _extract_errno(s):
    """Extract multiple errnos from error string

    When we connect to a host that has multiple underlying IP addresses, say
    ``localhost`` having ``::1`` and ``127.0.0.1``, we get
    ``OSError("Multiple exceptions:...")`` error without ``.errno`` attribute
    set. There are multiple ones in the text, so we extract all of them.
    """
    result = []
    for match in ERRNO_RE.finditer(s):
        result.append(int(match.group(1)))
    if result:
        return result


class _AsyncIOConnectionImpl:

    def __init__(self):
        self._addr = None
        self._transport = None
        self._protocol = None

    def is_closed(self):
        transport = self._transport
        protocol = self._protocol
        return not (
            transport and not transport.is_closing() and
            protocol and protocol.connected
        )

    async def connect(self, loop, addrs, config, params):
        addr = None
        max_time = time.monotonic() + config.wait_until_available
        iteration = 1

        while True:
            for addr in addrs:
                try:
                    await asyncio.wait_for(
                        self._connect_addr(loop, addr, params),
                        config.connect_timeout,
                    )
                except asyncio.TimeoutError as e:
                    if iteration == 1 or time.monotonic() < max_time:
                        continue
                    else:
                        raise errors.ConnectionTimeoutError(
                            f"connecting to {addr} failed in"
                            f" {config.connect_timeout} sec"
                        ) from e
                except errors.ClientConnectionError as e:
                    if (
                        e.has_tag(errors.SHOULD_RECONNECT) and
                        (iteration == 1 or time.monotonic() < max_time)
                    ):
                        continue
                    nice_err = e.__class__(
                        con_utils.render_client_no_connection_error(
                            e,
                            addr,
                        ))
                    raise nice_err from e.__cause__
                else:
                    return

            iteration += 1
            await asyncio.sleep(0.01 + random.random() * 0.2)

    async def _connect_addr(self, loop, addr, params):

        factory = lambda: asyncio_proto.AsyncIOProtocol(
            params, loop)

        try:
            if isinstance(addr, str):
                # UNIX socket
                tr, pr = await loop.create_unix_connection(factory, addr)
            else:
                tr, pr = await loop.create_connection(factory, *addr)
        except socket.gaierror as e:
            # All name resolution errors are considered temporary
            raise errors.ClientConnectionFailedTemporarilyError(str(e)) from e
        except OSError as e:
            message = str(e)
            if e.errno is None:
                errnos = _extract_errno(message)
            else:
                errnos = [e.errno]
            if any((code in TEMPORARY_ERRORS for code in errnos)):
                err = errors.ClientConnectionFailedTemporarilyError(message)
            else:
                err = errors.ClientConnectionFailedError(message)
            raise err from e

        await pr.connect()
        self._transport = tr
        self._protocol = pr
        self._adddr = addr

    async def execute(self, query):
        await self._protocol.simple_query(query)

    def close(self):
        if self._protocol:
            self._protocol.abort()


class AsyncIOConnection(base_con.BaseConnection, abstract.AsyncIOExecutor):

    def __init__(self, loop, addrs, config, params, *,
                 codecs_registry, query_cache):
        super().__init__(addrs, config, params,
                         codecs_registry=codecs_registry,
                         query_cache=query_cache)
        self._loop = loop
        self._impl = None

    def __repr__(self):
        if self.is_closed():
            return '<{classname} [closed] {id:#x}>'.format(
                classname=self.__class__.__name__, id=id(self))
        else:
            return '<{classname} [connected to {addr}] {id:#x}>'.format(
                classname=self.__class__.__name__,
                addr=self.connected_addr(),
                id=id(self))

    def _dispatch_log_message(self, msg):
        for cb in self._log_listeners:
            self._loop.call_soon(cb, self._ensure_proxied(), msg)

    async def ensure_connected(self):
        if not self._impl or self._impl.is_closed():
            await self._reconnect()

    # overriden by connection pool
    async def _reconnect(self):
        self._impl = _AsyncIOConnectionImpl()
        await self._impl.connect(self._loop, self._addrs,
                                 self._config, self._params)

    async def _fetchall(
        self,
        query: str,
        *args,
        __limit__: int=0,
        __typeids__: bool=False,
        __typenames__: bool=False,
        __allow_capabilities__: typing.Optional[int]=None,
        **kwargs,
    ) -> datatypes.Set:
        if not self._impl or self._impl.is_closed():
            await self._reconnect()
        result, _ = await self._impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            implicit_limit=__limit__,
            inline_typeids=__typeids__,
            inline_typenames=__typenames__,
            io_format=protocol.IoFormat.BINARY,
            allow_capabilities=__allow_capabilities__,
        )
        return result

    async def _fetchall_with_headers(
        self,
        query: str,
        *args,
        __limit__: int=0,
        __typeids__: bool=False,
        __typenames__: bool=False,
        __allow_capabilities__: typing.Optional[int]=None,
        **kwargs,
    ) -> typing.Tuple[datatypes.Set, typing.Dict[int, bytes]]:
        if not self._impl or self._impl.is_closed():
            await self._reconnect()
        return await self._impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            implicit_limit=__limit__,
            inline_typeids=__typeids__,
            inline_typenames=__typenames__,
            io_format=protocol.IoFormat.BINARY,
            allow_capabilities=__allow_capabilities__,
        )

    async def _fetchall_json(
        self,
        query: str,
        *args,
        __limit__: int=0,
        **kwargs,
    ) -> datatypes.Set:
        if not self._impl or self._impl.is_closed():
            await self._reconnect()
        result, _ = await self._impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            implicit_limit=__limit__,
            inline_typenames=False,
            io_format=protocol.IoFormat.JSON,
        )
        return result

    async def query(self, query: str, *args, **kwargs) -> datatypes.Set:
        if not self._impl or self._impl.is_closed():
            await self._reconnect()
        result, _ = await self._impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.BINARY,
        )
        return result

    async def query_one(self, query: str, *args, **kwargs) -> typing.Any:
        if not self._impl or self._impl.is_closed():
            await self._reconnect()
        result, _ = await self._impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.BINARY,
        )
        return result

    async def query_json(self, query: str, *args, **kwargs) -> str:
        if not self._impl or self._impl.is_closed():
            await self._reconnect()
        result, _ = await self._impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.JSON,
        )
        return result

    async def _fetchall_json_elements(
            self, query: str, *args, **kwargs) -> typing.List[str]:
        if not self._impl or self._impl.is_closed():
            await self._reconnect()
        result, _ = await self._impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.JSON_ELEMENTS,
        )
        return result

    async def query_one_json(self, query: str, *args, **kwargs) -> str:
        if not self._impl or self._impl.is_closed():
            await self._reconnect()
        result, _ = await self._impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.JSON,
        )
        return result

    async def execute(self, query: str) -> None:
        """Execute an EdgeQL command (or commands).

        Example:

        .. code-block:: pycon

            >>> await con.execute('''
            ...     CREATE TYPE MyType { CREATE PROPERTY a -> int64 };
            ...     FOR x IN {100, 200, 300} UNION INSERT MyType { a := x };
            ... ''')
        """
        if not self._impl or self._impl.is_closed():
            await self._reconnect()
        await self._impl._protocol.simple_query(query)

    def transaction(self, *, isolation: str = None, readonly: bool = None,
                    deferrable: bool = None) -> transaction.AsyncIOTransaction:
        return transaction.AsyncIOTransaction(
            self, isolation, readonly, deferrable)

    async def aclose(self) -> None:
        self.terminate()

    def terminate(self) -> None:
        if not self.is_closed():
            self._impl.close()
        self._cleanup()

    def _set_proxy(self, proxy):
        if self._proxy is not None and proxy is not None:
            # Should not happen unless there is a bug in `Pool`.
            raise errors.InterfaceError(
                'internal client error: connection is already proxied')

        self._proxy = proxy

    def is_closed(self) -> bool:
        return self._impl.is_closed()

    async def fetchall(self, query: str, *args, **kwargs) -> datatypes.Set:
        warnings.warn(
            'The "fetchall()" method is deprecated and is scheduled to be '
            'removed. Use the "query()" method instead.',
            DeprecationWarning, 2)
        return await self.query(query, *args, **kwargs)

    async def fetchone(self, query: str, *args, **kwargs) -> typing.Any:
        warnings.warn(
            'The "fetchone()" method is deprecated and is scheduled to be '
            'removed. Use the "query_one()" method instead.',
            DeprecationWarning, 2)
        return await self.query_one(query, *args, **kwargs)

    async def fetchall_json(self, query: str, *args, **kwargs) -> str:
        warnings.warn(
            'The "fetchall_json()" method is deprecated and is scheduled to '
            'be removed. Use the "query_json()" method instead.',
            DeprecationWarning, 2)
        return await self.query_json(query, *args, **kwargs)

    async def fetchone_json(self, query: str, *args, **kwargs) -> str:
        warnings.warn(
            'The "fetchone_json()" method is deprecated and is scheduled to '
            'be removed. Use the "query_one_json()" method instead.',
            DeprecationWarning, 2)
        return await self.query_one_json(query, *args, **kwargs)


async def async_connect(dsn: str = None, *,
                        host: str = None, port: int = None,
                        user: str = None, password: str = None,
                        admin: bool = None,
                        database: str = None,
                        connection_class=None,
                        wait_until_available: int = 30,
                        timeout: int = 10) -> AsyncIOConnection:

    loop = asyncio.get_event_loop()

    if connection_class is None:
        connection_class = AsyncIOConnection

    addrs, params, config = con_utils.parse_connect_arguments(
        dsn=dsn, host=host, port=port, user=user, password=password,
        database=database, admin=admin, timeout=timeout,
        wait_until_available=wait_until_available,

        # ToDos
        command_timeout=None,
        server_settings=None)

    connection = connection_class(
        loop, addrs, config, params,
        codecs_registry=_CodecsRegistry(),
        query_cache=_QueryCodecsCache(),
    )
    await connection.ensure_connected()
    return connection


async def _connect_addr(loop, addrs, config, params,
                        query_cache, codecs_registry, connection_class):

    if connection_class is None:
        connection_class = AsyncIOConnection

    connection = connection_class(
        loop, addrs, config, params,
        codecs_registry=codecs_registry,
        query_cache=query_cache,
    )
    await connection.ensure_connected()
    return connection
