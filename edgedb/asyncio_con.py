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
import functools
import random
import socket
import ssl
import time
import typing

from . import abstract
from . import base_con
from . import compat
from . import con_utils
from . import errors
from . import enums
from . import options
from . import retry as _retry

from .datatypes import datatypes
from .protocol import asyncio_proto
from .protocol import protocol
from .protocol.protocol import CodecsRegistry as _CodecsRegistry
from .protocol.protocol import QueryCodecsCache as _QueryCodecsCache


class _AsyncIOConnectionImpl(base_con.RawConnection):

    def __init__(self, codecs_registry, query_cache):
        super().__init__(codecs_registry, query_cache)
        self._transport = None

    def is_closed(self):
        protocol = self._protocol
        return protocol is None or not protocol.connected

    async def connect(self, loop, addrs, config, params, *,
                      single_attempt=False, connection):
        addr = None
        start = time.monotonic()
        if single_attempt:
            max_time = 0
        else:
            max_time = start + config.wait_until_available
        iteration = 1

        while True:
            for addr in addrs:
                try:
                    await compat.wait_for(
                        self._connect_addr(loop, addr, params, connection),
                        config.connect_timeout,
                    )
                except asyncio.TimeoutError as e:
                    if iteration == 1 or time.monotonic() < max_time:
                        continue
                    else:
                        raise errors.ClientConnectionTimeoutError(
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
                            attempts=iteration,
                            duration=time.monotonic() - start,
                        ))
                    raise nice_err from e.__cause__
                else:
                    return

            iteration += 1
            await asyncio.sleep(0.01 + random.random() * 0.2)

    async def _connect_addr(self, loop, addr, params, connection):

        factory = functools.partial(
            asyncio_proto.AsyncIOProtocol, params, loop
        )
        tr = None

        try:
            if isinstance(addr, str):
                # UNIX socket
                tr, pr = await loop.create_unix_connection(factory, addr)
            else:
                try:
                    tr, pr = await loop.create_connection(
                        factory, *addr, ssl=params.ssl_ctx
                    )
                except ssl.CertificateError as e:
                    raise con_utils.wrap_error(e) from e
                except ssl.SSLError as e:
                    if e.reason == 'CERTIFICATE_VERIFY_FAILED':
                        raise con_utils.wrap_error(e) from e
                    tr, pr = await loop.create_connection(
                        functools.partial(factory, tls_compat=True), *addr
                    )
                else:
                    con_utils.check_alpn_protocol(
                        tr.get_extra_info('ssl_object')
                    )
        except socket.gaierror as e:
            # All name resolution errors are considered temporary
            raise errors.ClientConnectionFailedTemporarilyError(str(e)) from e
        except OSError as e:
            raise con_utils.wrap_error(e) from e
        except Exception:
            if tr is not None:
                tr.close()
            raise

        pr.set_connection(connection)

        try:
            await pr.connect()
        except OSError as e:
            if tr is not None:
                tr.close()
            raise con_utils.wrap_error(e) from e
        except Exception:
            if tr is not None:
                tr.close()
            raise

        self._transport = tr
        self._protocol = pr
        self._addr = addr

    async def privileged_execute(self, query):
        await self._protocol.simple_query(query, enums.Capability.ALL)

    async def aclose(self):
        """Send graceful termination message wait for connection to drop."""
        if not self.is_closed():
            try:
                self._protocol.terminate()
                await self._protocol.wait_for_disconnect()
            except (Exception, asyncio.CancelledError):
                self.terminate()
                raise

    def terminate(self):
        if not self.is_closed():
            self._protocol.abort()


class AsyncIOConnection(
    options._OptionsMixin,
    base_con.BaseConnection,
    abstract.AsyncIOExecutor,
):
    _connection: typing.Optional[_AsyncIOConnectionImpl]

    def __init__(self, loop, addrs, config, params, *,
                 codecs_registry, query_cache):
        self._loop = loop
        super().__init__(addrs, config, params,
                         codecs_registry=codecs_registry,
                         query_cache=query_cache)

    def _dispatch_log_message(self, msg):
        for cb in self._log_listeners:
            self._loop.call_soon(cb, self, msg)

    def _shallow_clone(self):
        new_conn = self.__class__.__new__(self.__class__)
        new_conn._connection = self._connection
        new_conn._addrs = self._addrs
        new_conn._config = self._config
        new_conn._params = self._params
        new_conn._codecs_registry = self._codecs_registry
        new_conn._query_cache = self._query_cache
        new_conn._log_listeners = set()
        return new_conn

    def __repr__(self):
        if self.is_closed():
            return '<{classname} [closed] {id:#x}>'.format(
                classname=self.__class__.__name__, id=id(self))
        else:
            return '<{classname} [connected to {addr}] {id:#x}>'.format(
                classname=self.__class__.__name__,
                addr=self.connected_addr(),
                id=id(self))

    async def ensure_connected(self, *, single_attempt=False):
        if not self._connection or self._connection.is_closed():
            await self._reconnect(single_attempt=single_attempt)

    # overriden by connection pool
    async def _reconnect(self, single_attempt=False):
        impl = _AsyncIOConnectionImpl(
            self._codecs_registry, self._query_cache)
        await impl.connect(self._loop, self._addrs,
                           self._config, self._params,
                           single_attempt=single_attempt,
                           connection=self)
        self._connection = impl

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
        if not self._connection or self._connection.is_closed():
            await self._reconnect()
        result, _ = await self._connection._protocol.execute_anonymous(
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
        if not self._connection or self._connection.is_closed():
            await self._reconnect()
        return await self._connection._protocol.execute_anonymous(
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
        if not self._connection or self._connection.is_closed():
            await self._reconnect()
        result, _ = await self._connection._protocol.execute_anonymous(
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

    async def _execute(
        self,
        query: str,
        args,
        kwargs,
        io_format,
        expect_one=False,
        required_one=False,
    ):
        if not self._connection or self._connection.is_closed():
            await self._reconnect()

        reconnect = False
        capabilities = None
        i = 0
        while True:
            i += 1
            try:
                if reconnect:
                    await self._reconnect(single_attempt=True)
                result, _ = \
                    await self._connection._protocol.execute_anonymous(
                        query=query,
                        args=args,
                        kwargs=kwargs,
                        reg=self._codecs_registry,
                        qc=self._query_cache,
                        io_format=io_format,
                        expect_one=expect_one,
                        required_one=required_one,
                        allow_capabilities=enums.Capability.EXECUTE,
                    )
                return result
            except errors.EdgeDBError as e:
                if not e.has_tag(errors.SHOULD_RETRY):
                    raise e
                if capabilities is None:
                    cache_item = self._query_cache.get(
                        query=query,
                        io_format=io_format,
                        implicit_limit=0,
                        inline_typenames=False,
                        inline_typeids=False,
                        expect_one=expect_one,
                    )
                    if cache_item is not None:
                        _, _, _, capabilities = cache_item
                # A query is read-only if it has no capabilities i.e.
                # capabilities == 0. Read-only queries are safe to retry.
                # Explicit transaction conflicts as well.
                if (
                    capabilities != 0
                    and not isinstance(e, errors.TransactionConflictError)
                ):
                    raise e
                rule = self._options.retry_options.get_rule_for_exception(e)
                if i >= rule.attempts:
                    raise e
                await asyncio.sleep(rule.backoff(i))
                reconnect = self.is_closed()

    async def query(self, query: str, *args, **kwargs) -> datatypes.Set:
        return await self._execute(
            query=query,
            args=args,
            kwargs=kwargs,
            io_format=protocol.IoFormat.BINARY,
        )

    async def query_single(
        self, query: str, *args, **kwargs
    ) -> typing.Union[typing.Any, None]:
        return await self._execute(
            query=query,
            args=args,
            kwargs=kwargs,
            expect_one=True,
            io_format=protocol.IoFormat.BINARY,
        )

    async def query_required_single(
        self, query: str, *args, **kwargs
    ) -> typing.Any:
        return await self._execute(
            query=query,
            args=args,
            kwargs=kwargs,
            expect_one=True,
            required_one=True,
            io_format=protocol.IoFormat.BINARY,
        )

    async def query_json(self, query: str, *args, **kwargs) -> str:
        return await self._execute(
            query=query,
            args=args,
            kwargs=kwargs,
            io_format=protocol.IoFormat.JSON,
        )

    async def _fetchall_json_elements(
            self, query: str, *args, **kwargs) -> typing.List[str]:
        if not self._connection or self._connection.is_closed():
            await self._reconnect()
        result, _ = await self._connection._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.JSON_ELEMENTS,
            allow_capabilities=enums.Capability.EXECUTE,
        )
        return result

    async def query_single_json(self, query: str, *args, **kwargs) -> str:
        return await self._execute(
            query=query,
            args=args,
            kwargs=kwargs,
            io_format=protocol.IoFormat.JSON,
            expect_one=True,
        )

    async def query_required_single_json(
        self, query: str, *args, **kwargs
    ) -> str:
        return await self._execute(
            query=query,
            args=args,
            kwargs=kwargs,
            io_format=protocol.IoFormat.JSON,
            expect_one=True,
            required_one=True
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
        if not self._connection or self._connection.is_closed():
            await self._reconnect()
        await self._connection._protocol.simple_query(
            query, enums.Capability.EXECUTE)

    def transaction(self) -> _retry.AsyncIORetry:
        return _retry.AsyncIORetry(self)

    async def aclose(self) -> None:
        try:
            await self._connection.aclose()
        finally:
            self._cleanup()

    def terminate(self) -> None:
        try:
            self._connection.terminate()
        finally:
            self._cleanup()

    def _set_proxy(self, proxy):
        if self._proxy is not None and proxy is not None:
            # Should not happen unless there is a bug in `Pool`.
            raise errors.InterfaceError(
                'internal client error: connection is already proxied')

        self._proxy = proxy

    def is_closed(self) -> bool:
        return self._connection.is_closed()


async def async_connect_raw(
    dsn: str = None,
    *,
    host: str = None,
    port: int = None,
    credentials: str = None,
    credentials_file: str = None,
    user: str = None,
    password: str = None,
    database: str = None,
    tls_ca: str = None,
    tls_ca_file: str = None,
    tls_security: str = None,
    connection_class=None,
    wait_until_available: int = 30,
    timeout: int = 10,
) -> AsyncIOConnection:

    loop = asyncio.get_event_loop()

    if connection_class is None:
        connection_class = AsyncIOConnection

    connect_config, client_config = con_utils.parse_connect_arguments(
        dsn=dsn,
        host=host,
        port=port,
        credentials=credentials,
        credentials_file=credentials_file,
        user=user,
        password=password,
        database=database,
        timeout=timeout,
        tls_ca=tls_ca,
        tls_ca_file=tls_ca_file,
        tls_security=tls_security,
        wait_until_available=wait_until_available,

        # ToDos
        command_timeout=None,
        server_settings=None,
    )

    connection = connection_class(
        loop, [connect_config.address], client_config, connect_config,
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
