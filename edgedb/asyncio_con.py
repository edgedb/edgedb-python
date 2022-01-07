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

from . import abstract
from . import base_con
from . import compat
from . import con_utils
from . import errors
from . import enums

from .protocol import asyncio_proto


class AsyncIOConnection(base_con.BaseConnection):

    def __init__(self, loop, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._loop = loop

    def is_closed(self):
        protocol = self._protocol
        return protocol is None or not protocol.connected

    async def connect(self, *, single_attempt=False):
        start = time.monotonic()
        if single_attempt:
            max_time = 0
        else:
            max_time = start + self._config.wait_until_available
        iteration = 1

        while True:
            for addr in self._addrs:
                try:
                    await compat.wait_for(
                        self._connect_addr(addr),
                        self._config.connect_timeout,
                    )
                except asyncio.TimeoutError as e:
                    if iteration == 1 or time.monotonic() < max_time:
                        continue
                    else:
                        raise errors.ClientConnectionTimeoutError(
                            f"connecting to {addr} failed in"
                            f" {self._config.connect_timeout} sec"
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

    def _protocol_factory(self, tls_compat=False):
        return asyncio_proto.AsyncIOProtocol(
            self._params, self._loop, tls_compat=tls_compat
        )

    async def _connect_addr(self, addr):
        tr = None

        try:
            if isinstance(addr, str):
                # UNIX socket
                tr, pr = await self._loop.create_unix_connection(
                    self._protocol_factory, addr
                )
            else:
                try:
                    tr, pr = await self._loop.create_connection(
                        self._protocol_factory, *addr, ssl=self._params.ssl_ctx
                    )
                except ssl.CertificateError as e:
                    raise con_utils.wrap_error(e) from e
                except ssl.SSLError as e:
                    if e.reason == 'CERTIFICATE_VERIFY_FAILED':
                        raise con_utils.wrap_error(e) from e
                    tr, pr = await self._loop.create_connection(
                        functools.partial(
                            self._protocol_factory, tls_compat=True
                        ),
                        *addr,
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

        pr.set_connection(self)

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
            finally:
                self._cleanup()

    def terminate(self):
        if not self.is_closed():
            try:
                self._protocol.abort()
            finally:
                self._cleanup()

    def _cleanup(self):
        pass

    def _dispatch_log_message(self, msg):
        for cb in self._log_listeners:
            self._loop.call_soon(cb, self, msg)

    def __repr__(self):
        if self.is_closed():
            return '<{classname} [closed] {id:#x}>'.format(
                classname=self.__class__.__name__, id=id(self))
        else:
            return '<{classname} [connected to {addr}] {id:#x}>'.format(
                classname=self.__class__.__name__,
                addr=self.connected_addr(),
                id=id(self))

    async def raw_query(self, query_context: abstract.QueryContext):
        if self.is_closed():
            await self.connect()

        reconnect = False
        capabilities = None
        i = 0
        while True:
            i += 1
            try:
                if reconnect:
                    await self.connect(single_attempt=True)
                return await self._protocol.execute_anonymous(
                    query=query_context.query.query,
                    args=query_context.query.args,
                    kwargs=query_context.query.kwargs,
                    reg=query_context.cache.codecs_registry,
                    qc=query_context.cache.query_cache,
                    io_format=query_context.query_options.io_format,
                    expect_one=query_context.query_options.expect_one,
                    required_one=query_context.query_options.required_one,
                    allow_capabilities=enums.Capability.EXECUTE,
                )
            except errors.EdgeDBError as e:
                if query_context.retry_options is None:
                    raise
                if not e.has_tag(errors.SHOULD_RETRY):
                    raise e
                if capabilities is None:
                    cache_item = query_context.cache.query_cache.get(
                        query=query_context.query.query,
                        io_format=query_context.query_options.io_format,
                        implicit_limit=0,
                        inline_typenames=False,
                        inline_typeids=False,
                        expect_one=query_context.query_options.expect_one,
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
                rule = query_context.retry_options.get_rule_for_exception(e)
                if i >= rule.attempts:
                    raise e
                await asyncio.sleep(rule.backoff(i))
                reconnect = self.is_closed()

    async def execute(self, query: str) -> None:
        await self._protocol.simple_query(
            query, enums.Capability.EXECUTE)


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
    loop=None,
) -> AsyncIOConnection:

    if loop is None:
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
    )
    await connection.connect()
    return connection


async def _connect_addr(loop, addrs, config, params, connection_class):

    if connection_class is None:
        connection_class = AsyncIOConnection

    connection = connection_class(
        loop, addrs, config, params,
    )
    await connection.connect()
    return connection
