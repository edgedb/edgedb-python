#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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
import logging
import random
import socket
import ssl
import time
import typing
import uuid

from . import abstract
from . import base_client
from . import compat
from . import con_utils
from . import enums
from . import errors
from . import retry as _retry
from .protocol import asyncio_proto


__all__ = (
    'create_async_client', 'AsyncIOClient'
)


logger = logging.getLogger(__name__)


class AsyncIOConnection(base_client.BaseConnection):
    __slots__ = ("_loop",)

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


class _PoolConnectionHolder(base_client.PoolConnectionHolder):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._release_event = asyncio.Event()
        self._release_event.set()

    async def close(self, *, wait=True):
        if self._con is None:
            return
        if wait:
            await self._con.aclose()
        else:
            self._pool._loop.create_task(self._con.aclose())

    async def wait_until_released(self, timeout=None):
        await self._release_event.wait()


class _AsyncIOPoolImpl(base_client.BasePoolImpl):
    __slots__ = ('_loop',)
    _holder_class = _PoolConnectionHolder

    def __init__(
        self,
        connect_args,
        *,
        concurrency: typing.Optional[int],
        on_connect=None,
        on_acquire=None,
        on_release=None,
        connection_class,
    ):
        super().__init__(
            connect_args,
            connection_class=connection_class,
            concurrency=concurrency,
            on_connect=on_connect,
            on_acquire=on_acquire,
            on_release=on_release,
        )
        self._loop = None
        if not issubclass(connection_class, AsyncIOConnection):
            raise TypeError(
                f'connection_class is expected to be a subclass of '
                f'edgedb.asyncio_client.AsyncIOConnection, '
                f'got {connection_class}')

    def _ensure_initialized(self):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
            self._queue = asyncio.LifoQueue(maxsize=self._concurrency)
            self._first_connect_lock = asyncio.Lock()
            self._resize_holder_pool()

    def _set_queue_maxsize(self, maxsize):
        self._queue._maxsize = maxsize

    async def _new_connection_with_params(self, addr, config, params):
        con = self._connection_class(self._loop, [addr], config, params)
        await con.connect()
        return con

    async def _maybe_get_first_connection(self):
        async with self._first_connect_lock:
            if self._working_addr is None:
                return await self._get_first_connection()

    async def _callback(self, cb, con):
        try:
            await cb(con)
        except (Exception, asyncio.CancelledError) as ex:
            # If a user-defined callback function fails, we don't
            # know if the connection is safe for re-use, hence
            # we close it.  A new connection will be created
            # when `acquire` is called again.
            try:
                # Use `aclose()` to close the connection gracefully.
                # An exception in `setup` isn't necessarily caused
                # by an IO or a protocol error.  aclose() will
                # do the necessary cleanup via _release_on_close().
                await con.aclose()
            finally:
                raise ex

    async def acquire(self, timeout=None):
        self._ensure_initialized()

        async def _acquire_impl():
            ch = await self._queue.get()  # type: _PoolConnectionHolder
            try:
                proxy = await ch.acquire()  # type: AsyncIOConnection
            except (Exception, asyncio.CancelledError):
                self._queue.put_nowait(ch)
                raise
            else:
                # Record the timeout, as we will apply it by default
                # in release().
                ch._timeout = timeout
                return proxy

        if self._closing:
            raise errors.InterfaceError('pool is closing')

        if timeout is None:
            return await _acquire_impl()
        else:
            return await compat.wait_for(
                _acquire_impl(), timeout=timeout)

    async def _release(self, holder):

        if not isinstance(holder._con, AsyncIOConnection):
            raise errors.InterfaceError(
                f'AsyncIOPool.release() received invalid connection: '
                f'{holder._con!r} does not belong to any connection pool'
            )

        timeout = None

        # Use asyncio.shield() to guarantee that task cancellation
        # does not prevent the connection from being returned to the
        # pool properly.
        return await asyncio.shield(holder.release(timeout))

    async def aclose(self):
        """Attempt to gracefully close all connections in the pool.

        Wait until all pool connections are released, close them and
        shut down the pool.  If any error (including cancellation) occurs
        in ``close()`` the pool will terminate by calling
        AsyncIOPool.terminate() .

        It is advisable to use :func:`python:asyncio.wait_for` to set
        a timeout.
        """
        if self._closed:
            return

        if not self._loop:
            self._closed = True
            return

        self._closing = True

        try:
            warning_callback = self._loop.call_later(
                60, self._warn_on_long_close)

            release_coros = [
                ch.wait_until_released() for ch in self._holders]
            await asyncio.gather(*release_coros)

            close_coros = [
                ch.close() for ch in self._holders]
            await asyncio.gather(*close_coros)

        except (Exception, asyncio.CancelledError):
            self.terminate()
            raise

        finally:
            warning_callback.cancel()
            self._closed = True
            self._closing = False

    def _warn_on_long_close(self):
        logger.warning(
            'AsyncIOPool.aclose() is taking over 60 seconds to complete. '
            'Check if you have any unreleased connections left. '
            'Use asyncio.wait_for() to set a timeout for '
            'AsyncIOPool.aclose().')


class AsyncIOClient(abstract.AsyncIOExecutor, base_client.BaseClient):
    """A lazy connection pool.

    A Client can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool.  Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Clients are created by calling
    :func:`~edgedb.asyncio_client.create_async_client`.
    """

    _impl_class = _AsyncIOPoolImpl

    async def ensure_connected(self):
        await self._impl.ensure_connected()
        return self

    def _clear_codecs_cache(self):
        self._impl.codecs_registry.clear_cache()

    def _set_type_codec(
        self,
        typeid: uuid.UUID,
        *,
        encoder: typing.Callable[[typing.Any], typing.Any],
        decoder: typing.Callable[[typing.Any], typing.Any],
        format: str
    ):
        self._impl.codecs_registry.set_type_codec(
            typeid,
            encoder=encoder,
            decoder=decoder,
            format=format,
        )

    async def _query(self, query_context: abstract.QueryContext):
        con = await self._impl.acquire()
        try:
            result, _ = await con.raw_query(query_context)
            return result
        finally:
            await self._impl.release(con)

    async def execute(self, query: str) -> None:
        """Execute an EdgeQL command (or commands).

        Example:

        .. code-block:: pycon

            >>> await con.execute('''
            ...     CREATE TYPE MyType { CREATE PROPERTY a -> int64 };
            ...     FOR x IN {100, 200, 300} UNION INSERT MyType { a := x };
            ... ''')
        """
        con = await self._impl.acquire()
        try:
            await con._protocol.simple_query(
                query, enums.Capability.EXECUTE)
        finally:
            await self._impl.release(con)

    async def aclose(self):
        """Attempt to gracefully close all connections in the pool.

        Wait until all pool connections are released, close them and
        shut down the pool.  If any error (including cancellation) occurs
        in ``close()`` the pool will terminate by calling
        AsyncIOPool.terminate() .

        It is advisable to use :func:`python:asyncio.wait_for` to set
        a timeout.
        """
        await self._impl.aclose()

    def terminate(self):
        """Terminate all connections in the pool."""
        self._impl.terminate()

    def transaction(self) -> _retry.AsyncIORetry:
        return _retry.AsyncIORetry(self)

    async def expire_connections(self):
        """Expire all currently open connections.

        Cause all currently open connections to get replaced on the
        next query.
        """
        await self._impl.expire_connections()

    async def __aenter__(self):
        return await self.ensure_connected()

    async def __aexit__(self, exc_type, exc_val, exc_tb):
        await self.aclose()


def create_async_client(dsn=None, *, concurrency=None, **kwargs):
    return AsyncIOClient(
        connection_class=AsyncIOConnection,
        concurrency=concurrency,

        # connect arguments
        dsn=dsn,
        **kwargs,
    )
