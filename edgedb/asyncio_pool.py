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
import logging
import typing
import uuid

from . import abstract
from . import asyncio_con
from . import compat
from . import enums
from . import errors
from . import options
from . import retry as _retry
from .protocol.protocol import CodecsRegistry as _CodecsRegistry
from .protocol.protocol import QueryCodecsCache as _QueryCodecsCache


__all__ = (
    'create_async_client', 'AsyncIOClient'
)


logger = logging.getLogger(__name__)


class PoolConnection(asyncio_con.AsyncIOConnection):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._holder = None

    def _cleanup(self):
        if self._holder:
            self._holder._release_on_close()


class PoolConnectionHolder:

    __slots__ = ('_con', '_pool',
                 '_on_acquire', '_on_release',
                 '_in_use', '_timeout', '_generation')

    def __init__(self, pool, *, on_acquire, on_release):

        self._pool = pool
        self._con = None

        self._on_acquire = on_acquire
        self._on_release = on_release
        self._in_use = None  # type: asyncio.Future
        self._timeout = None
        self._generation = None

    async def connect(self):
        if self._con is not None:
            raise errors.InternalClientError(
                'PoolConnectionHolder.connect() called while another '
                'connection already exists')

        self._con = await self._pool._get_new_connection()
        assert self._con._holder is None
        self._con._holder = self
        self._generation = self._pool._generation

    async def acquire(self) -> PoolConnection:
        if self._con is None or self._con.is_closed():
            self._con = None
            await self.connect()

        elif self._generation != self._pool._generation:
            # Connections have been expired, re-connect the holder.
            self._pool._loop.create_task(
                self._con.aclose(timeout=self._timeout))
            self._con = None
            await self.connect()

        if self._on_acquire is not None:
            try:
                await self._on_acquire(self._con)
            except (Exception, asyncio.CancelledError) as ex:
                # If a user-defined `on_acquire` function fails, we don't
                # know if the connection is safe for re-use, hence
                # we close it.  A new connection will be created
                # when `acquire` is called again.
                try:
                    # Use `close()` to close the connection gracefully.
                    # An exception in `on_acquire` isn't necessarily caused
                    # by an IO or a protocol error.  close() will
                    # do the necessary cleanup via _release_on_close().
                    await self._con.aclose()
                finally:
                    raise ex

        self._in_use = self._pool._loop.create_future()

        return self._con

    async def release(self, timeout):
        if self._in_use is None:
            raise errors.InternalClientError(
                'PoolConnectionHolder.release() called on '
                'a free connection holder')

        if self._con.is_closed():
            # This is usually the case when the connection is broken rather
            # than closed by the user, so we need to call _release_on_close()
            # here to release the holder back to the queue, because
            # self._con._cleanup() was never called. On the other hand, it is
            # safe to call self._release() twice - the second call is no-op.
            self._release_on_close()
            return

        self._timeout = None

        if self._generation != self._pool._generation:
            # The connection has expired because it belongs to
            # an older generation (AsyncIOPool.expire_connections() has
            # been called.)
            await self._con.aclose()
            return

        if self._on_release is not None:
            try:
                await self._on_release(self._con)
            except (Exception, asyncio.CancelledError) as ex:
                # If a user-defined `on_release` function fails, we don't
                # know if the connection is safe for re-use, hence
                # we close it.  A new connection will be created
                # when `acquire` is called again.
                try:
                    # Use `close()` to close the connection gracefully.
                    # An exception in `setup` isn't necessarily caused
                    # by an IO or a protocol error.  close() will
                    # do the necessary cleanup via _release_on_close().
                    await self._con.aclose()
                finally:
                    raise ex

        # Free this connection holder and invalidate the
        # connection proxy.
        self._release()

    async def wait_until_released(self):
        if self._in_use is None:
            return
        else:
            await self._in_use

    async def aclose(self):
        if self._con is not None:
            # AsyncIOConnection.aclose() will call _release_on_close() to
            # finish holder cleanup.
            await self._con.aclose()

    def terminate(self):
        if self._con is not None:
            # AsyncIOConnection.terminate() will call _release_on_close() to
            # finish holder cleanup.
            self._con.terminate()

    def _release_on_close(self):
        self._release()
        self._con = None

    def _release(self):
        """Release this connection holder."""
        if self._in_use is None:
            # The holder is not checked out.
            return

        if not self._in_use.done():
            self._in_use.set_result(None)
        self._in_use = None

        # Put ourselves back to the pool queue.
        self._pool._queue.put_nowait(self)


class _AsyncIOPoolImpl:
    __slots__ = ('_queue', '_loop', '_user_concurrency', '_concurrency',
                 '_first_connect_lock',
                 '_on_connect', '_connect_args',
                 '_working_addr', '_working_config', '_working_params',
                 '_codecs_registry', '_query_cache',
                 '_holders', '_initialized', '_initializing', '_closing',
                 '_closed', '_connection_class', '_generation',
                 '_on_acquire', '_on_release')

    def __init__(self, connect_args, *,
                 concurrency: typing.Optional[int],
                 on_acquire,
                 on_release,
                 on_connect,
                 connection_class):
        super().__init__()

        self._loop = None

        if concurrency is not None and concurrency <= 0:
            raise ValueError('concurrency is expected to be greater than zero')

        if not issubclass(connection_class, PoolConnection):
            raise TypeError(
                f'connection_class is expected to be a subclass of '
                f'edgedb.asyncio_pool.PoolConnection, '
                f'got {connection_class}')

        self._user_concurrency = concurrency
        self._concurrency = concurrency if concurrency else 1

        self._on_acquire = on_acquire
        self._on_release = on_release

        self._holders = []
        self._queue = None

        self._first_connect_lock = None
        self._working_addr = None
        self._working_config = None
        self._working_params = None

        self._connection_class = connection_class

        self._closing = False
        self._closed = False
        self._generation = 0
        self._on_connect = on_connect
        self._connect_args = connect_args
        self._codecs_registry = _CodecsRegistry()
        self._query_cache = _QueryCodecsCache()

    def _ensure_initialized(self):
        if self._loop is None:
            self._loop = asyncio.get_event_loop()
            self._queue = asyncio.LifoQueue(maxsize=self._concurrency)
            self._first_connect_lock = asyncio.Lock()
            self._resize_holder_pool()

    def _resize_holder_pool(self):
        resize_diff = self._concurrency - len(self._holders)

        if (resize_diff > 0):
            if self._queue.maxsize != self._concurrency:
                self._queue._maxsize = self._concurrency

            for _ in range(resize_diff):
                ch = PoolConnectionHolder(
                    self,
                    on_acquire=self._on_acquire,
                    on_release=self._on_release)

                self._holders.append(ch)
                self._queue.put_nowait(ch)
        elif resize_diff < 0:
            # TODO: shrink the pool
            pass

    def set_connect_args(self, dsn=None, **connect_kwargs):
        r"""Set the new connection arguments for this pool.

        The new connection arguments will be used for all subsequent
        new connection attempts.  Existing connections will remain until
        they expire. Use AsyncIOPool.expire_connections() to expedite
        the connection expiry.

        :param str dsn:
            Connection arguments specified using as a single string in
            the following format:
            ``edgedb://user:pass@host:port/database?option=value``.

        :param \*\*connect_kwargs:
            Keyword arguments for the :func:`~edgedb.asyncio_con.connect`
            function.
        """

        connect_kwargs["dsn"] = dsn
        self._connect_args = connect_kwargs
        self._working_addr = None
        self._working_config = None
        self._working_params = None
        self._codecs_registry = _CodecsRegistry()
        self._query_cache = _QueryCodecsCache()

    async def _get_first_connection(self):
        # First connection attempt on this pool.
        con = await asyncio_con.async_connect_raw(
            connection_class=self._connection_class,
            **self._connect_args)

        self._working_addr = con.connected_addr()
        self._working_config = con._config
        self._working_params = con._params

        if self._user_concurrency is None:
            suggested_concurrency = con.get_settings().get(
                'suggested_pool_concurrency')
            if suggested_concurrency:
                self._concurrency = suggested_concurrency
                self._resize_holder_pool()
        return con

    async def _get_new_connection(self):
        con = None
        if self._working_addr is None:
            async with self._first_connect_lock:
                if self._working_addr is None:
                    con = await self._get_first_connection()
        if con is None:
            assert self._working_addr is not None
            # We've connected before and have a resolved address,
            # and parsed options and config.
            con = await asyncio_con._connect_addr(
                loop=self._loop,
                addrs=[self._working_addr],
                config=self._working_config,
                params=self._working_params,
                connection_class=self._connection_class)

        if self._on_connect is not None:
            try:
                await self._on_connect(con)
            except (Exception, asyncio.CancelledError) as ex:
                # If a user-defined `connect` function fails, we don't
                # know if the connection is safe for re-use, hence
                # we close it.  A new connection will be created
                # when `acquire` is called again.
                try:
                    # Use `close()` to close the connection gracefully.
                    # An exception in `init` isn't necessarily caused
                    # by an IO or a protocol error.  close() will
                    # do the necessary cleanup via _release_on_close().
                    await con.aclose()
                finally:
                    raise ex

        return con

    async def _acquire(self, timeout=None):
        self._ensure_initialized()

        async def _acquire_impl():
            ch = await self._queue.get()  # type: PoolConnectionHolder
            try:
                proxy = await ch.acquire()  # type: PoolConnection
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

    async def release(self, connection):

        if not isinstance(connection, PoolConnection):
            raise errors.InterfaceError(
                f'AsyncIOPool.release() received invalid connection: '
                f'{connection!r} does not belong to any connection pool'
            )

        ch = connection._holder
        if ch is None:
            # Already released, do nothing.
            return

        if ch._pool is not self:
            raise errors.InterfaceError(
                f'AsyncIOPool.release() received invalid connection: '
                f'{connection!r} is not a member of this pool'
            )

        timeout = None

        # Use asyncio.shield() to guarantee that task cancellation
        # does not prevent the connection from being returned to the
        # pool properly.
        return await asyncio.shield(ch.release(timeout))

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
                ch.aclose() for ch in self._holders]
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

    def terminate(self):
        """Terminate all connections in the pool."""
        if self._closed:
            return
        for ch in self._holders:
            ch.terminate()
        self._closed = True

    async def expire_connections(self):
        """Expire all currently open connections.

        Cause all currently open connections to get replaced on the
        next AsyncIOPool.acquire() call.
        """
        self._generation += 1

    async def ensure_connected(self):
        self._ensure_initialized()

        for ch in self._holders:
            if ch._con is not None and not ch._con.is_closed():
                return

        ch = self._holders[0]
        ch._con = None
        await ch.connect()

    # TODO: never implemented or used?
    # def _drop_statement_cache(self):
    #     # Drop statement cache for all connections in the pool.
    #     for ch in self._holders:
    #         if ch._con is not None:
    #             ch._con._drop_local_statement_cache()
    #
    # def _drop_type_cache(self):
    #     # Drop type codec cache for all connections in the pool.
    #     for ch in self._holders:
    #         if ch._con is not None:
    #             ch._con._drop_local_type_cache()


class AsyncIOClient(abstract.AsyncIOExecutor, options._OptionsMixin):
    """A lazy connection pool.

    A Client can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool.  Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Clients are created by calling
    :func:`~edgedb.asyncio_pool.create_async_client`.
    """

    __slots__ = ('_impl', '_options')

    def __init__(self, *,
                 concurrency: int,
                 on_acquire,
                 on_release,
                 on_connect,
                 connection_class,
                 **connect_args):
        super().__init__()
        self._impl = _AsyncIOPoolImpl(
            connect_args,
            concurrency=concurrency,
            on_acquire=on_acquire,
            on_release=on_release,
            on_connect=on_connect,
            connection_class=connection_class,
        )

    @property
    def concurrency(self) -> int:
        """Max number of connections in the pool."""

        return self._impl._concurrency

    async def ensure_connected(self):
        await self._impl.ensure_connected()
        return self

    def _clear_codecs_cache(self):
        self._impl._codecs_registry.clear_cache()

    def _set_type_codec(
        self,
        typeid: uuid.UUID,
        *,
        encoder: typing.Callable[[typing.Any], typing.Any],
        decoder: typing.Callable[[typing.Any], typing.Any],
        format: str
    ):
        self._impl._codecs_registry.set_type_codec(
            typeid,
            encoder=encoder,
            decoder=decoder,
            format=format,
        )

    def _get_query_cache(self) -> abstract.QueryCache:
        return abstract.QueryCache(
            codecs_registry=self._impl._codecs_registry,
            query_cache=self._impl._query_cache,
        )

    def _get_retry_options(self) -> typing.Optional[options.RetryOptions]:
        return self._options.retry_options

    async def _query(self, query_context: abstract.QueryContext):
        con = await self._impl._acquire()
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
        con = await self._impl._acquire()
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

    def _shallow_clone(self):
        new_pool = self.__class__.__new__(self.__class__)
        new_pool._impl = self._impl
        return new_pool


def create_async_client(
    dsn=None,
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
    wait_until_available: int = 30,
    timeout: int = 10,
    concurrency=None,
):
    return AsyncIOClient(
        connection_class=PoolConnection,
        concurrency=concurrency,
        on_acquire=None,
        on_release=None,
        on_connect=None,

        # connect arguments
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
    )
