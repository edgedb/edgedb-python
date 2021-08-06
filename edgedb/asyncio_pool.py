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
import warnings

from . import abstract
from . import asyncio_con
from . import compat
from . import errors
from . import options
from . import retry as _retry
from . import transaction as _transaction

from .datatypes import datatypes


__all__ = ('create_async_pool', 'AsyncIOPool')


logger = logging.getLogger(__name__)


class PoolConnection(asyncio_con.AsyncIOConnection):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self._inner._holder = None
        self._inner._detached = False

    async def _reconnect(self, single_attempt=False):
        if self._inner._detached:
            # initial connection
            raise errors.InterfaceError(
                "the underlying connection has been released back to the pool"
            )
        return await super()._reconnect(single_attempt=single_attempt)

    def _detach(self):
        new_conn = self._shallow_clone()
        inner = self._inner
        holder = inner._holder
        inner._holder = None
        inner._detached = True
        new_conn._inner = self._inner._detach()
        new_conn._inner._holder = holder
        new_conn._inner._detached = False
        return new_conn

    def _cleanup(self):
        if self._inner._holder:
            self._inner._holder._release_on_close()
        super()._cleanup()

    def __repr__(self):
        if self._inner._holder is None:
            return '<{classname} [released] {id:#x}>'.format(
                classname=self.__class__.__name__, id=id(self))
        else:
            return super().__repr__()


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
        assert self._con._inner._holder is None
        self._con._inner._holder = self
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
            # When closing, pool connections perform the necessary
            # cleanup, so we don't have to do anything else here.
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

        self._con = self._con._detach()

        # Put ourselves back to the pool queue.
        self._pool._queue.put_nowait(self)


class _AsyncIOPoolImpl:
    __slots__ = ('_queue', '_loop', '_minsize', '_maxsize', '_on_connect',
                 '_connect_args', '_connect_kwargs',
                 '_working_addr', '_working_config', '_working_params',
                 '_codecs_registry', '_query_cache',
                 '_holders', '_initialized', '_initializing', '_closing',
                 '_closed', '_connection_class', '_generation',
                 '_on_acquire', '_on_release')

    def __init__(self, *connect_args,
                 min_size: int,
                 max_size: int,
                 on_acquire,
                 on_release,
                 on_connect,
                 connection_class,
                 **connect_kwargs):
        super().__init__()

        loop = asyncio.get_event_loop()
        self._loop = loop

        if max_size <= 0:
            raise ValueError('max_size is expected to be greater than zero')

        if min_size < 0:
            raise ValueError(
                'min_size is expected to be greater or equal to zero')

        if min_size > max_size:
            raise ValueError('min_size is greater than max_size')

        if not issubclass(connection_class, PoolConnection):
            raise TypeError(
                f'connection_class is expected to be a subclass of '
                f'edgedb.asyncio_pool.PoolConnection, '
                f'got {connection_class}')

        self._minsize = min_size
        self._maxsize = max_size

        self._on_acquire = on_acquire
        self._on_release = on_release

        self._holders = []
        self._initialized = False
        self._initializing = False
        self._queue = None

        self._working_addr = None
        self._working_config = None
        self._working_params = None

        self._connection_class = connection_class

        self._closing = False
        self._closed = False
        self._generation = 0
        self._on_connect = on_connect
        self._connect_args = connect_args
        self._connect_kwargs = connect_kwargs

    async def _async__init__(self):
        if self._initialized:
            return
        if self._initializing:
            raise errors.InterfaceError(
                'pool is being initialized in another task')
        if self._closed:
            raise errors.InterfaceError('pool is closed')

        self._initializing = True

        self._queue = asyncio.LifoQueue(maxsize=self._maxsize)
        for _ in range(self._maxsize):
            ch = PoolConnectionHolder(
                self,
                on_acquire=self._on_acquire,
                on_release=self._on_release)

            self._holders.append(ch)
            self._queue.put_nowait(ch)

        try:
            await self._initialize()
            return self
        finally:
            self._initializing = False
            self._initialized = True

    async def _initialize(self):
        if self._minsize:
            # Since we use a LIFO queue, the first items in the queue will be
            # the last ones in `self._holders`.  We want to pre-connect the
            # first few connections in the queue, therefore we want to walk
            # `self._holders` in reverse.

            # Connect the first connection holder in the queue so that it
            # can record `_working_addr` and `_working_opts`, which will
            # speed up successive connection attempts.
            first_ch = self._holders[-1]  # type: PoolConnectionHolder
            await first_ch.connect()

            if self._minsize > 1:
                connect_tasks = []
                for i, ch in enumerate(reversed(self._holders[:-1])):
                    # `minsize - 1` because we already have first_ch
                    if i >= self._minsize - 1:
                        break
                    connect_tasks.append(ch.connect())

                await asyncio.gather(*connect_tasks)

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

        self._connect_args = [dsn]
        self._connect_kwargs = connect_kwargs
        self._working_addr = None
        self._working_config = None
        self._working_params = None
        self._codecs_registry = None
        self._query_cache = None

    async def _get_new_connection(self):
        if self._working_addr is None:
            # First connection attempt on this pool.
            con = await asyncio_con.async_connect(
                *self._connect_args,
                connection_class=self._connection_class,
                **self._connect_kwargs)

            self._working_addr = con.connected_addr()
            self._working_config = con._inner._config
            self._working_params = con._inner._params
            self._codecs_registry = con._inner._codecs_registry
            self._query_cache = con._inner._query_cache

        else:
            # We've connected before and have a resolved address,
            # and parsed options and config.
            con = await asyncio_con._connect_addr(
                loop=self._loop,
                addrs=[self._working_addr],
                config=self._working_config,
                params=self._working_params,
                query_cache=self._query_cache,
                codecs_registry=self._codecs_registry,
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

    async def _acquire(self, timeout, options):
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
                proxy._options = options
                return proxy

        if self._closing:
            raise errors.InterfaceError('pool is closing')
        self._check_init()

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

        ch = connection._inner._holder
        if ch is None:
            # Already released, do nothing.
            return

        if ch._pool is not self:
            raise errors.InterfaceError(
                f'AsyncIOPool.release() received invalid connection: '
                f'{connection!r} is not a member of this pool'
            )

        self._check_init()

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
        self._check_init()

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
        self._check_init()
        for ch in self._holders:
            ch.terminate()
        self._closed = True

    async def expire_connections(self):
        """Expire all currently open connections.

        Cause all currently open connections to get replaced on the
        next AsyncIOPool.acquire() call.
        """
        self._generation += 1

    def _check_init(self):
        if not self._initialized:
            if self._initializing:
                raise errors.InterfaceError(
                    'pool is being initialized, but not yet ready: '
                    'likely there is a race between creating a pool and '
                    'using it')
            raise errors.InterfaceError('pool is not initialized')
        if self._closed:
            raise errors.InterfaceError('pool is closed')

    def _drop_statement_cache(self):
        # Drop statement cache for all connections in the pool.
        for ch in self._holders:
            if ch._con is not None:
                ch._con._drop_local_statement_cache()

    def _drop_type_cache(self):
        # Drop type codec cache for all connections in the pool.
        for ch in self._holders:
            if ch._con is not None:
                ch._con._drop_local_type_cache()


class AsyncIOPool(abstract.AsyncIOExecutor, options._OptionsMixin):
    """A connection pool.

    Connection pool can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool.  Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Pools are created by calling :func:`~edgedb.asyncio_pool.create_pool`.
    """

    __slots__ = ('_impl', '_options')

    def __init__(self, *connect_args,
                 min_size: int,
                 max_size: int,
                 on_acquire,
                 on_release,
                 on_connect,
                 connection_class,
                 **connect_kwargs):
        super().__init__()
        self._impl = _AsyncIOPoolImpl(
            *connect_args,
            min_size=min_size,
            max_size=max_size,
            on_acquire=on_acquire,
            on_release=on_release,
            on_connect=on_connect,
            connection_class=connection_class,
            **connect_kwargs,
        )

    @property
    def min_size(self) -> int:
        """Number of connection the pool was initialized with."""

        return self._impl._minsize

    @property
    def max_size(self) -> int:
        """Max number of connections in the pool."""

        return self._impl._maxsize

    @property
    def free_size(self) -> int:
        """Number of available connections in the pool."""

        if self._impl._queue is None:
            # Queue has not been initialized yet
            return self._impl._maxsize

        return self._impl._queue.qsize()

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
        self._impl.set_connect_args(dsn, **connect_kwargs)

    async def query(self, query, *args, **kwargs):
        async with self.acquire() as con:
            return await con.query(query, *args, **kwargs)

    async def query_single(self, query, *args, **kwargs):
        async with self.acquire() as con:
            return await con.query_single(query, *args, **kwargs)

    async def query_json(self, query, *args, **kwargs):
        async with self.acquire() as con:
            return await con.query_json(query, *args, **kwargs)

    async def query_single_json(self, query, *args, **kwargs):
        async with self.acquire() as con:
            return await con.query_single_json(query, *args, **kwargs)

    async def fetchall(self, query: str, *args, **kwargs) -> datatypes.Set:
        warnings.warn(
            'The "fetchall()" method is deprecated and is scheduled to be '
            'removed. Use the "query()" method instead.',
            DeprecationWarning, 2)
        return await self.query(query, *args, **kwargs)

    async def fetchone(self, query: str, *args, **kwargs) -> typing.Any:
        warnings.warn(
            'The "fetchone()" method is deprecated and is scheduled to be '
            'removed. Use the "query_single()" method instead.',
            DeprecationWarning, 2)
        return await self.query_single(query, *args, **kwargs)

    async def fetchall_json(self, query: str, *args, **kwargs) -> str:
        warnings.warn(
            'The "fetchall_json()" method is deprecated and is scheduled to '
            'be removed. Use the "query_json()" method instead.',
            DeprecationWarning, 2)
        return await self.query_json(query, *args, **kwargs)

    async def fetchone_json(self, query: str, *args, **kwargs) -> str:
        warnings.warn(
            'The "fetchone_json()" method is deprecated and is scheduled to '
            'be removed. Use the "query_single_json()" method instead.',
            DeprecationWarning, 2)
        return await self.query_single_json(query, *args, **kwargs)

    async def execute(self, query):
        async with self.acquire() as con:
            return await con.execute(query)

    def acquire(self):
        """Acquire a database connection from the pool.

        :return: An instance of :class:`~edgedb.asyncio_con.AsyncIOConnection`.

        Can be used in an ``await`` expression or with an ``async with`` block.

        .. code-block:: python

            async with pool.acquire() as con:
                await con.execute(...)

        Or:

        .. code-block:: python

            con = await pool.acquire()
            try:
                await con.execute(...)
            finally:
                await pool.release(con)
        """
        return PoolAcquireContext(self, timeout=None, options=self._options)

    async def release(self, connection):
        """Release a database connection back to the pool.

        :param Connection connection:
            A :class:`~edgedb.asyncio_con.AsyncIOConnection` object
            to release.
        """
        await self._impl.release(connection)

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

    async def expire_connections(self):
        """Expire all currently open connections.

        Cause all currently open connections to get replaced on the
        next AsyncIOPool.acquire() call.
        """
        await self._impl.expire_connections()

    def __await__(self):
        return self.__aenter__().__await__()

    async def __aenter__(self):
        await self._impl._async__init__()
        return self

    async def __aexit__(self, *exc):
        await self.aclose()

    def raw_transaction(self) -> _transaction.AsyncIOTransaction:
        return _transaction.AsyncIOTransaction(
            self,
            self._options.transaction_options,
        )

    def retrying_transaction(self) -> _retry.AsyncIORetry:
        return _retry.AsyncIORetry(self)


class PoolAcquireContext:

    __slots__ = ('timeout', 'connection', 'done', 'pool')

    def __init__(self, pool, timeout, options):
        self.pool = pool
        self.timeout = timeout
        self.connection = None
        self.done = False

    async def __aenter__(self):
        if self.connection is not None or self.done:
            raise errors.InterfaceError('a connection is already acquired')
        self.connection = await self.pool._impl._acquire(
            self.timeout,
            self.pool._options,
        )
        return self.connection

    async def __aexit__(self, *exc):
        self.done = True
        con = self.connection
        self.connection = None
        await self.pool.release(con)

    def __await__(self):
        self.done = True
        return self.pool._impl._acquire(
            self.timeout,
            self.pool._options,
        ).__await__()


def create_async_pool(dsn=None, *,
                      min_size=10,
                      max_size=10,
                      on_acquire=None,
                      on_release=None,
                      on_connect=None,
                      connection_class=PoolConnection,
                      **connect_kwargs):
    r"""Create an asynchronous connection pool.

    Can be used either with an ``async with`` block:

    .. code-block:: python

        async with edgedb.create_async_pool(user='edgedb') as pool:
            async with pool.acquire() as con:
                await con.fetchall('SELECT {1, 2, 3}')

    Or directly with ``await``:

    .. code-block:: python

        pool = await edgedb.create_async_pool(user='edgedb')
        con = await pool.acquire()
        try:
            await con.fetchall('SELECT {1, 2, 3}')
        finally:
            await pool.release(con)

    :param str dsn:
        If this parameter does not start with ``edgedb://`` then this is
        a :ref:`name of an instance <edgedb-instances>`.

        Otherwies it specifies as a single string in the following format:
        ``edgedb://user:pass@host:port/database?option=value``.

    :param \*\*connect_kwargs:
        Keyword arguments for the async_connect() function.

    :param Connection connection_class:
        The class to use for connections.  Must be a subclass of
        :class:`~edgedb.asyncio_con.AsyncIOConnection`.

    :param int min_size:
        Number of connection the pool will be initialized with.

    :param int max_size:
        Max number of connections in the pool.

    :param coroutine on_acquire:
        A coroutine to prepare a connection right before it is returned
        from AsyncIOPool.acquire().

    :param coroutine on_release:
        A coroutine called when a connection is about to be released
        to the pool.

    :param coroutine on_connect:
        A coroutine to initialize a connection when it is created.

    :return: An instance of :class:`~edgedb.AsyncIOPool`.
    """
    return AsyncIOPool(
        dsn,
        connection_class=connection_class,
        min_size=min_size,
        max_size=max_size,
        on_acquire=on_acquire,
        on_release=on_release,
        on_connect=on_connect,
        **connect_kwargs)
