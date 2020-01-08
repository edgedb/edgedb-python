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
import inspect
import logging

from . import asyncio_con
from . import errors

__all__ = ('create_async_pool', 'AsyncIOPool')


logger = logging.getLogger(__name__)


class PoolConnectionProxyMeta(type):

    def __new__(mcls, name, bases, dct, *, wrap=False):
        if wrap:
            for attrname in dir(asyncio_con.AsyncIOConnection):
                if attrname.startswith('_') or attrname in dct:
                    continue

                meth = getattr(asyncio_con.AsyncIOConnection, attrname)
                if not inspect.isfunction(meth):
                    continue

                wrapper = mcls._wrap_connection_method(attrname)
                wrapper = functools.update_wrapper(wrapper, meth)
                dct[attrname] = wrapper

            if '__doc__' not in dct:
                dct['__doc__'] = asyncio_con.AsyncIOConnection.__doc__

        return super().__new__(mcls, name, bases, dct)

    def __init__(cls, name, bases, dct, *, wrap=False):
        # Needed for Python 3.5 to handle `wrap` class keyword argument.
        super().__init__(name, bases, dct)

    @staticmethod
    def _wrap_connection_method(meth_name):
        def call_con_method(self, *args, **kwargs):
            # This method will be owned by PoolConnectionProxy class.
            if self._con is None:
                raise errors.InterfaceError(
                    'cannot call AsyncIOConnection.{}(): '
                    'connection has been released back to the pool'.format(
                        meth_name))

            meth = getattr(self._con.__class__, meth_name)
            return meth(self._con, *args, **kwargs)

        return call_con_method


class PoolConnectionProxy(asyncio_con._ConnectionProxy,
                          metaclass=PoolConnectionProxyMeta,
                          wrap=True):

    __slots__ = ('_con', '_holder')

    def __init__(self, holder: 'PoolConnectionHolder',
                 con: asyncio_con.AsyncIOConnection):
        self._con = con
        self._holder = holder
        con._set_proxy(self)

    def __getattr__(self, attr):
        # Proxy all unresolved attributes to the wrapped Connection object.
        return getattr(self._con, attr)

    def _detach(self) -> asyncio_con.AsyncIOConnection:
        if self._con is None:
            return

        con, self._con = self._con, None
        con._set_proxy(None)
        return con

    def __repr__(self):
        if self._con is None:
            return '<{classname} [released] {id:#x}>'.format(
                classname=self.__class__.__name__, id=id(self))
        else:
            return '<{classname} {con!r} {id:#x}>'.format(
                classname=self.__class__.__name__, con=self._con, id=id(self))


class PoolConnectionHolder:

    __slots__ = ('_con', '_pool', '_proxy',
                 '_on_acquire', '_on_release',
                 '_in_use', '_timeout', '_generation')

    def __init__(self, pool, *, on_acquire, on_release):

        self._pool = pool
        self._con = None
        self._proxy = None

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
        self._generation = self._pool._generation

    async def acquire(self) -> PoolConnectionProxy:
        if self._con is None or self._con.is_closed():
            self._con = None
            await self.connect()

        elif self._generation != self._pool._generation:
            # Connections have been expired, re-connect the holder.
            self._pool._loop.create_task(
                self._con.aclose(timeout=self._timeout))
            self._con = None
            await self.connect()

        self._proxy = proxy = PoolConnectionProxy(self, self._con)

        if self._on_acquire is not None:
            try:
                await self._on_acquire(proxy)
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

        return proxy

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
                await self._on_release(self._proxy)
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

        # Deinitialize the connection proxy.  All subsequent
        # operations on it will fail.
        if self._proxy is not None:
            self._proxy._detach()
            self._proxy = None

        # Put ourselves back to the pool queue.
        self._pool._queue.put_nowait(self)


class AsyncIOPool:
    """A connection pool.

    Connection pool can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool.  Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Pools are created by calling :func:`~edgedb.asyncio_pool.create_pool`.
    """

    __slots__ = ('_queue', '_loop', '_minsize', '_maxsize', '_on_connect',
                 '_connect_args', '_connect_kwargs',
                 '_working_addr', '_working_config', '_working_params',
                 '_codecs_registry', '_query_cache',
                 '_holders', '_initialized', '_initializing', '_closing',
                 '_closed', '_connection_class', '_generation',
                 '_on_acquire', '_on_release')

    def __init__(self, *connect_args,
                 min_size,
                 max_size,
                 on_acquire,
                 on_release,
                 on_connect,
                 connection_class,
                 **connect_kwargs):

        loop = asyncio.get_event_loop()
        self._loop = loop

        if max_size <= 0:
            raise ValueError('max_size is expected to be greater than zero')

        if min_size < 0:
            raise ValueError(
                'min_size is expected to be greater or equal to zero')

        if min_size > max_size:
            raise ValueError('min_size is greater than max_size')

        if not issubclass(connection_class, asyncio_con.AsyncIOConnection):
            raise TypeError(
                'connection_class is expected to be a subclass of '
                'edgedb.AsyncIOConnection, got {!r}'.format(connection_class))

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

            self._working_addr = con._addr
            self._working_config = con._config
            self._working_params = con._params
            self._codecs_registry = con._codecs_registry
            self._query_cache = con._query_cache

        else:
            # We've connected before and have a resolved address,
            # and parsed options and config.
            con = await asyncio_con._connect_addr(
                loop=self._loop,
                addr=self._working_addr,
                timeout=self._working_params.connect_timeout,
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

    async def fetchall(self, query, *args, **kwargs):
        async with self.acquire() as con:
            return await con.fetchall(query, *args, **kwargs)

    async def fetchone(self, query, *args, **kwargs):
        async with self.acquire() as con:
            return await con.fetchone(query, *args, **kwargs)

    async def fetchall_json(self, query, *args, **kwargs):
        async with self.acquire() as con:
            return await con.fetchall_json(query, *args, **kwargs)

    async def fetchone_json(self, query, *args, **kwargs):
        async with self.acquire() as con:
            return await con.fetchone_json(query, *args, **kwargs)

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
        return PoolAcquireContext(self, timeout=None)

    async def _acquire(self, timeout):
        async def _acquire_impl():
            ch = await self._queue.get()  # type: PoolConnectionHolder
            try:
                proxy = await ch.acquire()  # type: PoolConnectionProxy
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
        self._check_init()

        if timeout is None:
            return await _acquire_impl()
        else:
            return await asyncio.wait_for(
                _acquire_impl(), timeout=timeout)

    async def release(self, connection):
        """Release a database connection back to the pool.

        :param Connection connection:
            A :class:`~edgedb.asyncio_con.AsyncIOConnection` object
            to release.
        """
        if (type(connection) is not PoolConnectionProxy or
                connection._holder._pool is not self):
            raise errors.InterfaceError(
                'AsyncIOPool.release() received invalid connection: '
                '{connection!r} is not a member of this pool'.format(
                    connection=connection))

        if connection._con is None:
            # Already released, do nothing.
            return

        self._check_init()

        # Let the connection do its internal housekeeping when its released.
        connection._con._on_release()

        ch = connection._holder
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

    def __await__(self):
        return self._async__init__().__await__()

    async def __aenter__(self):
        await self._async__init__()
        return self

    async def __aexit__(self, *exc):
        await self.aclose()


class PoolAcquireContext:

    __slots__ = ('timeout', 'connection', 'done', 'pool')

    def __init__(self, pool, timeout):
        self.pool = pool
        self.timeout = timeout
        self.connection = None
        self.done = False

    async def __aenter__(self):
        if self.connection is not None or self.done:
            raise errors.InterfaceError('a connection is already acquired')
        self.connection = await self.pool._acquire(self.timeout)
        return self.connection

    async def __aexit__(self, *exc):
        self.done = True
        con = self.connection
        self.connection = None
        await self.pool.release(con)

    def __await__(self):
        self.done = True
        return self.pool._acquire(self.timeout).__await__()


def create_async_pool(dsn=None, *,
                      min_size=10,
                      max_size=10,
                      on_acquire=None,
                      on_release=None,
                      on_connect=None,
                      connection_class=asyncio_con.AsyncIOConnection,
                      **connect_kwargs):
    r"""Create an asynchronous connection pool.

    Can be used either with an ``async with`` block:

    .. code-block:: python

        async with edgedb.create_pool(user='edgedb') as pool:
            async with pool.acquire() as con:
                await con.fetchall('SELECT {1, 2, 3}')

    Or directly with ``await``:

    .. code-block:: python

        pool = await edgedb.create_pool(user='edgedb')
        con = await pool.acquire()
        try:
            await con.fetchall('SELECT {1, 2, 3}')
        finally:
            await pool.release(con)

    :param str dsn:
        Connection arguments specified using as a single string in
        the following format:
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
