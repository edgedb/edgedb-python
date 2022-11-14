#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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


import datetime
import queue
import socket
import ssl
import threading
import time
import typing

from . import abstract
from . import base_client
from . import con_utils
from . import errors
from . import transaction
from .protocol import blocking_proto


DEFAULT_PING_BEFORE_IDLE_TIMEOUT = datetime.timedelta(seconds=5)
MINIMUM_PING_WAIT_TIME = datetime.timedelta(seconds=1)


class BlockingIOConnection(base_client.BaseConnection):
    __slots__ = ("_ping_wait_time",)

    async def connect_addr(self, addr, timeout):
        deadline = time.monotonic() + timeout

        if isinstance(addr, str):
            # UNIX socket
            sock = socket.socket(socket.AF_UNIX)
        else:
            sock = socket.socket(socket.AF_INET)

        try:
            sock.settimeout(timeout)

            try:
                sock.connect(addr)

                if not isinstance(addr, str):
                    time_left = deadline - time.monotonic()
                    if time_left <= 0:
                        raise TimeoutError

                    # Upgrade to TLS
                    if self._params.ssl_ctx.check_hostname:
                        server_hostname = addr[0]
                    else:
                        server_hostname = None
                    sock.settimeout(time_left)
                    try:
                        sock = self._params.ssl_ctx.wrap_socket(
                            sock, server_hostname=server_hostname
                        )
                    except ssl.CertificateError as e:
                        raise con_utils.wrap_error(e) from e
                    except ssl.SSLError as e:
                        raise con_utils.wrap_error(e) from e
                    else:
                        con_utils.check_alpn_protocol(sock)
            except socket.gaierror as e:
                # All name resolution errors are considered temporary
                err = errors.ClientConnectionFailedTemporarilyError(str(e))
                raise err from e
            except OSError as e:
                raise con_utils.wrap_error(e) from e

            time_left = deadline - time.monotonic()
            if time_left <= 0:
                raise TimeoutError

            if not isinstance(addr, str):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            proto = blocking_proto.BlockingIOProtocol(self._params, sock)
            proto.set_connection(self)

            try:
                sock.settimeout(time_left)
                await proto.connect()
                sock.settimeout(None)
            except OSError as e:
                raise con_utils.wrap_error(e) from e

            self._protocol = proto
            self._addr = addr
            self._ping_wait_time = max(
                (
                    getattr(
                        self.get_settings().get("system_config"),
                        "session_idle_timeout",
                    )
                    - DEFAULT_PING_BEFORE_IDLE_TIMEOUT
                ),
                MINIMUM_PING_WAIT_TIME,
            ).total_seconds()

        except Exception:
            sock.close()
            raise

    async def sleep(self, seconds):
        time.sleep(seconds)

    def is_closed(self):
        proto = self._protocol
        return not (proto and proto.sock is not None and
                    proto.sock.fileno() >= 0 and proto.connected)

    async def close(self, timeout=None):
        """Send graceful termination message wait for connection to drop."""
        if not self.is_closed():
            try:
                self._protocol.terminate()
                if timeout is None:
                    await self._protocol.wait_for_disconnect()
                else:
                    await self._protocol.wait_for(
                        self._protocol.wait_for_disconnect(), timeout
                    )
            except Exception:
                self.terminate()
                raise
            finally:
                self._cleanup()

    def _dispatch_log_message(self, msg):
        for cb in self._log_listeners:
            cb(self, msg)

    async def raw_query(self, query_context: abstract.QueryContext):
        try:
            if (
                time.monotonic() - self._protocol.last_active_timestamp
                > self._ping_wait_time
            ):
                await self._protocol.ping()
        except (errors.IdleSessionTimeoutError, errors.ClientConnectionError):
            await self.connect()

        return await super().raw_query(query_context)


class _PoolConnectionHolder(base_client.PoolConnectionHolder):
    __slots__ = ()
    _event_class = threading.Event

    async def close(self, *, wait=True, timeout=None):
        if self._con is None:
            return
        await self._con.close(timeout=timeout)

    async def wait_until_released(self, timeout=None):
        return self._release_event.wait(timeout)


class _PoolImpl(base_client.BasePoolImpl):
    _holder_class = _PoolConnectionHolder

    def __init__(
        self,
        connect_args,
        *,
        max_concurrency: typing.Optional[int],
        connection_class,
    ):
        if not issubclass(connection_class, BlockingIOConnection):
            raise TypeError(
                f'connection_class is expected to be a subclass of '
                f'edgedb.blocking_client.BlockingIOConnection, '
                f'got {connection_class}')
        super().__init__(
            connect_args,
            connection_class,
            max_concurrency=max_concurrency,
        )

    def _ensure_initialized(self):
        if self._queue is None:
            self._queue = queue.LifoQueue(maxsize=self._max_concurrency)
            self._first_connect_lock = threading.Lock()
            self._resize_holder_pool()

    def _set_queue_maxsize(self, maxsize):
        with self._queue.mutex:
            self._queue.maxsize = maxsize

    async def _maybe_get_first_connection(self):
        with self._first_connect_lock:
            if self._working_addr is None:
                return await self._get_first_connection()

    async def acquire(self, timeout=None):
        self._ensure_initialized()

        if self._closing:
            raise errors.InterfaceError('pool is closing')

        ch = self._queue.get(timeout=timeout)
        try:
            con = await ch.acquire()
        except Exception:
            self._queue.put_nowait(ch)
            raise
        else:
            # Record the timeout, as we will apply it by default
            # in release().
            ch._timeout = timeout
            return con

    async def _release(self, holder):
        if not isinstance(holder._con, BlockingIOConnection):
            raise errors.InterfaceError(
                f'release() received invalid connection: '
                f'{holder._con!r} does not belong to any connection pool'
            )

        timeout = None
        return await holder.release(timeout)

    async def close(self, timeout=None):
        if self._closed:
            return
        self._closing = True
        try:
            if timeout is None:
                for ch in self._holders:
                    await ch.wait_until_released()
                for ch in self._holders:
                    await ch.close()
            else:
                deadline = time.monotonic() + timeout
                for ch in self._holders:
                    secs = deadline - time.monotonic()
                    if secs <= 0:
                        raise TimeoutError
                    if not await ch.wait_until_released(secs):
                        raise TimeoutError
                for ch in self._holders:
                    secs = deadline - time.monotonic()
                    if secs <= 0:
                        raise TimeoutError
                    await ch.close(timeout=secs)
        except TimeoutError as e:
            self.terminate()
            raise errors.InterfaceError(
                "client is not fully closed in {} seconds; "
                "terminating now.".format(timeout)
            ) from e
        except Exception:
            self.terminate()
            raise
        finally:
            self._closed = True
            self._closing = False


class Iteration(transaction.BaseTransaction, abstract.Executor):

    __slots__ = ("_managed",)

    def __init__(self, retry, client, iteration):
        super().__init__(retry, client, iteration)
        self._managed = False

    def __enter__(self):
        if self._managed:
            raise errors.InterfaceError(
                'cannot enter context: already in a `with` block')
        self._managed = True
        return self

    def __exit__(self, extype, ex, tb):
        self._managed = False
        return self._client._iter_coroutine(self._exit(extype, ex))

    async def _ensure_transaction(self):
        if not self._managed:
            raise errors.InterfaceError(
                "Only managed retriable transactions are supported. "
                "Use `with transaction:`"
            )
        await super()._ensure_transaction()

    def _query(self, query_context: abstract.QueryContext):
        return self._client._iter_coroutine(super()._query(query_context))

    def _execute(self, execute_context: abstract.ExecuteContext) -> None:
        self._client._iter_coroutine(super()._execute(execute_context))


class Retry(transaction.BaseRetry):

    def __iter__(self):
        return self

    def __next__(self):
        # Note: when changing this code consider also
        # updating AsyncIORetry.__anext__.
        if self._done:
            raise StopIteration
        if self._next_backoff:
            time.sleep(self._next_backoff)
        self._done = True
        iteration = Iteration(self, self._owner, self._iteration)
        self._iteration += 1
        return iteration


class Client(base_client.BaseClient, abstract.Executor):
    """A lazy connection pool.

    A Client can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool.  Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Clients are created by calling
    :func:`~edgedb.blocking_client.create_client`.
    """

    __slots__ = ()
    _impl_class = _PoolImpl

    def _iter_coroutine(self, coro):
        try:
            coro.send(None)
        except StopIteration as ex:
            if ex.args:
                result = ex.args[0]
            else:
                result = None
        finally:
            coro.close()
        return result

    def _query(self, query_context: abstract.QueryContext):
        return self._iter_coroutine(super()._query(query_context))

    def _execute(self, execute_context: abstract.ExecuteContext) -> None:
        self._iter_coroutine(super()._execute(execute_context))

    def ensure_connected(self):
        self._iter_coroutine(self._impl.ensure_connected())
        return self

    def transaction(self) -> Retry:
        return Retry(self)

    def close(self, timeout=None):
        """Attempt to gracefully close all connections in the client.

        Wait until all pool connections are released, close them and
        shut down the pool.  If any error (including cancellation) occurs
        in ``close()`` the pool will terminate by calling
        Client.terminate() .
        """
        self._iter_coroutine(self._impl.close(timeout))

    def __enter__(self):
        return self.ensure_connected()

    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()

    def _describe_query(
        self, query: str, *, inject_type_names: bool = False
    ) -> abstract.DescribeResult:
        return self._iter_coroutine(self._describe(abstract.DescribeContext(
            query=query,
            state=self._get_state(),
            inject_type_names=inject_type_names,
        )))


def create_client(
    dsn=None,
    *,
    max_concurrency=None,
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
):
    return Client(
        connection_class=BlockingIOConnection,
        max_concurrency=max_concurrency,

        # connect arguments
        dsn=dsn,
        host=host,
        port=port,
        credentials=credentials,
        credentials_file=credentials_file,
        user=user,
        password=password,
        database=database,
        tls_ca=tls_ca,
        tls_ca_file=tls_ca_file,
        tls_security=tls_security,
        wait_until_available=wait_until_available,
        timeout=timeout,
    )
