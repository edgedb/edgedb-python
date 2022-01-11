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


import random
import socket
import ssl
import time
import typing

from . import abstract
from . import base_client
from . import con_utils
from . import enums
from . import errors
from . import retry as _retry
from .protocol import blocking_proto


class BlockingIOConnection(base_client.BaseConnection):
    __slots__ = ()

    def connect(self, *, single_attempt=False):
        start = time.monotonic()
        if single_attempt:
            max_time = 0
        else:
            max_time = start + self._config.wait_until_available
        iteration = 1

        while True:
            for addr in self._addrs:
                try:
                    self._connect_addr(addr)
                except TimeoutError as e:
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
                    assert self._protocol
                    return

            iteration += 1
            time.sleep(0.01 + random.random() * 0.2)

    def _connect_addr(self, addr):
        timeout = self._config.connect_timeout
        deadline = time.monotonic() + timeout
        tls_compat = False

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
                        if e.reason == 'CERTIFICATE_VERIFY_FAILED':
                            raise con_utils.wrap_error(e) from e

                        # Retry in plain text
                        time_left = deadline - time.monotonic()
                        if time_left <= 0:
                            raise TimeoutError
                        sock.close()
                        sock = socket.socket(socket.AF_INET)
                        sock.settimeout(time_left)
                        sock.connect(addr)
                        tls_compat = True
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

            proto = blocking_proto.BlockingIOProtocol(
                self._params, sock, tls_compat
            )
            proto.set_connection(self)

            try:
                sock.settimeout(time_left)
                proto.sync_connect()
                sock.settimeout(None)
            except OSError as e:
                raise con_utils.wrap_error(e) from e

            self._protocol = proto
            self._addr = addr

        except Exception:
            sock.close()
            raise

    def privileged_execute(self, query):
        self._protocol.sync_simple_query(query, enums.Capability.ALL)

    def is_closed(self):
        proto = self._protocol
        return not (proto and proto.sock is not None and
                    proto.sock.fileno() >= 0 and proto.connected)

    def close(self):
        if self._protocol:
            self._protocol.abort()

    def _get_protocol(self):
        if self.is_closed():
            self.connect()
        return self._protocol

    def _dump(
        self,
        *,
        on_header: typing.Callable[[bytes], None],
        on_data: typing.Callable[[bytes], None],
    ) -> None:
        self._get_protocol().sync_dump(
            header_callback=on_header,
            block_callback=on_data)

    def _restore(
        self,
        *,
        header: bytes,
        data_gen: typing.Iterable[bytes],
    ) -> None:
        self._get_protocol().sync_restore(
            header=header,
            data_gen=data_gen
        )

    def _dispatch_log_message(self, msg):
        for cb in self._log_listeners:
            cb(self, msg)

    def raw_query(self, query_context: abstract.QueryContext):
        reconnect = False
        capabilities = None
        i = 0
        while True:
            try:
                if reconnect:
                    self.connect(single_attempt=True)
                return self._get_protocol().sync_execute_anonymous(
                    query=query_context.query.query,
                    args=query_context.query.args,
                    kwargs=query_context.query.kwargs,
                    reg=query_context.cache.codecs_registry,
                    qc=query_context.cache.query_cache,
                    expect_one=query_context.query_options.expect_one,
                    required_one=query_context.query_options.required_one,
                    io_format=query_context.query_options.io_format,
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
                time.sleep(rule.backoff(i))
                reconnect = self.is_closed()

    def execute(self, query: str) -> None:
        self._get_protocol().sync_simple_query(query, enums.Capability.EXECUTE)


class _SingleConnectionPoolImpl(base_client.BaseImpl):
    __slots__ = ("_connection", "_acquired", "_closed")

    def __init__(self, connect_args):
        super().__init__(connect_args)
        self._connection = None
        self._acquired = False
        self._closed = False

    def get_concurrency(self):
        return 0

    def get_free_size(self):
        return 0 if self._acquired else 1

    def ensure_connected(self):
        self.release(self.acquire())

    def acquire(self):
        if self._acquired:
            raise errors.InterfaceError("cannot acquire twice")
        self._acquired = True
        if self._connection is None:
            connect_config, client_config = self._parse_connect_args()
            con = BlockingIOConnection(
                addrs=[connect_config.address],
                params=connect_config,
                config=client_config,
            )
            con.connect()
            self._connection = con
        return self._connection

    def release(self, connection):
        if self._connection is not connection:
            raise errors.InterfaceError("cannot release foreign connections")
        if not self._acquired:
            raise errors.InterfaceError("cannot release twice")
        self._acquired = False

    def close(self):
        if self._closed:
            return
        try:
            if self._connection is not None:
                self._connection.close()
        finally:
            self._closed = True


class Client(abstract.Executor, base_client.BaseClient):
    def _create_single_connection_pool(self, connect_args, **kwargs):
        return _SingleConnectionPoolImpl(connect_args)

    def _query(self, query_context: abstract.QueryContext):
        con = self._impl.acquire()
        try:
            return con.raw_query(query_context)
        finally:
            self._impl.release(con)

    def execute(self, query: str) -> None:
        con = self._impl.acquire()
        try:
            con.execute(query)
        finally:
            self._impl.release(con)

    def ensure_connected(self):
        self._impl.ensure_connected()

    def transaction(self) -> _retry.Retry:
        return _retry.Retry(self)

    def close(self):
        self._impl.close()


def create_client(dsn=None, *, concurrency=0, **kwargs):
    return Client(
        concurrency=concurrency,
        # connect arguments
        dsn=dsn,
        **kwargs,
    )
