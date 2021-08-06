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


import random
import socket
import ssl
import time
import typing
import warnings

from . import abstract
from . import base_con
from . import con_utils
from . import enums
from . import errors
from . import options
from . import transaction as _transaction
from . import retry as _retry
from . import legacy_transaction

from .datatypes import datatypes
from .protocol import blocking_proto, protocol
from .protocol.protocol import CodecsRegistry as _CodecsRegistry
from .protocol.protocol import QueryCodecsCache as _QueryCodecsCache


class _BlockingIOConnectionImpl:

    def __init__(self, codecs_registry, query_cache):
        self._addr = None
        self._protocol = None
        self._codecs_registry = codecs_registry
        self._query_cache = query_cache

    def connect(self, addrs, config, params, *,
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
                    self._connect_addr(addr, config, params, connection)
                except TimeoutError as e:
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
                    assert self._protocol
                    return

            iteration += 1
            time.sleep(0.01 + random.random() * 0.2)

    def _connect_addr(self, addr, config, params, connection):
        timeout = config.connect_timeout
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
                    if params.ssl_ctx.check_hostname:
                        server_hostname = addr[0]
                    else:
                        server_hostname = None
                    sock.settimeout(time_left)
                    try:
                        sock = params.ssl_ctx.wrap_socket(
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
                params, sock, tls_compat
            )
            proto.set_connection(connection)

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


class BlockingIOConnection(
    base_con.BaseConnection,
    abstract.Executor,
    options._OptionsMixin,
):

    def __init__(self, addrs, config, params, *,
                 codecs_registry, query_cache):
        self._inner = base_con._InnerConnection(
            addrs, config, params,
            codecs_registry=codecs_registry,
            query_cache=query_cache)
        super().__init__()

    def _shallow_clone(self):
        if self._inner._borrowed_for:
            raise base_con.borrow_error(self._inner._borrowed_for)
        new_conn = self.__class__.__new__(self.__class__)
        new_conn._inner = self._inner
        return new_conn

    def ensure_connected(self, single_attempt=False):
        inner = self._inner
        if inner._borrowed_for:
            raise base_con.borrow_error(inner._borrowed_for)
        if not inner._impl or inner._impl.is_closed():
            self._reconnect(single_attempt=single_attempt)

    def _reconnect(self, single_attempt=False):
        inner = self._inner
        assert not inner._borrowed_for, inner._borrowed_for
        inner._impl = _BlockingIOConnectionImpl(
            inner._codecs_registry, inner._query_cache)
        inner._impl.connect(inner._addrs, inner._config, inner._params,
                            single_attempt=single_attempt, connection=inner)
        assert inner._impl._protocol

    def _get_protocol(self):
        inner = self._inner
        if inner._borrowed_for:
            raise base_con.borrow_error(inner._borrowed_for)
        if not inner._impl or inner._impl.is_closed():
            self._reconnect()
        return inner._impl._protocol

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
        for cb in self._inner._log_listeners:
            cb(self, msg)

    def _fetchall(
        self,
        query: str,
        *args,
        __limit__: int=0,
        __typenames__: bool=False,
        **kwargs,
    ) -> datatypes.Set:
        inner = self._inner
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=inner._codecs_registry,
            qc=inner._query_cache,
            implicit_limit=__limit__,
            inline_typenames=__typenames__,
            io_format=protocol.IoFormat.BINARY,
        )

    def _fetchall_json(
        self,
        query: str,
        *args,
        __limit__: int=0,
        **kwargs,
    ) -> datatypes.Set:
        inner = self._inner
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=inner._codecs_registry,
            qc=inner._query_cache,
            implicit_limit=__limit__,
            inline_typenames=False,
            io_format=protocol.IoFormat.JSON,
        )

    def query(self, query: str, *args, **kwargs) -> datatypes.Set:
        inner = self._inner
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=inner._codecs_registry,
            qc=inner._query_cache,
            io_format=protocol.IoFormat.BINARY,
        )

    def query_single(self, query: str, *args, **kwargs) -> typing.Any:
        inner = self._inner
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=inner._codecs_registry,
            qc=inner._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.BINARY,
        )

    def query_json(self, query: str, *args, **kwargs) -> str:
        inner = self._inner
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=inner._codecs_registry,
            qc=inner._query_cache,
            io_format=protocol.IoFormat.JSON,
        )

    def _fetchall_json_elements(
            self, query: str, *args, **kwargs) -> typing.List[str]:
        inner = self._inner
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=inner._codecs_registry,
            qc=inner._query_cache,
            io_format=protocol.IoFormat.JSON_ELEMENTS,
        )

    def query_single_json(self, query: str, *args, **kwargs) -> str:
        inner = self._inner
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=inner._codecs_registry,
            qc=inner._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.JSON,
        )

    def fetchall(self, query: str, *args, **kwargs) -> datatypes.Set:
        warnings.warn(
            'The "fetchall()" method is deprecated and is scheduled to be '
            'removed. Use the "query()" method instead.',
            DeprecationWarning, 2)
        return self.query(query, *args, **kwargs)

    def query_one(self, query: str, *args, **kwargs) -> typing.Any:
        warnings.warn(
            'The "query_one()" method is deprecated and is scheduled to be '
            'removed. Use the "query_single()" method instead.',
            DeprecationWarning, 2)
        return self.query_single(query, *args, **kwargs)

    def fetchone(self, query: str, *args, **kwargs) -> typing.Any:
        warnings.warn(
            'The "fetchone()" method is deprecated and is scheduled to be '
            'removed. Use the "query_single()" method instead.',
            DeprecationWarning, 2)
        return self.query_single(query, *args, **kwargs)

    def fetchall_json(self, query: str, *args, **kwargs) -> str:
        warnings.warn(
            'The "fetchall_json()" method is deprecated and is scheduled to '
            'be removed. Use the "query_json()" method instead.',
            DeprecationWarning, 2)
        return self.query_json(query, *args, **kwargs)

    def query_one_json(self, query: str, *args, **kwargs) -> str:
        warnings.warn(
            'The "query_one_json()" method is deprecated and is scheduled to '
            'be removed. Use the "query_single_json()" method instead.',
            DeprecationWarning, 2)
        return self.query_single_json(query, *args, **kwargs)

    def fetchone_json(self, query: str, *args, **kwargs) -> str:
        warnings.warn(
            'The "fetchone_json()" method is deprecated and is scheduled to '
            'be removed. Use the "query_single_json()" method instead.',
            DeprecationWarning, 2)
        return self.query_single_json(query, *args, **kwargs)

    def execute(self, query: str) -> None:
        self._get_protocol().sync_simple_query(query, enums.Capability.EXECUTE)

    def transaction(self, *, isolation: str = None, readonly: bool = None,
                    deferrable: bool = None) -> legacy_transaction.Transaction:
        warnings.warn(
            'The "transaction()" method is deprecated and is scheduled to be '
            'removed. Use the "retrying_transaction()" or "raw_transaction()" '
            'method instead.',
            DeprecationWarning, 2)
        return legacy_transaction.Transaction(
            self, isolation, readonly, deferrable)

    def raw_transaction(self) -> _transaction.Transaction:
        return _transaction.Transaction(
            self,
            self._options.transaction_options,
        )

    def retrying_transaction(self) -> _retry.Retry:
        return _retry.Retry(self)

    def close(self) -> None:
        if not self.is_closed():
            self._inner._impl.close()

    def is_closed(self) -> bool:
        return self._inner._impl is None or self._inner._impl.is_closed()


def connect(dsn: str = None, *,
            host: str = None, port: int = None,
            user: str = None, password: str = None,
            admin: bool = None,
            database: str = None,
            tls_ca_file: str = None,
            tls_verify_hostname: bool = None,
            timeout: int = 10,
            wait_until_available: int = 30) -> BlockingIOConnection:

    addrs, params, config = con_utils.parse_connect_arguments(
        dsn=dsn, host=host, port=port, user=user, password=password,
        database=database, admin=admin,
        timeout=timeout,
        wait_until_available=wait_until_available,
        tls_ca_file=tls_ca_file, tls_verify_hostname=tls_verify_hostname,

        # ToDos
        command_timeout=None,
        server_settings=None)

    conn = BlockingIOConnection(
        addrs=addrs, params=params, config=config,
        codecs_registry=_CodecsRegistry(),
        query_cache=_QueryCodecsCache())
    conn.ensure_connected()
    return conn
