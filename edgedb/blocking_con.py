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


import errno
import random
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
from .protocol import blocking_proto, protocol
from .protocol.protocol import CodecsRegistry as _CodecsRegistry
from .protocol.protocol import QueryCodecsCache as _QueryCodecsCache


TEMPORARY_ERRORS = frozenset({
    errno.ECONNREFUSED,
    errno.ECONNABORTED,
    errno.ECONNRESET,
    errno.ENOENT,
})


class _BlockingIOConnectionImpl:

    def __init__(self):
        self._addr = None
        self._protocol = None

    def connect(self, addrs, config, params):
        addr = None
        max_time = time.monotonic() + config.wait_until_available
        iteration = 1

        while True:
            for addr in addrs:
                try:
                    self._connect_addr(addr, config, params)
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
                        ))
                    raise nice_err from e.__cause__
                else:
                    assert self._protocol
                    return

            iteration += 1
            time.sleep(0.01 + random.random() * 0.2)

    def _connect_addr(self, addr, config, params):
        timeout = config.connect_timeout
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
            except socket.gaierror as e:
                # All name resolution errors are considered temporary
                err = errors.ClientConnectionFailedTemporarilyError(str(e))
                raise err from e
            except OSError as e:
                message = str(e)
                if e.errno in TEMPORARY_ERRORS:
                    err = errors.ClientConnectionFailedTemporarilyError(
                        message
                    )
                else:
                    err = errors.ClientConnectionFailedError(message)
                raise err from e

            time_left = deadline - time.monotonic()
            if time_left <= 0:
                raise TimeoutError

            if not isinstance(addr, str):
                sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

            proto = blocking_proto.BlockingIOProtocol(params, sock)

            sock.settimeout(time_left)
            proto.sync_connect()
            sock.settimeout(None)

            self._protocol = proto
            self._addr = addr

        except Exception:
            sock.close()
            raise

    def execute(self, query: str) -> None:
        self._protocol.sync_simple_query(query)

    def is_closed(self):
        proto = self._protocol
        return not (proto and proto.sock is not None and
                    proto.sock.fileno() >= 0 and proto.connected)

    def close(self):
        if self._protocol:
            self._protocol.abort()


class BlockingIOConnection(base_con.BaseConnection, abstract.Executor):

    def __init__(self, addrs, config, params, *,
                 codecs_registry, query_cache):
        super().__init__(addrs, config, params,
                         codecs_registry=codecs_registry,
                         query_cache=query_cache)
        self._impl = None

    def ensure_connected(self):
        self._get_protocol()

    def _reconnect(self):
        self._impl = _BlockingIOConnectionImpl()
        self._impl.connect(self._addrs, self._config, self._params)
        assert self._impl._protocol

    def _get_protocol(self):
        if not self._impl or self._impl.is_closed():
            self._reconnect()
        return self._impl._protocol

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

    def _fetchall(
        self,
        query: str,
        *args,
        __limit__: int=0,
        __typenames__: bool=False,
        **kwargs,
    ) -> datatypes.Set:
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
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
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            implicit_limit=__limit__,
            inline_typenames=False,
            io_format=protocol.IoFormat.JSON,
        )

    def query(self, query: str, *args, **kwargs) -> datatypes.Set:
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.BINARY,
        )

    def query_one(self, query: str, *args, **kwargs) -> typing.Any:
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.BINARY,
        )

    def query_json(self, query: str, *args, **kwargs) -> str:
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.JSON,
        )

    def _fetchall_json_elements(
            self, query: str, *args, **kwargs) -> typing.List[str]:
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.JSON_ELEMENTS,
        )

    def query_one_json(self, query: str, *args, **kwargs) -> str:
        return self._get_protocol().sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.JSON,
        )

    def fetchall(self, query: str, *args, **kwargs) -> datatypes.Set:
        warnings.warn(
            'The "fetchall()" method is deprecated and is scheduled to be '
            'removed. Use the "query()" method instead.',
            DeprecationWarning, 2)
        return self.query(query, *args, **kwargs)

    def fetchone(self, query: str, *args, **kwargs) -> typing.Any:
        warnings.warn(
            'The "fetchone()" method is deprecated and is scheduled to be '
            'removed. Use the "query_one()" method instead.',
            DeprecationWarning, 2)
        return self.query_one(query, *args, **kwargs)

    def fetchall_json(self, query: str, *args, **kwargs) -> str:
        warnings.warn(
            'The "fetchall_json()" method is deprecated and is scheduled to '
            'be removed. Use the "query_json()" method instead.',
            DeprecationWarning, 2)
        return self.query_json(query, *args, **kwargs)

    def fetchone_json(self, query: str, *args, **kwargs) -> str:
        warnings.warn(
            'The "fetchone_json()" method is deprecated and is scheduled to '
            'be removed. Use the "query_one_json()" method instead.',
            DeprecationWarning, 2)
        return self.query_one_json(query, *args, **kwargs)

    def execute(self, query: str) -> None:
        self._get_protocol().sync_simple_query(query)

    def transaction(self, *, isolation: str = None, readonly: bool = None,
                    deferrable: bool = None) -> transaction.Transaction:
        return transaction.Transaction(
            self, isolation, readonly, deferrable)

    def close(self) -> None:
        if not self.is_closed():
            self._impl.close()

    def is_closed(self) -> bool:
        return self._impl is None or self._impl.is_closed()


def connect(dsn: str = None, *,
            host: str = None, port: int = None,
            user: str = None, password: str = None,
            admin: bool = None,
            database: str = None,
            timeout: int = 10,
            wait_until_available: int = 30) -> BlockingIOConnection:

    addrs, params, config = con_utils.parse_connect_arguments(
        dsn=dsn, host=host, port=port, user=user, password=password,
        database=database, admin=admin,
        timeout=timeout,
        wait_until_available=wait_until_available,

        # ToDos
        command_timeout=None,
        server_settings=None)

    conn = BlockingIOConnection(
        addrs=addrs, params=params, config=config,
        codecs_registry=_CodecsRegistry(),
        query_cache=_QueryCodecsCache())
    conn.ensure_connected()
    return conn
