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


import socket
import time
import typing

from . import base_con
from . import con_utils
from . import errors
from . import transaction

from .datatypes import datatypes
from .protocol import blocking_proto
from .protocol import protocol


class BlockingIOConnection(base_con.BaseConnection):

    def _dump(
        self,
        *,
        on_header: typing.Callable[[bytes], None],
        on_data: typing.Callable[[bytes], None],
    ) -> None:
        # Private API: do not use.
        self._protocol.sync_dump(
            header_callback=on_header,
            block_callback=on_data)

    def _restore(
        self,
        *,
        header: bytes,
        data_gen: typing.Iterable[bytes],
    ) -> None:
        self._protocol.sync_restore(
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
        **kwargs,
    ) -> datatypes.Set:
        return self._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            implicit_limit=__limit__,
            io_format=protocol.IoFormat.BINARY,
        )

    def _fetchall_json(
        self,
        query: str,
        *args,
        __limit__: int=0,
        **kwargs,
    ) -> datatypes.Set:
        return self._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            implicit_limit=__limit__,
            io_format=protocol.IoFormat.JSON,
        )

    def fetchall(self, query: str, *args, **kwargs) -> datatypes.Set:
        return self._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.BINARY,
        )

    def fetchone(self, query: str, *args, **kwargs) -> typing.Any:
        return self._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.BINARY,
        )

    def fetchall_json(self, query: str, *args, **kwargs) -> str:
        return self._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.JSON,
        )

    def _fetchall_json_elements(
            self, query: str, *args, **kwargs) -> typing.List[str]:
        return self._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            io_format=protocol.IoFormat.JSON_ELEMENTS,
        )

    def fetchone_json(self, query: str, *args, **kwargs) -> str:
        return self._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=self._codecs_registry,
            qc=self._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.JSON,
        )

    def execute(self, query: str) -> None:
        self._protocol.sync_simple_query(query)

    def transaction(self, *, isolation: str = None, readonly: bool = None,
                    deferrable: bool = None) -> transaction.Transaction:
        return transaction.Transaction(
            self, isolation, readonly, deferrable)

    def close(self) -> None:
        if not self.is_closed():
            self._protocol.abort()

    def is_closed(self) -> bool:
        return (self._protocol.sock is None or
                self._protocol.sock.fileno() < 0 or
                not self._protocol.connected)


def _connect_addr(*, addr, timeout, params, config, connection_class):
    if timeout <= 0:
        raise TimeoutError

    if isinstance(addr, str):
        # UNIX socket
        sock = socket.socket(socket.AF_UNIX)
    else:
        sock = socket.socket(socket.AF_INET)

    try:
        before = time.monotonic()
        sock.settimeout(timeout)

        try:
            sock.connect(addr)
        except (ConnectionError, FileNotFoundError) as e:
            msg = con_utils.render_client_no_connection_error(e, addr)
            raise errors.ClientConnectionError(msg) from e

        timeout -= time.monotonic() - before

        if timeout <= 0:
            raise TimeoutError

        if not isinstance(addr, str):
            sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        proto = blocking_proto.BlockingIOProtocol(params, sock)

        sock.settimeout(timeout)
        proto.sync_connect()
        sock.settimeout(None)

        return connection_class(proto, addr, config, params)

    except Exception:
        sock.close()
        raise


def connect(dsn: str = None, *,
            host: str = None, port: int = None,
            user: str = None, password: str = None,
            admin: str = None,
            database: str = None,
            timeout: int = 60) -> BlockingIOConnection:

    addrs, params, config = con_utils.parse_connect_arguments(
        dsn=dsn, host=host, port=port, user=user, password=password,
        database=database, admin=admin,

        # ToDos
        timeout=None,
        command_timeout=None,
        server_settings=None)

    last_error = None
    addr = None
    for addr in addrs:
        before = time.monotonic()
        try:
            con = _connect_addr(
                addr=addr, timeout=timeout,
                params=params, config=config,
                connection_class=BlockingIOConnection)
        except (OSError, TimeoutError, ConnectionError, socket.error,
                errors.ClientConnectionError) as ex:
            last_error = ex
        else:
            return con
        finally:
            timeout -= time.monotonic() - before

    raise last_error
