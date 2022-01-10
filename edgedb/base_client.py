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


import abc
import typing

from . import abstract
from . import con_utils
from . import errors
from . import options
from .protocol import protocol


BaseConnection_T = typing.TypeVar('BaseConnection_T', bound='BaseConnection')


class BaseConnection:
    _protocol: typing.Any
    _addr: typing.Optional[typing.Union[str, typing.Tuple[str, int]]]
    _addrs: typing.Iterable[typing.Union[str, typing.Tuple[str, int]]]
    _config: con_utils.ClientConfiguration
    _params: con_utils.ResolvedConnectConfig
    _log_listeners: typing.Set[
        typing.Callable[[BaseConnection_T, errors.EdgeDBMessage], None]
    ]

    def __init__(
        self,
        addrs: typing.Iterable[typing.Union[str, typing.Tuple[str, int]]],
        config: con_utils.ClientConfiguration,
        params: con_utils.ResolvedConnectConfig,
    ):
        self._addr = None
        self._protocol = None
        self._addrs = addrs
        self._config = config
        self._params = params
        self._log_listeners = set()

    def _dispatch_log_message(self, msg):
        raise NotImplementedError

    def _on_log_message(self, msg):
        if self._log_listeners:
            self._dispatch_log_message(msg)

    def connected_addr(self):
        return self._addr

    def _get_last_status(self) -> typing.Optional[str]:
        if self._protocol is None:
            return None
        status = self._protocol.last_status
        if status is not None:
            status = status.decode()
        return status

    def _cleanup(self):
        self._log_listeners.clear()

    def add_log_listener(
        self: BaseConnection_T,
        callback: typing.Callable[[BaseConnection_T, errors.EdgeDBMessage],
                                  None]
    ) -> None:
        """Add a listener for EdgeDB log messages.

        :param callable callback:
            A callable receiving the following arguments:
            **connection**: a Connection the callback is registered with;
            **message**: the `edgedb.EdgeDBMessage` message.
        """
        self._log_listeners.add(callback)

    def remove_log_listener(
        self: BaseConnection_T,
        callback: typing.Callable[[BaseConnection_T, errors.EdgeDBMessage],
                                  None]
    ) -> None:
        """Remove a listening callback for log messages."""
        self._log_listeners.discard(callback)

    @property
    def dbname(self) -> str:
        return self._params.database

    def is_closed(self) -> bool:
        raise NotImplementedError

    def is_in_transaction(self) -> bool:
        """Return True if Connection is currently inside a transaction.

        :return bool: True if inside transaction, False otherwise.
        """
        return self._protocol.is_in_transaction()

    def get_settings(self) -> typing.Dict[str, typing.Any]:
        return self._protocol.get_settings()


class BaseImpl(abc.ABC):
    __slots__ = ("_connect_args", "_codecs_registry", "_query_cache")

    def __init__(self, connect_args):
        self._connect_args = connect_args
        self._codecs_registry = protocol.CodecsRegistry()
        self._query_cache = protocol.QueryCodecsCache()

    def _parse_connect_args(self):
        return con_utils.parse_connect_arguments(
            **self._connect_args,
            # ToDos
            command_timeout=None,
            server_settings=None,
        )

    @abc.abstractmethod
    def get_concurrency(self):
        ...

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
            Keyword arguments for the
            :func:`~edgedb.asyncio_client.create_async_client` function.
        """

        connect_kwargs["dsn"] = dsn
        self._connect_args = connect_kwargs
        self._codecs_registry = protocol.CodecsRegistry()
        self._query_cache = protocol.QueryCodecsCache()

    @property
    def codecs_registry(self):
        return self._codecs_registry

    @property
    def query_cache(self):
        return self._query_cache


class BaseClient(abstract.Executor, options._OptionsMixin, abc.ABC):
    __slots__ = ("_impl", "_options")

    def __init__(
        self,
        *,
        concurrency: typing.Optional[int],
        dsn=None,
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
        **kwargs,
    ):
        super().__init__()
        connect_args = {
            "dsn": dsn,
            "host": host,
            "port": port,
            "credentials": credentials,
            "credentials_file": credentials_file,
            "user": user,
            "password": password,
            "database": database,
            "timeout": timeout,
            "tls_ca": tls_ca,
            "tls_ca_file": tls_ca_file,
            "tls_security": tls_security,
            "wait_until_available": wait_until_available,
        }
        if concurrency == 0:
            self._impl = self._create_single_connection_pool(
                connect_args, **kwargs
            )
        else:
            self._impl = self._create_connection_pool(
                connect_args, concurrency=concurrency, **kwargs
            )

    def _shallow_clone(self):
        new_client = self.__class__.__new__(self.__class__)
        new_client._impl = self._impl
        return new_client

    def _get_query_cache(self) -> abstract.QueryCache:
        return abstract.QueryCache(
            codecs_registry=self._impl.codecs_registry,
            query_cache=self._impl.query_cache,
        )

    def _get_retry_options(self) -> typing.Optional[options.RetryOptions]:
        return self._options.retry_options

    def _create_single_connection_pool(
        self, connect_args, **kwargs
    ) -> BaseImpl:
        raise errors.InterfaceError("single-connection is not implemented")

    def _create_connection_pool(
        self, connect_args, *, concurrency, **kwargs
    ) -> BaseImpl:
        raise errors.InterfaceError("concurrency is not implemented")

    @property
    def concurrency(self) -> int:
        """Max number of connections in the pool."""

        return self._impl.get_concurrency()
