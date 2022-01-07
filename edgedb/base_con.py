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


import typing

from . import errors

from .con_utils import ClientConfiguration
from .con_utils import ResolvedConnectConfig


BaseConnection_T = typing.TypeVar('BaseConnection_T', bound='BaseConnection')


class BaseConnection:
    _protocol: typing.Any
    _addr: typing.Optional[typing.Union[str, typing.Tuple[str, int]]]
    _addrs: typing.Iterable[typing.Union[str, typing.Tuple[str, int]]]
    _config: ClientConfiguration
    _params: ResolvedConnectConfig
    _log_listeners: typing.Set[
        typing.Callable[[BaseConnection_T, errors.EdgeDBMessage], None]
    ]

    def __init__(
        self,
        addrs: typing.Iterable[typing.Union[str, typing.Tuple[str, int]]],
        config: ClientConfiguration,
        params: ResolvedConnectConfig,
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
