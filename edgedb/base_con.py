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


import itertools
import typing
import uuid

from . import errors

from .protocol.protocol import CodecsRegistry as _CodecsRegistry
from .protocol.protocol import QueryCodecsCache as _QueryCodecsCache


BaseConnection_T = typing.TypeVar('BaseConnection_T', bound='BaseConnection')


class BaseConnection:

    def __init__(self, protocol, addr, config, params, *,
                 codecs_registry=None, query_cache=None):
        self._protocol = protocol
        self._protocol.set_connection(self)

        self._log_listeners = set()

        self._addr = addr
        self._config = config
        self._params = params

        if codecs_registry is not None:
            self._codecs_registry = codecs_registry
        else:
            self._codecs_registry = _CodecsRegistry()

        if query_cache is not None:
            self._query_cache = query_cache
        else:
            self._query_cache = _QueryCodecsCache()

        self._top_xact = None

    def _set_type_codec(
        self,
        typeid: uuid.UUID,
        *,
        encoder: typing.Callable[[typing.Any], typing.Any],
        decoder: typing.Callable[[typing.Any], typing.Any],
        format: str
    ):
        self._codecs_registry.set_type_codec(
            typeid,
            encoder=encoder,
            decoder=decoder,
            format=format,
        )

    def _dispatch_log_message(self, msg):
        raise NotImplementedError

    def _on_log_message(self, msg):
        if self._log_listeners:
            self._dispatch_log_message(msg)

    def _get_unique_id(self, prefix):
        return f'_edgedb_{prefix}_{_uid_counter():x}_'

    def _get_last_status(self) -> typing.Optional[str]:
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
        if self.is_closed():
            raise errors.InterfaceError('connection is closed')
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

    def get_settings(self) -> typing.Dict[str, str]:
        return self._protocol.get_settings()


# Thread-safe "+= 1" counter.
_uid_counter = itertools.count(1).__next__
