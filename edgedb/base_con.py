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

from .protocol.protocol import CodecsRegistry as _CodecsRegistry
from .protocol.protocol import QueryCodecsCache as _QueryCodecsCache


class BaseConnection:

    def __init__(self, protocol, addr, config, params):
        self._protocol = protocol

        self._addr = addr
        self._config = config
        self._params = params

        self._codecs_registry = _CodecsRegistry()
        self._query_cache = _QueryCodecsCache()

        self._top_xact = None

    def _get_unique_id(self, prefix):
        return f'_edgedb_{prefix}_{_uid_counter():x}_'

    def _get_last_status(self):
        status = self._protocol.last_status
        if status is not None:
            status = status.decode()
        return status

    @property
    def dbname(self):
        return self._params.database

    def is_in_transaction(self):
        """Return True if Connection is currently inside a transaction.

        :return bool: True if inside transaction, False otherwise.
        """
        return self._protocol.is_in_transaction()

    def get_settings(self):
        return self._protocol.get_settings()


# Thread-safe "+= 1" counter.
_uid_counter = itertools.count(1).__next__
