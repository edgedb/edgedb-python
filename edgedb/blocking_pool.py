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


import typing

from . import abstract
from . import blocking_con
from . import errors
from . import options
from . import retry as _retry
from .protocol import protocol


class _SingleConnectionPoolImpl:
    __slots__ = (
        "_connect_args",
        "_connection",
        "_acquired",
        "_closed",
        "_codecs_registry",
        "_query_cache",
    )

    def __init__(self, connect_args):
        self._connect_args = connect_args
        self._connection = None
        self._acquired = False
        self._closed = False
        self._codecs_registry = protocol.CodecsRegistry()
        self._query_cache = protocol.QueryCodecsCache()

    def ensure_connected(self):
        self.release(self.acquire())

    def acquire(self):
        if self._acquired:
            raise errors.InterfaceError("cannot acquire twice")
        self._acquired = True
        if self._connection is None:
            self._connection = blocking_con.connect_raw(**self._connect_args)
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


class Client(abstract.Executor, options._OptionsMixin):
    __slots__ = ("_impl", "_options")

    def __init__(self, concurrency, **connect_args):
        super().__init__()

        if concurrency == 0:
            self._impl = _SingleConnectionPoolImpl(connect_args)
        else:
            raise errors.InterfaceError("concurrency is not implemented")

    def _shallow_clone(self):
        new_pool = self.__class__.__new__(self.__class__)
        new_pool._impl = self._impl
        return new_pool

    def _get_query_cache(self) -> abstract.QueryCache:
        return abstract.QueryCache(
            codecs_registry=self._impl._codecs_registry,
            query_cache=self._impl._query_cache,
        )

    def _get_retry_options(self) -> typing.Optional[options.RetryOptions]:
        return self._options.retry_options

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


def create_client(
    dsn=None,
    *,
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
    concurrency=0,
):
    return Client(
        concurrency=concurrency,
        # connect arguments
        dsn=dsn,
        host=host,
        port=port,
        credentials=credentials,
        credentials_file=credentials_file,
        user=user,
        password=password,
        database=database,
        timeout=timeout,
        tls_ca=tls_ca,
        tls_ca_file=tls_ca_file,
        tls_security=tls_security,
        wait_until_available=wait_until_available,
    )
