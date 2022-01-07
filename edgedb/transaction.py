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


import enum

from . import abstract
from . import errors
from . import options


class TransactionState(enum.Enum):
    NEW = 0
    STARTED = 1
    COMMITTED = 2
    ROLLEDBACK = 3
    FAILED = 4


class BaseTransaction:

    __slots__ = (
        '_client',
        '_connection',
        '_options',
        '_state',
        '_managed',
    )

    def __init__(self, client, options: options.TransactionOptions):
        self._client = client
        self._connection = None
        self._options = options
        self._state = TransactionState.NEW
        self._managed = False

    def is_active(self) -> bool:
        return self._state is TransactionState.STARTED

    def __check_state_base(self, opname):
        if self._state is TransactionState.COMMITTED:
            raise errors.InterfaceError(
                'cannot {}; the transaction is already committed'.format(
                    opname))
        if self._state is TransactionState.ROLLEDBACK:
            raise errors.InterfaceError(
                'cannot {}; the transaction is already rolled back'.format(
                    opname))
        if self._state is TransactionState.FAILED:
            raise errors.InterfaceError(
                'cannot {}; the transaction is in error state'.format(
                    opname))

    def __check_state(self, opname):
        if self._state is not TransactionState.STARTED:
            if self._state is TransactionState.NEW:
                raise errors.InterfaceError(
                    'cannot {}; the transaction is not yet started'.format(
                        opname))
            self.__check_state_base(opname)

    def _make_start_query(self):
        self.__check_state_base('start')
        if self._state is TransactionState.STARTED:
            raise errors.InterfaceError(
                'cannot start; the transaction is already started')

        return self._options.start_transaction_query()

    def _make_commit_query(self):
        self.__check_state('commit')
        return 'COMMIT;'

    def _make_rollback_query(self):
        self.__check_state('rollback')
        return 'ROLLBACK;'

    def __repr__(self):
        attrs = []
        attrs.append('state:{}'.format(self._state.name.lower()))
        attrs.append(repr(self._options))

        if self.__class__.__module__.startswith('edgedb.'):
            mod = 'edgedb'
        else:
            mod = self.__class__.__module__

        return '<{}.{} {} {:#x}>'.format(
            mod, self.__class__.__name__, ' '.join(attrs), id(self))


class BaseAsyncIOTransaction(BaseTransaction, abstract.AsyncIOExecutor):
    __slots__ = ()

    async def _start(self, single_connect=False) -> None:
        query = self._make_start_query()
        self._connection = await self._client._impl._acquire()
        if self._connection.is_closed():
            await self._connection.connect(
                single_attempt=single_connect
            )
        try:
            await self._connection.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.STARTED

    async def _commit(self):
        try:
            query = self._make_commit_query()
            try:
                await self._connection.privileged_execute(query)
            except BaseException:
                self._state = TransactionState.FAILED
                raise
            else:
                self._state = TransactionState.COMMITTED
        finally:
            await self._client._impl.release(self._connection)

    async def _rollback(self):
        try:
            query = self._make_rollback_query()
            try:
                await self._connection.privileged_execute(query)
            except BaseException:
                self._state = TransactionState.FAILED
                raise
            else:
                self._state = TransactionState.ROLLEDBACK
        finally:
            await self._client._impl.release(self._connection)

    async def _ensure_transaction(self):
        pass

    def _get_query_cache(self) -> abstract.QueryCache:
        return self._client._get_query_cache()

    async def _query(self, query_context: abstract.QueryContext):
        await self._ensure_transaction()
        result, _ = await self._connection.raw_query(query_context)
        return result

    async def execute(self, query: str) -> None:
        """Execute an EdgeQL command (or commands).

        Example:

        .. code-block:: pycon

            >>> await con.execute('''
            ...     CREATE TYPE MyType { CREATE PROPERTY a -> int64 };
            ...     FOR x IN {100, 200, 300} UNION INSERT MyType { a := x };
            ... ''')
        """
        await self._ensure_transaction()
        await self._connection.execute(query)


class BaseBlockingIOTransaction(BaseTransaction, abstract.Executor):
    __slots__ = ()

    def _start(self, single_connect=False) -> None:
        query = self._make_start_query()
        self._connection = self._client._impl.acquire()
        if self._connection.is_closed():
            self._connection.connect(single_attempt=single_connect)
        try:
            self._connection.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.STARTED

    def _commit(self):
        try:
            query = self._make_commit_query()
            try:
                self._connection.privileged_execute(query)
            except BaseException:
                self._state = TransactionState.FAILED
                raise
            else:
                self._state = TransactionState.COMMITTED
        finally:
            self._client._impl.release(self._connection)

    def _rollback(self):
        try:
            query = self._make_rollback_query()
            try:
                self._connection.privileged_execute(query)
            except BaseException:
                self._state = TransactionState.FAILED
                raise
            else:
                self._state = TransactionState.ROLLEDBACK
        finally:
            self._client._impl.release(self._connection)

    def _ensure_transaction(self):
        pass

    def _get_query_cache(self) -> abstract.QueryCache:
        return self._client._get_query_cache()

    def _query(self, query_context: abstract.QueryContext):
        self._ensure_transaction()
        return self._connection.raw_query(query_context)

    def execute(self, query: str) -> None:
        self._ensure_transaction()
        self._connection.execute(query)
