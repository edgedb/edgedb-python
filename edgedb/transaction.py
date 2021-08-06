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
import typing

from . import abstract
from . import base_con
from . import enums
from . import errors
from . import options
from .datatypes import datatypes
from .protocol import protocol


__all__ = ('Transaction', 'AsyncIOTransaction')


class TransactionState(enum.Enum):
    NEW = 0
    STARTED = 1
    COMMITTED = 2
    ROLLEDBACK = 3
    FAILED = 4


class BaseTransaction:

    __slots__ = ('_connection', '_options', '_state', '_managed')

    def __init__(self, owner, options: options.TransactionOptions):
        self._owner = owner
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


class AsyncIOTransaction(BaseTransaction, abstract.AsyncIOExecutor):
    __slots__ = ()

    async def __aenter__(self):
        if self._managed:
            raise errors.InterfaceError(
                'cannot enter context: already in an `async with` block')
        self._managed = True
        await self.start()
        return self

    async def __aexit__(self, extype, ex, tb):
        try:
            if extype is not None:
                await self.__rollback()
            else:
                await self.__commit()
        finally:
            self._managed = False

    async def start(self) -> None:
        """Enter the transaction or savepoint block."""
        await self._start()

    async def _start(self, single_connect=False) -> None:
        query = self._make_start_query()
        if isinstance(self._owner, base_con.BaseConnection):
            self._connection = self._owner
        else:
            self._connection = await self._owner.acquire()
        if self._connection._inner._borrowed_for:
            raise base_con.borrow_error(self._connection._inner._borrowed_for)
        await self._connection.ensure_connected(single_attempt=single_connect)
        self._connection_inner = self._connection._inner
        self._connection_impl = self._connection._inner._impl
        self._connection_inner._borrowed_for = (
            base_con.BorrowReason.TRANSACTION
        )
        try:
            await self._connection_impl.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            self._connection_inner._borrowed_for = None
            raise
        else:
            self._state = TransactionState.STARTED

    async def __commit(self):
        query = self._make_commit_query()
        try:
            await self._connection_impl.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.COMMITTED
        finally:
            self._connection_inner._borrowed_for = None
            if self._connection is not self._owner:
                await self._owner.release(self._connection)

    async def __rollback(self):
        query = self._make_rollback_query()
        try:
            await self._connection_impl.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.ROLLEDBACK
        finally:
            self._connection_inner._borrowed_for = None
            if self._connection is not self._owner:
                await self._owner.release(self._connection)

    async def commit(self) -> None:
        """Exit the transaction or savepoint block and commit changes."""
        if self._managed:
            raise errors.InterfaceError(
                'cannot manually commit from within an `async with` block')
        await self.__commit()

    async def rollback(self) -> None:
        """Exit the transaction or savepoint block and rollback changes."""
        if self._managed:
            raise errors.InterfaceError(
                'cannot manually rollback from within an `async with` block')
        await self.__rollback()

    async def query(self, query: str, *args, **kwargs) -> datatypes.Set:
        con = self._connection_inner
        result, _ = await self._connection_impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            io_format=protocol.IoFormat.BINARY,
        )
        return result

    async def query_single(self, query: str, *args, **kwargs) -> typing.Any:
        con = self._connection_inner
        result, _ = await self._connection_impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.BINARY,
        )
        return result

    async def query_json(self, query: str, *args, **kwargs) -> str:
        con = self._connection_inner
        result, _ = await self._connection_impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            io_format=protocol.IoFormat.JSON,
        )
        return result

    async def query_single_json(self, query: str, *args, **kwargs) -> str:
        con = self._connection_inner
        result, _ = await self._connection_impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.JSON,
        )
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
        await self._connection_impl._protocol.simple_query(
            query, enums.Capability.EXECUTE)


class Transaction(BaseTransaction, abstract.Executor):
    __slots__ = ()

    def __enter__(self):
        if self._managed:
            raise errors.InterfaceError(
                'cannot enter context: already in a `with` block')
        self._managed = True
        self.start()
        return self

    def __exit__(self, extype, ex, tb):
        try:
            if extype is not None:
                self.__rollback()
            else:
                self.__commit()
        finally:
            self._managed = False

    def start(self) -> None:
        """Enter the transaction or savepoint block."""
        self._start()

    def _start(self, single_connect=False) -> None:
        query = self._make_start_query()
        self._connection = self._owner  # no pools supported for blocking con
        if self._connection._inner._borrowed_for:
            raise base_con.borrow_error(self._connection_inner._borrowed_for)
        self._connection.ensure_connected(single_attempt=single_connect)
        self._connection_inner = self._connection._inner
        self._connection_inner._borrowed_for = (
            base_con.BorrowReason.TRANSACTION
        )
        self._connection_impl = self._connection_inner._impl
        try:
            self._connection_impl.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            self._connection_inner._borrowed_for = None
            raise
        else:
            self._state = TransactionState.STARTED

    def __commit(self):
        query = self._make_commit_query()
        try:
            self._connection_impl.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.COMMITTED
        finally:
            self._connection_inner._borrowed_for = None

    def __rollback(self):
        query = self._make_rollback_query()
        try:
            self._connection_impl.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.ROLLEDBACK
        finally:
            self._connection_inner._borrowed_for = None

    def commit(self) -> None:
        """Exit the transaction or savepoint block and commit changes."""
        if self._managed:
            raise errors.InterfaceError(
                'cannot manually commit from within a `with` block')
        self.__commit()

    def rollback(self) -> None:
        """Exit the transaction or savepoint block and rollback changes."""
        if self._managed:
            raise errors.InterfaceError(
                'cannot manually rollback from within a `with` block')
        self.__rollback()

    def query(self, query: str, *args, **kwargs) -> datatypes.Set:
        con = self._connection_inner
        return self._connection_impl._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            io_format=protocol.IoFormat.BINARY,
        )

    def query_single(self, query: str, *args, **kwargs) -> typing.Any:
        con = self._connection_inner
        return self._connection_impl._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.BINARY,
        )

    def query_json(self, query: str, *args, **kwargs) -> str:
        con = self._connection_inner
        return self._connection_impl._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            io_format=protocol.IoFormat.JSON,
        )

    def query_single_json(self, query: str, *args, **kwargs) -> str:
        con = self._connection_inner
        return self._connection_impl._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.JSON,
        )

    def execute(self, query: str) -> None:
        self._connection_impl._protocol.sync_simple_query(
            query, enums.Capability.EXECUTE)
