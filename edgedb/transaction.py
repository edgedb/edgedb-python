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


class TransactionState(enum.Enum):
    NEW = 0
    STARTED = 1
    COMMITTED = 2
    ROLLEDBACK = 3
    FAILED = 4


class BaseTransaction:

    __slots__ = (
        '_connection',
        '_connection_inner',
        '_connection_impl',
        '_pool',
        '_options',
        '_state',
        '_managed',
    )

    def __init__(self, owner, options: options.TransactionOptions):
        if isinstance(owner, base_con.BaseConnection):
            self._connection = owner
            self._connection_inner = owner._inner
            self._pool = None
        else:
            self._connection = None
            self._connection_inner = None
            self._pool = owner
        self._connection_impl = None
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

    def _borrow(self):
        inner = self._connection_inner
        if inner._borrowed_for:
            raise base_con.borrow_error(inner._borrowed_for)
        inner._borrowed_for = base_con.BorrowReason.TRANSACTION

    def _maybe_return(self):
        if self._connection_inner is not None:
            self._connection_inner._borrowed_for = None

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
        if self._pool is not None:
            self._connection = await self._pool._acquire()
            self._connection_inner = self._connection._inner
        inner = self._connection_inner
        if not inner._impl or inner._impl.is_closed():
            await self._connection._reconnect(single_attempt=single_connect)
        self._connection_impl = self._connection._inner._impl
        try:
            await self._connection_impl.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.STARTED

    async def _commit(self):
        try:
            query = self._make_commit_query()
            try:
                await self._connection_impl.privileged_execute(query)
            except BaseException:
                self._state = TransactionState.FAILED
                raise
            else:
                self._state = TransactionState.COMMITTED
        finally:
            self._maybe_return()
            if self._pool is not None:
                await self._pool._release(self._connection)

    async def _rollback(self):
        try:
            query = self._make_rollback_query()
            try:
                await self._connection_impl.privileged_execute(query)
            except BaseException:
                self._state = TransactionState.FAILED
                raise
            else:
                self._state = TransactionState.ROLLEDBACK
        finally:
            self._maybe_return()
            if self._pool is not None:
                await self._pool._release(self._connection)

    async def _ensure_transaction(self):
        pass

    async def query(self, query: str, *args, **kwargs) -> datatypes.Set:
        await self._ensure_transaction()
        con = self._connection_inner
        result, _ = await self._connection_impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            io_format=protocol.IoFormat.BINARY,
            allow_capabilities=enums.Capability.EXECUTE,
        )
        return result

    async def query_single(
        self, query: str, *args, **kwargs
    ) -> typing.Union[typing.Any, None]:
        await self._ensure_transaction()
        con = self._connection_inner
        result, _ = await self._connection_impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.BINARY,
            allow_capabilities=enums.Capability.EXECUTE,
        )
        return result

    async def query_required_single(
        self, query: str, *args, **kwargs
    ) -> typing.Any:
        await self._ensure_transaction()
        con = self._connection_inner
        result, _ = await self._connection_impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            expect_one=True,
            required_one=True,
            io_format=protocol.IoFormat.BINARY,
            allow_capabilities=enums.Capability.EXECUTE,
        )
        return result

    async def query_json(self, query: str, *args, **kwargs) -> str:
        await self._ensure_transaction()
        con = self._connection_inner
        result, _ = await self._connection_impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            io_format=protocol.IoFormat.JSON,
            allow_capabilities=enums.Capability.EXECUTE,
        )
        return result

    async def query_single_json(self, query: str, *args, **kwargs) -> str:
        await self._ensure_transaction()
        con = self._connection_inner
        result, _ = await self._connection_impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.JSON,
            allow_capabilities=enums.Capability.EXECUTE,
        )
        return result

    async def query_required_single_json(
        self, query: str, *args, **kwargs
    ) -> str:
        await self._ensure_transaction()
        con = self._connection_inner
        result, _ = await self._connection_impl._protocol.execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            expect_one=True,
            required_one=True,
            io_format=protocol.IoFormat.JSON,
            allow_capabilities=enums.Capability.EXECUTE,
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
        await self._ensure_transaction()
        await self._connection_impl._protocol.simple_query(
            query, enums.Capability.EXECUTE)


class BaseBlockingIOTransaction(BaseTransaction, abstract.Executor):
    __slots__ = ()

    def _start(self, single_connect=False) -> None:
        query = self._make_start_query()
        # no pools supported for blocking con
        inner = self._connection_inner
        if not inner._impl or inner._impl.is_closed():
            self._connection._reconnect(single_attempt=single_connect)
        self._connection_inner = self._connection._inner
        self._connection_impl = self._connection_inner._impl
        try:
            self._connection_impl.privileged_execute(query)
        except BaseException:
            self._state = TransactionState.FAILED
            raise
        else:
            self._state = TransactionState.STARTED

    def _commit(self):
        try:
            query = self._make_commit_query()
            try:
                self._connection_impl.privileged_execute(query)
            except BaseException:
                self._state = TransactionState.FAILED
                raise
            else:
                self._state = TransactionState.COMMITTED
        finally:
            self._maybe_return()

    def _rollback(self):
        try:
            query = self._make_rollback_query()
            try:
                self._connection_impl.privileged_execute(query)
            except BaseException:
                self._state = TransactionState.FAILED
                raise
            else:
                self._state = TransactionState.ROLLEDBACK
        finally:
            self._maybe_return()

    def _ensure_transaction(self):
        pass

    def query(self, query: str, *args, **kwargs) -> datatypes.Set:
        self._ensure_transaction()
        con = self._connection_inner
        return self._connection_impl._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            io_format=protocol.IoFormat.BINARY,
            allow_capabilities=enums.Capability.EXECUTE,
        )

    def query_single(
        self, query: str, *args, **kwargs
    ) -> typing.Union[typing.Any, None]:
        self._ensure_transaction()
        con = self._connection_inner
        return self._connection_impl._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.BINARY,
            allow_capabilities=enums.Capability.EXECUTE,
        )

    def query_required_single(self, query: str, *args, **kwargs) -> typing.Any:
        self._ensure_transaction()
        con = self._connection_inner
        return self._connection_impl._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            expect_one=True,
            required_one=True,
            io_format=protocol.IoFormat.BINARY,
            allow_capabilities=enums.Capability.EXECUTE,
        )

    def query_json(self, query: str, *args, **kwargs) -> str:
        self._ensure_transaction()
        con = self._connection_inner
        return self._connection_impl._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            io_format=protocol.IoFormat.JSON,
            allow_capabilities=enums.Capability.EXECUTE,
        )

    def query_single_json(self, query: str, *args, **kwargs) -> str:
        self._ensure_transaction()
        con = self._connection_inner
        return self._connection_impl._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            expect_one=True,
            io_format=protocol.IoFormat.JSON,
            allow_capabilities=enums.Capability.EXECUTE,
        )

    def query_required_single_json(self, query: str, *args, **kwargs) -> str:
        self._ensure_transaction()
        con = self._connection_inner
        return self._connection_impl._protocol.sync_execute_anonymous(
            query=query,
            args=args,
            kwargs=kwargs,
            reg=con._codecs_registry,
            qc=con._query_cache,
            expect_one=True,
            required_one=True,
            io_format=protocol.IoFormat.JSON,
            allow_capabilities=enums.Capability.EXECUTE,
        )

    def execute(self, query: str) -> None:
        self._ensure_transaction()
        self._connection_impl._protocol.sync_simple_query(
            query, enums.Capability.EXECUTE)
