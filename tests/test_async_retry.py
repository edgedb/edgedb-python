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

import asyncio

import logging

import edgedb
from edgedb import compat
from edgedb import RetryOptions
from edgedb import _testbase as tb

log = logging.getLogger(__name__)


class Barrier:
    def __init__(self, number):
        self._counter = number
        self._cond = asyncio.Condition()

    async def ready(self):
        if self._counter == 0:
            return
        async with self._cond:
            self._counter -= 1
            assert self._counter >= 0, self._counter
            if self._counter == 0:
                self._cond.notify_all()
            else:
                await self._cond.wait_for(lambda: self._counter == 0)


class TestAsyncRetry(tb.AsyncQueryTestCase):

    ISOLATED_METHODS = False

    SETUP = '''
        CREATE TYPE test::Counter EXTENDING std::Object {
            CREATE PROPERTY name -> std::str {
                CREATE CONSTRAINT std::exclusive;
            };
            CREATE PROPERTY value -> std::int32 {
                SET default := 0;
            };
        };
    '''

    TEARDOWN = '''
        DROP TYPE test::Counter;
    '''

    async def test_async_retry_01(self):
        async for tx in self.con.retrying_transaction():
            async with tx:
                await tx.execute('''
                    INSERT test::Counter {
                        name := 'counter1'
                    };
                ''')

    async def test_async_retry_02(self):
        with self.assertRaises(ZeroDivisionError):
            async for tx in self.con.retrying_transaction():
                async with tx:
                    await tx.execute('''
                        INSERT test::Counter {
                            name := 'counter_retry_02'
                        };
                    ''')
                    1 / 0
        with self.assertRaises(edgedb.NoDataError):
            await self.con.query_single('''
                SELECT test::Counter
                FILTER .name = 'counter_retry_02'
            ''')

    async def test_async_retry_conflict(self):
        await self.execute_conflict('counter2')

    async def test_async_conflict_no_retry(self):
        with self.assertRaises(edgedb.TransactionSerializationError):
            await self.execute_conflict(
                'counter3',
                RetryOptions(attempts=1, backoff=edgedb.default_backoff)
            )

    async def execute_conflict(self, name='counter2', options=None):
        con2 = await self.connect(database=self.get_database_name())
        self.addCleanup(con2.aclose)

        barrier = Barrier(2)
        lock = asyncio.Lock()
        iterations = 0

        async def transaction1(con):
            async for tx in con.retrying_transaction():
                nonlocal iterations
                iterations += 1
                async with tx:
                    # This magic query makes the test more reliable for some
                    # reason. I guess this is because starting a transaction
                    # in EdgeDB (and/or Postgres) is accomplished somewhat
                    # lazily, i.e. only start transaction on the first query
                    # rather than on the `START TRANSACTION`.
                    await tx.query("SELECT 1")

                    # Start both transactions at the same initial data.
                    # One should succeed other should fail and retry.
                    # On next attempt, the latter should succeed
                    await barrier.ready()

                    await lock.acquire()
                    res = await tx.query_single('''
                        SELECT (
                            INSERT test::Counter {
                                name := <str>$name,
                                value := 1,
                            } UNLESS CONFLICT ON .name
                            ELSE (
                                UPDATE test::Counter
                                SET { value := .value + 1 }
                            )
                        ).value
                    ''', name=name)
                lock.release()
            return res

        con = self.con
        if options:
            con = con.with_retry_options(options)
            con2 = con2.with_retry_options(options)

        results = await compat.wait_for(asyncio.gather(
            transaction1(con),
            transaction1(con2),
            return_exceptions=True,
        ), 10)
        for e in results:
            if isinstance(e, BaseException):
                raise e

        self.assertEqual(set(results), {1, 2})
        self.assertEqual(iterations, 3)

    async def test_async_transaction_interface_errors(self):
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'.*the transaction is already started'):
            async for tx in self.con.retrying_transaction():
                async with tx:
                    await tx.start()

        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'.*Use `async with transaction:`'):
            async for tx in self.con.retrying_transaction():
                await tx.start()
