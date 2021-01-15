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
        async for tx in self.con.retry():
            async with tx:
                await tx.execute('''
                    INSERT test::Counter {
                        name := 'counter1'
                    };
                ''')

    async def test_async_retry_conflict(self):
        con2 = await self.connect(database=self.get_database_name())
        self.addCleanup(con2.aclose)

        barrier = Barrier(2)
        iterations = 0

        async def transaction1(con):
            async for tx in con.retry():
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

                    res = await tx.query_one('''
                        SELECT (
                            INSERT test::Counter {
                                name := 'counter2',
                                value := 1,
                            } UNLESS CONFLICT ON .name
                            ELSE (
                                UPDATE test::Counter
                                SET { value := .value + 1 }
                            )
                        ).value
                    ''')
            return res

        results = await asyncio.wait_for(asyncio.gather(
            transaction1(self.con),
            transaction1(con2),
            return_exceptions=True,
        ), 10)
        for e in results:
            if isinstance(e, BaseException):
                log.exception("Coroutine exception", exc_info=e)

        self.assertEqual(set(results), {1, 2})
        self.assertEqual(iterations, 3)