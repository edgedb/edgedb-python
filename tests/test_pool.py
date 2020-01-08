#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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
import inspect
import random

import edgedb

from edgedb import _testbase as tb
from edgedb import asyncio_con
from edgedb import asyncio_pool


class TestPool(tb.AsyncQueryTestCase):
    def create_pool(self, **kwargs):
        conargs = self.get_connect_args().copy()
        conargs["database"] = self.con.dbname
        conargs.update(kwargs)

        return edgedb.create_async_pool(**conargs)

    async def test_pool_01(self):
        for n in {1, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                pool = await self.create_pool(min_size=5, max_size=10)

                async def worker():
                    con = await pool.acquire()
                    self.assertEqual(await con.fetchone("SELECT 1"), 1)
                    await pool.release(con)

                tasks = [worker() for _ in range(n)]
                await asyncio.gather(*tasks)
                await pool.aclose()

    async def test_pool_02(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                async with self.create_pool(min_size=5, max_size=5) as pool:

                    async def worker():
                        con = await pool.acquire()
                        self.assertEqual(await con.fetchone("SELECT 1"), 1)
                        await pool.release(con)

                    tasks = [worker() for _ in range(n)]
                    await asyncio.gather(*tasks)

    async def test_pool_04(self):
        pool = await self.create_pool(min_size=1, max_size=1)

        con = await pool.acquire()

        # Manual termination of pool connections releases the
        # pool item immediately.
        con.terminate()
        self.assertIsNone(pool._holders[0]._con)
        self.assertIsNone(pool._holders[0]._in_use)

        con = await pool.acquire()
        self.assertEqual(await con.fetchone("SELECT 1"), 1)

        await con.aclose()
        self.assertIsNone(pool._holders[0]._con)
        self.assertIsNone(pool._holders[0]._in_use)
        # Calling release should not hurt.
        await pool.release(con)

        pool.terminate()

    async def test_pool_05(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                pool = await self.create_pool(min_size=5, max_size=10)

                async def worker():
                    async with pool.acquire() as con:
                        self.assertEqual(await con.fetchone("SELECT 1"), 1)

                tasks = [worker() for _ in range(n)]
                await asyncio.gather(*tasks)
                await pool.aclose()

    async def test_pool_06(self):
        fut = asyncio.Future()

        async def on_acquire(con):
            fut.set_result(con)

        async with self.create_pool(
            min_size=5, max_size=5, on_acquire=on_acquire
        ) as pool:
            async with pool.acquire() as con:
                pass

        self.assertIs(con, await fut)

    async def test_pool_07(self):
        cons = set()

        async def on_acquire(con):
            if con._con not in cons:  # `con` is `PoolConnectionProxy`.
                raise RuntimeError("on_acquire was not called")

        async def on_connect(con):
            if con in cons:
                raise RuntimeError("on_connect was called more than once")
            cons.add(con)

        async def user(pool):
            async with pool.acquire() as con:
                if con._con not in cons:  # `con` is `PoolConnectionProxy`.
                    raise RuntimeError("init was not called")

        async with self.create_pool(
            min_size=2, max_size=5, on_connect=on_connect,
            on_acquire=on_acquire,
        ) as pool:
            users = asyncio.gather(*[user(pool) for _ in range(10)])
            await users

        self.assertEqual(len(cons), 5)

    async def test_pool_08(self):
        pool = await self.create_pool(min_size=1, max_size=1)

        try:
            con = await pool.acquire()
            with self.assertRaisesRegex(
                    edgedb.InterfaceError, "is not a member"):
                await pool.release(con._con)
        finally:
            await pool.release(con)
            await pool.aclose()

    async def test_pool_09(self):
        pool1 = await self.create_pool(min_size=1, max_size=1)

        pool2 = await self.create_pool(min_size=1, max_size=1)

        try:
            con = await pool1.acquire()
            with self.assertRaisesRegex(
                edgedb.InterfaceError, "is not a member"
            ):
                await pool2.release(con)
        finally:
            await pool1.release(con)

        await pool1.aclose()
        await pool2.aclose()

    async def test_pool_10(self):
        pool = await self.create_pool(min_size=1, max_size=1)

        con = await pool.acquire()
        await pool.release(con)
        await pool.release(con)

        await pool.aclose()

    async def test_pool_11(self):
        pool = await self.create_pool(min_size=1, max_size=1)

        async with pool.acquire() as con:
            self.assertIn(repr(con._con), repr(con))  # Test __repr__.
            txn = con.transaction()

        self.assertIn("[released]", repr(con))

        for meth in (
            "fetchone",
            "fetchall",
            "execute",
        ):
            with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r"cannot call.*Connection\..*released back to the pool",
            ):
                getattr(con, meth)("select 1")

        for meth in ("start", "commit", "rollback"):
            with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r"cannot call.*Transaction\.{meth}.*released "
                r"back to the pool".format(meth=meth),
            ):

                getattr(txn, meth)()

        await pool.aclose()

    async def test_pool_12(self):
        pool = await self.create_pool(min_size=1, max_size=1)

        async with pool.acquire() as con:
            self.assertTrue(isinstance(con, asyncio_con.AsyncIOConnection))
            self.assertFalse(isinstance(con, list))

        await pool.aclose()

    async def test_pool_13(self):
        pool = await self.create_pool(min_size=1, max_size=1)

        async with pool.acquire() as con:
            self.assertIn("Execute an EdgeQL command", con.execute.__doc__)
            self.assertEqual(con.execute.__name__, "execute")

            self.assertIn(
                str(inspect.signature(con.execute))[1:],
                str(inspect.signature(asyncio_con.AsyncIOConnection.execute)),
            )

        await pool.aclose()

    def test_pool_init_run_until_complete(self):
        pool_init = self.create_pool()
        pool = self.loop.run_until_complete(pool_init)
        self.assertIsInstance(pool, asyncio_pool.AsyncIOPool)
        self.loop.run_until_complete(pool.aclose())

    async def test_pool_exception_in_on_acquire_and_on_connect(self):
        class Error(Exception):
            pass

        async def callback(con):
            nonlocal setup_calls, last_con
            last_con = con
            setup_calls += 1
            if setup_calls > 1:
                cons.append(con)
            else:
                cons.append("error")
                raise Error

        with self.subTest(method="on_acquire"):
            setup_calls = 0
            last_con = None
            cons = []
            async with self.create_pool(
                min_size=1, max_size=1, on_acquire=callback
            ) as pool:
                with self.assertRaises(Error):
                    await pool.acquire()
                self.assertTrue(last_con.is_closed())

                async with pool.acquire() as con:
                    self.assertEqual(cons, ["error", con])

        with self.subTest(method="on_connect"):
            setup_calls = 0
            last_con = None
            cons = []
            async with self.create_pool(
                min_size=0, max_size=1, on_connect=callback
            ) as pool:
                with self.assertRaises(Error):
                    await pool.acquire()
                self.assertTrue(last_con.is_closed())

                async with pool.acquire() as con:
                    self.assertEqual(await con.fetchone("select 1"), 1)
                    self.assertEqual(cons, ["error", con._con])

    async def test_pool_no_acquire_deadlock(self):
        async with self.create_pool(
            min_size=1, max_size=1,
        ) as pool:

            async def sleep_and_release():
                async with pool.acquire() as con:
                    await con.execute("SELECT sys::sleep(1)")

            asyncio.ensure_future(sleep_and_release())
            await asyncio.sleep(0.5)

            async with pool.acquire() as con:
                await con.fetchone("SELECT 1")

    async def test_pool_config_persistence(self):
        N = 100
        cons = set()

        class MyConnection(asyncio_con.AsyncIOConnection):
            async def foo(self):
                return 42

            async def fetchone(self, query):
                res = await super().fetchone(query)
                return res + 1

        async def test(pool):
            async with pool.acquire() as con:
                self.assertEqual(await con.fetchone("SELECT 1"), 2)
                self.assertEqual(await con.foo(), 42)
                self.assertTrue(isinstance(con, MyConnection))
                cons.add(con)

        async with self.create_pool(
            min_size=10,
            max_size=10,
            connection_class=MyConnection,
        ) as pool:

            await asyncio.gather(*[test(pool) for _ in range(N)])

        self.assertEqual(len(cons), N)

    async def test_pool_connection_methods(self):
        async def test_fetchall(pool):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100)
            r = await pool.fetchall("SELECT {}".format(i))
            self.assertEqual(list(r), [i])
            return 1

        async def test_fetchone(pool):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100)
            r = await pool.fetchone("SELECT {}".format(i))
            self.assertEqual(r, i)
            return 1

        async def test_execute(pool):
            await asyncio.sleep(random.random() / 100)
            await pool.execute("SELECT {1, 2, 3, 4}")
            return 1

        async def run(N, meth):
            async with self.create_pool(min_size=5, max_size=10) as pool:

                coros = [meth(pool) for _ in range(N)]
                res = await asyncio.gather(*coros)
                self.assertEqual(res, [1] * N)

        methods = [
            test_fetchall,
            test_fetchone,
            test_execute,
        ]

        with tb.silence_asyncio_long_exec_warning():
            for method in methods:
                with self.subTest(method=method.__name__):
                    await run(200, method)

    async def test_pool_handles_transaction_exit_in_asyncgen_1(self):
        pool = await self.create_pool(min_size=1, max_size=1)

        async def iterate(con):
            async with con.transaction():
                for record in await con.fetchall("SELECT {1, 2, 3}"):
                    yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            async with pool.acquire() as con:
                agen = iterate(con)
                try:
                    async for _ in agen:  # noqa
                        raise MyException()
                finally:
                    await agen.aclose()

        await pool.aclose()

    async def test_pool_handles_transaction_exit_in_asyncgen_2(self):
        pool = await self.create_pool(min_size=1, max_size=1)

        async def iterate(con):
            async with con.transaction():
                for record in await con.fetchall("SELECT {1, 2, 3}"):
                    yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            async with pool.acquire() as con:
                iterator = iterate(con)
                try:
                    async for _ in iterator:  # noqa
                        raise MyException()
                finally:
                    await iterator.aclose()

            del iterator

        await pool.aclose()

    async def test_pool_handles_asyncgen_finalization(self):
        pool = await self.create_pool(min_size=1, max_size=1)

        async def iterate(con):
            for record in await con.fetchall("SELECT {1, 2, 3}"):
                yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            async with pool.acquire() as con:
                async with con.transaction():
                    agen = iterate(con)
                    try:
                        async for _ in agen:  # noqa
                            raise MyException()
                    finally:
                        await agen.aclose()

        await pool.aclose()

    async def test_pool_close_waits_for_release(self):
        pool = await self.create_pool(min_size=1, max_size=1)

        flag = self.loop.create_future()
        conn_released = False

        async def worker():
            nonlocal conn_released

            async with pool.acquire() as connection:
                async with connection.transaction():
                    flag.set_result(True)
                    await asyncio.sleep(0.1)

            conn_released = True

        self.loop.create_task(worker())

        await flag
        await pool.aclose()
        self.assertTrue(conn_released)

    async def test_pool_close_timeout(self):
        pool = await self.create_pool(min_size=1, max_size=1)

        flag = self.loop.create_future()

        async def worker():
            async with pool.acquire():
                flag.set_result(True)
                await asyncio.sleep(0.5)

        task = self.loop.create_task(worker())

        with self.assertRaises(asyncio.TimeoutError):
            await flag
            await asyncio.wait_for(pool.aclose(), timeout=0.1)

        await task

    async def test_pool_expire_connections(self):
        pool = await self.create_pool(min_size=1, max_size=1)

        con = await pool.acquire()
        try:
            await pool.expire_connections()
        finally:
            await pool.release(con)

        self.assertIsNone(pool._holders[0]._con)
        await pool.aclose()

    async def test_pool_init_race(self):
        pool = self.create_pool(min_size=1, max_size=1)

        t1 = asyncio.ensure_future(pool)
        t2 = asyncio.ensure_future(pool)

        await t1
        with self.assertRaisesRegex(
            edgedb.InterfaceError,
            r"pool is being initialized in another task",
        ):
            await t2

        await pool.aclose()

    async def test_pool_init_and_use_race(self):
        pool = self.create_pool(min_size=1, max_size=1)

        pool_task = asyncio.ensure_future(pool)
        await asyncio.sleep(0)

        with self.assertRaisesRegex(
            edgedb.InterfaceError, r"being initialized, but not yet ready"
        ):

            await pool.fetchone("SELECT 1")

        await pool_task
        await pool.aclose()
