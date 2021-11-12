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

from edgedb import compat
from edgedb import _testbase as tb
from edgedb import asyncio_con
from edgedb import asyncio_pool
from edgedb import errors


class TestClient(tb.AsyncQueryTestCase):
    def create_client(self, **kwargs):
        conargs = self.get_connect_args().copy()
        conargs["database"] = self.con.dbname
        conargs["timeout"] = 120
        conargs.update(kwargs)

        return edgedb.create_async_pool(**conargs)

    async def test_client_01(self):
        for n in {1, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                client = await self.create_client(max_size=10)

                async def worker():
                    con = await client.acquire()
                    self.assertEqual(await con.query_single("SELECT 1"), 1)
                    await client.release(con)

                tasks = [worker() for _ in range(n)]
                await asyncio.gather(*tasks)
                await client.aclose()

    async def test_client_02(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                async with self.create_client(max_size=5) as client:

                    async def worker():
                        con = await client.acquire()
                        self.assertEqual(await con.query_single("SELECT 1"), 1)
                        await client.release(con)

                    tasks = [worker() for _ in range(n)]
                    await asyncio.gather(*tasks)

    async def test_client_04(self):
        client = await self.create_client(max_size=1)

        con = await client.acquire()

        # Manual termination of pool connections releases the
        # pool item immediately.
        con.terminate()
        self.assertIsNone(client._impl._holders[0]._con)
        self.assertIsNone(client._impl._holders[0]._in_use)

        con = await client.acquire()
        self.assertEqual(await con.query_single("SELECT 1"), 1)

        await con.aclose()
        self.assertIsNone(client._impl._holders[0]._con)
        self.assertIsNone(client._impl._holders[0]._in_use)
        # Calling release should not hurt.
        await client.release(con)

        client.terminate()

    async def test_client_05(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                client = await self.create_client(max_size=10)

                async def worker():
                    async with client.acquire() as con:
                        self.assertEqual(await con.query_single("SELECT 1"), 1)

                    self.assertEqual(await client.query('SELECT 1'), [1])
                    self.assertEqual(await client.query_single('SELECT 1'), 1)
                    self.assertEqual(
                        await client.query_json('SELECT 1'), '[1]')
                    self.assertEqual(
                        await client.query_single_json('SELECT 1'), '1')

                tasks = [worker() for _ in range(n)]
                await asyncio.gather(*tasks)
                await client.aclose()

    async def test_client_06(self):
        fut = asyncio.Future()

        async def on_acquire(con):
            fut.set_result(con)

        async with self.create_client(
            max_size=5, on_acquire=on_acquire
        ) as client:
            async with client.acquire() as con:
                pass

        self.assertIs(con, await fut)

    async def test_client_07(self):
        cons = set()

        async def on_acquire(con):
            if con._inner._impl not in cons:  # check underlying connection
                raise RuntimeError("on_connect was not called")

        async def on_connect(con):
            if con._inner._impl in cons:  # check underlying connection
                raise RuntimeError("on_connect was called more than once")
            cons.add(con._inner._impl)

        async def user(pool):
            async with pool.acquire() as con:
                if con._inner._impl not in cons:
                    raise RuntimeError("init was not called")

        async with self.create_client(
            max_size=5, on_connect=on_connect,
            on_acquire=on_acquire,
        ) as client:
            users = asyncio.gather(*[user(client) for _ in range(10)])
            await users

        self.assertEqual(len(cons), 5)

    async def test_client_08(self):
        client = await self.create_client(max_size=1)

        try:
            con = await client.acquire()
            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    "does not belong to any connection pool"):
                await client.release(con._inner._impl)
        finally:
            await client.release(con)
            await client.aclose()

    async def test_client_09(self):
        client1 = await self.create_client(max_size=1)

        client2 = await self.create_client(max_size=1)

        try:
            con = await client1.acquire()
            with self.assertRaisesRegex(
                edgedb.InterfaceError, "is not a member"
            ):
                await client2.release(con)
        finally:
            await client1.release(con)

        await client1.aclose()
        await client2.aclose()

    async def test_client_10(self):
        client = await self.create_client(max_size=1)

        con = await client.acquire()
        await client.release(con)
        await client.release(con)

        await client.aclose()

    async def test_client_11(self):
        client = await self.create_client(max_size=1)

        async with client.acquire() as con:
            txn = con.raw_transaction()

        self.assertIn("[released]", repr(con))

        for meth in (
            "query_single",
            "query",
            "execute",
        ):
            with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r"released back to the pool",
            ):
                await getattr(con, meth)("select 1")

        with self.assertRaisesRegex(
            edgedb.InterfaceError,
            r"released back to the pool",
        ):
            await txn.start()

        for meth in ("commit", "rollback"):
            with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r"transaction is not yet started",
            ):
                await getattr(txn, meth)()

        await client.aclose()

    async def test_client_12(self):
        client = await self.create_client(max_size=1)

        async with client.acquire() as con:
            self.assertTrue(isinstance(con, asyncio_con.AsyncIOConnection))
            self.assertFalse(isinstance(con, list))

        await client.aclose()

    async def test_client_13(self):
        client = await self.create_client(max_size=1)

        async with client.acquire() as con:
            self.assertIn("Execute an EdgeQL command", con.execute.__doc__)
            self.assertEqual(con.execute.__name__, "execute")

            self.assertIn(
                str(inspect.signature(con.execute))[1:],
                str(inspect.signature(asyncio_con.AsyncIOConnection.execute)),
            )

        await client.aclose()

    async def test_client_transaction(self):
        client = await self.create_client(max_size=1)

        async for tx in client.transaction():
            async with tx:
                self.assertEqual(await tx.query_single("SELECT 7*8"), 56)

        await client.aclose()

    async def test_client_retry(self):
        client = await self.create_client(max_size=1)

        async for tx in client.transaction():
            async with tx:
                self.assertEqual(await tx.query_single("SELECT 7*8"), 56)

        await client.aclose()

    async def test_client_options(self):
        client = await self.create_client(max_size=1)

        client.with_transaction_options(
            edgedb.TransactionOptions(readonly=True))
        client.with_retry_options(
            edgedb.RetryOptions(attempts=1, backoff=edgedb.default_backoff))
        async for tx in client.transaction():
            async with tx:
                self.assertEqual(await tx.query_single("SELECT 7*8"), 56)

        await client.aclose()

    async def test_client_init_run_until_complete(self):
        client = await self.create_client()
        self.assertIsInstance(client, asyncio_pool.AsyncIOClient)
        await client.aclose()

    async def test_client_exception_in_on_acquire_and_on_connect(self):
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
            async with self.create_client(
                max_size=1, on_acquire=callback
            ) as client:
                with self.assertRaises(Error):
                    await client.acquire()
                self.assertTrue(last_con.is_closed())

                async with client.acquire() as con:
                    self.assertEqual(cons, ["error", con])

        with self.subTest(method="on_connect"):
            setup_calls = 0
            last_con = None
            cons = []
            async with self.create_client(
                max_size=1, on_connect=callback
            ) as client:
                with self.assertRaises(Error):
                    await client.acquire()
                self.assertTrue(last_con.is_closed())

                async with client.acquire() as con:
                    self.assertEqual(await con.query_single("select 1"), 1)
                    self.assertEqual(cons, ["error", con])

    async def test_client_no_acquire_deadlock(self):
        async with self.create_client(
            max_size=1,
        ) as client:

            async with client.acquire() as con:
                has_sleep = await con.query_single("""
                    SELECT EXISTS(
                        SELECT schema::Function FILTER .name = 'sys::_sleep'
                    )
                """)
                if not has_sleep:
                    self.skipTest("No sys::_sleep function")

            async def sleep_and_release():
                async with client.acquire() as con:
                    await con.execute("SELECT sys::_sleep(1)")

            asyncio.ensure_future(sleep_and_release())
            await asyncio.sleep(0.5)

            async with client.acquire() as con:
                await con.query_single("SELECT 1")

    async def test_client_config_persistence(self):
        N = 100
        cons = set()

        class MyConnection(asyncio_pool.PoolConnection):
            async def foo(self):
                return 42

            async def query_single(self, query):
                res = await super().query_single(query)
                return res + 1

        async def test(client):
            async with client.acquire() as con:
                self.assertEqual(await con.query_single("SELECT 1"), 2)
                self.assertEqual(await con.foo(), 42)
                self.assertTrue(isinstance(con, MyConnection))
                cons.add(con)

        async with self.create_client(
            max_size=10,
            connection_class=MyConnection,
        ) as client:

            await asyncio.gather(*[test(client) for _ in range(N)])

        self.assertEqual(len(cons), N)

    async def test_client_connection_methods(self):
        async def test_query(client):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100)
            r = await client.query("SELECT {}".format(i))
            self.assertEqual(list(r), [i])
            return 1

        async def test_query_single(client):
            i = random.randint(0, 20)
            await asyncio.sleep(random.random() / 100)
            r = await client.query_single("SELECT {}".format(i))
            self.assertEqual(r, i)
            return 1

        async def test_execute(client):
            await asyncio.sleep(random.random() / 100)
            await client.execute("SELECT {1, 2, 3, 4}")
            return 1

        async def run(N, meth):
            async with self.create_client(max_size=10) as client:

                coros = [meth(client) for _ in range(N)]
                res = await asyncio.gather(*coros)
                self.assertEqual(res, [1] * N)

        methods = [
            test_query,
            test_query_single,
            test_execute,
        ]

        with tb.silence_asyncio_long_exec_warning():
            for method in methods:
                with self.subTest(method=method.__name__):
                    await run(200, method)

    async def test_client_handles_transaction_exit_in_asyncgen_1(self):
        client = await self.create_client(max_size=1)

        async def iterate(con):
            async for tx in con.transaction():
                async with tx:
                    for record in await tx.query("SELECT {1, 2, 3}"):
                        yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            async with client.acquire() as con:
                agen = iterate(con)
                try:
                    async for _ in agen:  # noqa
                        raise MyException()
                finally:
                    await agen.aclose()

        await client.aclose()

    async def test_client_handles_transaction_exit_in_asyncgen_2(self):
        client = await self.create_client(max_size=1)

        async def iterate(con):
            async for tx in con.transaction():
                async with tx:
                    for record in await tx.query("SELECT {1, 2, 3}"):
                        yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            async with client.acquire() as con:
                iterator = iterate(con)
                try:
                    async for _ in iterator:  # noqa
                        raise MyException()
                finally:
                    await iterator.aclose()

            del iterator

        await client.aclose()

    async def test_client_handles_asyncgen_finalization(self):
        client = await self.create_client(max_size=1)

        async def iterate(tx):
            for record in await tx.query("SELECT {1, 2, 3}"):
                yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            async with client.acquire() as con:
                async for tx in con.transaction():
                    async with tx:
                        agen = iterate(tx)
                        try:
                            async for _ in agen:  # noqa
                                raise MyException()
                        finally:
                            await agen.aclose()

        await client.aclose()

    async def test_client_close_waits_for_release(self):
        client = await self.create_client(max_size=1)

        flag = self.loop.create_future()
        conn_released = False

        async def worker():
            nonlocal conn_released

            async with client.acquire() as connection:
                async with connection.raw_transaction():
                    flag.set_result(True)
                    await asyncio.sleep(0.1)

            conn_released = True

        self.loop.create_task(worker())

        await flag
        await client.aclose()
        self.assertTrue(conn_released)

    async def test_client_close_timeout(self):
        client = await self.create_client(max_size=1)

        flag = self.loop.create_future()

        async def worker():
            async with client.acquire():
                flag.set_result(True)
                await asyncio.sleep(0.5)

        task = self.loop.create_task(worker())

        with self.assertRaises(asyncio.TimeoutError):
            await flag
            await compat.wait_for(client.aclose(), timeout=0.1)

        await task

    async def test_client_expire_connections(self):
        client = await self.create_client(max_size=1)

        con = await client.acquire()
        try:
            await client.expire_connections()
        finally:
            await client.release(con)

        self.assertIsNone(client._impl._holders[0]._con)
        await client.aclose()

    async def test_client_properties(self):
        concurrency = 2

        client = await self.create_client(max_size=concurrency)
        self.assertEqual(client.concurrency, concurrency)
        self.assertEqual(client.concurrency, concurrency)

        async with client.acquire() as _:
            self.assertEqual(client.free_size, concurrency - 1)

        self.assertEqual(client.free_size, concurrency)

        await client.aclose()

    async def _test_connection_broken(self, executor, broken_evt):
        self.assertEqual(await executor.query_single("SELECT 123"), 123)
        broken_evt.set()
        with self.assertRaises(errors.ClientConnectionError):
            await executor.query_single("SELECT 123")
        broken_evt.clear()
        self.assertEqual(await executor.query_single("SELECT 123"), 123)

        tested = False
        async for tx in executor.transaction():
            async with tx:
                self.assertEqual(await tx.query_single("SELECT 123"), 123)
                if tested:
                    break
                tested = True
                broken_evt.set()
                try:
                    await tx.query_single("SELECT 123")
                except errors.ClientConnectionError:
                    broken_evt.clear()
                    raise
                else:
                    self.fail("ConnectionError not raised!")

    async def test_client_connection_broken(self):
        con_args = self.get_connect_args()
        broken = asyncio.Event()
        done = asyncio.Event()

        async def proxy(r: asyncio.StreamReader, w: asyncio.StreamWriter):
            while True:
                reader = self.loop.create_task(r.read(65536))
                waiter = self.loop.create_task(broken.wait())
                await asyncio.wait(
                    [reader, waiter],
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if waiter.done():
                    reader.cancel()
                    w.close()
                    break
                else:
                    waiter.cancel()
                    data = await reader
                    if not data:
                        break
                    w.write(data)

        async def cb(r: asyncio.StreamReader, w: asyncio.StreamWriter):
            ur, uw = await asyncio.open_connection(
                con_args['host'], con_args['port']
            )
            done.clear()
            task = self.loop.create_task(proxy(r, uw))
            try:
                await proxy(ur, w)
            finally:
                try:
                    await task
                finally:
                    done.set()

        server = await asyncio.start_server(
            cb, '127.0.0.1', 0
        )
        port = server.sockets[0].getsockname()[1]
        client = await self.create_client(
            host='127.0.0.1', port=port, max_size=1)
        conargs = self.get_connect_args().copy()
        conargs["database"] = self.con.dbname
        conargs["timeout"] = 120
        conargs["host"] = "127.0.0.1"
        conargs["port"] = port
        conn = await edgedb.async_connect_raw(**conargs)
        try:
            await self._test_connection_broken(conn, broken)
            await self._test_connection_broken(client, broken)
        finally:
            server.close()
            await server.wait_closed()
            await asyncio.wait_for(client.aclose(), 5)
            await asyncio.wait_for(conn.aclose(), 1)
            broken.set()
            await done.wait()

    async def test_client_suggested_concurrency(self):
        conargs = self.get_connect_args().copy()
        conargs["database"] = self.con.dbname
        conargs["timeout"] = 120

        client = edgedb.create_async_client(**conargs)

        self.assertEqual(client.concurrency, 1)

        await client.ensure_connected()
        self.assertGreater(client.concurrency, 1)

        await client.aclose()

        client = edgedb.create_async_client(**conargs, concurrency=5)

        self.assertEqual(client.concurrency, 5)

        await client.ensure_connected()
        self.assertEqual(client.concurrency, 5)

        await client.aclose()

    async def test_client_deprecated_pool(self):
        conargs = self.get_connect_args().copy()
        conargs["database"] = self.con.dbname
        conargs["timeout"] = 120

        pool = await edgedb.create_async_pool(**conargs)

        await pool.aclose()
