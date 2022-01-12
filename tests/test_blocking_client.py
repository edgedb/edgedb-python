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
import queue
import random
import threading
import time

import edgedb

from edgedb import _testbase as tb
from edgedb import errors
from edgedb import blocking_client


class TestBlockingClient(tb.SyncQueryTestCase):
    def create_client(self, **kwargs):
        conargs = self.get_connect_args().copy()
        conargs["database"] = self.get_database_name()
        conargs["timeout"] = 120
        conargs.update(kwargs)
        conargs.setdefault("on_acquire", None)
        conargs.setdefault("on_release", None)
        conargs.setdefault("on_connect", None)
        conargs.setdefault(
            "connection_class", blocking_client.BlockingIOConnection
        )
        conargs.setdefault("concurrency", None)

        return tb.TestClient(**conargs)

    def test_client_01(self):
        for n in {1, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                client = self.create_client(concurrency=10)

                def worker():
                    self.assertEqual(client.query_single("SELECT 1"), 1)

                tasks = [threading.Thread(target=worker) for _ in range(n)]
                for task in tasks:
                    task.start()
                for task in tasks:
                    task.join()
                client.close()

    def test_client_02(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                with self.create_client(concurrency=5) as client:

                    def worker():
                        self.assertEqual(client.query_single("SELECT 1"), 1)

                    tasks = [threading.Thread(target=worker) for _ in range(n)]
                    for task in tasks:
                        task.start()
                    for task in tasks:
                        task.join()

    def test_client_05(self):
        for n in {1, 3, 5, 10, 20, 100}:
            with self.subTest(tasksnum=n):
                client = self.create_client(concurrency=10)

                def worker():
                    self.assertEqual(client.query('SELECT 1'), [1])
                    self.assertEqual(client.query_single('SELECT 1'), 1)
                    self.assertEqual(client.query_json('SELECT 1'), '[1]')
                    self.assertEqual(client.query_single_json('SELECT 1'), '1')

                tasks = [threading.Thread(target=worker) for _ in range(n)]
                for task in tasks:
                    task.start()
                for task in tasks:
                    task.join()
                client.close()

    def test_client_06(self):
        evt = threading.Event()
        connection = None

        def on_acquire(con):
            nonlocal connection
            connection = con
            evt.set()

        with self.create_client(
            concurrency=5, on_acquire=on_acquire
        ) as client:
            self.assertEqual(client.query('SELECT 1'), [1])

        evt.wait(timeout=1)
        self.assertIsNotNone(connection)

    def test_client_07(self):
        cons = set()

        def on_acquire(con):
            if con not in cons:  # check underlying connection
                raise RuntimeError("on_connect was not called")

        def on_connect(con):
            if con in cons:  # check underlying connection
                raise RuntimeError("on_connect was called more than once")
            cons.add(con)

        def user():
            self.assertEqual(client.query('SELECT 1'), [1])

        with self.create_client(
            concurrency=5, on_connect=on_connect,
            on_acquire=on_acquire,
        ) as client:
            tasks = [threading.Thread(target=user) for _ in range(20)]
            for task in tasks:
                task.start()
            for task in tasks:
                task.join()

        self.assertEqual(len(cons), 5)

    def test_client_transaction(self):
        client = self.create_client(concurrency=1)

        for tx in client.transaction():
            with tx:
                self.assertEqual(tx.query_single("SELECT 7*8"), 56)

        client.close()

    def test_client_options(self):
        client = self.create_client(concurrency=1)

        client.with_transaction_options(
            edgedb.TransactionOptions(readonly=True))
        client.with_retry_options(
            edgedb.RetryOptions(attempts=1, backoff=edgedb.default_backoff))
        for tx in client.transaction():
            with tx:
                self.assertEqual(tx.query_single("SELECT 7*8"), 56)

        client.close()

    def test_client_init_run_until_complete(self):
        client = self.create_client()
        self.assertIsInstance(client, blocking_client.Client)
        client.close()

    def test_client_exception_in_on_acquire_and_on_connect(self):
        class Error(Exception):
            pass

        def callback(con):
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
            client = self.create_client(concurrency=1, on_acquire=callback)
            try:
                with self.assertRaises(Error):
                    client.query("SELECT 42")
                self.assertTrue(last_con.is_closed())

                client.query("SELECT 42")
                self.assertEqual(cons, ["error", last_con])
            finally:
                client.close()

        with self.subTest(method="on_connect"):
            setup_calls = 0
            last_con = None
            cons = []
            client = self.create_client(concurrency=1, on_connect=callback)
            try:
                with self.assertRaises(Error):
                    client.query("SELECT 42")
                self.assertTrue(last_con.is_closed())

                self.assertEqual(client.query_single("select 1"), 1)
                self.assertEqual(cons, ["error", last_con])
            finally:
                client.close()

    def test_client_no_acquire_deadlock(self):
        with self.create_client(
            concurrency=1,
        ) as client:

            has_sleep = client.query_single("""
                SELECT EXISTS(
                    SELECT schema::Function FILTER .name = 'sys::_sleep'
                )
            """)
            if not has_sleep:
                self.skipTest("No sys::_sleep function")

            def sleep_and_release():
                client.execute("SELECT sys::_sleep(1)")

            task = threading.Thread(target=sleep_and_release)
            task.start()
            time.sleep(0.5)

            client.query_single("SELECT 1")
            task.join()

    def test_client_config_persistence(self):
        N = 100
        cons = set()
        num_acquires = 0

        def on_acquire(con):
            nonlocal num_acquires

            self.assertTrue(isinstance(con, MyConnection))
            self.assertEqual(con.foo(), 42)
            cons.add(con)
            num_acquires += 1

        class MyConnection(blocking_client.BlockingIOConnection):
            def foo(self):
                return 42

            def raw_query(self, query_context):
                res = super().raw_query(query_context)
                return res + 1

        def test():
            for tx in client.transaction():
                with tx:
                    self.assertEqual(tx.query_single("SELECT 1"), 2)

        with self.create_client(
            concurrency=10,
            connection_class=MyConnection,
            on_acquire=on_acquire,
        ) as client:

            tasks = [threading.Thread(target=test) for _ in range(N)]
            for task in tasks:
                task.start()
            for task in tasks:
                task.join()

        self.assertEqual(num_acquires, N)
        self.assertEqual(len(cons), 10)

    def test_client_connection_methods(self):
        def test_query(client, q):
            i = random.randint(0, 20)
            time.sleep(random.random() / 100)
            r = client.query("SELECT {}".format(i))
            self.assertEqual(list(r), [i])
            q.put(1)

        def test_query_single(client, q):
            i = random.randint(0, 20)
            time.sleep(random.random() / 100)
            r = client.query_single("SELECT {}".format(i))
            self.assertEqual(r, i)
            q.put(1)

        def test_execute(client, q):
            time.sleep(random.random() / 100)
            client.execute("SELECT {1, 2, 3, 4}")
            q.put(1)

        def run(N, meth):
            with self.create_client(concurrency=10) as client:
                q = queue.Queue()
                coros = [
                    threading.Thread(target=meth, args=(client, q))
                    for _ in range(N)
                ]
                for coro in coros:
                    coro.start()
                for coro in coros:
                    coro.join()
                res = []
                while not q.empty():
                    res.append(q.get_nowait())
                self.assertEqual(res, [1] * N)

        methods = [
            test_query,
            test_query_single,
            test_execute,
        ]

        for method in methods:
            with self.subTest(method=method.__name__):
                run(200, method)

    def test_client_handles_transaction_exit_in_gen_1(self):
        client = self.create_client(concurrency=1)

        def iterate():
            for tx in client.transaction():
                with tx:
                    for record in tx.query("SELECT {1, 2, 3}"):
                        yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            agen = iterate()
            try:
                for _ in agen:  # noqa
                    raise MyException()
            finally:
                agen.close()

        client.close()

    def test_client_handles_transaction_exit_in_gen_2(self):
        client = self.create_client(concurrency=1)

        def iterate():
            for tx in client.transaction():
                with tx:
                    for record in tx.query("SELECT {1, 2, 3}"):
                        yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            iterator = iterate()
            try:
                for _ in iterator:  # noqa
                    raise MyException()
            finally:
                iterator.close()

            del iterator

        client.close()

    def test_client_handles_gen_finalization(self):
        client = self.create_client(concurrency=1)

        def iterate(tx):
            for record in tx.query("SELECT {1, 2, 3}"):
                yield record

        class MyException(Exception):
            pass

        with self.assertRaises(MyException):
            for tx in client.transaction():
                with tx:
                    agen = iterate(tx)
                    try:
                        for _ in agen:  # noqa
                            raise MyException()
                    finally:
                        agen.close()

        client.close()

    def test_client_close_waits_for_release(self):
        client = self.create_client(concurrency=1)

        flag = threading.Event()
        conn_released = False

        def worker():
            nonlocal conn_released

            for tx in client.transaction():
                with tx:
                    tx.query("SELECT 42")
                    flag.set()
                    time.sleep(0.1)

            conn_released = True

        task = threading.Thread(target=worker)
        task.start()

        flag.wait()
        client.close()
        self.assertTrue(conn_released)
        task.join()

    def test_client_close_timeout(self):
        client = self.create_client(concurrency=1)

        flag = threading.Event()

        def worker():
            with self.assertRaises(errors.ClientConnectionClosedError):
                for tx in client.transaction():
                    with tx:
                        tx.query_single("SELECT 42")
                        flag.set()
                        time.sleep(0.5)

        task = threading.Thread(target=worker)
        task.start()

        flag.wait()
        client.close(timeout=0.1)

        task.join()

    def test_client_expire_connections(self):
        client = self.create_client(concurrency=1)

        for tx in client.transaction():
            with tx:
                tx.query("SELECT 42")
                client.expire_connections()

        self.assertIsNone(client._impl._holders[0]._con)
        client.close()

    def test_client_properties(self):
        concurrency = 2

        client = self.create_client(concurrency=concurrency)
        self.assertEqual(client.concurrency, concurrency)
        self.assertEqual(client.concurrency, concurrency)

        for tx in client.transaction():
            with tx:
                tx.query("SELECT 42")
                self.assertEqual(client.free_size, concurrency - 1)

        self.assertEqual(client.free_size, concurrency)

        client.close()

    def _test_connection_broken(self, executor, broken_evt):
        self.loop.call_soon_threadsafe(broken_evt.set)

        with self.assertRaises(errors.ClientConnectionError):
            executor.query_single("SELECT 123")

        self.loop.call_soon_threadsafe(broken_evt.clear)

        self.assertEqual(executor.query_single("SELECT 123"), 123)
        self.loop.call_soon_threadsafe(broken_evt.set)
        with self.assertRaises(errors.ClientConnectionError):
            executor.query_single("SELECT 123")
        self.loop.call_soon_threadsafe(broken_evt.clear)
        self.assertEqual(executor.query_single("SELECT 123"), 123)

        tested = False
        for tx in executor.transaction():
            with tx:
                self.assertEqual(tx.query_single("SELECT 123"), 123)
                if tested:
                    break
                tested = True
                self.loop.call_soon_threadsafe(broken_evt.set)
                try:
                    tx.query_single("SELECT 123")
                except errors.ClientConnectionError:
                    self.loop.call_soon_threadsafe(broken_evt.clear)
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
                        w.close()
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
                    w.close()
                    uw.close()

        server = await asyncio.start_server(
            cb, '127.0.0.1', 0
        )
        port = server.sockets[0].getsockname()[1]
        client = self.create_client(
            host='127.0.0.1', port=port, concurrency=1, wait_until_available=5)
        try:
            await self.loop.run_in_executor(
                None, self._test_connection_broken, client, broken
            )
        finally:
            server.close()
            await server.wait_closed()
            await self.loop.run_in_executor(None, client.close, 5)
            broken.set()
            await done.wait()

    def test_client_suggested_concurrency(self):
        conargs = self.get_connect_args().copy()
        conargs["database"] = self.get_database_name()
        conargs["timeout"] = 120

        client = edgedb.create_client(**conargs)

        self.assertEqual(client.concurrency, 1)

        client.ensure_connected()
        self.assertGreater(client.concurrency, 1)

        client.close()

        client = edgedb.create_client(**conargs, concurrency=5)

        self.assertEqual(client.concurrency, 5)

        client.ensure_connected()
        self.assertEqual(client.concurrency, 5)

        client.close()
