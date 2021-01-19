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


import threading
from concurrent import futures

import edgedb
from edgedb import _testbase as tb


class Barrier:
    def __init__(self, number):
        self._counter = number
        self._cond = threading.Condition()

    def ready(self):
        if self._counter == 0:
            return
        with self._cond:
            self._counter -= 1
            assert self._counter >= 0, self._counter
            if self._counter == 0:
                self._cond.notify_all()
            else:
                self._cond.wait_for(lambda: self._counter == 0)


class TestSyncRetry(tb.SyncQueryTestCase):

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

    def test_sync_retry_01(self):
        for tx in self.con.retry():
            with tx:
                tx.execute('''
                    INSERT test::Counter {
                        name := 'counter1'
                    };
                ''')

    def test_async_retry_02(self):
        with self.assertRaises(ZeroDivisionError):
            for tx in self.con.retry():
                with tx:
                    tx.execute('''
                        INSERT test::Counter {
                            name := 'counter_retry_02'
                        };
                    ''')
                    1 / 0
        with self.assertRaises(edgedb.NoDataError):
            self.con.query_one('''
                SELECT test::Counter
                FILTER .name = 'counter_retry_02'
            ''')

    def test_sync_retry_conflict(self):
        con_args = self.get_connect_args().copy()
        con_args.update(database=self.get_database_name())
        con2 = edgedb.connect(**con_args)
        self.addCleanup(con2.close)

        barrier = Barrier(2)
        lock = threading.Lock()

        iterations = 0

        def transaction1(con):
            for tx in con.retry():
                nonlocal iterations
                iterations += 1
                with tx:
                    # This magic query makes the test more reliable for some
                    # reason. I guess this is because starting a transaction
                    # in EdgeDB (and/or Postgres) is accomplished somewhat
                    # lazily, i.e. only start transaction on the first query
                    # rather than on the `START TRANSACTION`.
                    tx.query("SELECT 1")

                    # Start both transactions at the same initial data.
                    # One should succeed other should fail and retry.
                    # On next attempt, the latter should succeed
                    barrier.ready()

                    lock.acquire()
                    res = tx.query_one('''
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
                lock.release()
            return res

        with futures.ThreadPoolExecutor(2) as pool:
            f1 = pool.submit(transaction1, self.con)
            f2 = pool.submit(transaction1, con2)
            results = {f1.result(), f2.result()}

        self.assertEqual(results, {1, 2})
        self.assertEqual(iterations, 3)

    def test_sync_transaction_interface_errors(self):
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'.*the transaction is already started'):
            for tx in self.con.retry():
                with tx:
                    tx.start()

        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'.*Use `with transaction:`'):
            for tx in self.con.retry():
                tx.start()
