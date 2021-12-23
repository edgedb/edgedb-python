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
import unittest.mock
from concurrent import futures

import edgedb
from edgedb import _testbase as tb
from edgedb import errors
from edgedb import RetryOptions


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
        for tx in self.con.transaction():
            with tx:
                tx.execute('''
                    INSERT test::Counter {
                        name := 'counter1'
                    };
                ''')

    def test_sync_retry_02(self):
        with self.assertRaises(ZeroDivisionError):
            for tx in self.con.transaction():
                with tx:
                    tx.execute('''
                        INSERT test::Counter {
                            name := 'counter_retry_02'
                        };
                    ''')
                    1 / 0
        with self.assertRaises(edgedb.NoDataError):
            self.con.query_required_single('''
                SELECT test::Counter
                FILTER .name = 'counter_retry_02'
            ''')
        self.assertEqual(
            self.con.query_single('''
                SELECT test::Counter
                FILTER .name = 'counter_retry_02'
            '''),
            None
        )

    def test_sync_retry_begin(self):
        patcher = unittest.mock.patch("edgedb.retry.Iteration._start")
        _start = patcher.start()

        def cleanup():
            try:
                patcher.stop()
            except RuntimeError:
                pass

        self.addCleanup(cleanup)

        _start.side_effect = errors.BackendUnavailableError()

        with self.assertRaises(errors.BackendUnavailableError):
            for tx in self.con.transaction():
                with tx:
                    tx.execute('''
                        INSERT test::Counter {
                            name := 'counter_retry_begin'
                        };
                    ''')
        with self.assertRaises(edgedb.NoDataError):
            self.con.query_required_single('''
                SELECT test::Counter
                FILTER .name = 'counter_retry_begin'
            ''')
        self.assertEqual(
            self.con.query_single('''
                SELECT test::Counter
                FILTER .name = 'counter_retry_begin'
            '''),
            None
        )

        def recover_after_first_error(*_, **__):
            patcher.stop()
            raise errors.BackendUnavailableError()

        _start.side_effect = recover_after_first_error
        call_count = _start.call_count

        for tx in self.con.transaction():
            with tx:
                tx.execute('''
                    INSERT test::Counter {
                        name := 'counter_retry_begin'
                    };
                ''')
        self.assertEqual(_start.call_count, call_count + 1)
        self.con.query_single('''
            SELECT test::Counter
            FILTER .name = 'counter_retry_begin'
        ''')

    def test_sync_retry_conflict(self):
        self.execute_conflict('counter2')

    def test_sync_conflict_no_retry(self):
        with self.assertRaises(edgedb.TransactionSerializationError):
            self.execute_conflict(
                'counter3',
                RetryOptions(attempts=1, backoff=edgedb.default_backoff)
            )

    def execute_conflict(self, name='counter2', options=None):
        con_args = self.get_connect_args().copy()
        con_args.update(database=self.get_database_name())
        con2 = edgedb.connect(**con_args)
        self.addCleanup(con2.close)

        barrier = Barrier(2)
        lock = threading.Lock()

        iterations = 0

        def transaction1(con):
            for tx in con.transaction():
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
                    res = tx.query_single('''
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

        with futures.ThreadPoolExecutor(2) as pool:
            f1 = pool.submit(transaction1, con)
            f2 = pool.submit(transaction1, con2)
            results = {f1.result(), f2.result()}

        self.assertEqual(results, {1, 2})
        self.assertEqual(iterations, 3)

    def test_sync_transaction_interface_errors(self):
        with self.assertRaisesRegex(
            AttributeError,
            "'Iteration' object has no attribute 'start'",
        ):
            for tx in self.con.transaction():
                with tx:
                    tx.start()

        with self.assertRaisesRegex(
            AttributeError,
            "'Iteration' object has no attribute 'rollback'",
        ):
            for tx in self.con.transaction():
                with tx:
                    tx.rollback()

        with self.assertRaisesRegex(
            AttributeError,
            "'Iteration' object has no attribute 'start'",
        ):
            for tx in self.con.transaction():
                tx.start()

        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'.*Use `with transaction:`'):
            for tx in self.con.transaction():
                tx.execute("SELECT 123")

        with self.assertRaisesRegex(
            edgedb.InterfaceError,
            r"already in a `with` block",
        ):
            for tx in self.con.transaction():
                with tx:
                    with tx:
                        pass

        with self.assertRaisesRegex(edgedb.InterfaceError, r".*is borrowed.*"):
            for tx in self.con.transaction():
                with tx:
                    self.con.execute("SELECT 123")
