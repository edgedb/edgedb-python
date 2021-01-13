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

    def test_sync_retry_conflict(self):
        con_args = self.get_connect_args().copy()
        con_args.update(database=self.get_database_name())
        con2 = edgedb.connect(**con_args)
        self.addCleanup(con2.close)

        def mark_as_done():
            nonlocal barrier_done
            barrier_done = True

        barrier_done = bool
        barrier = threading.Barrier(2, timeout=10, action=mark_as_done)

        iterations = 0

        def transaction1(con):
            for tx in con.retry():
                nonlocal iterations, barrier_done
                iterations += 1
                with tx:

                    # Start both transactions at the same initial data.
                    # One should succeed other should fail and retry.
                    # On next attempt, the latter should succeed
                    # (and avoid barrier)
                    if not barrier_done:
                        barrier.wait()

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
            return res

        with futures.ThreadPoolExecutor(2) as pool:
            f1 = pool.submit(transaction1, self.con)
            f2 = pool.submit(transaction1, con2)
            results = {f1.result(), f2.result()}

        self.assertEqual(results, {1, 2})
        self.assertEqual(iterations, 3)
