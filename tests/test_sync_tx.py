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

import itertools

import edgedb

from edgedb import _testbase as tb
from edgedb import TransactionOptions


class TestSyncTx(tb.SyncQueryTestCase):

    SETUP = '''
        CREATE TYPE test::TransactionTest EXTENDING std::Object {
            CREATE PROPERTY name -> std::str;
        };
    '''

    TEARDOWN = '''
        DROP TYPE test::TransactionTest;
    '''

    def test_sync_transaction_regular_01(self):
        self.assertIsNone(self.con._inner._borrowed_for)
        tr = self.con.transaction()
        self.assertIsNone(self.con._inner._borrowed_for)

        with self.assertRaises(ZeroDivisionError):
            for with_tr in tr:
                with with_tr:
                    with_tr.execute('''
                        INSERT test::TransactionTest {
                            name := 'Test Transaction'
                        };
                    ''')

                    1 / 0

        self.assertIsNone(self.con._inner._borrowed_for)

        result = self.con.query('''
            SELECT
                test::TransactionTest
            FILTER
                test::TransactionTest.name = 'Test Transaction';
        ''')

        self.assertEqual(result, [])

    async def test_sync_transaction_kinds(self):
        isolations = [
            None,
            edgedb.IsolationLevel.Serializable,
        ]
        booleans = [None, True, False]
        all = itertools.product(isolations, booleans, booleans)
        for isolation, readonly, deferrable in all:
            opt = dict(
                isolation=isolation,
                readonly=readonly,
                deferrable=deferrable,
            )
            # skip None
            opt = {k: v for k, v in opt.items() if v is not None}
            con = self.con.with_transaction_options(TransactionOptions(**opt))
            try:
                for tx in con.transaction():
                    with tx:
                        tx.execute(
                            'INSERT test::TransactionTest {name := "test"}')
            except edgedb.TransactionError:
                self.assertTrue(readonly)
            else:
                self.assertFalse(readonly)

            for tx in con.transaction():
                with tx:
                    pass
