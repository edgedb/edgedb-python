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


import edgedb

from edgedb import _testbase as tb


class TestSyncTxLegacy(tb.SyncQueryTestCase):

    ISOLATED_METHODS = False

    SETUP = '''
        CREATE TYPE test::TransactionTest EXTENDING std::Object {
            CREATE PROPERTY name -> std::str;
        };
    '''

    TEARDOWN = '''
        DROP TYPE test::TransactionTest;
    '''

    def test_sync_transaction_regular_01(self):
        self.assertIsNone(self.con._inner._top_xact)
        tr = self.con.transaction()
        self.assertIsNone(self.con._inner._top_xact)

        with self.assertRaises(ZeroDivisionError):
            with tr as with_tr:
                self.assertIs(self.con._inner._top_xact, tr)

                # We don't return the transaction object from __aenter__,
                # to make it harder for people to use '.rollback()' and
                # '.commit()' from within an 'with' block.
                self.assertIsNone(with_tr)

                self.con.execute('''
                    INSERT test::TransactionTest {
                        name := 'Test Transaction'
                    };
                ''')

                1 / 0

        self.assertIsNone(self.con._inner._top_xact)

        result = self.con.query('''
            SELECT
                test::TransactionTest
            FILTER
                test::TransactionTest.name = 'Test Transaction';
        ''')

        self.assertEqual(result, [])

    def test_sync_transaction_nested_01(self):
        self.assertIsNone(self.con._inner._top_xact)
        tr = self.con.transaction()
        self.assertIsNone(self.con._inner._top_xact)

        with self.assertRaises(ZeroDivisionError):
            with tr:
                self.assertIs(self.con._inner._top_xact, tr)

                with self.con.transaction():
                    self.assertIs(self.con._inner._top_xact, tr)

                    self.con.execute('''
                        INSERT test::TransactionTest {
                            name := 'TXTEST 1'
                        };
                    ''')

                self.assertIs(self.con._inner._top_xact, tr)

                with self.assertRaises(ZeroDivisionError):
                    in_tr = self.con.transaction()
                    with in_tr:

                        self.assertIs(self.con._inner._top_xact, tr)

                        self.con.query('''
                            INSERT test::TransactionTest {
                                name := 'TXTEST 2'
                            };
                        ''')

                        1 / 0

                recs = self.con.query('''
                    SELECT
                        test::TransactionTest {
                            name
                        }
                    FILTER
                        test::TransactionTest.name LIKE 'TXTEST%';
                ''')

                self.assertEqual(len(recs), 1)
                self.assertEqual(recs[0].name, 'TXTEST 1')
                self.assertIs(self.con._inner._top_xact, tr)

                1 / 0

        self.assertIs(self.con._inner._top_xact, None)

        recs = self.con.query('''
            SELECT
                test::TransactionTest {
                    name
                }
            FILTER
                test::TransactionTest.name LIKE 'TXTEST%';
        ''')

        self.assertEqual(len(recs), 0)

    def test_sync_transaction_nested_02(self):
        with self.con.transaction(isolation='repeatable_read'):
            with self.con.transaction():  # no explicit isolation, OK
                pass

        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'different isolation'):
            with self.con.transaction(isolation='repeatable_read'):
                with self.con.transaction(isolation='serializable'):
                    pass

        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'different read-write'):
            with self.con.transaction():
                with self.con.transaction(readonly=True):
                    pass

        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'different deferrable'):
            with self.con.transaction(deferrable=True):
                with self.con.transaction(deferrable=False):
                    pass

    def test_sync_transaction_interface_errors(self):
        self.assertIsNone(self.con._inner._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'cannot start; .* already started'):
            with tr:
                tr.start()

        self.assertTrue(repr(tr).startswith(
            '<edgedb.Transaction state:rolledback'))

        self.assertIsNone(self.con._inner._top_xact)

        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'cannot start; .* already rolled back'):
            with tr:
                pass

        self.assertIsNone(self.con._inner._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'cannot manually commit.*with'):
            with tr:
                tr.commit()

        self.assertIsNone(self.con._inner._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'cannot manually rollback.*with'):
            with tr:
                tr.rollback()

        self.assertIsNone(self.con._inner._top_xact)

        tr = self.con.transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'cannot enter context:.*with'):
            with tr:
                with tr:
                    pass
