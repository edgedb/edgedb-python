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


class TestAsyncTx(tb.AsyncQueryTestCase):

    ISOLATED_METHODS = False

    SETUP = '''
        CREATE TYPE test::TransactionTest EXTENDING std::Object {
            CREATE PROPERTY name -> std::str;
        };
    '''

    TEARDOWN = '''
        DROP TYPE test::TransactionTest;
    '''

    async def test_async_transaction_regular_01(self):
        self.assertIsNone(self.con._inner._borrowed_for)
        tr = self.con.raw_transaction()
        self.assertIsNone(self.con._inner._borrowed_for)

        with self.assertRaises(ZeroDivisionError):
            async with tr as with_tr:
                self.assertIs(self.con._inner._borrowed_for, 'transaction')

                with self.assertRaisesRegex(edgedb.InterfaceError,
                                            '.*is borrowed.*'):
                    await self.con.execute('''
                        INSERT test::TransactionTest {
                            name := 'Test Transaction'
                        };
                    ''')

                await with_tr.execute('''
                    INSERT test::TransactionTest {
                        name := 'Test Transaction'
                    };
                ''')

                1 / 0

        self.assertIsNone(self.con._inner._borrowed_for)

        result = await self.con.query('''
            SELECT
                test::TransactionTest
            FILTER
                test::TransactionTest.name = 'Test Transaction';
        ''')

        self.assertEqual(result, [])

    async def test_async_transaction_kinds(self):
        isolations = [
            None,
            edgedb.IsolationLevel.Serializable,
            edgedb.IsolationLevel.RepeatableRead,
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
            async with con.raw_transaction():
                pass
            async for tx in con.retrying_transaction():
                async with tx:
                    pass

    async def test_async_transaction_interface_errors(self):
        self.assertIsNone(self.con._inner._borrowed_for)

        tr = self.con.raw_transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'cannot start; .* already started'):
            async with tr:
                await tr.start()

        self.assertTrue(repr(tr).startswith(
            '<edgedb.AsyncIOTransaction state:rolledback'))

        self.assertIsNone(self.con._inner._borrowed_for)

        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'cannot start; .* already rolled back'):
            async with tr:
                pass

        self.assertIsNone(self.con._inner._borrowed_for)

        tr = self.con.raw_transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'cannot manually commit.*async with'):
            async with tr:
                await tr.commit()

        self.assertIsNone(self.con._inner._borrowed_for)

        tr = self.con.raw_transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'cannot manually rollback.*async with'):
            async with tr:
                await tr.rollback()

        self.assertIsNone(self.con._inner._borrowed_for)

        tr = self.con.raw_transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'cannot enter context:.*async with'):
            async with tr:
                async with tr:
                    pass

        tr = self.con.raw_transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'.*is borrowed.*'):
            async with tr:
                await self.con.query("SELECT 1")

        tr = self.con.raw_transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'.*is borrowed.*'):
            async with tr:
                await self.con.query_single("SELECT 1")

        tr = self.con.raw_transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'.*is borrowed.*'):
            async with tr:
                await self.con.query_json("SELECT 1")

        tr = self.con.raw_transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'.*is borrowed.*'):
            async with tr:
                await self.con.query_single_json("SELECT 1")

        tr = self.con.raw_transaction()
        with self.assertRaisesRegex(edgedb.InterfaceError,
                                    r'.*is borrowed.*'):
            async with tr:
                await self.con.execute("SELECT 1")
