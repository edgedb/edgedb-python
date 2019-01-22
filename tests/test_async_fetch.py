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
import edgedb

from edb.common import taskgroup as tg

from edgedb import _testbase as tb


class TestAsyncFetch(tb.AsyncQueryTestCase):

    ISOLATED_METHODS = False

    SETUP = '''
        CREATE TYPE test::Tmp {
            CREATE REQUIRED PROPERTY tmp -> std::str;
        };
    '''

    TEARDOWN = '''
        DROP TYPE test::Tmp;
    '''

    async def test_async_parse_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.fetch('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.fetch('select syntax error')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                await self.con.fetch('select (')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                await self.con.fetch_json('select (')

            for _ in range(10):
                self.assertEqual(
                    await self.con.fetch('select 1;'),
                    edgedb.Set((1,)))

    async def test_async_parse_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.execute('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.execute('select syntax error')

            for _ in range(10):
                await self.con.execute('select 1; select 2;'),

    async def test_async_exec_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.fetch('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.fetch('select 1 / 0;')

            for _ in range(10):
                self.assertEqual(
                    await self.con.fetch('select 1;'),
                    edgedb.Set((1,)))

    async def test_async_exec_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.execute('select 1 / 0;')

            for _ in range(10):
                await self.con.execute('select 1;')

    async def test_async_exec_error_recover_03(self):
        query = 'select 10 // <int64>$0;'
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                self.assertEqual(
                    await self.con.fetch(query, i),
                    edgedb.Set([10 // i]))
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.con.fetch(query, i)

    async def test_async_exec_error_recover_04(self):
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                await self.con.execute(f'select 10 // {i};')
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.con.fetch(f'select 10 // {i};')

    async def test_async_exec_error_recover_05(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    'cannot accept parameters'):
            await self.con.execute(f'select <int64>$0')
        self.assertEqual(
            await self.con.fetch('SELECT "HELLO"'),
            ["HELLO"])

    async def test_async_fetch_single_command_01(self):
        r = await self.con.fetch('''
            CREATE TYPE test::server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetch('''
            DROP TYPE test::server_fetch_single_command_01;
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetch_value('''
            CREATE TYPE test::server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertIsNone(r)

        r = await self.con.fetch_value('''
            DROP TYPE test::server_fetch_single_command_01;
        ''')
        self.assertIsNone(r)

        r = await self.con.fetch_json('''
            CREATE TYPE test::server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, '[]')

        r = await self.con.fetch_json('''
            DROP TYPE test::server_fetch_single_command_01;
        ''')
        self.assertEqual(r, '[]')

    async def test_async_fetch_single_command_02(self):
        r = await self.con.fetch('''
            SET MODULE default;
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetch('''
            SET ALIAS foo AS MODULE default,
                ALIAS bar AS MODULE std;
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetch_value('''
            SET MODULE default;
        ''')
        self.assertIsNone(r)

        r = await self.con.fetch_value('''
            SET ALIAS foo AS MODULE default,
                ALIAS bar AS MODULE std;
        ''')
        self.assertIsNone(r)

        r = await self.con.fetch_json('''
            SET MODULE default;
        ''')
        self.assertEqual(r, '[]')

        r = await self.con.fetch_json('''
            SET ALIAS foo AS MODULE default,
                ALIAS bar AS MODULE std;
        ''')
        self.assertEqual(r, '[]')

    async def test_async_fetch_single_command_03(self):
        qs = [
            'START TRANSACTION',
            'DECLARE SAVEPOINT t0',
            'ROLLBACK TO SAVEPOINT t0',
            'RELEASE SAVEPOINT t0',
            'ROLLBACK',
            'START TRANSACTION',
            'COMMIT',
        ]

        for _ in range(3):
            for q in qs:
                r = await self.con.fetch(q)
                self.assertEqual(r, [])

            for q in qs:
                r = await self.con.fetch_json(q)
                self.assertEqual(r, '[]')

        for q in qs:
            r = await self.con.fetch_value(q)
            self.assertIsNone(r)

    async def test_async_fetch_single_command_04(self):
        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            await self.con.fetch('''
                SELECT 1;
                SET MODULE blah;
            ''')

        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            await self.con.fetch_value('''
                SELECT 1;
                SET MODULE blah;
            ''')

        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            await self.con.fetch_json('''
                SELECT 1;
                SET MODULE blah;
            ''')

    async def test_async_basic_datatypes_01(self):
        for _ in range(10):
            self.assertEqual(
                await self.con.fetch_value(
                    'select ()'),
                ())

            self.assertEqual(
                await self.con.fetch(
                    'select (1,)'),
                edgedb.Set([(1,)]))

            self.assertEqual(
                await self.con.fetch_value(
                    'select <array<int64>>[]'),
                [])

            self.assertEqual(
                await self.con.fetch(
                    'select ["a", "b"]'),
                edgedb.Set([["a", "b"]]))

            self.assertEqual(
                await self.con.fetch('''
                    SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                            (a:=1, world := ("yo", 10))};
                '''),
                edgedb.Set([
                    edgedb.NamedTuple(a=42, world=("hello", 32)),
                    edgedb.NamedTuple(a=1, world=("yo", 10)),
                ]))

            with self.assertRaisesRegex(edgedb.InterfaceError,
                                        'the result set can be a multiset'):
                await self.con.fetch_value('SELECT {1, 2}')

            self.assertIsNone(
                await self.con.fetch_value('SELECT <int64>{}'))

    async def test_async_basic_datatypes_02(self):
        self.assertEqual(
            await self.con.fetch(
                r'''select [b"\x00a", b"b", b'', b'\na']'''),
            edgedb.Set([[b"\x00a", b"b", b'', b'\na']]))

        self.assertEqual(
            await self.con.fetch(
                r'select <bytes>$0', b'he\x00llo'),
            edgedb.Set([b'he\x00llo']))

    async def test_async_basic_datatypes_03(self):
        for _ in range(10):
            self.assertEqual(
                await self.con.fetch_json(
                    'select ()'),
                '[[]]')

            self.assertEqual(
                await self.con.fetch_json(
                    'select (1,)'),
                '[[1]]')

            self.assertEqual(
                await self.con.fetch_json(
                    'select <array<int64>>[]'),
                '[[]]')

            self.assertEqual(
                await self.con.fetch_json(
                    'select ["a", "b"]'),
                '[["a", "b"]]')

            self.assertEqual(
                await self.con.fetch_json('''
                    SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                            (a:=1, world := ("yo", 10))};
                '''),
                '[{"a": 42, "world": ["hello", 32]}, '
                '{"a": 1, "world": ["yo", 10]}]')

            self.assertEqual(
                await self.con.fetch_json('SELECT {1, 2}'),
                '[1, 2]')

            self.assertEqual(
                await self.con.fetch_json('SELECT <int64>{}'),
                '[]')

    async def test_async_args_01(self):
        self.assertEqual(
            await self.con.fetch(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_async_args_02(self):
        self.assertEqual(
            await self.con.fetch(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_async_args_03(self):
        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$0'):
            await self.con.fetch('select <int64>$1;')

        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$1'):
            await self.con.fetch('select <int64>$0 + <int64>$2;')

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'combine positional and named parameters'):
            await self.con.fetch('select <int64>$0 + <int64>$bar;')

    async def test_async_wait_cancel_01(self):
        # Test that client protocol handles waits interrupted
        # by closing.
        lock_key = tb.gen_lock_key()

        con2 = await self.cluster.connect(user='edgedb',
                                          database=self.con.dbname)

        await self.con.fetch('select sys::advisory_lock(<int64>$0)', lock_key)

        try:
            async with tg.TaskGroup() as g:

                async def exec_to_fail():
                    with self.assertRaises(ConnectionAbortedError):
                        await con2.fetch(
                            'select sys::advisory_lock(<int64>$0)', lock_key)

                g.create_task(exec_to_fail())

                await asyncio.sleep(0.1)
                await con2.close()

        finally:
            self.assertEqual(
                await self.con.fetch(
                    'select sys::advisory_unlock(<int64>$0)', lock_key),
                [True])
