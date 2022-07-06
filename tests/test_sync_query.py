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

import json

import edgedb

from edgedb import _testbase as tb


class TestSyncQuery(tb.SyncQueryTestCase):

    SETUP = '''
        CREATE TYPE test::Tmp {
            CREATE REQUIRED PROPERTY tmp -> std::str;
        };
    '''

    TEARDOWN = '''
        DROP TYPE test::Tmp;
    '''

    def test_sync_parse_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                self.client.query('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                self.client.query('select syntax error')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                self.client.query('select (')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                self.client.query_json('select (')

            for _ in range(10):
                self.assertEqual(
                    self.client.query('select 1;'),
                    edgedb.Set((1,)))

            self.assertFalse(self.client.connection.is_closed())

    def test_sync_parse_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                self.client.execute('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                self.client.execute('select syntax error')

            for _ in range(10):
                self.client.execute('select 1; select 2;'),

    def test_sync_exec_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.DivisionByZeroError):
                self.client.query('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                self.client.query('select 1 / 0;')

            for _ in range(10):
                self.assertEqual(
                    self.client.query('select 1;'),
                    edgedb.Set((1,)))

    def test_sync_exec_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(edgedb.DivisionByZeroError):
                self.client.execute('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                self.client.execute('select 1 / 0;')

            for _ in range(10):
                self.client.execute('select 1;')

    def test_sync_exec_error_recover_03(self):
        query = 'select 10 // <int64>$0;'
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                self.assertEqual(
                    self.client.query(query, i),
                    edgedb.Set([10 // i]))
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    self.client.query(query, i)

    def test_sync_exec_error_recover_04(self):
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                self.client.execute(f'select 10 // {i};')
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    self.client.query(f'select 10 // {i};')

    def test_sync_exec_error_recover_05(self):
        with self.assertRaises(edgedb.DivisionByZeroError):
            self.client.execute(f'select 1 / 0')
        self.assertEqual(
            self.client.query('SELECT "HELLO"'),
            ["HELLO"])

    async def test_async_query_single_01(self):
        res = self.client.query_single("SELECT 1")
        self.assertEqual(res, 1)
        res = self.client.query_single("SELECT <str>{}")
        self.assertEqual(res, None)
        res = self.client.query_required_single("SELECT 1")
        self.assertEqual(res, 1)

        with self.assertRaises(edgedb.NoDataError):
            self.client.query_required_single("SELECT <str>{}")

    def test_sync_query_single_command_01(self):
        r = self.client.query('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = self.client.query('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, [])

        r = self.client.query('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = self.client.query('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, [])

        r = self.client.query_json('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, '[]')

        r = self.client.query_json('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, '[]')

        self.assertTrue(
            self.client.connection._get_last_status().startswith('DROP')
        )

    def test_sync_query_no_return(self):
        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'cannot be executed with query_required_single\(\).*'
                r'not return'):
            self.client.query_required_single('create type Foo123')

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'cannot be executed with query_required_single_json\(\).*'
                r'not return'):
            self.client.query_required_single_json('create type Bar123')

    def test_sync_basic_datatypes_01(self):
        for _ in range(10):
            self.assertEqual(
                self.client.query_single(
                    'select ()'),
                ())

            self.assertEqual(
                self.client.query(
                    'select (1,)'),
                edgedb.Set([(1,)]))

            self.assertEqual(
                self.client.query_single(
                    'select <array<int64>>[]'),
                [])

            self.assertEqual(
                self.client.query(
                    'select ["a", "b"]'),
                edgedb.Set([["a", "b"]]))

            self.assertEqual(
                self.client.query('''
                    SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                            (a:=1, world := ("yo", 10))};
                '''),
                edgedb.Set([
                    edgedb.NamedTuple(a=42, world=("hello", 32)),
                    edgedb.NamedTuple(a=1, world=("yo", 10)),
                ]))

            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'query cannot be executed with query_single\('):
                self.client.query_single('SELECT {1, 2}')

            with self.assertRaisesRegex(edgedb.NoDataError,
                                        r'\bquery_required_single_json\('):
                self.client.query_required_single_json('SELECT <int64>{}')

    def test_sync_basic_datatypes_02(self):
        self.assertEqual(
            self.client.query(
                r'''select [b"\x00a", b"b", b'', b'\na']'''),
            edgedb.Set([[b"\x00a", b"b", b'', b'\na']]))

        self.assertEqual(
            self.client.query(
                r'select <bytes>$0', b'he\x00llo'),
            edgedb.Set([b'he\x00llo']))

    def test_sync_basic_datatypes_03(self):
        for _ in range(10):
            self.assertEqual(
                self.client.query_json(
                    'select ()'),
                '[[]]')

            self.assertEqual(
                self.client.query_json(
                    'select (1,)'),
                '[[1]]')

            self.assertEqual(
                self.client.query_json(
                    'select <array<int64>>[]'),
                '[[]]')

            self.assertEqual(
                json.loads(
                    self.client.query_json(
                        'select ["a", "b"]')),
                [["a", "b"]])

            self.assertEqual(
                json.loads(
                    self.client.query_single_json(
                        'select ["a", "b"]')),
                ["a", "b"])

            self.assertEqual(
                json.loads(
                    self.client.query_json('''
                        SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                                (a:=1, world := ("yo", 10))};
                    ''')),
                [
                    {"a": 42, "world": ["hello", 32]},
                    {"a": 1, "world": ["yo", 10]}
                ])

            self.assertEqual(
                json.loads(
                    self.client.query_json('SELECT {1, 2}')),
                [1, 2])

            self.assertEqual(
                json.loads(self.client.query_json('SELECT <int64>{}')),
                [])

            with self.assertRaises(edgedb.NoDataError):
                self.client.query_required_single_json('SELECT <int64>{}')

            self.assertEqual(
                json.loads(self.client.query_single_json('SELECT <int64>{}')),
                None
            )

    def test_sync_args_01(self):
        self.assertEqual(
            self.client.query(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            edgedb.Set(('aaabbb',)))

    def test_sync_args_02(self):
        self.assertEqual(
            self.client.query(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            edgedb.Set(('aaabbb',)))

    def test_sync_args_03(self):
        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$0'):
            self.client.query('select <int64>$1;')

        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$1'):
            self.client.query('select <int64>$0 + <int64>$2;')

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'combine positional and named parameters'):
            self.client.query('select <int64>$0 + <int64>$bar;')

    def test_sync_mismatched_args_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} arguments, "
                "got {'[bc]', '[bc]'}, "
                r"missed {'a'}, extra {'[bc]', '[bc]'}"):

            self.client.query("""SELECT <int64>$a;""", b=1, c=2)

    def test_sync_mismatched_args_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} arguments, "
                r"got {'[acd]', '[acd]', '[acd]'}, "
                r"missed {'b'}, extra {'[cd]', '[cd]'}"):

            self.client.query("""
                SELECT <int64>$a + <int64>$b;
            """, a=1, c=2, d=3)

    def test_sync_mismatched_args_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                "expected {'a'} arguments, got {'b'}, "
                "missed {'a'}, extra {'b'}"):

            self.client.query("""SELECT <int64>$a;""", b=1)

    def test_sync_mismatched_args_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} arguments, "
                r"got {'a'}, "
                r"missed {'b'}"):

            self.client.query("""SELECT <int64>$a + <int64>$b;""", a=1)

    def test_sync_mismatched_args_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} arguments, "
                r"got {'[ab]', '[ab]'}, "
                r"extra {'b'}"):

            self.client.query("""SELECT <int64>$a;""", a=1, b=2)

    def test_sync_mismatched_args_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} arguments, "
                r"got nothing, "
                r"missed {'a'}"):

            self.client.query("""SELECT <int64>$a;""")

    async def test_sync_log_message(self):
        msgs = []

        def on_log(con, msg):
            msgs.append(msg)

        self.client.ensure_connected()
        con = self.client.connection
        con.add_log_listener(on_log)
        try:
            self.client.query(
                'configure system set __internal_restart := true;'
            )
            # self.con.query('SELECT 1')
        finally:
            con.remove_log_listener(on_log)

        for msg in msgs:
            if (msg.get_severity_name() == 'NOTICE' and
                    'server restart is required' in str(msg)):
                break
        else:
            raise AssertionError('a notice message was not delivered')
