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

    ISOLATED_METHODS = False

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
                self.con.query('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                self.con.query('select syntax error')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                self.con.query('select (')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                self.con.query_json('select (')

            for _ in range(10):
                self.assertEqual(
                    self.con.query('select 1;'),
                    edgedb.Set((1,)))

            self.assertFalse(self.con.is_closed())

    def test_sync_parse_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                self.con.execute('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                self.con.execute('select syntax error')

            for _ in range(10):
                self.con.execute('select 1; select 2;'),

    def test_sync_exec_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.DivisionByZeroError):
                self.con.query('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                self.con.query('select 1 / 0;')

            for _ in range(10):
                self.assertEqual(
                    self.con.query('select 1;'),
                    edgedb.Set((1,)))

    def test_sync_exec_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(edgedb.DivisionByZeroError):
                self.con.execute('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                self.con.execute('select 1 / 0;')

            for _ in range(10):
                self.con.execute('select 1;')

    def test_sync_exec_error_recover_03(self):
        query = 'select 10 // <int64>$0;'
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                self.assertEqual(
                    self.con.query(query, i),
                    edgedb.Set([10 // i]))
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    self.con.query(query, i)

    def test_sync_exec_error_recover_04(self):
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                self.con.execute(f'select 10 // {i};')
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    self.con.query(f'select 10 // {i};')

    def test_sync_exec_error_recover_05(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    'cannot accept parameters'):
            self.con.execute(f'select <int64>$0')
        self.assertEqual(
            self.con.query('SELECT "HELLO"'),
            ["HELLO"])

    def test_sync_query_single_command_01(self):
        r = self.con.query('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = self.con.query('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, [])

        r = self.con.query('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = self.con.query('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, [])

        r = self.con.query_json('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, '[]')

        r = self.con.query_json('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, '[]')

        self.assertTrue(self.con._get_last_status().startswith('DROP'))

    def test_sync_query_single_command_02(self):
        r = self.con.query('''
            SET MODULE default;
        ''')
        self.assertEqual(r, [])

        r = self.con.query('''
            SET ALIAS foo AS MODULE default;
        ''')
        self.assertEqual(r, [])

        r = self.con.query('''
            SET MODULE default;
        ''')
        self.assertEqual(r, [])

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'query_single\(\)'):
            self.con.query_single('''
                SET ALIAS bar AS MODULE std;
            ''')

        self.con.query('''
            SET ALIAS bar AS MODULE std;
        ''')
        self.assertEqual(r, [])

        r = self.con.query_json('''
            SET MODULE default;
        ''')
        self.assertEqual(r, '[]')

        r = self.con.query_json('''
            SET ALIAS bar AS MODULE std;
        ''')
        self.assertEqual(r, '[]')

    def test_sync_query_single_command_03(self):
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
            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'cannot be executed with query_single\(\).*'
                    r'not return'):
                self.con.query_single('START TRANSACTION')

            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'cannot be executed with query_single_json\(\).*'
                    r'not return'):
                self.con.query_single_json('START TRANSACTION')

        for _ in range(3):
            for q in qs:
                r = self.con.query(q)
                self.assertEqual(r, [])

            for q in qs:
                r = self.con.query_json(q)
                self.assertEqual(r, '[]')

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'cannot be executed with query_single\(\).*'
                r'not return'):
            self.con.query_single('START TRANSACTION')

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'cannot be executed with query_single_json\(\).*'
                r'not return'):
            self.con.query_single_json('START TRANSACTION')

    def test_sync_query_single_command_04(self):
        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            self.con.query('''
                SELECT 1;
                SET MODULE blah;
            ''')

        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            self.con.query_single('''
                SELECT 1;
                SET MODULE blah;
            ''')

        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            self.con.query_json('''
                SELECT 1;
                SET MODULE blah;
            ''')

    def test_sync_basic_datatypes_01(self):
        for _ in range(10):
            self.assertEqual(
                self.con.query_single(
                    'select ()'),
                ())

            self.assertEqual(
                self.con.query(
                    'select (1,)'),
                edgedb.Set([(1,)]))

            self.assertEqual(
                self.con.query_single(
                    'select <array<int64>>[]'),
                [])

            self.assertEqual(
                self.con.query(
                    'select ["a", "b"]'),
                edgedb.Set([["a", "b"]]))

            self.assertEqual(
                self.con.query('''
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
                self.con.query_single('SELECT {1, 2}')

            with self.assertRaisesRegex(edgedb.NoDataError,
                                        r'\bquery_single_json\('):
                self.con.query_single_json('SELECT <int64>{}')

    def test_sync_basic_datatypes_02(self):
        self.assertEqual(
            self.con.query(
                r'''select [b"\x00a", b"b", b'', b'\na']'''),
            edgedb.Set([[b"\x00a", b"b", b'', b'\na']]))

        self.assertEqual(
            self.con.query(
                r'select <bytes>$0', b'he\x00llo'),
            edgedb.Set([b'he\x00llo']))

    def test_sync_basic_datatypes_03(self):
        for _ in range(10):
            self.assertEqual(
                self.con.query_json(
                    'select ()'),
                '[[]]')

            self.assertEqual(
                self.con.query_json(
                    'select (1,)'),
                '[[1]]')

            self.assertEqual(
                self.con.query_json(
                    'select <array<int64>>[]'),
                '[[]]')

            self.assertEqual(
                json.loads(
                    self.con.query_json(
                        'select ["a", "b"]')),
                [["a", "b"]])

            self.assertEqual(
                json.loads(
                    self.con.query_single_json(
                        'select ["a", "b"]')),
                ["a", "b"])

            self.assertEqual(
                json.loads(
                    self.con.query_json('''
                        SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                                (a:=1, world := ("yo", 10))};
                    ''')),
                [
                    {"a": 42, "world": ["hello", 32]},
                    {"a": 1, "world": ["yo", 10]}
                ])

            self.assertEqual(
                json.loads(
                    self.con.query_json('SELECT {1, 2}')),
                [1, 2])

            self.assertEqual(
                json.loads(self.con.query_json('SELECT <int64>{}')),
                [])

            with self.assertRaises(edgedb.NoDataError):
                self.con.query_single_json('SELECT <int64>{}')

    def test_sync_args_01(self):
        self.assertEqual(
            self.con.query(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            edgedb.Set(('aaabbb',)))

    def test_sync_args_02(self):
        self.assertEqual(
            self.con.query(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            edgedb.Set(('aaabbb',)))

    def test_sync_args_03(self):
        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$0'):
            self.con.query('select <int64>$1;')

        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$1'):
            self.con.query('select <int64>$0 + <int64>$2;')

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'combine positional and named parameters'):
            self.con.query('select <int64>$0 + <int64>$bar;')

    def test_sync_mismatched_args_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} keyword arguments, got {'[bc]', '[bc]'}, "
                r"missed {'a'}, extra {'[bc]', '[bc]'}"):

            self.con.query("""SELECT <int64>$a;""", b=1, c=2)

    def test_sync_mismatched_args_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} keyword arguments, "
                r"got {'[acd]', '[acd]', '[acd]'}, "
                r"missed {'b'}, extra {'[cd]', '[cd]'}"):

            self.con.query("""
                SELECT <int64>$a + <int64>$b;
            """, a=1, c=2, d=3)

    def test_sync_mismatched_args_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                "expected {'a'} keyword arguments, got {'b'}, "
                "missed {'a'}, extra {'b'}"):

            self.con.query("""SELECT <int64>$a;""", b=1)

    def test_sync_mismatched_args_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} keyword arguments, "
                r"got {'a'}, "
                r"missed {'b'}"):

            self.con.query("""SELECT <int64>$a + <int64>$b;""", a=1)

    def test_sync_mismatched_args_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} keyword arguments, "
                r"got {'[ab]', '[ab]'}, "
                r"extra {'b'}"):

            self.con.query("""SELECT <int64>$a;""", a=1, b=2)

    async def test_sync_log_message(self):
        msgs = []

        def on_log(con, msg):
            msgs.append(msg)

        self.con.add_log_listener(on_log)
        try:
            self.con.query('configure system set __internal_restart := true;')
            # self.con.query('SELECT 1')
        finally:
            self.con.remove_log_listener(on_log)

        for msg in msgs:
            if (msg.get_severity_name() == 'NOTICE' and
                    'server restart is required' in str(msg)):
                break
        else:
            raise AssertionError('a notice message was not delivered')
