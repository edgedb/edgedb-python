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


class TestSyncFetch(tb.SyncQueryTestCase):

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
                self.con.fetchall('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                self.con.fetchall('select syntax error')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                self.con.fetchall('select (')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                self.con.fetchall_json('select (')

            for _ in range(10):
                self.assertEqual(
                    self.con.fetchall('select 1;'),
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
                self.con.fetchall('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                self.con.fetchall('select 1 / 0;')

            for _ in range(10):
                self.assertEqual(
                    self.con.fetchall('select 1;'),
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
                    self.con.fetchall(query, i),
                    edgedb.Set([10 // i]))
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    self.con.fetchall(query, i)

    def test_sync_exec_error_recover_04(self):
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                self.con.execute(f'select 10 // {i};')
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    self.con.fetchall(f'select 10 // {i};')

    def test_sync_exec_error_recover_05(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    'cannot accept parameters'):
            self.con.execute(f'select <int64>$0')
        self.assertEqual(
            self.con.fetchall('SELECT "HELLO"'),
            ["HELLO"])

    def test_sync_fetch_single_command_01(self):
        r = self.con.fetchall('''
            CREATE TYPE test::server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = self.con.fetchall('''
            DROP TYPE test::server_fetch_single_command_01;
        ''')
        self.assertEqual(r, [])

        r = self.con.fetchall('''
            CREATE TYPE test::server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = self.con.fetchall('''
            DROP TYPE test::server_fetch_single_command_01;
        ''')
        self.assertEqual(r, [])

        r = self.con.fetchall_json('''
            CREATE TYPE test::server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, '[]')

        r = self.con.fetchall_json('''
            DROP TYPE test::server_fetch_single_command_01;
        ''')
        self.assertEqual(r, '[]')

    def test_sync_fetch_single_command_02(self):
        r = self.con.fetchall('''
            SET MODULE default;
        ''')
        self.assertEqual(r, [])

        r = self.con.fetchall('''
            SET ALIAS foo AS MODULE default;
        ''')
        self.assertEqual(r, [])

        r = self.con.fetchall('''
            SET MODULE default;
        ''')
        self.assertEqual(r, [])

        with self.assertRaisesRegex(edgedb.InterfaceError, r'fetchone\(\)'):
            self.con.fetchone('''
                SET ALIAS bar AS MODULE std;
            ''')

        self.con.fetchall('''
            SET ALIAS bar AS MODULE std;
        ''')
        self.assertEqual(r, [])

        r = self.con.fetchall_json('''
            SET MODULE default;
        ''')
        self.assertEqual(r, '[]')

        r = self.con.fetchall_json('''
            SET ALIAS bar AS MODULE std;
        ''')
        self.assertEqual(r, '[]')

    def test_sync_fetch_single_command_03(self):
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
                    r'cannot be executed with fetchone\(\).*'
                    r'not return'):
                self.con.fetchone('START TRANSACTION')

            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'cannot be executed with fetchone_json\(\).*'
                    r'not return'):
                self.con.fetchone_json('START TRANSACTION')

        for _ in range(3):
            for q in qs:
                r = self.con.fetchall(q)
                self.assertEqual(r, [])

            for q in qs:
                r = self.con.fetchall_json(q)
                self.assertEqual(r, '[]')

        for q in qs:
            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'cannot be executed with fetchone\(\).*'
                    r'not return'):
                self.con.fetchone(q)

            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'cannot be executed with fetchone_json\(\).*'
                    r'not return'):
                self.con.fetchone_json(q)

    def test_sync_fetch_single_command_04(self):
        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            self.con.fetchall('''
                SELECT 1;
                SET MODULE blah;
            ''')

        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            self.con.fetchone('''
                SELECT 1;
                SET MODULE blah;
            ''')

        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            self.con.fetchall_json('''
                SELECT 1;
                SET MODULE blah;
            ''')

    def test_sync_basic_datatypes_01(self):
        for _ in range(10):
            self.assertEqual(
                self.con.fetchone(
                    'select ()'),
                ())

            self.assertEqual(
                self.con.fetchall(
                    'select (1,)'),
                edgedb.Set([(1,)]))

            self.assertEqual(
                self.con.fetchone(
                    'select <array<int64>>[]'),
                [])

            self.assertEqual(
                self.con.fetchall(
                    'select ["a", "b"]'),
                edgedb.Set([["a", "b"]]))

            self.assertEqual(
                self.con.fetchall('''
                    SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                            (a:=1, world := ("yo", 10))};
                '''),
                edgedb.Set([
                    edgedb.NamedTuple(a=42, world=("hello", 32)),
                    edgedb.NamedTuple(a=1, world=("yo", 10)),
                ]))

            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'query cannot be executed with fetchone\('):
                self.con.fetchone('SELECT {1, 2}')

            with self.assertRaisesRegex(edgedb.NoDataError,
                                        r'\bfetchone_json\('):
                self.con.fetchone_json('SELECT <int64>{}')

    def test_sync_basic_datatypes_02(self):
        self.assertEqual(
            self.con.fetchall(
                r'''select [b"\x00a", b"b", b'', b'\na']'''),
            edgedb.Set([[b"\x00a", b"b", b'', b'\na']]))

        self.assertEqual(
            self.con.fetchall(
                r'select <bytes>$0', b'he\x00llo'),
            edgedb.Set([b'he\x00llo']))

    def test_sync_basic_datatypes_03(self):
        for _ in range(10):
            self.assertEqual(
                self.con.fetchall_json(
                    'select ()'),
                '[[]]')

            self.assertEqual(
                self.con.fetchall_json(
                    'select (1,)'),
                '[[1]]')

            self.assertEqual(
                self.con.fetchall_json(
                    'select <array<int64>>[]'),
                '[[]]')

            self.assertEqual(
                json.loads(
                    self.con.fetchall_json(
                        'select ["a", "b"]')),
                [["a", "b"]])

            self.assertEqual(
                json.loads(
                    self.con.fetchone_json(
                        'select ["a", "b"]')),
                ["a", "b"])

            self.assertEqual(
                json.loads(
                    self.con.fetchall_json('''
                        SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                                (a:=1, world := ("yo", 10))};
                    ''')),
                [
                    {"a": 42, "world": ["hello", 32]},
                    {"a": 1, "world": ["yo", 10]}
                ])

            self.assertEqual(
                json.loads(
                    self.con.fetchall_json('SELECT {1, 2}')),
                [1, 2])

            self.assertEqual(
                json.loads(self.con.fetchall_json('SELECT <int64>{}')),
                [])

            with self.assertRaises(edgedb.NoDataError):
                self.con.fetchone_json('SELECT <int64>{}')

    def test_sync_args_01(self):
        self.assertEqual(
            self.con.fetchall(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            edgedb.Set(('aaabbb',)))

    def test_sync_args_02(self):
        self.assertEqual(
            self.con.fetchall(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            edgedb.Set(('aaabbb',)))

    def test_sync_args_03(self):
        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$0'):
            self.con.fetchall('select <int64>$1;')

        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$1'):
            self.con.fetchall('select <int64>$0 + <int64>$2;')

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'combine positional and named parameters'):
            self.con.fetchall('select <int64>$0 + <int64>$bar;')

    def test_sync_mismatched_args_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} keyword arguments, got {'[bc]', '[bc]'}, "
                r"missed {'a'}, extra {'[bc]', '[bc]'}"):

            self.con.fetchall("""SELECT <int64>$a;""", b=1, c=2)

    def test_sync_mismatched_args_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} keyword arguments, "
                r"got {'[acd]', '[acd]', '[acd]'}, "
                r"missed {'b'}, extra {'[cd]', '[cd]'}"):

            self.con.fetchall("""
                SELECT <int64>$a + <int64>$b;
            """, a=1, c=2, d=3)

    def test_sync_mismatched_args_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                "expected {'a'} keyword arguments, got {'b'}, "
                "missed {'a'}, extra {'b'}"):

            self.con.fetchall("""SELECT <int64>$a;""", b=1)

    def test_sync_mismatched_args_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} keyword arguments, "
                r"got {'a'}, "
                r"missed {'b'}"):

            self.con.fetchall("""SELECT <int64>$a + <int64>$b;""", a=1)

    def test_sync_mismatched_args_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} keyword arguments, "
                r"got {'[ab]', '[ab]'}, "
                r"extra {'b'}"):

            self.con.fetchall("""SELECT <int64>$a;""", a=1, b=2)
