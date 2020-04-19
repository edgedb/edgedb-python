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
import datetime
import decimal
import json
import random
import unittest
import uuid

import edgedb

from edgedb import _taskgroup as tg
from edgedb import _testbase as tb


class TestAsyncFetch(tb.AsyncQueryTestCase):

    ISOLATED_METHODS = False

    SETUP = '''
        CREATE TYPE test::Tmp {
            CREATE REQUIRED PROPERTY tmp -> std::str;
        };

        CREATE SCALAR TYPE MyEnum EXTENDING enum<"A", "B">;
    '''

    TEARDOWN = '''
        DROP TYPE test::Tmp;
    '''

    async def test_async_parse_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.fetchall('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.fetchall('select syntax error')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                await self.con.fetchall('select (')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                await self.con.fetchall_json('select (')

            for _ in range(10):
                self.assertEqual(
                    await self.con.fetchall('select 1;'),
                    edgedb.Set((1,)))

            self.assertFalse(self.con.is_closed())

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
                await self.con.fetchall('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.fetchall('select 1 / 0;')

            for _ in range(10):
                self.assertEqual(
                    await self.con.fetchall('select 1;'),
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
                    await self.con.fetchall(query, i),
                    edgedb.Set([10 // i]))
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.con.fetchall(query, i)

    async def test_async_exec_error_recover_04(self):
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                await self.con.execute(f'select 10 // {i};')
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.con.fetchall(f'select 10 // {i};')

    async def test_async_exec_error_recover_05(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    'cannot accept parameters'):
            await self.con.execute(f'select <int64>$0')
        self.assertEqual(
            await self.con.fetchall('SELECT "HELLO"'),
            ["HELLO"])

    async def test_async_fetch_single_command_01(self):
        r = await self.con.fetchall('''
            CREATE TYPE test::server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetchall('''
            DROP TYPE test::server_fetch_single_command_01;
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetchall('''
            CREATE TYPE test::server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetchall_json('''
            DROP TYPE test::server_fetch_single_command_01;
        ''')
        self.assertEqual(r, '[]')

        r = await self.con.fetchall_json('''
            CREATE TYPE test::server_fetch_single_command_01 {
                CREATE REQUIRED PROPERTY server_fetch_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, '[]')

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'query cannot be executed with fetchone_json\('):
            await self.con.fetchone_json('''
                DROP TYPE test::server_fetch_single_command_01;
            ''')

        r = await self.con.fetchall_json('''
            DROP TYPE test::server_fetch_single_command_01;
        ''')
        self.assertEqual(r, '[]')

    async def test_async_fetch_single_command_02(self):
        r = await self.con.fetchall('''
            SET MODULE default;
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetchall('''
            RESET ALIAS *;
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetchall('''
            SET ALIAS bar AS MODULE std;
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetchall('''
            SET MODULE default;
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetchall('''
            SET ALIAS bar AS MODULE std;
        ''')
        self.assertEqual(r, [])

        r = await self.con.fetchall_json('''
            SET MODULE default;
        ''')
        self.assertEqual(r, '[]')

        r = await self.con.fetchall_json('''
            SET ALIAS foo AS MODULE default;
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
                r = await self.con.fetchall(q)
                self.assertEqual(r, [])

            for q in qs:
                r = await self.con.fetchall_json(q)
                self.assertEqual(r, '[]')

        for q in qs:
            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'cannot be executed with fetchone\(\).*'
                    r'not return'):
                await self.con.fetchone(q)

            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'cannot be executed with fetchone_json\(\).*'
                    r'not return'):
                await self.con.fetchone_json(q)

    async def test_async_fetch_single_command_04(self):
        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            await self.con.fetchall('''
                SELECT 1;
                SET MODULE blah;
            ''')

        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            await self.con.fetchone('''
                SELECT 1;
                SET MODULE blah;
            ''')

        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            await self.con.fetchall_json('''
                SELECT 1;
                SET MODULE blah;
            ''')

    async def test_async_basic_datatypes_01(self):
        for _ in range(10):
            self.assertEqual(
                await self.con.fetchone(
                    'select ()'),
                ())

            self.assertEqual(
                await self.con.fetchall(
                    'select (1,)'),
                edgedb.Set([(1,)]))

            async with self.con.transaction(isolation='repeatable_read'):
                self.assertEqual(
                    await self.con.fetchone(
                        'select <array<int64>>[]'),
                    [])

            self.assertEqual(
                await self.con.fetchall(
                    'select ["a", "b"]'),
                edgedb.Set([["a", "b"]]))

            self.assertEqual(
                await self.con.fetchall('''
                    SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                            (a:=1, world := ("yo", 10))};
                '''),
                edgedb.Set([
                    edgedb.NamedTuple(a=42, world=("hello", 32)),
                    edgedb.NamedTuple(a=1, world=("yo", 10)),
                ]))

            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'fetchone\(\) as it returns a multiset'):
                await self.con.fetchone('SELECT {1, 2}')

            with self.assertRaisesRegex(edgedb.NoDataError, r'\bfetchone\('):
                await self.con.fetchone('SELECT <int64>{}')

    async def test_async_basic_datatypes_02(self):
        self.assertEqual(
            await self.con.fetchall(
                r'''select [b"\x00a", b"b", b'', b'\na']'''),
            edgedb.Set([[b"\x00a", b"b", b'', b'\na']]))

        self.assertEqual(
            await self.con.fetchall(
                r'select <bytes>$0', b'he\x00llo'),
            edgedb.Set([b'he\x00llo']))

    async def test_async_basic_datatypes_03(self):
        for _ in range(10):  # test opportunistic execute
            self.assertEqual(
                await self.con.fetchall_json(
                    'select ()'),
                '[[]]')

            self.assertEqual(
                await self.con.fetchall_json(
                    'select (1,)'),
                '[[1]]')

            self.assertEqual(
                await self.con.fetchall_json(
                    'select <array<int64>>[]'),
                '[[]]')

            self.assertEqual(
                json.loads(
                    await self.con.fetchall_json(
                        'select ["a", "b"]')),
                [["a", "b"]])

            self.assertEqual(
                json.loads(
                    await self.con.fetchone_json(
                        'select ["a", "b"]')),
                ["a", "b"])

            self.assertEqual(
                json.loads(
                    await self.con.fetchall_json('''
                        SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                                (a:=1, world := ("yo", 10))};
                    ''')),
                [
                    {"a": 42, "world": ["hello", 32]},
                    {"a": 1, "world": ["yo", 10]}
                ])

            self.assertEqual(
                json.loads(
                    await self.con.fetchall_json('SELECT {1, 2}')),
                [1, 2])

            self.assertEqual(
                json.loads(await self.con.fetchall_json('SELECT <int64>{}')),
                [])

            with self.assertRaises(edgedb.NoDataError):
                await self.con.fetchone_json('SELECT <int64>{}')

    async def test_async_basic_datatypes_04(self):
        val = await self.con.fetchone(
            '''
                SELECT schema::ObjectType {
                    foo := {
                        [(a := 1, b := 2), (a := 3, b := 4)],
                        [(a := 5, b := 6)],
                        <array <tuple<a: int64, b: int64>>>[],
                    }
                } LIMIT 1
            '''
        )

        self.assertEqual(
            val.foo,
            edgedb.Set([
                edgedb.Array([
                    edgedb.NamedTuple(a=1, b=2),
                    edgedb.NamedTuple(a=3, b=4),
                ]),
                edgedb.Array([
                    edgedb.NamedTuple(a=5, b=6),
                ]),
                edgedb.Array([]),
            ]),
        )

    async def test_async_args_01(self):
        self.assertEqual(
            await self.con.fetchall(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_async_args_02(self):
        self.assertEqual(
            await self.con.fetchall(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_async_args_03(self):
        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$0'):
            await self.con.fetchall('select <int64>$1;')

        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$1'):
            await self.con.fetchall('select <int64>$0 + <int64>$2;')

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'combine positional and named parameters'):
            await self.con.fetchall('select <int64>$0 + <int64>$bar;')

    async def test_async_args_04(self):
        aware_datetime = datetime.datetime.now(datetime.timezone.utc)
        naive_datetime = datetime.datetime.now()

        date = datetime.date.today()
        naive_time = datetime.time(hour=11)
        aware_time = datetime.time(hour=11, tzinfo=datetime.timezone.utc)

        self.assertEqual(
            await self.con.fetchone(
                'select <datetime>$0;',
                aware_datetime),
            aware_datetime)

        self.assertEqual(
            await self.con.fetchone(
                'select <cal::local_datetime>$0;',
                naive_datetime),
            naive_datetime)

        self.assertEqual(
            await self.con.fetchone(
                'select <cal::local_date>$0;',
                date),
            date)

        self.assertEqual(
            await self.con.fetchone(
                'select <cal::local_time>$0;',
                naive_time),
            naive_time)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'a timezone-aware.*expected'):
            await self.con.fetchone(
                'select <datetime>$0;',
                naive_datetime)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'a naive time object.*expected'):
            await self.con.fetchone(
                'select <cal::local_time>$0;',
                aware_time)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'a naive datetime object.*expected'):
            await self.con.fetchone(
                'select <cal::local_datetime>$0;',
                aware_datetime)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'datetime.datetime object was expected'):
            await self.con.fetchone(
                'select <cal::local_datetime>$0;',
                date)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'datetime.datetime object was expected'):
            await self.con.fetchone(
                'select <datetime>$0;',
                date)

    async def test_async_mismatched_args_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} keyword arguments, got {'[bc]', '[bc]'}, "
                r"missed {'a'}, extra {'[bc]', '[bc]'}"):

            await self.con.fetchall("""SELECT <int64>$a;""", b=1, c=2)

    async def test_async_mismatched_args_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} keyword arguments, "
                r"got {'[acd]', '[acd]', '[acd]'}, "
                r"missed {'b'}, extra {'[cd]', '[cd]'}"):

            await self.con.fetchall("""
                SELECT <int64>$a + <int64>$b;
            """, a=1, c=2, d=3)

    async def test_async_mismatched_args_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                "expected {'a'} keyword arguments, got {'b'}, "
                "missed {'a'}, extra {'b'}"):

            await self.con.fetchall("""SELECT <int64>$a;""", b=1)

    async def test_async_mismatched_args_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} keyword arguments, "
                r"got {'a'}, "
                r"missed {'b'}"):

            await self.con.fetchall("""SELECT <int64>$a + <int64>$b;""", a=1)

    async def test_async_mismatched_args_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} keyword arguments, "
                r"got {'[ab]', '[ab]'}, "
                r"extra {'b'}"):

            await self.con.fetchall("""SELECT <int64>$a;""", a=1, b=2)

    async def test_async_args_uuid_pack(self):
        obj = await self.con.fetchone(
            'select schema::Object {id, name} limit 1')

        # Test that the custom UUID that our driver uses can be
        # passed back as a parameter.
        ot = await self.con.fetchone(
            'select schema::Object {name} filter .id=<uuid>$id',
            id=obj.id)
        self.assertEqual(obj, ot)

        # Test that a string UUID is acceptable.
        ot = await self.con.fetchone(
            'select schema::Object {name} filter .id=<uuid>$id',
            id=str(obj.id))
        self.assertEqual(obj, ot)

        # Test that a standard uuid.UUID is acceptable.
        ot = await self.con.fetchone(
            'select schema::Object {name} filter .id=<uuid>$id',
            id=uuid.UUID(bytes=obj.id.bytes))
        self.assertEqual(obj, ot)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'invalid UUID.*length must be'):
            await self.con.fetchall(
                'select schema::Object {name} filter .id=<uuid>$id',
                id='asdasas')

    async def test_async_args_bigint_basic(self):
        testar = [
            0,
            -0,
            +0,
            1,
            -1,
            123,
            -123,
            123789,
            -123789,
            19876,
            -19876,
            19876,
            -19876,
            198761239812739812739801279371289371932,
            -198761182763908473812974620938742386,
            98761239812739812739801279371289371932,
            -98761182763908473812974620938742386,
            8761239812739812739801279371289371932,
            -8761182763908473812974620938742386,
            761239812739812739801279371289371932,
            -761182763908473812974620938742386,
            61239812739812739801279371289371932,
            -61182763908473812974620938742386,
            1239812739812739801279371289371932,
            -1182763908473812974620938742386,
            9812739812739801279371289371932,
            -3908473812974620938742386,
            98127373373209,
            -4620938742386,
            100000000000,
            -100000000000,
            10000000000,
            -10000000000,
            10000000100,
            -10000000010,
            1000000000,
            -1000000000,
            100000000,
            -100000000,
            10000000,
            -10000000,
            1000000,
            -1000000,
            100000,
            -100000,
            10000,
            -10000,
            1000,
            -1000,
            100,
            -100,
            10,
            -10,
        ]

        for _ in range(500):
            num = ''
            for _ in range(random.randint(1, 50)):
                num += random.choice("0123456789")
            testar.append(int(num))

        for _ in range(500):
            num = ''
            for _ in range(random.randint(1, 50)):
                num += random.choice("0000000012")
            testar.append(int(num))

        val = await self.con.fetchone(
            'select <array<bigint>>$arg',
            arg=testar)

        self.assertEqual(testar, val)

    async def test_async_args_bigint_pack(self):
        val = await self.con.fetchone(
            'select <bigint>$arg',
            arg=10)
        self.assertEqual(val, 10)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.fetchall(
                'select <bigint>$arg',
                arg='bad int')

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.fetchall(
                'select <bigint>$arg',
                arg=10.11)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.fetchall(
                'select <bigint>$arg',
                arg=decimal.Decimal('10.0'))

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.fetchall(
                'select <bigint>$arg',
                arg=decimal.Decimal('10.11'))

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.fetchall(
                'select <bigint>$arg',
                arg='10')

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.fetchone(
                'select <bigint>$arg',
                arg=decimal.Decimal('10'))

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            class IntLike:
                def __int__(self):
                    return 10

            await self.con.fetchone(
                'select <bigint>$arg',
                arg=IntLike())

    async def test_async_args_intlike(self):
        class IntLike:
            def __int__(self):
                return 10

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.fetchone(
                'select <int16>$arg',
                arg=IntLike())

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.fetchone(
                'select <int32>$arg',
                arg=IntLike())

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.fetchone(
                'select <int64>$arg',
                arg=IntLike())

    async def test_async_args_decimal(self):
        class IntLike:
            def __int__(self):
                return 10

        val = await self.con.fetchone('select <decimal>$0',
                                      decimal.Decimal("10.0"))
        self.assertEqual(val, 10)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected a Decimal or an int'):
            await self.con.fetchone(
                'select <decimal>$arg',
                arg=IntLike())

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected a Decimal or an int'):
            await self.con.fetchone(
                'select <decimal>$arg',
                arg="10.2")

    async def test_async_wait_cancel_01(self):
        # Test that client protocol handles waits interrupted
        # by closing.
        lock_key = tb.gen_lock_key()

        con2 = await self.connect(database=self.con.dbname)

        await self.con.fetchone(
            'select sys::advisory_lock(<int64>$0)',
            lock_key)

        try:
            async with tg.TaskGroup() as g:

                async def exec_to_fail():
                    with self.assertRaises(ConnectionAbortedError):
                        await con2.fetchall(
                            'select sys::advisory_lock(<int64>$0)', lock_key)

                g.create_task(exec_to_fail())

                await asyncio.sleep(0.1)
                await con2.aclose()

        finally:
            self.assertEqual(
                await self.con.fetchall(
                    'select sys::advisory_unlock(<int64>$0)', lock_key),
                [True])

    async def test_empty_set_unpack(self):
        await self.con.fetchone('''
          select schema::Function {
            name,
            params: {
              kind,
            } limit 0,
            multi setarr := <array<int32>>{}
          }
          filter .name = 'std::str_repeat'
          limit 1
        ''')

    async def test_enum_argument_01(self):
        A = await self.con.fetchone('SELECT <MyEnum><str>$0', 'A')
        self.assertEqual(str(A), 'A')

        with self.assertRaisesRegex(
                edgedb.InvalidValueError, 'invalid input value for enum'):
            async with self.con.transaction():
                await self.con.fetchone(
                    'SELECT <MyEnum><str>$0', 'Oups')

        self.assertEqual(
            await self.con.fetchone('SELECT <MyEnum>$0', 'A'),
            A)

        self.assertEqual(
            await self.con.fetchone('SELECT <MyEnum>$0', A),
            A)

        with self.assertRaisesRegex(
                edgedb.InvalidValueError, 'invalid input value for enum'):
            async with self.con.transaction():
                await self.con.fetchone(
                    'SELECT <MyEnum>$0', 'Oups')

        with self.assertRaisesRegex(
                edgedb.InvalidArgumentError, 'a str or edgedb.EnumValue'):
            await self.con.fetchone('SELECT <MyEnum>$0', 123)

    async def test_json(self):
        self.assertEqual(
            await self.con.fetchall_json('SELECT {"aaa", "bbb"}'),
            '["aaa", "bbb"]')

    @unittest.skip  # not merged into edgedb yet
    async def test_json_elements(self):
        self.assertEqual(
            await self.con._fetchall_json_elements('SELECT {"aaa", "bbb"}'),
            edgedb.Set(['"aaa"', '"bbb"']))
