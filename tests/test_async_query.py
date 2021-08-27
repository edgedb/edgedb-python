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


import datetime
import decimal
import json
import random
import uuid

import asyncio
import edgedb

from edgedb import compat
from edgedb import _taskgroup as tg
from edgedb import _testbase as tb


class TestAsyncQuery(tb.AsyncQueryTestCase):

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

    def setUp(self):
        super().setUp()
        self.con._clear_codecs_cache()

    async def test_async_parse_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.query('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.con.query('select syntax error')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                await self.con.query('select (')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                await self.con.query_json('select (')

            for _ in range(10):
                self.assertEqual(
                    await self.con.query('select 1;'),
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
                await self.con.query('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.con.query('select 1 / 0;')

            for _ in range(10):
                self.assertEqual(
                    await self.con.query('select 1;'),
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
                    await self.con.query(query, i),
                    edgedb.Set([10 // i]))
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.con.query(query, i)

    async def test_async_exec_error_recover_04(self):
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                await self.con.execute(f'select 10 // {i};')
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.con.query(f'select 10 // {i};')

    async def test_async_exec_error_recover_05(self):
        with self.assertRaisesRegex(edgedb.QueryError,
                                    'cannot accept parameters'):
            await self.con.execute(f'select <int64>$0')
        self.assertEqual(
            await self.con.query('SELECT "HELLO"'),
            ["HELLO"])

    async def test_async_query_single_command_01(self):
        r = await self.con.query('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = await self.con.query('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, [])

        r = await self.con.query('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = await self.con.query_json('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, '[]')

        r = await self.con.query_json('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, '[]')

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'query cannot be executed with query_single_json\('):
            await self.con.query_single_json('''
                DROP TYPE test::server_query_single_command_01;
            ''')

        r = await self.con.query_json('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, '[]')

        self.assertTrue(self.con._get_last_status().startswith('DROP'))

    async def test_async_query_single_command_02(self):
        r = await self.con.query('''
            SET MODULE default;
        ''')
        self.assertEqual(r, [])

        r = await self.con.query('''
            RESET ALIAS *;
        ''')
        self.assertEqual(r, [])

        r = await self.con.query('''
            SET ALIAS bar AS MODULE std;
        ''')
        self.assertEqual(r, [])

        r = await self.con.query('''
            SET MODULE default;
        ''')
        self.assertEqual(r, [])

        r = await self.con.query('''
            SET ALIAS bar AS MODULE std;
        ''')
        self.assertEqual(r, [])

        r = await self.con.query_json('''
            SET MODULE default;
        ''')
        self.assertEqual(r, '[]')

        r = await self.con.query_json('''
            SET ALIAS foo AS MODULE default;
        ''')
        self.assertEqual(r, '[]')

    async def test_async_query_single_command_03(self):
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
                r = await self.con.query(q)
                self.assertEqual(r, [])

            for q in qs:
                r = await self.con.query_json(q)
                self.assertEqual(r, '[]')

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'cannot be executed with query_single\(\).*'
                r'not return'):
            await self.con.query_single('START TRANSACTION')

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'cannot be executed with query_single_json\(\).*'
                r'not return'):
            await self.con.query_single_json('START TRANSACTION')

    async def test_async_query_single_command_04(self):
        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            await self.con.query('''
                SELECT 1;
                SET MODULE blah;
            ''')

        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            await self.con.query_single('''
                SELECT 1;
                SET MODULE blah;
            ''')

        with self.assertRaisesRegex(edgedb.ProtocolError,
                                    'expected one statement'):
            await self.con.query_json('''
                SELECT 1;
                SET MODULE blah;
            ''')

    async def test_async_basic_datatypes_01(self):
        for _ in range(10):
            self.assertEqual(
                await self.con.query_single(
                    'select ()'),
                ())

            self.assertEqual(
                await self.con.query(
                    'select (1,)'),
                edgedb.Set([(1,)]))

            async with self.con.transaction(isolation='repeatable_read'):
                self.assertEqual(
                    await self.con.query_single(
                        'select <array<int64>>[]'),
                    [])

            self.assertEqual(
                await self.con.query(
                    'select ["a", "b"]'),
                edgedb.Set([["a", "b"]]))

            self.assertEqual(
                await self.con.query('''
                    SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                            (a:=1, world := ("yo", 10))};
                '''),
                edgedb.Set([
                    edgedb.NamedTuple(a=42, world=("hello", 32)),
                    edgedb.NamedTuple(a=1, world=("yo", 10)),
                ]))

            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'query_single\(\) as it returns a multiset'):
                await self.con.query_single('SELECT {1, 2}')

            with self.assertRaisesRegex(
                    edgedb.NoDataError,
                    r'\bquery_single\('):
                await self.con.query_single('SELECT <int64>{}')

    async def test_async_basic_datatypes_02(self):
        self.assertEqual(
            await self.con.query(
                r'''select [b"\x00a", b"b", b'', b'\na']'''),
            edgedb.Set([[b"\x00a", b"b", b'', b'\na']]))

        self.assertEqual(
            await self.con.query(
                r'select <bytes>$0', b'he\x00llo'),
            edgedb.Set([b'he\x00llo']))

    async def test_async_basic_datatypes_03(self):
        for _ in range(10):  # test opportunistic execute
            self.assertEqual(
                await self.con.query_json(
                    'select ()'),
                '[[]]')

            self.assertEqual(
                await self.con.query_json(
                    'select (1,)'),
                '[[1]]')

            self.assertEqual(
                await self.con.query_json(
                    'select <array<int64>>[]'),
                '[[]]')

            self.assertEqual(
                json.loads(
                    await self.con.query_json(
                        'select ["a", "b"]')),
                [["a", "b"]])

            self.assertEqual(
                json.loads(
                    await self.con.query_single_json(
                        'select ["a", "b"]')),
                ["a", "b"])

            self.assertEqual(
                json.loads(
                    await self.con.query_json('''
                        SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                                (a:=1, world := ("yo", 10))};
                    ''')),
                [
                    {"a": 42, "world": ["hello", 32]},
                    {"a": 1, "world": ["yo", 10]}
                ])

            self.assertEqual(
                json.loads(
                    await self.con.query_json('SELECT {1, 2}')),
                [1, 2])

            self.assertEqual(
                json.loads(await self.con.query_json('SELECT <int64>{}')),
                [])

            with self.assertRaises(edgedb.NoDataError):
                await self.con.query_single_json('SELECT <int64>{}')

    async def test_async_basic_datatypes_04(self):
        val = await self.con.query_single(
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
            await self.con.query(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_async_args_02(self):
        self.assertEqual(
            await self.con.query(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_async_args_03(self):
        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$0'):
            await self.con.query('select <int64>$1;')

        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$1'):
            await self.con.query('select <int64>$0 + <int64>$2;')

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'combine positional and named parameters'):
            await self.con.query('select <int64>$0 + <int64>$bar;')

    async def test_async_args_04(self):
        aware_datetime = datetime.datetime.now(datetime.timezone.utc)
        naive_datetime = datetime.datetime.now()

        date = datetime.date.today()
        naive_time = datetime.time(hour=11)
        aware_time = datetime.time(hour=11, tzinfo=datetime.timezone.utc)

        self.assertEqual(
            await self.con.query_single(
                'select <datetime>$0;',
                aware_datetime),
            aware_datetime)

        self.assertEqual(
            await self.con.query_single(
                'select <cal::local_datetime>$0;',
                naive_datetime),
            naive_datetime)

        self.assertEqual(
            await self.con.query_single(
                'select <cal::local_date>$0;',
                date),
            date)

        self.assertEqual(
            await self.con.query_single(
                'select <cal::local_time>$0;',
                naive_time),
            naive_time)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'a timezone-aware.*expected'):
            await self.con.query_single(
                'select <datetime>$0;',
                naive_datetime)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'a naive time object.*expected'):
            await self.con.query_single(
                'select <cal::local_time>$0;',
                aware_time)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'a naive datetime object.*expected'):
            await self.con.query_single(
                'select <cal::local_datetime>$0;',
                aware_datetime)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'datetime.datetime object was expected'):
            await self.con.query_single(
                'select <cal::local_datetime>$0;',
                date)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'datetime.datetime object was expected'):
            await self.con.query_single(
                'select <datetime>$0;',
                date)

    async def _test_async_args_05(self):  # XXX move to edgedb/edgedb
        # Argument's cardinality must affect the input type ID hash.
        # If the cardinality isn't accounted, the first query's input
        # codec would be cached and then used for the second query,
        # which would make it fail.

        self.assertEqual(
            await self.con.query('select <int32>$a', a=1),
            [1]
        )
        self.assertEqual(
            await self.con.query('select <optional int32>$a', a=None),
            []
        )

    async def _test_async_args_06(self):  # XXX move to edgedb/edgedb
        # A version of test_async_args_05.
        # Also tests that argument cardinality is enforced on the
        # client side too.

        self.assertEqual(
            await self.con.query('select <optional int32>$a', a=1),
            [1]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidArgumentError,
                r'argument \$a is required, but received None'):
            self.assertEqual(
                await self.con.query('select <int32>$a', a=None),
                []
            )

    async def test_async_mismatched_args_01(self):
        # XXX: remove (?:keyword )? once protocol version 0.12 is stable
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} (?:keyword )?arguments, "
                "got {'[bc]', '[bc]'}, "
                r"missed {'a'}, extra {'[bc]', '[bc]'}"):

            await self.con.query("""SELECT <int64>$a;""", b=1, c=2)

    async def test_async_mismatched_args_02(self):
        # XXX: remove (?:keyword )? once protocol version 0.12 is stable
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} (?:keyword )?arguments, "
                r"got {'[acd]', '[acd]', '[acd]'}, "
                r"missed {'b'}, extra {'[cd]', '[cd]'}"):

            await self.con.query("""
                SELECT <int64>$a + <int64>$b;
            """, a=1, c=2, d=3)

    async def test_async_mismatched_args_03(self):
        # XXX: remove (?:keyword )? once protocol version 0.12 is stable
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                "expected {'a'} (?:keyword )?arguments, got {'b'}, "
                "missed {'a'}, extra {'b'}"):

            await self.con.query("""SELECT <int64>$a;""", b=1)

    async def test_async_mismatched_args_04(self):
        # XXX: remove (?:keyword )? once protocol version 0.12 is stable
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} (?:keyword )?arguments, "
                r"got {'a'}, "
                r"missed {'b'}"):

            await self.con.query("""SELECT <int64>$a + <int64>$b;""", a=1)

    async def test_async_mismatched_args_05(self):
        # XXX: remove (?:keyword )? once protocol version 0.12 is stable
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} (?:keyword )?arguments, "
                r"got {'[ab]', '[ab]'}, "
                r"extra {'b'}"):

            await self.con.query("""SELECT <int64>$a;""", a=1, b=2)

    async def test_async_args_uuid_pack(self):
        obj = await self.con.query_single(
            'select schema::Object {id, name} limit 1')

        # Test that the custom UUID that our driver uses can be
        # passed back as a parameter.
        ot = await self.con.query_single(
            'select schema::Object {name} filter .id=<uuid>$id',
            id=obj.id)
        self.assertEqual(obj, ot)

        # Test that a string UUID is acceptable.
        ot = await self.con.query_single(
            'select schema::Object {name} filter .id=<uuid>$id',
            id=str(obj.id))
        self.assertEqual(obj, ot)

        # Test that a standard uuid.UUID is acceptable.
        ot = await self.con.query_single(
            'select schema::Object {name} filter .id=<uuid>$id',
            id=uuid.UUID(bytes=obj.id.bytes))
        self.assertEqual(obj, ot)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'invalid UUID.*length must be'):
            await self.con.query(
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

        val = await self.con.query_single(
            'select <array<bigint>>$arg',
            arg=testar)

        self.assertEqual(testar, val)

    async def test_async_args_bigint_pack(self):
        val = await self.con.query_single(
            'select <bigint>$arg',
            arg=10)
        self.assertEqual(val, 10)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.query(
                'select <bigint>$arg',
                arg='bad int')

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.query(
                'select <bigint>$arg',
                arg=10.11)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.query(
                'select <bigint>$arg',
                arg=decimal.Decimal('10.0'))

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.query(
                'select <bigint>$arg',
                arg=decimal.Decimal('10.11'))

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.query(
                'select <bigint>$arg',
                arg='10')

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.query_single(
                'select <bigint>$arg',
                arg=decimal.Decimal('10'))

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            class IntLike:
                def __int__(self):
                    return 10

            await self.con.query_single(
                'select <bigint>$arg',
                arg=IntLike())

    async def test_async_args_intlike(self):
        class IntLike:
            def __int__(self):
                return 10

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.query_single(
                'select <int16>$arg',
                arg=IntLike())

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.query_single(
                'select <int32>$arg',
                arg=IntLike())

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.con.query_single(
                'select <int64>$arg',
                arg=IntLike())

    async def test_async_args_decimal(self):
        class IntLike:
            def __int__(self):
                return 10

        val = await self.con.query_single('select <decimal>$0',
                                          decimal.Decimal("10.0"))
        self.assertEqual(val, 10)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected a Decimal or an int'):
            await self.con.query_single(
                'select <decimal>$arg',
                arg=IntLike())

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected a Decimal or an int'):
            await self.con.query_single(
                'select <decimal>$arg',
                arg="10.2")

    async def test_async_wait_cancel_01(self):
        underscored_lock = await self.con.query_single("""
            SELECT EXISTS(
                SELECT schema::Function FILTER .name = 'sys::_advisory_lock'
            )
        """)
        if not underscored_lock:
            self.skipTest("No sys::_advisory_lock function")

        # Test that client protocol handles waits interrupted
        # by closing.
        lock_key = tb.gen_lock_key()

        con2 = await self.connect(database=self.con.dbname)

        async with self.con.raw_transaction() as tx:
            self.assertTrue(await tx.query_single(
                'select sys::_advisory_lock(<int64>$0)',
                lock_key))

            try:
                async with tg.TaskGroup() as g:

                    fut = asyncio.Future()

                    async def exec_to_fail():
                        with self.assertRaises((
                            edgedb.ClientConnectionClosedError,
                            ConnectionResetError,
                        )):
                            async with con2.raw_transaction() as tx2:
                                fut.set_result(None)
                                await tx2.query(
                                    'select sys::_advisory_lock(<int64>$0)',
                                    lock_key,
                                )

                    g.create_task(exec_to_fail())

                    await asyncio.wait_for(fut, 1)
                    await asyncio.sleep(0.1)

                    with self.assertRaises(asyncio.TimeoutError):
                        # aclose() will ask the server nicely to disconnect,
                        # but since the server is blocked on the lock,
                        # aclose() will timeout and get cancelled, which,
                        # in turn, will terminate the connection rudely,
                        # and exec_to_fail() will get ConnectionResetError.
                        await compat.wait_for(con2.aclose(), timeout=0.5)

            finally:
                self.assertEqual(
                    await tx.query(
                        'select sys::_advisory_unlock(<int64>$0)', lock_key),
                    [True])

    async def test_empty_set_unpack(self):
        await self.con.query_single('''
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
        A = await self.con.query_single('SELECT <MyEnum><str>$0', 'A')
        self.assertEqual(str(A), 'A')

        with self.assertRaisesRegex(
                edgedb.InvalidValueError, 'invalid input value for enum'):
            async with self.con.raw_transaction() as tx:
                await tx.query_single('SELECT <MyEnum><str>$0', 'Oups')

        self.assertEqual(
            await self.con.query_single('SELECT <MyEnum>$0', 'A'),
            A)

        self.assertEqual(
            await self.con.query_single('SELECT <MyEnum>$0', A),
            A)

        with self.assertRaisesRegex(
                edgedb.InvalidValueError, 'invalid input value for enum'):
            async with self.con.raw_transaction() as tx:
                await tx.query_single('SELECT <MyEnum>$0', 'Oups')

        with self.assertRaisesRegex(
                edgedb.InvalidArgumentError, 'a str or edgedb.EnumValue'):
            await self.con.query_single('SELECT <MyEnum>$0', 123)

    async def test_json(self):
        self.assertEqual(
            await self.con.query_json('SELECT {"aaa", "bbb"}'),
            '["aaa", "bbb"]')

    async def test_json_elements(self):
        self.assertEqual(
            await self.con._fetchall_json_elements('SELECT {"aaa", "bbb"}'),
            edgedb.Set(['"aaa"', '"bbb"']))

    async def test_async_cancel_01(self):
        has_sleep = await self.con.query_single("""
            SELECT EXISTS(
                SELECT schema::Function FILTER .name = 'sys::_sleep'
            )
        """)
        if not has_sleep:
            self.skipTest("No sys::_sleep function")

        con = await self.connect(database=self.con.dbname)

        try:
            self.assertEqual(await con.query_single('SELECT 1'), 1)

            conn_before = con._inner._impl

            with self.assertRaises(asyncio.TimeoutError):
                await compat.wait_for(
                    con.query_single('SELECT sys::_sleep(10)'),
                    timeout=0.1)

            await con.query('SELECT 2')

            conn_after = con._inner._impl
            self.assertIsNot(conn_before, conn_after, "Reconnect expected")
        finally:
            await con.aclose()

    async def test_async_log_message(self):
        msgs = []

        def on_log(con, msg):
            msgs.append(msg)

        self.con.add_log_listener(on_log)
        try:
            await self.con.query(
                'configure system set __internal_restart := true;')
            await asyncio.sleep(0.01)  # allow the loop to call the callback
        finally:
            self.con.remove_log_listener(on_log)

        for msg in msgs:
            if (msg.get_severity_name() == 'NOTICE' and
                    'server restart is required' in str(msg)):
                break
        else:
            raise AssertionError('a notice message was not delivered')
