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
import unittest
import uuid

import asyncio
import edgedb

from edgedb import abstract
from edgedb import compat
from edgedb import _taskgroup as tg
from edgedb import _testbase as tb
from edgedb.options import RetryOptions
from edgedb.protocol import protocol


class TestAsyncQuery(tb.AsyncQueryTestCase):

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
        self.client._clear_codecs_cache()

    async def test_async_parse_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.client.query('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.client.query('select syntax error')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                await self.client.query('select (')

            with self.assertRaisesRegex(edgedb.EdgeQLSyntaxError,
                                        'Unexpected end of line'):
                await self.client.query_json('select (')

            for _ in range(10):
                self.assertEqual(
                    await self.client.query('select 1;'),
                    edgedb.Set((1,)))

            self.assertFalse(self.client.connection.is_closed())

    async def test_async_parse_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.client.execute('select syntax error')

            with self.assertRaises(edgedb.EdgeQLSyntaxError):
                await self.client.execute('select syntax error')

            for _ in range(10):
                await self.client.execute('select 1; select 2;'),

    async def test_async_exec_error_recover_01(self):
        for _ in range(2):
            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.client.query('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.client.query('select 1 / 0;')

            for _ in range(10):
                self.assertEqual(
                    await self.client.query('select 1;'),
                    edgedb.Set((1,)))

    async def test_async_exec_error_recover_02(self):
        for _ in range(2):
            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.client.execute('select 1 / 0;')

            with self.assertRaises(edgedb.DivisionByZeroError):
                await self.client.execute('select 1 / 0;')

            for _ in range(10):
                await self.client.execute('select 1;')

    async def test_async_exec_error_recover_03(self):
        query = 'select 10 // <int64>$0;'
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                self.assertEqual(
                    await self.client.query(query, i),
                    edgedb.Set([10 // i]))
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.client.query(query, i)

    async def test_async_exec_error_recover_04(self):
        for i in [1, 2, 0, 3, 1, 0, 1]:
            if i:
                await self.client.execute(f'select 10 // {i};')
            else:
                with self.assertRaises(edgedb.DivisionByZeroError):
                    await self.client.query(f'select 10 // {i};')

    async def test_async_exec_error_recover_05(self):
        with self.assertRaises(edgedb.DivisionByZeroError):
            await self.client.execute(f'select 1 / 0')
        self.assertEqual(
            await self.client.query('SELECT "HELLO"'),
            ["HELLO"])

    async def test_async_query_single_01(self):
        res = await self.client.query_single("SELECT 1")
        self.assertEqual(res, 1)
        res = await self.client.query_single("SELECT <str>{}")
        self.assertEqual(res, None)
        res = await self.client.query_required_single("SELECT 1")
        self.assertEqual(res, 1)

        with self.assertRaises(edgedb.NoDataError):
            await self.client.query_required_single("SELECT <str>{}")

    async def test_async_query_single_command_01(self):
        r = await self.client.query('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = await self.client.query('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, [])

        r = await self.client.query('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, [])

        r = await self.client.query_json('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, '[]')

        r = await self.client.query_json('''
            CREATE TYPE test::server_query_single_command_01 {
                CREATE REQUIRED PROPERTY server_query_single_command_01 ->
                    std::str;
            };
        ''')
        self.assertEqual(r, '[]')

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'query cannot be executed with query_required_single_json\('):
            await self.client.query_required_single_json('''
                DROP TYPE test::server_query_single_command_01;
            ''')

        r = await self.client.query_json('''
            DROP TYPE test::server_query_single_command_01;
        ''')
        self.assertEqual(r, '[]')

        self.assertTrue(
            self.client.connection._get_last_status().startswith('DROP')
        )

    async def test_async_query_no_return(self):
        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'cannot be executed with query_required_single\(\).*'
                r'not return'):
            await self.client.query_required_single('create type Foo456')

        with self.assertRaisesRegex(
                edgedb.InterfaceError,
                r'cannot be executed with query_required_single_json\(\).*'
                r'not return'):
            await self.client.query_required_single_json('create type Bar456')

    async def test_async_basic_datatypes_01(self):
        for _ in range(10):
            self.assertEqual(
                await self.client.query_single(
                    'select ()'),
                ())

            self.assertEqual(
                await self.client.query(
                    'select (1,)'),
                edgedb.Set([(1,)]))

            self.assertEqual(
                await self.client.query(
                    'select ["a", "b"]'),
                edgedb.Set([["a", "b"]]))

            self.assertEqual(
                await self.client.query('''
                    SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                            (a:=1, world := ("yo", 10))};
                '''),
                edgedb.Set([
                    edgedb.NamedTuple(a=42, world=("hello", 32)),
                    edgedb.NamedTuple(a=1, world=("yo", 10)),
                ]))

            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'query_single\(\) as it may return more than one element'
            ):
                await self.client.query_single('SELECT {1, 2}')

            with self.assertRaisesRegex(
                    edgedb.InterfaceError,
                    r'query_required_single\(\) as it may return '
                    r'more than one element'):
                await self.client.query_required_single('SELECT {1, 2}')

            with self.assertRaisesRegex(
                    edgedb.NoDataError,
                    r'\bquery_required_single\('):
                await self.client.query_required_single('SELECT <int64>{}')

    async def test_async_basic_datatypes_02(self):
        self.assertEqual(
            await self.client.query(
                r'''select [b"\x00a", b"b", b'', b'\na']'''),
            edgedb.Set([[b"\x00a", b"b", b'', b'\na']]))

        self.assertEqual(
            await self.client.query(
                r'select <bytes>$0', b'he\x00llo'),
            edgedb.Set([b'he\x00llo']))

    async def test_async_basic_datatypes_03(self):
        for _ in range(10):  # test opportunistic execute
            self.assertEqual(
                await self.client.query_json(
                    'select ()'),
                '[[]]')

            self.assertEqual(
                await self.client.query_json(
                    'select (1,)'),
                '[[1]]')

            self.assertEqual(
                await self.client.query_json(
                    'select <array<int64>>[]'),
                '[[]]')

            self.assertEqual(
                json.loads(
                    await self.client.query_json(
                        'select ["a", "b"]')),
                [["a", "b"]])

            self.assertEqual(
                json.loads(
                    await self.client.query_single_json(
                        'select ["a", "b"]')),
                ["a", "b"])

            self.assertEqual(
                json.loads(
                    await self.client.query_json('''
                        SELECT {(a := 1 + 1 + 40, world := ("hello", 32)),
                                (a:=1, world := ("yo", 10))};
                    ''')),
                [
                    {"a": 42, "world": ["hello", 32]},
                    {"a": 1, "world": ["yo", 10]}
                ])

            self.assertEqual(
                json.loads(
                    await self.client.query_json('SELECT {1, 2}')),
                [1, 2])

            self.assertEqual(
                json.loads(await self.client.query_json('SELECT <int64>{}')),
                [])

            with self.assertRaises(edgedb.NoDataError):
                await self.client.query_required_single_json(
                    'SELECT <int64>{}'
                )

            self.assertEqual(
                json.loads(
                    await self.client.query_single_json('SELECT <int64>{}')
                ),
                None
            )

    async def test_async_basic_datatypes_04(self):
        val = await self.client.query_single(
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
            await self.client.query(
                'select (<array<str>>$foo)[0] ++ (<array<str>>$bar)[0];',
                foo=['aaa'], bar=['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_async_args_02(self):
        self.assertEqual(
            await self.client.query(
                'select (<array<str>>$0)[0] ++ (<array<str>>$1)[0];',
                ['aaa'], ['bbb']),
            edgedb.Set(('aaabbb',)))

    async def test_async_args_03(self):
        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$0'):
            await self.client.query('select <int64>$1;')

        with self.assertRaisesRegex(edgedb.QueryError, r'missing \$1'):
            await self.client.query('select <int64>$0 + <int64>$2;')

        with self.assertRaisesRegex(edgedb.QueryError,
                                    'combine positional and named parameters'):
            await self.client.query('select <int64>$0 + <int64>$bar;')

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    "None is not allowed"):
            await self.client.query(
                "select <array<int64>>$0", [1, None, 3]
            )

    async def test_async_args_04(self):
        aware_datetime = datetime.datetime.now(datetime.timezone.utc)
        naive_datetime = datetime.datetime.now()

        date = datetime.date.today()
        naive_time = datetime.time(hour=11)
        aware_time = datetime.time(hour=11, tzinfo=datetime.timezone.utc)

        self.assertEqual(
            await self.client.query_single(
                'select <datetime>$0;',
                aware_datetime),
            aware_datetime)

        self.assertEqual(
            await self.client.query_single(
                'select <cal::local_datetime>$0;',
                naive_datetime),
            naive_datetime)

        self.assertEqual(
            await self.client.query_single(
                'select <cal::local_date>$0;',
                date),
            date)

        self.assertEqual(
            await self.client.query_single(
                'select <cal::local_time>$0;',
                naive_time),
            naive_time)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'a timezone-aware.*expected'):
            await self.client.query_single(
                'select <datetime>$0;',
                naive_datetime)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'a naive time object.*expected'):
            await self.client.query_single(
                'select <cal::local_time>$0;',
                aware_time)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'a naive datetime object.*expected'):
            await self.client.query_single(
                'select <cal::local_datetime>$0;',
                aware_datetime)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'datetime.datetime object was expected'):
            await self.client.query_single(
                'select <cal::local_datetime>$0;',
                date)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    r'datetime.datetime object was expected'):
            await self.client.query_single(
                'select <datetime>$0;',
                date)

    async def _test_async_args_05(self):  # XXX move to edgedb/edgedb
        # Argument's cardinality must affect the input type ID hash.
        # If the cardinality isn't accounted, the first query's input
        # codec would be cached and then used for the second query,
        # which would make it fail.

        self.assertEqual(
            await self.client.query('select <int32>$a', a=1),
            [1]
        )
        self.assertEqual(
            await self.client.query('select <optional int32>$a', a=None),
            []
        )

    async def _test_async_args_06(self):  # XXX move to edgedb/edgedb
        # A version of test_async_args_05.
        # Also tests that argument cardinality is enforced on the
        # client side too.

        self.assertEqual(
            await self.client.query('select <optional int32>$a', a=1),
            [1]
        )

        with self.assertRaisesRegex(
                edgedb.InvalidArgumentError,
                r'argument \$a is required, but received None'):
            self.assertEqual(
                await self.client.query('select <int32>$a', a=None),
                []
            )

    async def test_async_mismatched_args_01(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} arguments, "
                "got {'[bc]', '[bc]'}, "
                r"missed {'a'}, extra {'[bc]', '[bc]'}"):

            await self.client.query("""SELECT <int64>$a;""", b=1, c=2)

    async def test_async_mismatched_args_02(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} arguments, "
                r"got {'[acd]', '[acd]', '[acd]'}, "
                r"missed {'b'}, extra {'[cd]', '[cd]'}"):

            await self.client.query("""
                SELECT <int64>$a + <int64>$b;
            """, a=1, c=2, d=3)

    async def test_async_mismatched_args_03(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                "expected {'a'} arguments, got {'b'}, "
                "missed {'a'}, extra {'b'}"):

            await self.client.query("""SELECT <int64>$a;""", b=1)

    async def test_async_mismatched_args_04(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'[ab]', '[ab]'} arguments, "
                r"got {'a'}, "
                r"missed {'b'}"):

            await self.client.query("""SELECT <int64>$a + <int64>$b;""", a=1)

    async def test_async_mismatched_args_05(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} arguments, "
                r"got {'[ab]', '[ab]'}, "
                r"extra {'b'}"):

            await self.client.query("""SELECT <int64>$a;""", a=1, b=2)

    async def test_async_mismatched_args_06(self):
        with self.assertRaisesRegex(
                edgedb.QueryArgumentError,
                r"expected {'a'} arguments, "
                r"got nothing, "
                r"missed {'a'}"):

            await self.client.query("""SELECT <int64>$a;""")

    async def test_async_mismatched_args_07(self):
        with self.assertRaisesRegex(
            edgedb.QueryArgumentError,
            "expected no named arguments",
        ):

            await self.client.query("""SELECT 42""", a=1, b=2)

    async def test_async_args_uuid_pack(self):
        obj = await self.client.query_single(
            'select schema::Object {id, name} limit 1')

        # Test that the custom UUID that our driver uses can be
        # passed back as a parameter.
        ot = await self.client.query_single(
            'select schema::Object {name} filter .id=<uuid>$id',
            id=obj.id)
        self.assertEqual(obj.id, ot.id)
        self.assertEqual(obj.name, ot.name)

        # Test that a string UUID is acceptable.
        ot = await self.client.query_single(
            'select schema::Object {name} filter .id=<uuid>$id',
            id=str(obj.id))
        self.assertEqual(obj.id, ot.id)
        self.assertEqual(obj.name, ot.name)

        # Test that a standard uuid.UUID is acceptable.
        ot = await self.client.query_single(
            'select schema::Object {name} filter .id=<uuid>$id',
            id=uuid.UUID(bytes=obj.id.bytes))
        self.assertEqual(obj.id, ot.id)
        self.assertEqual(obj.name, ot.name)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'invalid UUID.*length must be'):
            await self.client.query(
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

        val = await self.client.query_single(
            'select <array<bigint>>$arg',
            arg=testar)

        self.assertEqual(testar, val)

    async def test_async_args_bigint_pack(self):
        val = await self.client.query_single(
            'select <bigint>$arg',
            arg=10)
        self.assertEqual(val, 10)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.client.query(
                'select <bigint>$arg',
                arg='bad int')

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.client.query(
                'select <bigint>$arg',
                arg=10.11)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.client.query(
                'select <bigint>$arg',
                arg=decimal.Decimal('10.0'))

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.client.query(
                'select <bigint>$arg',
                arg=decimal.Decimal('10.11'))

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.client.query(
                'select <bigint>$arg',
                arg='10')

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.client.query_single(
                'select <bigint>$arg',
                arg=decimal.Decimal('10'))

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            class IntLike:
                def __int__(self):
                    return 10

            await self.client.query_single(
                'select <bigint>$arg',
                arg=IntLike())

    async def test_async_args_intlike(self):
        class IntLike:
            def __int__(self):
                return 10

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.client.query_single(
                'select <int16>$arg',
                arg=IntLike())

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.client.query_single(
                'select <int32>$arg',
                arg=IntLike())

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected an int'):
            await self.client.query_single(
                'select <int64>$arg',
                arg=IntLike())

    async def test_async_args_decimal(self):
        class IntLike:
            def __int__(self):
                return 10

        val = await self.client.query_single(
            'select <decimal>$0', decimal.Decimal("10.0")
        )
        self.assertEqual(val, 10)

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected a Decimal or an int'):
            await self.client.query_single(
                'select <decimal>$arg',
                arg=IntLike())

        with self.assertRaisesRegex(edgedb.InvalidArgumentError,
                                    'expected a Decimal or an int'):
            await self.client.query_single(
                'select <decimal>$arg',
                arg="10.2")

    async def test_async_range_01(self):
        has_range = await self.client.query(
            "select schema::ObjectType filter .name = 'schema::Range'")
        if not has_range:
            raise unittest.SkipTest("server has no support for std::range")

        samples = [
            ('range<int64>', [
                edgedb.Range(1, 2, inc_lower=True, inc_upper=False),
                dict(
                    input=edgedb.Range(1, 2, inc_lower=True, inc_upper=True),
                    output=edgedb.Range(1, 3, inc_lower=True, inc_upper=False),
                ),
                edgedb.Range(empty=True),
                dict(
                    input=edgedb.Range(1, 1, inc_lower=True, inc_upper=False),
                    output=edgedb.Range(empty=True),
                ),
                edgedb.Range(lower=None, upper=None),
            ]),
        ]

        for typename, sample_data in samples:
            for sample in sample_data:
                with self.subTest(sample=sample, typname=typename):
                    stmt = f"SELECT <{typename}>$0"
                    if isinstance(sample, dict):
                        inputval = sample['input']
                        outputval = sample['output']
                    else:
                        inputval = outputval = sample

                    result = await self.client.query_single(stmt, inputval)
                    err_msg = (
                        "unexpected result for {} when passing {!r}: "
                        "received {!r}, expected {!r}".format(
                            typename, inputval, result, outputval))

                    self.assertEqual(result, outputval, err_msg)

    async def test_async_range_02(self):
        has_range = await self.client.query(
            "select schema::ObjectType filter .name = 'schema::Range'")
        if not has_range:
            raise unittest.SkipTest("server has no support for std::range")

        result = await self.client.query_single(
            "SELECT <array<range<int32>>>$0",
            [edgedb.Range(1, 2)]
        )
        self.assertEqual([edgedb.Range(1, 2)], result)

    async def test_async_wait_cancel_01(self):
        underscored_lock = await self.client.query_single("""
            SELECT EXISTS(
                SELECT schema::Function FILTER .name = 'sys::_advisory_lock'
            )
        """)
        if not underscored_lock:
            self.skipTest("No sys::_advisory_lock function")

        # Test that client protocol handles waits interrupted
        # by closing.
        lock_key = tb.gen_lock_key()

        client = self.client.with_retry_options(RetryOptions(attempts=1))
        client2 = self.make_test_client(
            database=self.client.dbname
        ).with_retry_options(
            RetryOptions(attempts=1)
        )
        await client2.ensure_connected()

        async for tx in client.transaction():
            async with tx:
                self.assertTrue(await tx.query_single(
                    'select sys::_advisory_lock(<int64>$0)',
                    lock_key))

                try:
                    async with tg.TaskGroup() as g:

                        fut = asyncio.Future()

                        async def exec_to_fail():
                            with self.assertRaises(
                                edgedb.ClientConnectionClosedError,
                            ):
                                async for tx2 in client2.transaction():
                                    async with tx2:
                                        # start the lazy transaction
                                        await tx2.query('SELECT 42;')
                                        fut.set_result(None)

                                        await tx2.query(
                                            'select sys::_advisory_lock(' +
                                            '<int64>$0)',
                                            lock_key,
                                        )

                        g.create_task(exec_to_fail())

                        await asyncio.wait_for(fut, 1)
                        await asyncio.sleep(0.1)

                        with self.assertRaises(asyncio.TimeoutError):
                            # aclose() will ask the server nicely to
                            # disconnect, but since the server is blocked on
                            # the lock, aclose() will timeout and get
                            # cancelled, which, in turn, will terminate the
                            # connection rudely, and exec_to_fail() will get
                            # ConnectionResetError.
                            await compat.wait_for(
                                client2.aclose(), timeout=0.5
                            )

                finally:
                    self.assertEqual(
                        await tx.query(
                            'select sys::_advisory_unlock(<int64>$0)',
                            lock_key),
                        [True])

    async def test_empty_set_unpack(self):
        await self.client.query_single('''
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
        A = await self.client.query_single('SELECT <MyEnum><str>$0', 'A')
        self.assertEqual(str(A), 'A')

        with self.assertRaisesRegex(
                edgedb.InvalidValueError, 'invalid input value for enum'):
            async for tx in self.client.transaction():
                async with tx:
                    await tx.query_single('SELECT <MyEnum><str>$0', 'Oups')

        self.assertEqual(
            await self.client.query_single('SELECT <MyEnum>$0', 'A'),
            A)

        self.assertEqual(
            await self.client.query_single('SELECT <MyEnum>$0', A),
            A)

        with self.assertRaisesRegex(
                edgedb.InvalidValueError, 'invalid input value for enum'):
            async for tx in self.client.transaction():
                async with tx:
                    await tx.query_single('SELECT <MyEnum>$0', 'Oups')

        with self.assertRaisesRegex(
                edgedb.InvalidArgumentError, 'a str or edgedb.EnumValue'):
            await self.client.query_single('SELECT <MyEnum>$0', 123)

    async def test_json(self):
        self.assertEqual(
            await self.client.query_json('SELECT {"aaa", "bbb"}'),
            '["aaa", "bbb"]')

    async def test_json_elements(self):
        result = await self.client.connection.raw_query(
            abstract.QueryContext(
                query=abstract.QueryWithArgs(
                    'SELECT {"aaa", "bbb"}', (), {}
                ),
                cache=self.client._get_query_cache(),
                query_options=abstract.QueryOptions(
                    output_format=protocol.OutputFormat.JSON_ELEMENTS,
                    expect_one=False,
                    required_one=False,
                ),
                retry_options=None,
                state=None,
            )
        )
        self.assertEqual(
            result,
            edgedb.Set(['"aaa"', '"bbb"']))

    async def test_async_cancel_01(self):
        has_sleep = await self.client.query_single("""
            SELECT EXISTS(
                SELECT schema::Function FILTER .name = 'sys::_sleep'
            )
        """)
        if not has_sleep:
            self.skipTest("No sys::_sleep function")

        client = self.make_test_client(database=self.client.dbname)

        try:
            self.assertEqual(await client.query_single('SELECT 1'), 1)

            protocol_before = client._impl._holders[0]._con._protocol

            with self.assertRaises(asyncio.TimeoutError):
                await compat.wait_for(
                    client.query_single('SELECT sys::_sleep(10)'),
                    timeout=0.1)

            await client.query('SELECT 2')

            protocol_after = client._impl._holders[0]._con._protocol
            self.assertIsNot(
                protocol_before, protocol_after, "Reconnect expected"
            )
        finally:
            await client.aclose()

    async def test_async_log_message(self):
        msgs = []

        def on_log(con, msg):
            msgs.append(msg)

        self.client.connection.add_log_listener(on_log)
        try:
            await self.client.query(
                'configure system set __internal_restart := true;')
            await asyncio.sleep(0.01)  # allow the loop to call the callback
        finally:
            self.client.connection.remove_log_listener(on_log)

        for msg in msgs:
            if (msg.get_severity_name() == 'NOTICE' and
                    'server restart is required' in str(msg)):
                break
        else:
            raise AssertionError('a notice message was not delivered')

    async def test_async_banned_transaction(self):
        with self.assertRaisesRegex(
                edgedb.CapabilityError,
                r'cannot execute transaction control commands'):
            await self.client.query('start transaction')

        with self.assertRaisesRegex(
                edgedb.CapabilityError,
                r'cannot execute transaction control commands'):
            await self.client.execute('start transaction')

    async def test_dup_link_prop_name(self):
        obj = await self.client.query_single('''
            CREATE TYPE test::dup_link_prop_name {
                CREATE PROPERTY val -> str;
            };
            CREATE TYPE test::dup_link_prop_name_p {
                CREATE LINK l -> test::dup_link_prop_name {
                    CREATE PROPERTY val -> int32;
                }
            };
            INSERT test::dup_link_prop_name_p {
                l := (INSERT test::dup_link_prop_name {
                    val := "hello",
                    @val := 42,
                })
            };
            SELECT test::dup_link_prop_name_p {
                l: {
                    val,
                    @val
                }
            } LIMIT 1;
        ''')

        self.assertEqual(obj.l.val, "hello")
        self.assertEqual(obj.l["@val"], 42)

        await self.client.execute('''
            DROP TYPE test::dup_link_prop_name_p;
            DROP TYPE test::dup_link_prop_name;
        ''')
