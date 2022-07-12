#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


from edgedb import _testbase as tb


class TestGlobals(tb.AsyncQueryTestCase):

    async def test_globals_01(self):
        db = self.client
        if db.is_proto_lt_1_0:
            self.skipTest("Global is added in EdgeDB 2.0")

        if (
            not await db.query_required_single(
                "select exists "
                "(select schema::Type filter .name = 'cfg::memory')"
            )
        ):
            self.skipTest("feature not implemented")

        await db.execute('''
            CREATE GLOBAL glob -> str;
            CREATE REQUIRED GLOBAL req_glob -> str {
                SET default := '!';
            };
            CREATE GLOBAL def_glob -> str {
                SET default := '!';
            };
        ''')

        async with db.with_globals(glob='test') as gdb:
            x = await gdb.query_single('select global glob')
            self.assertEqual(x, 'test')

            x = await gdb.query_single('select global req_glob')
            self.assertEqual(x, '!')

            x = await gdb.query_single('select global def_glob')
            self.assertEqual(x, '!')

        async with db.with_globals(req_glob='test') as gdb:
            x = await gdb.query_single('select global req_glob')
            self.assertEqual(x, 'test')

        async with db.with_globals(def_glob='test') as gdb:
            x = await gdb.query_single('select global def_glob')
            self.assertEqual(x, 'test')

        # Setting def_glob explicitly to None should override
        async with db.with_globals(def_glob=None) as gdb:
            x = await gdb.query_single('select global def_glob')
            self.assertEqual(x, None)
