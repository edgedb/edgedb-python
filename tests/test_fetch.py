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


import os

from edgedb import _testbase as tb


class TestFetch(tb.QueryTestCase):

    SCHEMA = os.path.join(os.path.dirname(__file__), 'schemas',
                          'issues.eschema')

    SETUP = os.path.join(os.path.dirname(__file__), 'schemas',
                         'issues_setup.eql')

    async def test_fetch_1(self):
        r = await self.con.fetch(r'''
            WITH MODULE test
            SELECT
                Issue.watchers.<owner[IS Issue] {
                    name
                } ORDER BY .name;
        ''')
        self.assertEqual(
            repr(r),
            "Set{Object{name := 'Improve EdgeDB repl output rendering.'}, "
            "Object{name := 'Regression.'}, Object{name := 'Release EdgeDB'}, "
            "Object{name := 'Repl tweak.'}}")

    async def test_fetch_2(self):
        r = await self.con.fetch(r'''
            SELECT ();
        ''')
        self.assertEqual(repr(r), "Set{()}")

    async def test_fetch_3(self):
        r = await self.con.fetch(r'''
            SELECT <int32>$0;
        ''', 42)
        self.assertEqual(repr(r), "Set{42}")

    async def test_fetch_4(self):
        r = await self.con.fetch(r'''
            SELECT <int32>$foo;
        ''', foo=43)
        self.assertEqual(repr(r), "Set{43}")

    async def test_fetch_5(self):
        r = await self.con.fetch('''
            WITH MODULE test
            SELECT (<array<str>>$foo)[0] + (<array<str>>$bar)[0];
        ''', bar=['ice'], foo=['Al'])
        self.assertEqual(repr(r), "Set{'Alice'}")
