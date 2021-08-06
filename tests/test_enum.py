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


import uuid

import edgedb
from edgedb import _testbase as tb


class TestEnum(tb.AsyncQueryTestCase):

    ISOLATED_METHODS = False

    SETUP = '''
        CREATE SCALAR TYPE CellType EXTENDING enum<'red', 'white'>;
        CREATE SCALAR TYPE Color EXTENDING enum<'red', 'white'>;
    '''

    async def test_enum_01(self):
        ct_red = await self.con.query_single('SELECT <CellType>"red"')
        ct_white = await self.con.query_single('SELECT <CellType>"white"')
        c_red = await self.con.query_single('SELECT <Color>"red"')

        self.assertTrue(isinstance(ct_red, edgedb.EnumValue))
        self.assertTrue(isinstance(ct_red.__tid__, uuid.UUID))

        self.assertEqual(repr(ct_red), "<EnumValue 'red'>")

        self.assertEqual(str(ct_red), 'red')
        self.assertNotEqual(ct_red, 'red')
        self.assertFalse(ct_red == 'red')
        self.assertFalse(ct_red == c_red)

        self.assertEqual(ct_red, ct_red)
        self.assertNotEqual(ct_red, ct_white)
        self.assertNotEqual(ct_red, c_red)

        self.assertLess(ct_red, ct_white)
        self.assertLessEqual(ct_red, ct_red)
        self.assertGreater(ct_white, ct_red)
        self.assertGreaterEqual(ct_white, ct_white)

        with self.assertRaises(TypeError):
            _ = ct_red < 'red'
        with self.assertRaises(TypeError):
            _ = ct_red > 'red'
        with self.assertRaises(TypeError):
            _ = ct_red <= 'red'
        with self.assertRaises(TypeError):
            _ = ct_red >= 'red'

        with self.assertRaises(TypeError):
            _ = ct_red < c_red
        with self.assertRaises(TypeError):
            _ = ct_red > c_red
        with self.assertRaises(TypeError):
            _ = ct_red <= c_red
        with self.assertRaises(TypeError):
            _ = ct_red >= c_red

        self.assertNotEqual(hash(ct_red), hash(c_red))
        self.assertNotEqual(hash(ct_red), hash('red'))
