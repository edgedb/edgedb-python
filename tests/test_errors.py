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


import unittest


from gel import errors
from gel.errors import _base as base_errors


class TestErrors(unittest.TestCase):

    def test_errors_1(self):
        new = base_errors.EdgeDBError._from_code

        e = new(0x_04_00_00_00, 'aa')
        self.assertIs(type(e), errors.QueryError)
        self.assertEqual(e.get_code(), 0x_04_00_00_00)

        e = new(0x_04_01_00_00, 'aa')
        self.assertIs(type(e), errors.InvalidSyntaxError)
        self.assertEqual(e.get_code(), 0x_04_01_00_00)

        e = new(0x_04_01_01_00, 'aa')
        self.assertIs(type(e), errors.EdgeQLSyntaxError)
        self.assertEqual(e.get_code(), 0x_04_01_01_00)

        e = new(0x_04_01_01_FF, 'aa')
        self.assertIs(type(e), errors.EdgeQLSyntaxError)
        self.assertEqual(e.get_code(), 0x_04_01_01_FF)

        e = new(0x_04_01_FF_FF, 'aa')
        self.assertIs(type(e), errors.InvalidSyntaxError)
        self.assertEqual(e.get_code(), 0x_04_01_FF_FF)

        e = new(0x_04_00_FF_FF, 'aa')
        self.assertIs(type(e), errors.QueryError)
        self.assertEqual(e.get_code(), 0x_04_00_FF_FF)

    def test_errors_2(self):
        new = base_errors.EdgeDBError._from_code

        e = new(0x_F9_1E_FF_F1, 'aa')
        self.assertEqual(e.get_code(), 0x_F9_1E_FF_F1)
        self.assertIs(type(e), errors.EdgeDBError)
