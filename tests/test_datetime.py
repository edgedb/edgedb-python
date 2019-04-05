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


import random

import edgedb
from edgedb import _testbase as tb


class TestDatetimeTypes(tb.SyncQueryTestCase):

    ISOLATED_METHODS = False

    async def test_duration_01(self):

        duration_kwargs = [
            dict(),
            dict(microseconds=1),
            dict(microseconds=-1),
            dict(days=1),
            dict(days=-1),
            dict(months=1),
            dict(months=-1),
            dict(microseconds=1, days=1, months=1),
            dict(microseconds=-1, days=-1, months=-1),
        ]

        # Fuzz it!
        for _ in range(5000):
            duration_kwargs.append(
                dict(
                    microseconds=random.randint(-1000000000, 1000000000),
                    days=random.randint(-500, 500),
                    months=random.randint(-50, 50)
                )
            )

        durs = [edgedb.Duration(**d) for d in duration_kwargs]

        # Test that Duration.__str__ formats the same as <str><duration>
        durs_as_text = self.con.fetchall('''
            WITH args := array_unpack(<array<duration>>$0)
            SELECT <str>args;
        ''', durs)

        # Test encode/decode roundtrip
        durs_from_db = self.con.fetchall('''
            WITH args := array_unpack(<array<duration>>$0)
            SELECT args;
        ''', durs)

        self.assertEqual(durs_as_text, [str(d) for d in durs])
        self.assertEqual(list(durs_from_db), durs)

    async def test_duration_02(self):
        d1 = edgedb.Duration(microseconds=1)
        d2 = edgedb.Duration(microseconds=2)
        d3 = edgedb.Duration(microseconds=1)

        self.assertNotEqual(d1, d2)
        self.assertEqual(d1, d3)

        self.assertEqual(hash(d1), hash(d3))
        if hash(1) != hash(2):
            self.assertNotEqual(hash(d1), hash(d2))

        self.assertEqual(d1.days, 0)
        self.assertEqual(d1.months, 0)
        self.assertEqual(d1.microseconds, 1)

        self.assertEqual(repr(d1), '<edgedb.Duration "00:00:00.000001">')
