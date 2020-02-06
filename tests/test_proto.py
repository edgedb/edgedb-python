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

import unittest

import edgedb

from edgedb import _testbase as tb


class TestProto(tb.SyncQueryTestCase):

    ISOLATED_METHODS = False

    def test_json(self):
        self.assertEqual(
            self.con.fetchall_json('SELECT {"aaa", "bbb"}'),
            '["aaa", "bbb"]')

    @unittest.skip  # not merged into edgedb yet
    def test_json_elements(self):
        self.assertEqual(
            self.con._fetchall_json_elements('SELECT {"aaa", "bbb"}'),
            edgedb.Set(['"aaa"', '"bbb"']))

    async def test_proto_codec_error_recovery_01(self):
        for _ in range(5):  # execute a few times for OE
            with self.assertRaisesRegex(
                    edgedb.ClientError,
                    "unable to decode data to Python objects"):
                # Python dattime.Date object can't represent this date, so
                # we know that the codec will fail.
                # The test will be rewritten once it's possible to override
                # default codecs.
                self.con.fetchall("""
                    SELECT
                        cal::to_local_date('0001-01-01 BC', 'YYYY-MM-DD AD');
                """)

            # The protocol, though, shouldn't be in some inconsistent
            # state; it should allow new queries to execute successfully.
            self.assertEqual(
                self.con.fetchall('SELECT {"aaa", "bbb"}'),
                ['aaa', 'bbb'])

    async def test_proto_codec_error_recovery_02(self):
        for _ in range(5):  # execute a few times for OE
            with self.assertRaisesRegex(
                    edgedb.ClientError,
                    "unable to decode data to Python objects"):
                # Python dattime.Date object can't represent this date, so
                # we know that the codec will fail.
                # The test will be rewritten once it's possible to override
                # default codecs.
                self.con.fetchall(r"""
                    SELECT cal::to_local_date(
                        {
                            '2010-01-01 AD',
                            '2010-01-02 AD',
                            '2010-01-03 AD',
                            '0001-01-01 BC',
                            '2010-01-04 AD',
                            '2010-01-05 AD',
                        },
                        'YYYY-MM-DD AD'
                    );
                """)

            # The protocol, though, shouldn't be in some inconsistent
            # state; it should allow new queries to execute successfully.
            self.assertEqual(
                self.con.fetchall('SELECT {"aaa", "bbb"}'),
                ['aaa', 'bbb'])
