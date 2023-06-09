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
import edgedb

import array


# An array.array subtype where indexing doesn't work.
# We use this to verify that the non-boxing memoryview based
# fast path works, since the slow path won't work on this object.
class brokenarray(array.array):
    def __getitem__(self, i):
        raise AssertionError("the fast path wasn't used!")


class TestVector(tb.SyncQueryTestCase):
    def setUp(self):
        super().setUp()

        if not self.client.query_required_single('''
            select exists (
              select sys::ExtensionPackage filter .name = 'pgvector'
            )
        '''):
            self.skipTest("feature not implemented")

        self.client.execute('''
            create extension pgvector;
        ''')

    def tearDown(self):
        try:
            self.client.execute('''
                drop extension pgvector;
            ''')
        finally:
            super().tearDown()

    async def test_vector_01(self):
        val = self.client.query_single('''
            select <ext::pgvector::vector>[1.5,2.0,3.8]
        ''')
        self.assertTrue(isinstance(val, array.array))
        self.assertEqual(val, array.array('f', [1.5, 2.0, 3.8]))

        val = self.client.query_single(
            '''
                select <json><ext::pgvector::vector>$0
            ''',
            [3.0, 9.0, -42.5],
        )
        self.assertEqual(val, '[3, 9, -42.5]')

        val = self.client.query_single(
            '''
                select <json><ext::pgvector::vector>$0
            ''',
            array.array('f', [3.0, 9.0, -42.5])
        )
        self.assertEqual(val, '[3, 9, -42.5]')

        val = self.client.query_single(
            '''
                select <json><ext::pgvector::vector>$0
            ''',
            array.array('i', [1, 2, 3]),
        )
        self.assertEqual(val, '[1, 2, 3]')

        # Test that the fast-path works: if the encoder tries to
        # call __getitem__ on this brokenarray, it will fail.
        val = self.client.query_single(
            '''
                select <json><ext::pgvector::vector>$0
            ''',
            brokenarray('f', [3.0, 9.0, -42.5])
        )
        self.assertEqual(val, '[3, 9, -42.5]')

        # I don't think it's worth adding a dependency to test this,
        # but this works too:
        # import numpy as np
        # val = self.client.query_single(
        #     '''
        #     select <json><ext::pgvector::vector>$0
        #     ''',
        #     np.asarray([3.0, 9.0, -42.5], dtype=np.float32),
        # )
        # self.assertEqual(val, '[3,9,-42.5]')

        # Some sad path tests
        with self.assertRaises(edgedb.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::vector>$0
                ''',
                [3.0, None, -42.5],
            )

        with self.assertRaises(edgedb.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::vector>$0
                ''',
                [3.0, 'x', -42.5],
            )

        with self.assertRaises(edgedb.InvalidArgumentError):
            self.client.query_single(
                '''
                    select <ext::pgvector::vector>$0
                ''',
                'foo',
            )
