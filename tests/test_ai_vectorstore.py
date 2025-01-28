#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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
from gel import _testbase as tb


class TestAIVectorstore(tb.SyncQueryTestCase):
    VECTORSTORE_VER = None

    SETUP = None
    SCHEMA = None

    @classmethod
    def setUpClass(cls):
        super().setUpClass()

        cls.VECTORSTORE_VER = cls.client.query_single("""
            select assert_single((
              select sys::ExtensionPackage filter .name = 'vectorstore'
            )).version
        """)

        if cls.VECTORSTORE_VER is None:
            raise unittest.SkipTest("feature not implemented")

        cls.client.execute("""
            create extension vectorstore;
        """)

    @classmethod
    def tearDownClass(cls):
        try:
            cls.client.execute("""
                drop extension vectorstore;
            """)
        finally:
            super().tearDownClass()

    def test_filter(self):
        raise NotImplementedError

    def test_add(self):
        raise NotImplementedError

    def test_similarity_search(self):
        raise NotImplementedError

    def test_get_by_id(self):
        raise NotImplementedError

    def test_delete(self):
        raise NotImplementedError

    async def test_vectorstore_similarity(self):
        embedding = [1.0 for _ in range(1536)]

        await self.assert_query_result(
            """
                with module ext::vectorstore,
                select 1 - ext::pgvector::cosine_distance(
                     <ext::pgvector::vector>$query_embedding,
                     <ext::pgvector::vector>$query_embedding
                )
            """,
            [1],
            variables={"query_embedding": embedding},
            json_only=True,
        )

        await self.assert_query_result(
            """
                with module ext::vectorstore,
                select ext::vectorstore::DefaultRecord {
                    external_id,
                    cosine_similarity := 1 - ext::pgvector::cosine_distance(
                        .embedding, <ext::pgvector::vector>$query_embedding
                    )
                } order by .cosine_similarity desc empty last
                limit 1;
            """,
            [
                {
                    "external_id": "00000000-0000-0000-0000-000000000007",
                    "cosine_similarity": 0.9354143466934853,
                }
            ],
            variables={"query_embedding": embedding},
            json_only=True,
        )

    async def test_vectorstore_metadata_filtering(self):
        embedding = [1.0 for _ in range(1536)]

        await self.assert_query_result(
            """
                with module ext::vectorstore,
                select ext::vectorstore::DefaultRecord {
                    external_id,
                    cosine_similarity := 1 - ext::pgvector::cosine_distance(
                        .embedding, <ext::pgvector::vector>$query_embedding
                    )
                } filter <str>json_get(.metadata, "str_field") = "least_similar"
                order by .cosine_similarity desc empty last
                limit 1;
            """,
            [
                {
                    "external_id": "00000000-0000-0000-0000-000000000004",
                    "cosine_similarity": 0.7071067811865476,
                }
            ],
            variables={"query_embedding": embedding},
            json_only=True,
        )

