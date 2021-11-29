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


import edgedb

from edgedb import _testbase as tb


class TestClient(tb.AsyncQueryTestCase):

    async def test_client_suggested_concurrency(self):
        conargs = self.get_connect_args().copy()
        conargs["database"] = self.con.dbname
        conargs["timeout"] = 120

        client = edgedb.create_async_client(**conargs)

        self.assertEqual(client.concurrency, 1)

        await client.ensure_connected()
        self.assertGreater(client.concurrency, 1)

        await client.aclose()

        client = edgedb.create_async_client(**conargs, concurrency=5)

        self.assertEqual(client.concurrency, 5)

        await client.ensure_connected()
        self.assertEqual(client.concurrency, 5)

        await client.aclose()
