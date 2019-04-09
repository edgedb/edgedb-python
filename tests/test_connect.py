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


import edgedb

from edgedb import _cluster
from edgedb import _testbase as tb


class TestConnect(tb.AsyncQueryTestCase):

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.port = _cluster.find_available_port()

    async def test_connect_async_01(self):
        orig_conn_args = self.get_connect_args()
        conn_args = orig_conn_args.copy()
        conn_args['port'] = self.port

        with self.assertRaisesRegex(
                edgedb.ClientConnectionError,
                f'(?s).*Is the server running.*port {self.port}.*'):
            conn_args['host'] = '127.0.0.1'
            await edgedb.async_connect(**conn_args)

        with self.assertRaisesRegex(
                edgedb.ClientConnectionError,
                f'(?s).*Is the server running.*port {self.port}.*'):
            conn_args['host'] = orig_conn_args['host']
            await edgedb.async_connect(**conn_args)

    def test_connect_sync_01(self):
        orig_conn_args = self.get_connect_args()
        conn_args = orig_conn_args.copy()
        conn_args['port'] = self.port

        with self.assertRaisesRegex(
                edgedb.ClientConnectionError,
                f'(?s).*Is the server running.*port {self.port}.*'):
            conn_args['host'] = '127.0.0.1'
            edgedb.connect(**conn_args)

        with self.assertRaisesRegex(
                edgedb.ClientConnectionError,
                f'(?s).*Is the server running.*port {self.port}.*'):
            conn_args['host'] = orig_conn_args['host']
            edgedb.connect(**conn_args)
