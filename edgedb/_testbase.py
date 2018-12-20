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


from edb.lang.common import devmode
from edb.server import _testbase as tb


class QueryTestCase(tb.QueryTestCase):

    BASE_TEST_CLASS = True

    @classmethod
    def setUpClass(cls):
        devmode.enable_dev_mode()
        super().setUpClass()

    @classmethod
    def connect(cls, loop, cluster, database=None):
        connect_args = cluster.get_connect_args().copy()
        connect_args['user'] = 'edgedb'
        connect_args['port'] += 1  # XXX
        connect_args['database'] = database
        return loop.run_until_complete(edgedb.connect(**connect_args))
