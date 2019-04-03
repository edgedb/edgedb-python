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


import contextlib
import logging

import edgedb

from edb.testbase import server as tb


@contextlib.contextmanager
def silence_asyncio_long_exec_warning():
    def flt(log_record):
        msg = log_record.getMessage()
        return not msg.startswith('Executing ')

    logger = logging.getLogger('asyncio')
    logger.addFilter(flt)
    try:
        yield
    finally:
        logger.removeFilter(flt)


class AsyncQueryTestCase(tb.QueryTestCase):
    pass


class SyncQueryTestCase(tb.QueryTestCase):

    def setUp(self):
        super().setUp()

        cls = type(self)
        cls.async_con = cls.con

        conargs = cls.get_connect_args().copy()
        conargs.update(dict(database=cls.async_con.dbname))

        cls.con = edgedb.connect(**conargs)

    def tearDown(self):
        cls = type(self)
        cls.con.close()
        cls.con = cls.async_con
        del cls.async_con


gen_lock_key = tb.gen_lock_key
