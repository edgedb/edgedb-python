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


import asyncio
import atexit
import contextlib
import functools
import inspect
import json
import logging
import os
import re
import unittest

import edgedb
from edgedb import _cluster as edgedb_cluster


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


_default_cluster = None


def _init_cluster(data_dir=None, *, cleanup_atexit=True):
    if (not os.environ.get('EDGEDB_DEBUG_SERVER') and
            not os.environ.get('EDGEDB_LOG_LEVEL')):
        _env = {'EDGEDB_LOG_LEVEL': 'silent'}
    else:
        _env = {}

    if data_dir is None:
        cluster = edgedb_cluster.TempCluster(env=_env, testmode=True)
        destroy = True
    else:
        cluster = edgedb_cluster.Cluster(data_dir=data_dir, env=_env)
        destroy = False

    if cluster.get_status() == 'not-initialized':
        cluster.init()

    cluster.start(port='dynamic')
    cluster.set_superuser_password('test')

    if cleanup_atexit:
        atexit.register(_shutdown_cluster, cluster, destroy=destroy)

    return cluster


def _start_cluster(*, cleanup_atexit=True):
    global _default_cluster

    if _default_cluster is None:
        cluster_addr = os.environ.get('EDGEDB_TEST_CLUSTER_ADDR')
        if cluster_addr:
            conn_spec = json.loads(cluster_addr)
            _default_cluster = edgedb_cluster.RunningCluster(**conn_spec)
        else:
            data_dir = os.environ.get('EDGEDB_TEST_DATA_DIR')
            _default_cluster = _init_cluster(
                data_dir=data_dir, cleanup_atexit=cleanup_atexit)

    return _default_cluster


def _shutdown_cluster(cluster, *, destroy=True):
    cluster.stop()
    if destroy:
        cluster.destroy()


class TestCaseMeta(type(unittest.TestCase)):
    _database_names = set()

    @staticmethod
    def _iter_methods(bases, ns):
        for base in bases:
            for methname in dir(base):
                if not methname.startswith('test_'):
                    continue

                meth = getattr(base, methname)
                if not inspect.iscoroutinefunction(meth):
                    continue

                yield methname, meth

        for methname, meth in ns.items():
            if not methname.startswith('test_'):
                continue

            if not inspect.iscoroutinefunction(meth):
                continue

            yield methname, meth

    @classmethod
    def wrap(mcls, meth):
        @functools.wraps(meth)
        def wrapper(self, *args, __meth__=meth, **kwargs):
            try_no = 1

            while True:
                try:
                    # There might be unobvious serializability
                    # anomalies across the test suite, so, rather
                    # than hunting them down every time, simply
                    # retry the test.
                    self.loop.run_until_complete(
                        __meth__(self, *args, **kwargs))
                except edgedb.TransactionSerializationError:
                    if try_no == 3:
                        raise
                    else:
                        self.loop.run_until_complete(self.con.execute(
                            'ROLLBACK;'
                        ))
                        try_no += 1
                else:
                    break

        return wrapper

    @classmethod
    def add_method(mcls, methname, ns, meth):
        ns[methname] = mcls.wrap(meth)

    def __new__(mcls, name, bases, ns):
        for methname, meth in mcls._iter_methods(bases, ns.copy()):
            if methname in ns:
                del ns[methname]
            mcls.add_method(methname, ns, meth)

        cls = super().__new__(mcls, name, bases, ns)
        if not ns.get('BASE_TEST_CLASS') and hasattr(cls, 'get_database_name'):
            dbname = cls.get_database_name()

            if name in mcls._database_names:
                raise TypeError(
                    f'{name} wants duplicate database name: {dbname}')

            mcls._database_names.add(name)

        return cls


class TestCase(unittest.TestCase, metaclass=TestCaseMeta):
    @classmethod
    def setUpClass(cls):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        cls.loop = loop

    @classmethod
    def tearDownClass(cls):
        cls.loop.close()
        asyncio.set_event_loop(None)

    def add_fail_notes(self, **kwargs):
        if not hasattr(self, 'fail_notes'):
            self.fail_notes = {}
        self.fail_notes.update(kwargs)

    @contextlib.contextmanager
    def annotate(self, **kwargs):
        # Annotate the test in case the nested block of code fails.
        try:
            yield
        except Exception:
            self.add_fail_notes(**kwargs)
            raise

    @contextlib.contextmanager
    def assertRaisesRegex(self, exception, regex, msg=None,
                          **kwargs):
        with super().assertRaisesRegex(exception, regex, msg=msg):
            try:
                yield
            except BaseException as e:
                if isinstance(e, exception):
                    for attr_name, expected_val in kwargs.items():
                        val = getattr(e, attr_name)
                        if val != expected_val:
                            raise self.failureException(
                                f'{exception.__name__} context attribute '
                                f'{attr_name!r} is {val} (expected '
                                f'{expected_val!r})') from e
                raise


class ClusterTestCase(TestCase):

    BASE_TEST_CLASS = True

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.cluster = _start_cluster(cleanup_atexit=True)


class ConnectedTestCaseMixin:

    @classmethod
    async def connect(cls, *,
                      cluster=None,
                      database='edgedb',
                      user='edgedb',
                      password='test'):
        conargs = cls.get_connect_args(
            cluster=cluster, database=database, user=user, password=password)
        return await edgedb.async_connect(**conargs)

    @classmethod
    def get_connect_args(cls, *,
                         cluster=None,
                         database='edgedb',
                         user='edgedb',
                         password='test'):
        if cluster is None:
            cluster = cls.cluster
        conargs = cluster.get_connect_args().copy()
        conargs.update(dict(user=user,
                            password=password,
                            database=database))
        return conargs


class DatabaseTestCase(ClusterTestCase, ConnectedTestCaseMixin):
    SETUP = None
    TEARDOWN = None
    SCHEMA = None

    SETUP_METHOD = None
    TEARDOWN_METHOD = None

    # Some tests may want to manage transactions manually,
    # in which case ISOLATED_METHODS will be False.
    ISOLATED_METHODS = True
    # Turns on "EdgeDB developer" mode which allows using restricted
    # syntax like FROM SQL and similar. It allows modifying standard
    # library (e.g. declaring casts).
    INTERNAL_TESTMODE = True

    BASE_TEST_CLASS = True

    def setUp(self):
        if self.INTERNAL_TESTMODE:
            self.loop.run_until_complete(
                self.con.execute(
                    'CONFIGURE SESSION SET __internal_testmode := true;'))

        if self.ISOLATED_METHODS:
            self.xact = self.con.transaction()
            self.loop.run_until_complete(self.xact.start())

        if self.SETUP_METHOD:
            self.loop.run_until_complete(
                self.con.execute(self.SETUP_METHOD))

        super().setUp()

    def tearDown(self):
        try:
            if self.TEARDOWN_METHOD:
                self.loop.run_until_complete(
                    self.con.execute(self.TEARDOWN_METHOD))
        finally:
            try:
                if self.ISOLATED_METHODS:
                    self.loop.run_until_complete(self.xact.rollback())
                    del self.xact

                if self.con.is_in_transaction():
                    self.loop.run_until_complete(
                        self.con.execute('ROLLBACK'))
                    raise AssertionError(
                        'test connection is still in transaction '
                        '*after* the test')

                if not self.ISOLATED_METHODS:
                    self.loop.run_until_complete(
                        self.con.execute('RESET ALIAS *;'))

            finally:
                super().tearDown()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        dbname = cls.get_database_name()

        cls.admin_conn = None
        cls.con = None

        class_set_up = os.environ.get('EDGEDB_TEST_CASES_SET_UP')

        # Only open an extra admin connection if necessary.
        if not class_set_up:
            script = f'CREATE DATABASE {dbname};'
            cls.admin_conn = cls.loop.run_until_complete(cls.connect())
            cls.loop.run_until_complete(cls.admin_conn.execute(script))

        cls.con = cls.loop.run_until_complete(cls.connect(database=dbname))

        if not class_set_up:
            script = cls.get_setup_script()
            if script:
                # The setup is expected to contain a CREATE MIGRATION,
                # which needs to be wrapped in a transaction.
                tx = cls.con.transaction()
                cls.loop.run_until_complete(tx.start())
                cls.loop.run_until_complete(cls.con.execute(script))
                cls.loop.run_until_complete(tx.commit())
                del tx

    @classmethod
    def get_database_name(cls):
        if cls.__name__.startswith('TestEdgeQL'):
            dbname = cls.__name__[len('TestEdgeQL'):]
        elif cls.__name__.startswith('Test'):
            dbname = cls.__name__[len('Test'):]
        else:
            dbname = cls.__name__

        return dbname.lower()

    @classmethod
    def get_setup_script(cls):
        script = ''

        # Look at all SCHEMA entries and potentially create multiple
        # modules, but always create the 'test' module.
        schema = ['\nmodule test {}']
        for name, val in cls.__dict__.items():
            m = re.match(r'^SCHEMA(?:_(\w+))?', name)
            if m:
                module_name = (m.group(1) or 'test').lower().replace(
                    '__', '.')

                with open(val, 'r') as sf:
                    module = sf.read()

                schema.append(f'\nmodule {module_name} {{ {module} }}')

        # Don't wrap the script into a transaction here, so that
        # potentially it's easier to stitch multiple such scripts
        # together in a fashion similar to what `edb inittestdb` does.
        script += f'\nCREATE MIGRATION test_migration'
        script += f' TO {{ {"".join(schema)} }};'
        script += f'\nCOMMIT MIGRATION test_migration;'

        if cls.SETUP:
            if not isinstance(cls.SETUP, (list, tuple)):
                scripts = [cls.SETUP]
            else:
                scripts = cls.SETUP

            for scr in scripts:
                if '\n' not in scr and os.path.exists(scr):
                    with open(scr, 'rt') as f:
                        setup = f.read()
                else:
                    setup = scr

                script += '\n' + setup

        return script.strip(' \n')

    @classmethod
    def tearDownClass(cls):
        script = ''

        class_set_up = os.environ.get('EDGEDB_TEST_CASES_SET_UP')

        if cls.TEARDOWN and not class_set_up:
            script = cls.TEARDOWN.strip()

        try:
            if script:
                cls.loop.run_until_complete(
                    cls.con.execute(script))
        finally:
            try:
                cls.loop.run_until_complete(cls.con.aclose())

                if not class_set_up:
                    dbname = cls.get_database_name()
                    script = f'DROP DATABASE {dbname};'

                    cls.loop.run_until_complete(
                        cls.admin_conn.execute(script))

            finally:
                try:
                    if cls.admin_conn is not None:
                        cls.loop.run_until_complete(
                            cls.admin_conn.aclose())
                finally:
                    super().tearDownClass()


class AsyncQueryTestCase(DatabaseTestCase):
    BASE_TEST_CLASS = True


class SyncQueryTestCase(DatabaseTestCase):
    BASE_TEST_CLASS = True

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


_lock_cnt = 0


def gen_lock_key():
    global _lock_cnt
    _lock_cnt += 1
    return os.getpid() * 1000 + _lock_cnt
