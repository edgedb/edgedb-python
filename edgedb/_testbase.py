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
import subprocess
import sys
import tempfile
import time
import unittest

import edgedb
from edgedb import asyncio_client
from edgedb import blocking_client


log = logging.getLogger(__name__)


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


def _get_wsl_path(win_path):
    return (
        re.sub(r'^([A-Z]):', lambda m: f'/mnt/{m.group(1)}', win_path)
        .replace("\\", '/')
        .lower()
    )


_default_cluster = None


def _start_cluster(*, cleanup_atexit=True):
    global _default_cluster

    if isinstance(_default_cluster, Exception):
        # when starting a server fails
        # don't retry starting one for every TestCase
        # because repeating the failure can take a while
        raise _default_cluster

    if _default_cluster:
        return _default_cluster

    try:
        tmpdir = tempfile.TemporaryDirectory()
        status_file = os.path.join(tmpdir.name, 'server-status')

        # if running on windows adjust the path for WSL
        status_file_unix = _get_wsl_path(status_file)

        env = os.environ.copy()
        # Make sure the PYTHONPATH of _this_ process does
        # not interfere with the server's.
        env.pop('PYTHONPATH', None)

        edgedb_server = env.get('EDGEDB_SERVER_BINARY', 'edgedb-server')
        args = [
            edgedb_server,
            "--temp-dir",
            "--testmode",
            f"--emit-server-status={status_file_unix}",
            "--port=auto",
            "--auto-shutdown",
            "--bootstrap-command=ALTER ROLE edgedb { SET password := 'test' }",
        ]

        help_args = [edgedb_server, "--help"]
        if sys.platform == 'win32':
            help_args = ['wsl', '-u', 'edgedb'] + help_args

        supported_opts = subprocess.run(
            help_args,
            universal_newlines=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            encoding="utf-8",
            env=env,
            cwd=tmpdir.name,
        ).stdout

        if "--tls-cert-mode" in supported_opts:
            args.append("--tls-cert-mode=generate_self_signed")
        elif "--generate-self-signed-cert" in supported_opts:
            args.append("--generate-self-signed-cert")

        if sys.platform == 'win32':
            args = ['wsl', '-u', 'edgedb'] + args

        if env.get('EDGEDB_DEBUG_SERVER'):
            server_stdout = None
        else:
            server_stdout = subprocess.DEVNULL

        p = subprocess.Popen(
            args,
            env=env,
            cwd=tmpdir.name,
            stdout=server_stdout,
            stderr=subprocess.STDOUT,
        )

        for _ in range(250):
            try:
                with open(status_file, 'rb') as f:
                    for line in f:
                        if line.startswith(b'READY='):
                            break
                    else:
                        raise RuntimeError('not ready')
                break
            except Exception:
                time.sleep(1)
        else:
            raise RuntimeError('server status file not found')

        data = json.loads(line.split(b'READY=')[1])
        con_args = dict(host='localhost', port=data['port'])
        if 'tls_cert_file' in data:
            if sys.platform == 'win32':
                con_args['tls_ca_file'] = os.path.join(
                    tmpdir.name, "edbtlscert.pem"
                )
                subprocess.check_call(
                    [
                        'wsl',
                        '-u',
                        'edgedb',
                        'cp',
                        data['tls_cert_file'],
                        _get_wsl_path(con_args['tls_ca_file'])
                    ]
                )
            else:
                con_args['tls_ca_file'] = data['tls_cert_file']

        client = edgedb.create_client(password='test', **con_args)
        client.ensure_connected()
        _default_cluster = {
            'proc': p,
            'client': client,
            'con_args': con_args,
        }

        if 'tls_cert_file' in data:
            # Keep the temp dir which we also copied the cert from WSL
            _default_cluster['_tmpdir'] = tmpdir

        atexit.register(client.close)
    except Exception as e:
        _default_cluster = e
        raise e

    return _default_cluster


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
                        self.loop.run_until_complete(self.client.execute(
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

    def addCleanup(self, func, *args, **kwargs):
        @functools.wraps(func)
        def cleanup():
            res = func(*args, **kwargs)
            if inspect.isawaitable(res):
                self.loop.run_until_complete(res)
        super().addCleanup(cleanup)


class ClusterTestCase(TestCase):

    BASE_TEST_CLASS = True

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.cluster = _start_cluster(cleanup_atexit=True)


class TestAsyncIOClient(edgedb.AsyncIOClient):
    def _clear_codecs_cache(self):
        self._impl.codecs_registry.clear_cache()

    @property
    def connection(self):
        return self._impl._holders[0]._con

    @property
    def dbname(self):
        return self._impl._working_params.database

    @property
    def is_proto_lt_1_0(self):
        return self.connection._protocol.is_legacy


class TestClient(edgedb.Client):
    @property
    def connection(self):
        return self._impl._holders[0]._con

    @property
    def is_proto_lt_1_0(self):
        return self.connection._protocol.is_legacy

    @property
    def dbname(self):
        return self._impl._working_params.database


class ConnectedTestCaseMixin:
    is_client_async = True

    @classmethod
    def make_test_client(
        cls, *,
        cluster=None,
        database='edgedb',
        user='edgedb',
        password='test',
        connection_class=...,
    ):
        conargs = cls.get_connect_args(
            cluster=cluster, database=database, user=user, password=password)
        if connection_class is ...:
            connection_class = (
                asyncio_client.AsyncIOConnection
                if cls.is_client_async
                else blocking_client.BlockingIOConnection
            )
        return (TestAsyncIOClient if cls.is_client_async else TestClient)(
            connection_class=connection_class,
            max_concurrency=1,
            **conargs,
        )

    @classmethod
    def get_connect_args(cls, *,
                         cluster=None,
                         database='edgedb',
                         user='edgedb',
                         password='test'):
        conargs = cls.cluster['con_args'].copy()
        conargs.update(dict(user=user,
                            password=password,
                            database=database))
        return conargs

    @classmethod
    def adapt_call(cls, coro):
        return cls.loop.run_until_complete(coro)


class DatabaseTestCase(ClusterTestCase, ConnectedTestCaseMixin):
    SETUP = None
    TEARDOWN = None
    SCHEMA = None

    SETUP_METHOD = None
    TEARDOWN_METHOD = None

    BASE_TEST_CLASS = True
    TEARDOWN_RETRY_DROP_DB = 1

    def setUp(self):
        if self.SETUP_METHOD:
            self.adapt_call(
                self.client.execute(self.SETUP_METHOD))

        super().setUp()

    def tearDown(self):
        try:
            if self.TEARDOWN_METHOD:
                self.adapt_call(
                    self.client.execute(self.TEARDOWN_METHOD))
        finally:
            try:
                if self.client.connection.is_in_transaction():
                    raise AssertionError(
                        'test connection is still in transaction '
                        '*after* the test')

            finally:
                super().tearDown()

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        dbname = cls.get_database_name()

        cls.admin_client = None

        class_set_up = os.environ.get('EDGEDB_TEST_CASES_SET_UP')

        # Only open an extra admin connection if necessary.
        if not class_set_up:
            script = f'CREATE DATABASE {dbname};'
            cls.admin_client = cls.make_test_client()
            cls.adapt_call(cls.admin_client.execute(script))

        cls.client = cls.make_test_client(database=dbname)

        if not class_set_up:
            script = cls.get_setup_script()
            if script:
                # The setup is expected to contain a CREATE MIGRATION,
                # which needs to be wrapped in a transaction.
                if cls.is_client_async:
                    async def execute():
                        async for tr in cls.client.transaction():
                            async with tr:
                                await tr.execute(script)
                else:
                    def execute():
                        for tr in cls.client.transaction():
                            with tr:
                                tr.execute(script)
                cls.adapt_call(execute())

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
        script += f'\nSTART MIGRATION TO {{ {"".join(schema)} }};'
        script += f'\nPOPULATE MIGRATION; \nCOMMIT MIGRATION;'

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

        if cls.TEARDOWN:
            script = cls.TEARDOWN.strip()

        try:
            if script:
                cls.adapt_call(
                    cls.client.execute(script))
        finally:
            try:
                if cls.is_client_async:
                    cls.adapt_call(cls.client.aclose())
                else:
                    cls.client.close()

                dbname = cls.get_database_name()
                script = f'DROP DATABASE {dbname};'

                retry = cls.TEARDOWN_RETRY_DROP_DB
                for i in range(retry):
                    try:
                        cls.adapt_call(
                            cls.admin_client.execute(script))
                    except edgedb.errors.ExecutionError:
                        if i < retry - 1:
                            time.sleep(0.1)
                        else:
                            raise
                    except edgedb.errors.UnknownDatabaseError:
                        break

            except Exception:
                log.exception('error running teardown')
                # skip the exception so that original error is shown instead
                # of finalizer error
            finally:
                try:
                    if cls.admin_client is not None:
                        if cls.is_client_async:
                            cls.adapt_call(
                                cls.admin_client.aclose())
                        else:
                            cls.admin_client.close()
                finally:
                    super().tearDownClass()


class AsyncQueryTestCase(DatabaseTestCase):
    BASE_TEST_CLASS = True


class SyncQueryTestCase(DatabaseTestCase):
    BASE_TEST_CLASS = True
    TEARDOWN_RETRY_DROP_DB = 5
    is_client_async = False

    @classmethod
    def adapt_call(cls, result):
        return result


_lock_cnt = 0


def gen_lock_key():
    global _lock_cnt
    _lock_cnt += 1
    return os.getpid() * 1000 + _lock_cnt


if os.environ.get('USE_UVLOOP'):
    import uvloop
    uvloop.install()
elif sys.platform == 'win32' and sys.version_info[:2] == (3, 7):
    # The default policy on win32 of Python 3.7 is SelectorEventLoop, which
    # does not implement some subprocess functions required by the tests,
    # so we have to manually set it to the proactor one (default in 3.8).
    asyncio.set_event_loop_policy(asyncio.WindowsProactorEventLoopPolicy())
