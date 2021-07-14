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


import contextlib
import json
import os
import pathlib
import tempfile
import unittest
from unittest import mock


from edgedb import con_utils
from edgedb import errors


class TestConUtils(unittest.TestCase):

    TESTS = [
        {
            'user': 'user',
            'host': 'localhost',
            'result': (
                [("localhost", 5656)],
                {
                    'user': 'user',
                    'database': 'edgedb',
                },
                {'wait_until_available': 30},
            )
        },

        {
            'env': {
                'EDGEDB_USER': 'user',
                'EDGEDB_DATABASE': 'testdb',
                'EDGEDB_PASSWORD': 'passw',
                'EDGEDB_HOST': 'host',
                'EDGEDB_PORT': '123'
            },
            'result': (
                [('host', 123)],
                {
                    'user': 'user',
                    'password': 'passw',
                    'database': 'testdb'
                },
                {'wait_until_available': 30},
            )
        },

        {
            'env': {
                'EDGEDB_USER': 'user',
                'EDGEDB_DATABASE': 'testdb',
                'EDGEDB_PASSWORD': 'passw',
                'EDGEDB_HOST': 'host',
                'EDGEDB_PORT': '123'
            },

            'host': 'host2',
            'port': '456',
            'user': 'user2',
            'password': 'passw2',
            'database': 'db2',

            'result': (
                [('host2', 456)],
                {
                    'user': 'user2',
                    'password': 'passw2',
                    'database': 'db2'
                },
                {'wait_until_available': 30},
            )
        },

        {
            'env': {
                'EDGEDB_USER': 'user',
                'EDGEDB_DATABASE': 'testdb',
                'EDGEDB_PASSWORD': 'passw',
                'EDGEDB_HOST': 'host',
                'EDGEDB_PORT': '123',
                'PGSSLMODE': 'prefer'
            },

            'dsn': 'edgedb://user3:123123@localhost/abcdef',

            'host': 'host2',
            'port': '456',
            'user': 'user2',
            'password': 'passw2',
            'database': 'db2',
            'server_settings': {'ssl': 'False'},

            'result': (
                [('host2', 456)],
                {
                    'user': 'user2',
                    'password': 'passw2',
                    'database': 'db2',
                    'server_settings': {'ssl': 'False'},
                },
                {'wait_until_available': 30},
            )
        },

        {
            'env': {
                'EDGEDB_USER': 'user',
                'EDGEDB_DATABASE': 'testdb',
                'EDGEDB_PASSWORD': 'passw',
                'EDGEDB_HOST': 'host',
                'EDGEDB_PORT': '123',
            },

            'dsn': 'edgedb://user3:123123@localhost:5555/abcdef',
            'command_timeout': 10,

            'result': (
                [('localhost', 5555)],
                {
                    'user': 'user3',
                    'password': '123123',
                    'database': 'abcdef',
                }, {
                    'command_timeout': 10,
                    'wait_until_available': 30,
                })
        },

        {
            'dsn': 'edgedb://user3:123123@localhost:5555/abcdef',
            'result': (
                [('localhost', 5555)],
                {
                    'user': 'user3',
                    'password': '123123',
                    'database': 'abcdef'
                },
                {'wait_until_available': 30},
            )
        },

        {
            'dsn': 'edgedb://user@host1,host2/db',
            'result': (
                [('host1', 5656), ('host2', 5656)],
                {
                    'database': 'db',
                    'user': 'user',
                },
                {'wait_until_available': 30},
            )
        },

        {
            'dsn': 'edgedb://user@host1:1111,host2:2222/db',
            'result': (
                [('host1', 1111), ('host2', 2222)],
                {
                    'database': 'db',
                    'user': 'user',
                },
                {'wait_until_available': 30},
            )
        },

        {
            'env': {
                'EDGEDB_HOST': 'host1:1111,host2:2222',
                'EDGEDB_USER': 'foo',
            },
            'dsn': 'edgedb:///db',
            'result': (
                [('host1', 1111), ('host2', 2222)],
                {
                    'database': 'db',
                    'user': 'foo',
                },
                {'wait_until_available': 30},
            )
        },

        {
            'env': {
                'EDGEDB_USER': 'foo',
            },
            'dsn': 'edgedb:///db?host=host1:1111,host2:2222',
            'result': (
                [('host1', 1111), ('host2', 2222)],
                {
                    'database': 'db',
                    'user': 'foo',
                },
                {'wait_until_available': 30},
            )
        },

        {
            'env': {
                'EDGEDB_USER': 'foo',
            },
            'dsn': 'edgedb:///db',
            'host': ['host1', 'host2'],
            'result': (
                [('host1', 5656), ('host2', 5656)],
                {
                    'database': 'db',
                    'user': 'foo',
                },
                {'wait_until_available': 30},
            )
        },

        {
            'dsn': 'edgedb://user3:123123@localhost:5555/'
                   'abcdef?param=sss&param=123&host=testhost&user=testuser'
                   '&port=2222&database=testdb',
            'host': '127.0.0.1',
            'port': '888',
            'user': 'me',
            'password': 'ask',
            'database': 'db',
            'result': (
                [('127.0.0.1', 888)],
                {
                    'server_settings': {'param': '123'},
                    'user': 'me',
                    'password': 'ask',
                    'database': 'db',
                },
                {'wait_until_available': 30},
            )
        },

        {
            'dsn': 'edgedb://user3:123123@localhost:5555/'
                   'abcdef?param=sss&param=123&host=testhost&user=testuser'
                   '&port=2222&database=testdb',
            'host': '127.0.0.1',
            'port': '888',
            'user': 'me',
            'password': 'ask',
            'database': 'db',
            'server_settings': {'aa': 'bb'},
            'result': (
                [('127.0.0.1', 888)],
                {
                    'server_settings': {'aa': 'bb', 'param': '123'},
                    'user': 'me',
                    'password': 'ask',
                    'database': 'db',
                },
                {'wait_until_available': 30},
            )
        },

        {
            'dsn': 'edgedb:///dbname?host=/unix_sock/test&user=spam',
            'result': (
                [os.path.join('/unix_sock/test', '.s.EDGEDB.5656')],
                {
                    'user': 'spam',
                    'database': 'dbname'
                },
                {'wait_until_available': 30},
            )
        },

        {
            'dsn': 'pq:///dbname?host=/unix_sock/test&user=spam',
            'error': (
                ValueError,
                "dsn "
                "'pq:///dbname\\?host=/unix_sock/test&user=spam' "
                "is neither a edgedb:// URI nor valid instance name"
            )
        },

        {
            'dsn': 'edgedb://host1,host2,host3/db',
            'port': [111, 222],
            'error': (
                errors.InterfaceError,
                'could not match 2 port numbers to 3 hosts'
            )
        },

        {
            'dsn': 'edgedb://user@?port=56226&host=%2Ftmp',
            'result': (
                [os.path.join('/tmp', '.s.EDGEDB.56226')],
                {
                    'user': 'user',
                    'database': 'edgedb',
                },
                {'wait_until_available': 30},
            )
        },

        {
            'dsn': 'edgedb://user@?host=%2Ftmp',
            'admin': True,
            'result': (
                [os.path.join('/tmp', '.s.EDGEDB.admin.5656')],
                {
                    'user': 'user',
                    'database': 'edgedb',
                },
                {'wait_until_available': 30},
            )
        },

        {
            'dsn': 'edgedbadmin://user@?host=%2Ftmp',
            'result': (
                [os.path.join('/tmp', '.s.EDGEDB.admin.5656')],
                {
                    'user': 'user',
                    'database': 'edgedb',
                },
                {'wait_until_available': 30},
            )
        },
    ]

    @contextlib.contextmanager
    def environ(self, **kwargs):
        old_vals = {}
        for key in kwargs:
            if key in os.environ:
                old_vals[key] = os.environ[key]

        for key, val in kwargs.items():
            if val is None:
                if key in os.environ:
                    del os.environ[key]
            else:
                os.environ[key] = val

        try:
            yield
        finally:
            for key in kwargs:
                if key in os.environ:
                    del os.environ[key]
            for key, val in old_vals.items():
                os.environ[key] = val

    def run_testcase(self, testcase):
        env = testcase.get('env', {})
        test_env = {'EDGEDB_HOST': None, 'EDGEDB_PORT': None,
                    'EDGEDB_USER': None, 'EDGEDB_PASSWORD': None,
                    'EDGEDB_DATABASE': None, 'PGSSLMODE': None}
        test_env.update(env)

        dsn = testcase.get('dsn')
        user = testcase.get('user')
        port = testcase.get('port')
        host = testcase.get('host')
        password = testcase.get('password')
        database = testcase.get('database')
        tls_ca_file = testcase.get('tls_ca_file')
        tls_verify_hostname = testcase.get('tls_verify_hostname')
        admin = testcase.get('admin')
        timeout = testcase.get('timeout')
        command_timeout = testcase.get('command_timeout')
        server_settings = testcase.get('server_settings')

        expected = testcase.get('result')
        expected_error = testcase.get('error')
        if expected is None and expected_error is None:
            raise RuntimeError(
                'invalid test case: either "result" or "error" key '
                'has to be specified')
        if expected is not None and expected_error is not None:
            raise RuntimeError(
                'invalid test case: either "result" or "error" key '
                'has to be specified, got both')

        result = None
        with contextlib.ExitStack() as es:
            es.enter_context(self.subTest(dsn=dsn, env=env))
            es.enter_context(self.environ(**test_env))

            if expected_error:
                es.enter_context(self.assertRaisesRegex(*expected_error))

            addrs, params, config = con_utils.parse_connect_arguments(
                dsn=dsn, host=host, port=port, user=user, password=password,
                database=database, admin=admin,
                tls_ca_file=tls_ca_file,
                tls_verify_hostname=tls_verify_hostname,
                timeout=timeout, command_timeout=command_timeout,
                server_settings=server_settings,
                wait_until_available=30)

            params = {k: v for k, v in params._asdict().items()
                      if v is not None}
            config = {k: v for k, v in config._asdict().items()
                      if v is not None}
            params.pop('ssl_ctx', None)

            result = (addrs, params, config)

        if expected is not None:
            for k, v in expected[1].items():
                # If `expected` contains a type, allow that to "match" any
                # instance of that type that `result` may contain.
                if isinstance(v, type) and isinstance(result[1].get(k), v):
                    result[1][k] = v
            self.assertEqual(expected, result, 'Testcase: {}'.format(testcase))

    def test_test_connect_params_environ(self):
        self.assertNotIn('AAAAAAAAAA123', os.environ)
        self.assertNotIn('AAAAAAAAAA456', os.environ)
        self.assertNotIn('AAAAAAAAAA789', os.environ)

        try:

            os.environ['AAAAAAAAAA456'] = '123'
            os.environ['AAAAAAAAAA789'] = '123'

            with self.environ(AAAAAAAAAA123='1',
                              AAAAAAAAAA456='2',
                              AAAAAAAAAA789=None):

                self.assertEqual(os.environ['AAAAAAAAAA123'], '1')
                self.assertEqual(os.environ['AAAAAAAAAA456'], '2')
                self.assertNotIn('AAAAAAAAAA789', os.environ)

            self.assertNotIn('AAAAAAAAAA123', os.environ)
            self.assertEqual(os.environ['AAAAAAAAAA456'], '123')
            self.assertEqual(os.environ['AAAAAAAAAA789'], '123')

        finally:
            for key in {'AAAAAAAAAA123', 'AAAAAAAAAA456', 'AAAAAAAAAA789'}:
                if key in os.environ:
                    del os.environ[key]

    def test_test_connect_params_run_testcase(self):
        with self.environ(EDGEDB_PORT='777'):
            self.run_testcase({
                'env': {
                    'EDGEDB_USER': '__test__'
                },
                'host': 'abc',
                'result': (
                    [('abc', 5656)],
                    {'user': '__test__', 'database': 'edgedb'},
                    {'wait_until_available': 30},
                )
            })

    def test_connect_params(self):
        for testcase in self.TESTS:
            self.run_testcase(testcase)

    @mock.patch("edgedb.platform.config_dir",
                lambda: pathlib.Path("/home/user/.config/edgedb"))
    @mock.patch("edgedb.platform.IS_WINDOWS", False)
    @mock.patch("pathlib.Path.exists", lambda p: True)
    @mock.patch("os.path.realpath", lambda p: p)
    def test_stash_path(self):
        self.assertEqual(
            con_utils._stash_path("/home/user/work/project1"),
            pathlib.Path("/home/user/.config/edgedb/projects/project1-"
                         "cf1c841351bf7f147d70dcb6203441cf77a05249"),
        )

    def test_project_config(self):
        with tempfile.TemporaryDirectory() as tmp:
            tmp = os.path.realpath(tmp)
            base = pathlib.Path(tmp)
            home = base / 'home'
            project = base / 'project'
            projects = home / '.edgedb' / 'projects'
            creds = home / '.edgedb' / 'credentials'
            os.makedirs(projects)
            os.makedirs(creds)
            os.makedirs(project)
            with open(project / 'edgedb.toml', 'wt') as f:
                f.write('')  # app don't read toml file
            with open(creds / 'inst1.json', 'wt') as f:
                f.write(json.dumps({
                    "host": "inst1.example.org",
                    "port": 12323,
                    "user": "inst1_user",
                    "password": "passw1",
                    "database": "inst1_db",
                }))

            with mock.patch('edgedb.platform.config_dir',
                            lambda: home / '.edgedb'), \
                    mock.patch('os.getcwd', lambda: str(project)):
                stash_path = con_utils._stash_path(project)
                instance_file = stash_path / 'instance-name'
                os.makedirs(stash_path)
                with open(instance_file, 'wt') as f:
                    f.write('inst1')

                addrs, params, _config = con_utils.parse_connect_arguments(
                    dsn=None, host=None, port=None, user=None, password=None,
                    database=None, admin=None,
                    tls_ca_file=None, tls_verify_hostname=None,
                    timeout=10, command_timeout=None,
                    server_settings=None,
                    wait_until_available=30)

        self.assertEqual(addrs, [('inst1.example.org', 12323)])
        self.assertEqual(params.user, 'inst1_user')
        self.assertEqual(params.password, 'passw1')
        self.assertEqual(params.database, 'inst1_db')
