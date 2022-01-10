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
import sys
import pathlib
import tempfile
import unittest
from unittest import mock


from edgedb import con_utils
from edgedb import errors


class TestConUtils(unittest.TestCase):

    error_mapping = {
        'credentials_file_not_found': (
            RuntimeError, 'cannot read credentials'),
        'project_not_initialised': (
            errors.ClientConnectionError,
            'Found `edgedb.toml` but the project is not initialized'),
        'no_options_or_toml': (
            errors.ClientConnectionError,
            'no `edgedb.toml` found and no connection options specified'),
        'invalid_credentials_file': (
            RuntimeError, 'cannot read credentials'),
        'invalid_dsn_or_instance_name': (
            ValueError, 'invalid DSN or instance name'),
        'invalid_dsn': (ValueError, 'invalid DSN'),
        'unix_socket_unsupported': (
            ValueError, 'unix socket paths not supported'),
        'invalid_host': (ValueError, 'invalid host'),
        'invalid_port': (ValueError, 'invalid port'),
        'invalid_user': (ValueError, 'invalid user'),
        'invalid_database': (ValueError, 'invalid database'),
        'multiple_compound_env': (
            errors.ClientConnectionError,
            'Cannot have more than one of the following connection '
            + 'environment variables'),
        'multiple_compound_opts': (
            errors.ClientConnectionError,
            'Cannot have more than one of the following connection options'),
        'exclusive_options': (
            errors.ClientConnectionError,
            'are mutually exclusive'),
        'env_not_found': (
            ValueError, 'environment variable ".*" doesn\'t exist'),
        'file_not_found': (FileNotFoundError, 'No such file or directory'),
        'invalid_tls_security': (
            ValueError, 'tls_security can only be one of `insecure`, '
            '|tls_security must be set to strict')
    }

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
                    'EDGEDB_DATABASE': None, 'PGSSLMODE': None,
                    'XDG_CONFIG_HOME': None}
        test_env.update(env)

        fs = testcase.get('fs')

        opts = testcase.get('opts', {})
        dsn = opts.get('dsn')
        credentials = opts.get('credentials')
        credentials_file = opts.get('credentialsFile')
        host = opts.get('host')
        port = opts.get('port')
        database = opts.get('database')
        user = opts.get('user')
        password = opts.get('password')
        tls_ca = opts.get('tlsCA')
        tls_ca_file = opts.get('tlsCAFile')
        tls_security = opts.get('tlsSecurity')
        server_settings = opts.get('serverSettings')

        other_opts = testcase.get('other_opts', {})
        timeout = other_opts.get('timeout')
        command_timeout = other_opts.get('command_timeout')

        expected = (testcase.get('result'), testcase.get('other_results'))
        expected_error = testcase.get('error')
        if expected_error and expected_error.get('type'):
            expected_error = self.error_mapping.get(expected_error.get('type'))
            if not expected_error:
                raise RuntimeError(
                    f"unknown error type: {testcase.get('error').get('type')}")

        if expected[0] is None and expected_error is None:
            raise RuntimeError(
                'invalid test case: either "result" or "error" key '
                'has to be specified')
        if expected[0] is not None and expected_error is not None:
            raise RuntimeError(
                'invalid test case: either "result" or "error" key '
                'has to be specified, got both')

        result = None
        with contextlib.ExitStack() as es:
            es.enter_context(self.subTest(dsn=dsn, env=env, opts=opts))
            es.enter_context(self.environ(**test_env))

            stat_result = os.stat(os.getcwd())
            es.enter_context(
                mock.patch('os.stat', lambda _: stat_result)
            )

            if fs:
                cwd = fs.get('cwd')
                homedir = fs.get('homedir')
                files = fs.get('files')

                if cwd:
                    es.enter_context(mock.patch('os.getcwd', lambda: cwd))
                if homedir:
                    homedir = pathlib.Path(homedir)
                    es.enter_context(
                        mock.patch('pathlib.Path.home', lambda: homedir)
                    )
                if files:
                    for f, v in files.copy().items():
                        if "${HASH}" in f:
                            hash = con_utils._hash_path(v['project-path'])
                            dir = f.replace("${HASH}", hash)
                            files[dir] = ""
                            instance = os.path.join(dir, 'instance-name')
                            files[instance] = v['instance-name']
                            project = os.path.join(dir, 'project-path')
                            files[project] = v['project-path']
                            del files[f]

                    es.enter_context(
                        mock.patch(
                            'os.path.exists',
                            lambda filepath: str(filepath) in files
                        )
                    )
                    es.enter_context(
                        mock.patch(
                            'os.path.isfile',
                            lambda filepath: str(filepath) in files
                        )
                    )

                    es.enter_context(mock.patch(
                        'os.stat',
                        lambda d: mock.Mock(st_dev=0),
                    ))

                    es.enter_context(
                        mock.patch('os.path.realpath', lambda f: f)
                    )

                    def mocked_open(filepath, *args, **kwargs):
                        if str(filepath) in files:
                            return mock.mock_open(
                                read_data=files.get(str(filepath))
                            )()
                        raise FileNotFoundError(
                            f"[Errno 2] No such file or directory: " +
                            f"'{filepath}'"
                        )
                    es.enter_context(mock.patch('builtins.open', mocked_open))

            if expected_error:
                es.enter_context(self.assertRaisesRegex(*expected_error))

            connect_config, client_config = con_utils.parse_connect_arguments(
                dsn=dsn,
                host=host,
                port=port,
                credentials=credentials,
                credentials_file=credentials_file,
                database=database,
                user=user,
                password=password,
                tls_ca=tls_ca,
                tls_ca_file=tls_ca_file,
                tls_security=tls_security,
                timeout=timeout,
                command_timeout=command_timeout,
                server_settings=server_settings,
                wait_until_available=30,
            )

            result = (
                {
                    'address': [
                        connect_config.address[0], connect_config.address[1]
                    ],
                    'database': connect_config.database,
                    'user': connect_config.user,
                    'password': connect_config.password,
                    'tlsCAData': connect_config._tls_ca_data,
                    'tlsSecurity': connect_config.tls_security,
                    'serverSettings': connect_config.server_settings
                }, {
                    k: v for k, v in client_config._asdict().items()
                    if v is not None
                } if testcase.get('other_results') else None
            )

        if expected[0] is not None:
            if (expected[1]):
                for k, v in expected[1].items():
                    # If `expected` contains a type, allow that to "match" any
                    # instance of that type that `result` may contain.
                    if isinstance(v, type) and isinstance(result[1].get(k), v):
                        result[1][k] = v
            self.assertEqual(expected, result)

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
                    'EDGEDB_HOST': 'abc'
                },
                'opts': {
                    'user': '__test__',
                },
                'result': {
                    'address': ['abc', 5656],
                    'database': 'edgedb',
                    'user': '__test__',
                    'password': None,
                    'tlsCAData': None,
                    'tlsSecurity': 'strict',
                    'serverSettings': {}
                },
                'other_results': {
                    'wait_until_available': 30
                },
            })

    def test_connect_params(self):
        testcases_path = (
            pathlib.Path(__file__).parent
            / "shared-client-testcases"
            / "connection_testcases.json"
        )
        try:
            with open(testcases_path, encoding="utf-8") as f:
                testcases = json.load(f)
        except FileNotFoundError as err:
            raise FileNotFoundError(
                f'Failed to read "connection_testcases.json": {err}.\n' +
                f'Is the "shared-client-testcases" submodule initialised? ' +
                f'Try running "git submodule update --init".'
            )

        for i, testcase in enumerate(testcases):
            with self.subTest(i=i):
                platform = testcase.get('platform')
                if testcase.get('fs') and (
                    sys.platform == 'win32' or platform == 'windows'
                    or (platform is None and sys.platform == 'darwin')
                    or (platform == 'macos' and sys.platform != 'darwin')
                ):
                    continue

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

                connect_config, client_config = (
                    con_utils.parse_connect_arguments(
                        dsn=None,
                        host=None,
                        port=None,
                        credentials=None,
                        credentials_file=None,
                        user=None,
                        password=None,
                        database=None,
                        tls_ca=None,
                        tls_ca_file=None,
                        tls_security=None,
                        timeout=10,
                        command_timeout=None,
                        server_settings=None,
                        wait_until_available=30,
                    )
                )

        self.assertEqual(connect_config.address, ('inst1.example.org', 12323))
        self.assertEqual(connect_config.user, 'inst1_user')
        self.assertEqual(connect_config.password, 'passw1')
        self.assertEqual(connect_config.database, 'inst1_db')
