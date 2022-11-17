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
        wait_until_available = opts.get('waitUntilAvailable')

        other_opts = testcase.get('other_opts', {})
        timeout = other_opts.get('timeout')
        command_timeout = other_opts.get('command_timeout')

        expected = testcase.get('result')
        expected_error = testcase.get('error')
        if expected_error and expected_error.get('type'):
            expected_error = self.error_mapping.get(expected_error.get('type'))
            if not expected_error:
                raise RuntimeError(
                    f"unknown error type: {testcase.get('error').get('type')}")

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
            es.enter_context(self.subTest(dsn=dsn, env=env, opts=opts))
            es.enter_context(self.environ(**test_env))

            stat_result = os.stat(os.getcwd())
            es.enter_context(
                mock.patch('os.stat', lambda _, **__: stat_result)
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
                        lambda d, **_: mock.Mock(st_dev=0),
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
                wait_until_available=wait_until_available,
            )

            result = {
                'address': [
                    connect_config.address[0], connect_config.address[1]
                ],
                'database': connect_config.database,
                'user': connect_config.user,
                'password': connect_config.password,
                'tlsCAData': connect_config._tls_ca_data,
                'tlsSecurity': connect_config.tls_security,
                'serverSettings': connect_config.server_settings,
                'waitUntilAvailable': client_config.wait_until_available,
            }

        if expected is not None:
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
                    'serverSettings': {},
                    'waitUntilAvailable': 30,
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
                wait_until_available = \
                    testcase.get('result', {}).get('waitUntilAvailable')
                if wait_until_available:
                    testcase['result']['waitUntilAvailable'] = \
                        con_utils._validate_wait_until_available(
                            wait_until_available)
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

    def test_validate_wait_until_available(self):
        invalid = [
            ' ',
            ' PT1S',
            '',
            '-.1 s',
            '-.1s',
            '-.5 second',
            '-.5 seconds',
            '-.5second',
            '-.5seconds',
            '-.s',
            '-1.s',
            '.s',
            '.seconds',
            '1.s',
            '1h-120m3600s',
            '1hour-120minute3600second',
            '1hours-120minutes3600seconds',
            '1hours120minutes3600seconds',
            '2.0hour46.0minutes39.0seconds',
            '2.0hours46.0minutes39.0seconds',
            '20 hours with other stuff should not be valid',
            '20 minutes with other stuff should not be valid',
            '20 ms with other stuff should not be valid',
            '20 seconds with other stuff should not be valid',
            '20 us with other stuff should not be valid',
            '2hour46minute39second',
            '2hours46minutes39seconds',
            '3 hours is longer than 10 seconds',
            'P-.D',
            'P-D',
            'PD',
            'PT.S',
            'PT1S ',
            '\t',
            'not a duration',
            's',
        ]

        for string in invalid:
            with self.subTest(string):
                with self.assertRaises(ValueError):
                    con_utils._validate_wait_until_available(string)

        valid = [
            (" 1s ", 1),
            (" 1s", 1),
            ("-0s", 0),
            ("-1.0h", -3600),
            ("-1.0hour", -3600),
            ("-1.0hours", -3600),
            ("-1.0m", -60),
            ("-1.0minute", -60),
            ("-1.0minutes", -60),
            ("-1.0ms", -0.001),
            ("-1.0s", -1),
            ("-1.0second", -1),
            ("-1.0seconds", -1),
            ("-1.0us", -0.000001),
            ("-1h", -3600),
            ("-1hour", -3600),
            ("-1hours", -3600),
            ("-1m", -60),
            ("-1minute", -60),
            ("-1minutes", -60),
            ("-1ms", -0.001),
            ("-1s", -1),
            ("-1second", -1),
            ("-1seconds", -1),
            ("-1us", -0.000001),
            ("-2h 60m 3600s", 0),
            ("-\t2\thour\t60\tminute\t3600\tsecond", 0),
            (".1h", 360),
            (".1hour", 360),
            (".1hours", 360),
            (".1m", 6),
            (".1minute", 6),
            (".1minutes", 6),
            (".1ms", 0.0001),
            (".1s", 0.1),
            (".1second", 0.1),
            (".1seconds", 0.1),
            (".1us", 0.0000001),
            ("1   hour 60  minute -   7200   second", 0),
            ("1   hours 60  minutes -   7200   seconds", 0),
            ("1.0h", 3600),
            ("1.0hour", 3600),
            ("1.0hours", 3600),
            ("1.0m", 60),
            ("1.0minute", 60),
            ("1.0minutes", 60),
            ("1.0ms", 0.001),
            ("1.0s", 1),
            ("1.0second", 1),
            ("1.0seconds", 1),
            ("1.0us", 0.000001),
            ("1h -120m 3600s", 0),
            ("1h -120m3600s", 0),
            ("1h 60m -7200s", 0),
            ("1h", 3600),
            ("1hour", 3600),
            ("1hours -120minutes 3600seconds", 0),
            ("1hours", 3600),
            ("1m", 60),
            ("1minute", 60),
            ("1minutes", 60),
            ("1ms", 0.001),
            ("1s ", 1),
            ("1s", 1),
            ("1s\t", 1),
            ("1second", 1),
            ("1seconds", 1),
            ("1us", 0.000001),
            ("2  h  46  m  39  s", 9999),
            ("2  hour  46  minute  39  second", 9999),
            ("2  hours  46  minutes  39  seconds", 9999),
            ("2.0  h  46.0  m  39.0  s", 9999),
            ("2.0  hour  46.0  minute  39.0  second", 9999),
            ("2.0  hours  46.0  minutes  39.0  seconds", 9999),
            ("2.0h 46.0m 39.0s", 9999),
            ("2.0h46.0m39.0s", 9999),
            ("2.0hour 46.0minute 39.0second", 9999),
            ("2.0hours 46.0minutes 39.0seconds", 9999),
            ("2h 46m 39s", 9999),
            ("2h46m39s", 9999),
            ("2hour 46minute 39second", 9999),
            ("2hours 46minutes 39seconds", 9999),
            ("39.0\tsecond 2.0  hour  46.0  minute", 9999),
            ("PT", 0),
            ("PT-.1", -360),
            ("PT-.1H", -360),
            ("PT-.1M", -6),
            ("PT-.1S", -0.1),
            ("PT-0.000001S", -0.000001),
            ("PT-0S", 0),
            ("PT-1", -3600),
            ("PT-1.", -3600),
            ("PT-1.0", -3600),
            ("PT-1.0H", -3600),
            ("PT-1.0M", -60),
            ("PT-1.0S", -1),
            ("PT-1.H", -3600),
            ("PT-1.M", -60),
            ("PT-1.S", -1),
            ("PT-1H", -3600),
            ("PT-1M", -60),
            ("PT-1S", -1),
            ("PT.1", 360),
            ("PT.1H", 360),
            ("PT.1M", 6),
            ("PT.1S", 0.1),
            ("PT0.000001S", 0.000001),
            ("PT0S", 0),
            ("PT1", 3600),
            ("PT1.", 3600),
            ("PT1.0", 3600),
            ("PT1.0H", 3600),
            ("PT1.0M", 60),
            ("PT1.0M", 60),
            ("PT1.0S", 1),
            ("PT1.H", 3600),
            ("PT1.M", 60),
            ("PT1.S", 1),
            ("PT1H", 3600),
            ("PT1M", 60),
            ("PT1S", 1),
            ("PT2.0H46.0M39.0S", 9999),
            ("PT2H46M39S", 9999),
            ("\t-\t2\thours\t60\tminutes\t3600\tseconds\t", 0),
            ("\t1s", 1),
            ("\t1s\t", 1),
        ]
        for string, expected in valid:
            with self.subTest(duration=string):
                self.assertEqual(
                    expected,
                    con_utils._validate_wait_until_available(string)
                )
