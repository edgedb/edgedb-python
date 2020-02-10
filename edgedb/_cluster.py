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


import errno
import os
import pathlib
import random
import re
import shutil
import socket
import subprocess
import sys
import tempfile
import time

import edgedb


def escape_string(s):
    split = re.split(r"(\n|\\\\|\\')", s)

    if len(split) == 1:
        return s.replace(r"'", r"\'")

    return ''.join((r if i % 2 else r.replace(r"'", r"\'"))
                   for i, r in enumerate(split))


def quote_literal(string):
    return "'" + escape_string(string) + "'"


def find_available_port(port_range=(49152, 65535), max_tries=1000):
    low, high = port_range

    port = low
    try_no = 0

    while try_no < max_tries:
        try_no += 1
        port = random.randint(low, high)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(('localhost', port))
        except socket.error as e:
            if e.errno == errno.EADDRINUSE:
                continue
        finally:
            sock.close()

        break
    else:
        port = None

    return port


class ClusterError(Exception):
    pass


class Cluster:
    def __init__(
            self, data_dir, *, port=5656,
            runstate_dir, env=None, testmode=False):
        self._data_dir = data_dir
        self._location = data_dir
        self._edgedb_cmd = ['edgedb-server', '-D', self._data_dir]

        if testmode:
            self._edgedb_cmd.append('--testmode')

        self._runstate_dir = runstate_dir
        self._edgedb_cmd.extend(['--runstate-dir', runstate_dir])
        self._daemon_process = None
        self._port = port
        self._effective_port = None
        self._env = env

    def get_status(self):
        data_dir = pathlib.Path(self._data_dir)
        if not data_dir.exists():
            return 'not-initialized'
        elif not (data_dir / 'postmaster.pid').exists():
            return 'stopped'
        else:
            return 'running'

    def get_connect_args(self):
        return {
            'host': 'localhost',
            'port': self._effective_port
        }

    def get_data_dir(self):
        return self._data_dir

    async def async_connect(self, **kwargs):
        connect_args = self.get_connect_args().copy()
        connect_args.update(kwargs)

        return await edgedb.async_connect(**connect_args)

    def connect(self, **kwargs):
        connect_args = self.get_connect_args().copy()
        connect_args.update(kwargs)

        return edgedb.connect(**connect_args)

    def init(self):
        cluster_status = self.get_status()

        if not cluster_status.startswith('not-initialized'):
            raise ClusterError(
                'cluster in {!r} has already been initialized'.format(
                    self._location))

        self._init()

    def start(self, wait=60, **settings):
        port = settings.pop('port', None) or self._port
        if port == 'dynamic':
            port = find_available_port()

        self._effective_port = port

        extra_args = ['--{}={}'.format(k.replace('_', '-'), v)
                      for k, v in settings.items()]
        extra_args.append('--port={}'.format(self._effective_port))

        env = os.environ.copy()
        # Make sure the PYTHONPATH of _this_ process does
        # not interfere with the server's.
        env.pop('PYTHONPATH', None)

        if self._env:
            env.update(self._env)

        self._daemon_process = subprocess.Popen(
            self._edgedb_cmd + extra_args,
            stdout=sys.stdout, stderr=sys.stderr,
            env=env, cwd=str(self._data_dir))

        self._test_connection()

    def stop(self, wait=60):
        if (
                self._daemon_process is not None and
                self._daemon_process.returncode is None):
            self._daemon_process.terminate()
            self._daemon_process.wait(wait)

    def destroy(self):
        status = self.get_status()
        if status == 'stopped' or status == 'not-initialized':
            shutil.rmtree(self._data_dir)
        else:
            raise ClusterError('cannot destroy {} cluster'.format(status))

    def _init(self):
        if self._env:
            env = os.environ.copy()
            env.update(self._env)
        else:
            env = None

        init = subprocess.run(
            self._edgedb_cmd + ['--bootstrap'],
            stdout=sys.stdout, stderr=sys.stderr,
            env=env)

        if init.returncode != 0:
            raise ClusterError(
                f'edgedb-server --bootstrap failed with '
                f'exit code {init.returncode}')

    def _test_connection(self, timeout=60):
        while True:
            started = time.monotonic()
            left = timeout
            try:
                conn = edgedb.connect(
                    host=str(self._runstate_dir),
                    port=self._effective_port,
                    admin=True,
                    database='edgedb',
                    user='edgedb',
                    timeout=left)
            except (OSError, socket.error, TimeoutError,
                    edgedb.ClientConnectionError):
                left -= (time.monotonic() - started)
                if left > 0.05:
                    time.sleep(0.05)
                    left -= 0.05
                    continue
                raise ClusterError(
                    f'could not connect to edgedb-server '
                    f'within {timeout} seconds')
            else:
                conn.close()
                return

    def _admin_query(self, query):
        conn_args = self.get_connect_args().copy()
        conn_args['host'] = str(self._runstate_dir)
        conn_args['admin'] = True
        conn = self.connect(**conn_args)

        try:
            return conn.fetchall(query)
        finally:
            conn.close()

    def set_superuser_password(self, password):
        self._admin_query(f'''
            ALTER ROLE edgedb
            SET password := {quote_literal(password)}
        ''')

    def trust_local_connections(self):
        self._admin_query('''
            CONFIGURE SYSTEM INSERT Auth {
                host := 'localhost',
                priority := 0,
                method := (INSERT Trust),
            }
        ''')


class TempCluster(Cluster):
    def __init__(
            self, *, data_dir_suffix=None, data_dir_prefix=None,
            data_dir_parent=None, env=None, testmode=False):
        tempdir = tempfile.mkdtemp(
            suffix=data_dir_suffix, prefix=data_dir_prefix,
            dir=data_dir_parent)
        super().__init__(data_dir=tempdir, runstate_dir=tempdir, env=env,
                         testmode=testmode)


class RunningCluster(Cluster):
    def __init__(self, **conn_args):
        self.conn_args = conn_args

    def is_managed(self):
        return False

    def get_connect_args(self):
        return dict(self.conn_args)

    def get_status(self):
        return 'running'

    def init(self, **settings):
        pass

    def start(self, wait=60, **settings):
        pass

    def stop(self, wait=60):
        pass

    def destroy(self):
        pass
