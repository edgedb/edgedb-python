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
import platform
import re
import typing
import urllib.parse
import warnings
import pathlib

from . import errors
from . import credentials


EDGEDB_PORT = 5656
ERRNO_RE = re.compile(r"\[Errno (\d+)\]")
TEMPORARY_ERRORS = (
    ConnectionAbortedError,
    ConnectionRefusedError,
    ConnectionResetError,
    FileNotFoundError,
)
TEMPORARY_ERROR_CODES = frozenset({
    errno.ECONNREFUSED,
    errno.ECONNABORTED,
    errno.ECONNRESET,
    errno.ENOENT,
})


class ConnectionParameters(typing.NamedTuple):

    user: str
    password: str
    database: str
    connect_timeout: float
    server_settings: typing.Mapping[str, str]


class ClientConfiguration(typing.NamedTuple):

    connect_timeout: float
    command_timeout: float
    wait_until_available: float


_system = platform.uname().system


def _validate_port_spec(hosts, port):
    if isinstance(port, list):
        # If there is a list of ports, its length must
        # match that of the host list.
        if len(port) != len(hosts):
            raise errors.InterfaceError(
                'could not match {} port numbers to {} hosts'.format(
                    len(port), len(hosts)))
    else:
        port = [port for _ in range(len(hosts))]

    return port


def _parse_hostlist(hostlist, port):
    if ',' in hostlist:
        # A comma-separated list of host addresses.
        hostspecs = hostlist.split(',')
    else:
        hostspecs = [hostlist]

    hosts = []
    hostlist_ports = []

    if not port:
        portspec = os.environ.get('EDGEDB_PORT')
        if portspec:
            if ',' in portspec:
                default_port = [int(p) for p in portspec.split(',')]
            else:
                default_port = int(portspec)
        else:
            default_port = EDGEDB_PORT

        default_port = _validate_port_spec(hostspecs, default_port)

    else:
        port = _validate_port_spec(hostspecs, port)

    for i, hostspec in enumerate(hostspecs):
        addr, _, hostspec_port = hostspec.partition(':')
        hosts.append(addr)

        if not port:
            if hostspec_port:
                hostlist_ports.append(int(hostspec_port))
            else:
                hostlist_ports.append(default_port[i])

    if not port:
        port = hostlist_ports

    return hosts, port


def _parse_connect_dsn_and_args(*, dsn, host, port, user,
                                password, database, admin,
                                connect_timeout, server_settings):
    using_credentials = False

    if admin:
        warnings.warn(
            'The "admin=True" parameter is deprecated and is scheduled to be '
            'removed. Admin socket should never be used in applications. '
            'Use command-line tool `edgedb` to setup proper credentials.',
            DeprecationWarning, 4)

    if dsn and dsn.startswith(("edgedb://", "edgedbadmin://")):
        parsed = urllib.parse.urlparse(dsn)

        if parsed.scheme not in ('edgedb', 'edgedbadmin'):
            raise ValueError(
                f'invalid DSN: scheme is expected to be '
                f'"edgedb" or "edgedbadmin", got {parsed.scheme!r}')

        if parsed.scheme == 'edgedbadmin':
            warnings.warn(
                'The `edgedbadmin` scheme is deprecated and is scheduled '
                'to be removed. Admin socket should never be used in '
                'applications. Use command-line tool `edgedb` to setup '
                'proper credentials.',
                DeprecationWarning, 4)

        if admin is None:
            admin = parsed.scheme == 'edgedbadmin'

        if not host and parsed.netloc:
            if '@' in parsed.netloc:
                auth, _, hostspec = parsed.netloc.partition('@')
            else:
                hostspec = parsed.netloc

            if hostspec:
                host, port = _parse_hostlist(hostspec, port)

        if parsed.path and database is None:
            database = parsed.path
            if database.startswith('/'):
                database = database[1:]

        if parsed.username and user is None:
            user = parsed.username

        if parsed.password and password is None:
            password = parsed.password

        if parsed.query:
            query = urllib.parse.parse_qs(parsed.query, strict_parsing=True)
            for key, val in query.items():
                if isinstance(val, list):
                    query[key] = val[-1]

            if 'port' in query:
                val = query.pop('port')
                if not port and val:
                    port = [int(p) for p in val.split(',')]

            if 'host' in query:
                val = query.pop('host')
                if not host and val:
                    host, port = _parse_hostlist(val, port)

            if 'dbname' in query:
                val = query.pop('dbname')
                if database is None:
                    database = val

            if 'database' in query:
                val = query.pop('database')
                if database is None:
                    database = val

            if 'user' in query:
                val = query.pop('user')
                if user is None:
                    user = val

            if 'password' in query:
                val = query.pop('password')
                if password is None:
                    password = val

            if query:
                if server_settings is None:
                    server_settings = query
                else:
                    server_settings = {**query, **server_settings}
    elif dsn:
        if not dsn.isidentifier():
            raise ValueError(
                f"dsn {dsn!r} is neither a edgedb:// URI "
                f"nor valid instance name"
            )

        using_credentials = True
        path = (pathlib.Path.home() /
                '.edgedb' / 'credentials' / (dsn + '.json'))
        try:
            creds = credentials.read_credentials(path)
        except Exception as e:
            raise errors.ClientError(
                f"cannot read credentials of instance {dsn!r}"
            ) from e

        if port is None:
            port = creds['port']
        if user is None:
            user = creds['user']
        if host is None and 'host' in creds:
            host = creds['host']
        if password is None and 'password' in creds:
            password = creds['password']
        if database is None and 'database' in creds:
            database = creds['database']

    if not host:
        hostspec = os.environ.get('EDGEDB_HOST')
        if hostspec:
            host, port = _parse_hostlist(hostspec, port)

    if not host:
        if _system == 'Windows' or using_credentials:
            host = []
        else:
            host = ['/run/edgedb', '/var/run/edgedb']

        if not admin:
            host.append('localhost')

    if not isinstance(host, list):
        host = [host]

    if not port:
        portspec = os.environ.get('EDGEDB_PORT')
        if portspec:
            if ',' in portspec:
                port = [int(p) for p in portspec.split(',')]
            else:
                port = int(portspec)
        else:
            port = EDGEDB_PORT

    elif isinstance(port, (list, tuple)):
        port = [int(p) for p in port]

    else:
        port = int(port)

    port = _validate_port_spec(host, port)

    if user is None:
        user = os.getenv('EDGEDB_USER')
        if not user:
            user = 'edgedb'

    if password is None:
        password = os.getenv('EDGEDB_PASSWORD')

    if database is None:
        database = os.getenv('EDGEDB_DATABASE')

    if database is None:
        database = 'edgedb'

    if user is None:
        raise errors.InterfaceError(
            'could not determine user name to connect with')

    if database is None:
        raise errors.InterfaceError(
            'could not determine database name to connect to')

    have_unix_sockets = False
    addrs = []
    for h, p in zip(host, port):
        if h.startswith('/'):
            # UNIX socket name
            if '.s.EDGEDB.' not in h:
                if admin:
                    sock_name = f'.s.EDGEDB.admin.{p}'
                else:
                    sock_name = f'.s.EDGEDB.{p}'
                h = os.path.join(h, sock_name)
                have_unix_sockets = True
            addrs.append(h)
        elif not admin:
            # TCP host/port
            addrs.append((h, p))

    if admin and not have_unix_sockets:
        raise ValueError(
            'admin connections are only supported over UNIX sockets')

    if not addrs:
        raise ValueError(
            'could not determine the database address to connect to')

    if server_settings is not None and (
            not isinstance(server_settings, dict) or
            not all(isinstance(k, str) for k in server_settings) or
            not all(isinstance(v, str) for v in server_settings.values())):
        raise ValueError(
            'server_settings is expected to be None or '
            'a Dict[str, str]')

    params = ConnectionParameters(
        user=user,
        password=password,
        database=database,
        connect_timeout=connect_timeout,
        server_settings=server_settings)

    return addrs, params


def parse_connect_arguments(*, dsn, host, port, user, password,
                            database, admin,
                            timeout, command_timeout, wait_until_available,
                            server_settings):

    if command_timeout is not None:
        try:
            if isinstance(command_timeout, bool):
                raise ValueError
            command_timeout = float(command_timeout)
            if command_timeout <= 0:
                raise ValueError
        except ValueError:
            raise ValueError(
                'invalid command_timeout value: '
                'expected greater than 0 float (got {!r})'.format(
                    command_timeout)) from None

    addrs, params = _parse_connect_dsn_and_args(
        dsn=dsn, host=host, port=port, user=user,
        password=password, admin=admin,
        database=database, connect_timeout=timeout,
        server_settings=server_settings,
    )

    config = ClientConfiguration(
        connect_timeout=timeout,
        command_timeout=command_timeout,
        wait_until_available=wait_until_available or 0,
    )

    return addrs, params, config


def render_client_no_connection_error(prefix, addr, attempts, duration):
    if isinstance(addr, str):
        msg = (
            f'{prefix}'
            f'\n\tAfter {attempts} attempts in {duration:.1f} sec'
            f'\n\tIs the server running locally and accepting '
            f'\n\tconnections on Unix domain socket {addr!r}?'
        )
    else:
        msg = (
            f'{prefix}'
            f'\n\tAfter {attempts} attempts in {duration:.1f} sec'
            f'\n\tIs the server running on host {addr[0]!r} '
            f'and accepting '
            f'\n\tTCP/IP connections on port {addr[1]}?'
        )
    return msg


def _extract_errno(s):
    """Extract multiple errnos from error string

    When we connect to a host that has multiple underlying IP addresses, say
    ``localhost`` having ``::1`` and ``127.0.0.1``, we get
    ``OSError("Multiple exceptions:...")`` error without ``.errno`` attribute
    set. There are multiple ones in the text, so we extract all of them.
    """
    result = []
    for match in ERRNO_RE.finditer(s):
        result.append(int(match.group(1)))
    if result:
        return result


def wrap_error(e):
    message = str(e)
    if e.errno is None:
        errnos = _extract_errno(message)
    else:
        errnos = [e.errno]

    if errnos:
        is_temp = any((code in TEMPORARY_ERROR_CODES for code in errnos))
    else:
        is_temp = isinstance(e, TEMPORARY_ERRORS)

    if is_temp:
        return errors.ClientConnectionFailedTemporarilyError(message)
    else:
        return errors.ClientConnectionFailedError(message)
