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
import re
import ssl
import typing
import urllib.parse
import warnings
import hashlib

from . import errors
from . import credentials
from . import platform


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


class ClientConfiguration(typing.NamedTuple):

    connect_timeout: float
    command_timeout: float
    wait_until_available: float


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


def _stash_path(path):
    path = os.path.realpath(path)
    if platform.IS_WINDOWS and not path.startswith('\\\\'):
        path = '\\\\?\\' + path
    hash = hashlib.sha1(str(path).encode('utf-8')).hexdigest()
    base_name = os.path.basename(path)
    dir_name = base_name + '-' + hash
    return platform.search_config_dir('projects', dir_name)


def _parse_verify_hostname(val: typing.Union[str, bool]) -> bool:
    if isinstance(val, bool):
        return val

    val = val.lower()
    if val in {"1", "yes", "true", "y", "t", "on"}:
        return True
    elif val in {"0", "no", "false", "n", "f", "off"}:
        return False
    else:
        raise ValueError(
            "tls_verify_hostname can only be one of yes/no"
        )


class ResolvedConnectConfig:
    _host = None
    _host_source = None

    _port = None
    _port_source = None

    _database = None
    _database_source = None

    _user = None
    _user_source = None

    _password = None
    _password_source = None

    _tls_ca_data = None
    _tls_ca_data_source = None

    _tls_verify_hostname = None
    _tls_verify_hostname_source = None

    server_settings = {}

    def _set_param(self, param, value, source, validator=None):
        param_name = '_' + param
        if getattr(self, param_name) is None:
            setattr(self, param_name + '_source', source)
            if value is not None:
                setattr(
                    self,
                    param_name,
                    validator(value) if validator else value
                )

    def set_host(self, host, source):
        self._set_param('host', host, source, _validate_host)

    def set_port(self, port, source):
        self._set_param('port', port, source, _validate_port)

    def set_database(self, database, source):
        self._set_param('database', database, source, _validate_database)

    def set_user(self, user, source):
        self._set_param('user', user, source, _validate_user)

    def set_password(self, password, source):
        self._set_param('password', password, source)

    def set_tls_ca_data(self, ca_data, source):
        self._set_param('tls_ca_data', ca_data, source)

    def set_tls_ca_file(self, ca_file, source):
        def read_ca_file(file_path):
            with open(file_path) as f:
                return f.read()

        self._set_param('tls_ca_data', ca_file, source, read_ca_file)

    def set_tls_verify_hostname(self, verify_hostname, source):
        self._set_param('tls_verify_hostname', verify_hostname, source,
                        _parse_verify_hostname)

    def add_server_settings(self, server_settings):
        _validate_server_settings(server_settings)
        self.server_settings = {**server_settings, **self.server_settings}

    @property
    def address(self):
        return (
            self._host if self._host else 'localhost',
            self._port if self._port else 5656
        )

    @property
    def database(self):
        return self._database if self._database else 'edgedb'

    @property
    def user(self):
        return self._user if self._user else 'edgedb'

    @property
    def password(self):
        return self._password

    @property
    def tls_verify_hostname(self):
        return (self._tls_verify_hostname
                if self._tls_verify_hostname is not None
                else self._tls_ca_data is None)

    _ssl_ctx = None

    @property
    def ssl_ctx(self):
        if (self._ssl_ctx):
            return self._ssl_ctx

        self._ssl_ctx = ssl.SSLContext(ssl.PROTOCOL_TLS_CLIENT)
        self._ssl_ctx.verify_mode = ssl.CERT_REQUIRED
        if self._tls_ca_data:
            self._ssl_ctx.load_verify_locations(
                cadata=self._tls_ca_data
            )
        else:
            self._ssl_ctx.load_default_certs(ssl.Purpose.SERVER_AUTH)
            if platform.IS_WINDOWS:
                import certifi
                self._ssl_ctx.load_verify_locations(cafile=certifi.where())
        self._ssl_ctx.check_hostname = self.tls_verify_hostname
        self._ssl_ctx.set_alpn_protocols(['edgedb-binary'])

        return self._ssl_ctx


def _validate_host(host):
    if '/' in host:
        raise ValueError('unix socket paths not supported')
    if host == '' or ',' in host:
        raise ValueError(f'invalid host: "{host}"')
    return host


def _validate_port(port):
    try:
        if isinstance(port, str):
            port = int(port)
        if not isinstance(port, int):
            raise ValueError()
    except Exception:
        raise ValueError(f'invalid port: {port}, not an integer')
    if port < 1 or port > 65535:
        raise ValueError(f'invalid port: {port}, must be between 1 and 65535')
    return port


def _validate_database(database):
    if database == '':
        raise ValueError(f'invalid database name: {database}')
    return database


def _validate_user(user):
    if user == '':
        raise ValueError(f'invalid user name: {user}')
    return user


def _validate_server_settings(server_settings):
    if (
        not isinstance(server_settings, dict) or
        not all(isinstance(k, str) for k in server_settings) or
        not all(isinstance(v, str) for v in server_settings.values())
    ):
        raise ValueError(
            'server_settings is expected to be None or '
            'a Dict[str, str]')


def _parse_connect_dsn_and_args(*, dsn, credentials_file, host, port, user,
                                password, database,
                                tls_ca_file, tls_verify_hostname,
                                server_settings):

    resolved_config = ResolvedConnectConfig()

    dsn, instance_name = (
        (dsn, None)
        if dsn is not None and re.match('(?i)^[a-z]+://', dsn)
        else (None, dsn)
    )

    has_compound_options = _resolve_config_options(
        resolved_config,
        'Cannot have more than one of the following connection options: '
        + '"dsn", "credentials_file" or "host"/"port"',
        dsn=(dsn, '"dsn" option') if dsn is not None else None,
        instance_name=(
            (instance_name, '"dsn" option (parsed as instance name)')
            if instance_name is not None else None
        ),
        credentials_file=(
            (credentials_file, '"credentials_file" option')
            if credentials_file is not None else None
        ),
        host=(host, '"host" option') if host is not None else None,
        port=(port, '"port" option') if port is not None else None,
        database=(
            (database, '"database" option')
            if database is not None else None
        ),
        user=(user, '"user" option') if user is not None else None,
        password=(
            (password, '"password" option')
            if password is not None else None
        ),
        tls_ca_file=(
            (tls_ca_file, '"tls_ca_file" option')
            if tls_ca_file is not None else None
        ),
        tls_verify_hostname=(
            (tls_verify_hostname, '"tls_verify_hostname" option')
            if tls_verify_hostname is not None else None
        ),
        server_settings=(
            (server_settings, '"server_settings" option')
            if server_settings is not None else None
        ),
    )

    if has_compound_options is False:
        env_port = os.getenv("EDGEDB_PORT")
        if (
            resolved_config._port is None and
            env_port and env_port.startswith('tcp://')
        ):
            # EDGEDB_PORT is set by 'docker --link' so ignore and warn
            warnings.warn('EDGEDB_PORT in "tcp://host:port" format, ' +
                          'so will be ignored')
            env_port = None

        env_dsn = os.getenv('EDGEDB_DSN')
        env_instance = os.getenv('EDGEDB_INSTANCE')
        env_credentials_file = os.getenv('EDGEDB_CREDENTIALS_FILE')
        env_host = os.getenv('EDGEDB_HOST')
        env_database = os.getenv('EDGEDB_DATABASE')
        env_user = os.getenv('EDGEDB_USER')
        env_password = os.getenv('EDGEDB_PASSWORD')
        env_tls_ca_file = os.getenv('EDGEDB_TLS_CA_FILE')
        env_tls_verify_hostname = os.getenv('EDGEDB_TLS_VERIFY_HOSTNAME')

        has_compound_options = _resolve_config_options(
            resolved_config,
            'Cannot have more than one of the following connection '
            + 'environment variables: "EDGEDB_DSN", "EDGEDB_INSTANCE", '
            + '"EDGEDB_CREDENTIALS_FILE" or "EDGEDB_HOST"/"EDGEDB_PORT"',
            dsn=(
                (env_dsn, '"EDGEDB_DSN" environment variable')
                if env_dsn is not None else None
            ),
            instance_name=(
                (env_instance, '"EDGEDB_INSTANCE" environment variable')
                if env_instance is not None else None
            ),
            credentials_file=(
                (env_credentials_file,
                 '"EDGEDB_CREDENTIALS_FILE" environment variable')
                if env_credentials_file is not None else None
            ),
            host=(
                (env_host, '"EDGEDB_HOST" environment variable')
                if env_host is not None else None
            ),
            port=(
                (env_port, '"EDGEDB_PORT" environment variable')
                if env_port is not None else None
            ),
            database=(
                (env_database, '"EDGEDB_DATABASE" environment variable')
                if env_database is not None else None
            ),
            user=(
                (env_user, '"EDGEDB_USER" environment variable')
                if env_user is not None else None
            ),
            password=(
                (env_password, '"EDGEDB_PASSWORD" environment variable')
                if env_password is not None else None
            ),
            tls_ca_file=(
                (env_tls_ca_file, '"EDGEDB_TLS_CA_FILE" environment variable')
                if env_tls_ca_file is not None else None
            ),
            tls_verify_hostname=(
                (env_tls_verify_hostname,
                 '"EDGEDB_TLS_VERIFY_HOSTNAME" environment variable')
                if env_tls_verify_hostname is not None else None
            ),
        )

    if has_compound_options is False:
        dir = find_edgedb_project_dir()
        stash_dir = _stash_path(dir)
        if os.path.exists(stash_dir):
            with open(os.path.join(stash_dir, 'instance-name'), 'rt') as f:
                instance_name = f.read().strip()

                _resolve_config_options(
                    resolved_config,
                    '',
                    instance_name=(
                        instance_name,
                        f'project linked instance ("{instance_name}")'
                    )
                )
        else:
            raise errors.ClientConnectionError(
                f'Found `edgedb.toml` but the project is not initialized. '
                f'Run `edgedb project init`.'
            )

    return resolved_config


def _parse_dsn_into_config(
    resolved_config: ResolvedConnectConfig,
    dsn: tuple[str, str]
):
    dsn_str, source = dsn

    try:
        parsed = urllib.parse.urlparse(dsn_str)
        host = parsed.hostname
        port = parsed.port
        database = parsed.path
        user = parsed.username
        password = parsed.password
    except Exception as e:
        raise ValueError(f'invalid DSN: {str(e)}')

    if parsed.scheme != 'edgedb':
        raise ValueError(
            f'invalid DSN: scheme is expected to be '
            f'"edgedb", got {parsed.scheme!r}')

    query = (
        urllib.parse.parse_qs(parsed.query, keep_blank_values=True)
        if parsed.query != ''
        else {}
    )
    for key, val in query.items():
        if isinstance(val, list):
            if len(val) > 1:
                raise ValueError(
                    f'invalid DSN: duplicate query parameter {key}')
            query[key] = val[-1]

    def handle_dsn_part(
        paramName, value, currentValue, setter,
        formatter=lambda val: val
    ):
        param_values = [
            (value if value != '' else None),
            query.get(paramName),
            query.get(paramName + '_env'),
            query.get(paramName + '_file')
        ]
        if len([p for p in param_values if p is not None]) > 1:
            raise ValueError(
                f'invalid DSN: more than one of ' +
                f'{(paramName + ", ") if value else ""}' +
                f'?{paramName}=, ?{paramName}_env=, ?{paramName}_file= ' +
                f'was specified'
            )

        if currentValue is None:
            param = (
                value if (value is not None and value != '')
                else query.get(paramName)
            )
            paramSource = source

            if param is None:
                env = query.get(paramName + '_env')
                if env is not None:
                    param = os.getenv(env)
                    if param is None:
                        raise ValueError(
                            f'{paramName}_env environment variable "{env}" ' +
                            f'doesn\'t exist')
                    paramSource = paramSource + f' ({paramName}_env: {env})'
            if param is None:
                filename = query.get(paramName + '_file')
                if filename is not None:
                    with open(filename) as f:
                        param = f.read()
                    paramSource = (
                        paramSource + f' ({paramName}_file: {filename})'
                    )

            param = formatter(param) if param is not None else None

            setter(param, paramSource)

        query.pop(paramName, None)
        query.pop(paramName + '_env', None)
        query.pop(paramName + '_file', None)

    handle_dsn_part(
        'host', host, resolved_config._host, resolved_config.set_host
    )

    handle_dsn_part(
        'port', port, resolved_config._port, resolved_config.set_port
    )

    def strip_leading_slash(str):
        return str[1:] if str.startswith('/') else str

    handle_dsn_part(
        'database', strip_leading_slash(database),
        resolved_config._database, resolved_config.set_database,
        strip_leading_slash
    )

    handle_dsn_part(
        'user', user, resolved_config._user, resolved_config.set_user
    )

    handle_dsn_part(
        'password', password,
        resolved_config._password, resolved_config.set_password
    )

    handle_dsn_part(
        'tls_cert_file', None,
        resolved_config._tls_ca_data, resolved_config.set_tls_ca_file
    )

    handle_dsn_part(
        'tls_verify_hostname', None,
        resolved_config._tls_verify_hostname,
        resolved_config.set_tls_verify_hostname
    )

    resolved_config.add_server_settings(query)


def _resolve_config_options(
    resolved_config: ResolvedConnectConfig,
    compound_error: str,
    *,
    dsn=None,
    instance_name=None,
    credentials_file=None,
    host=None,
    port=None,
    database=None,
    user=None,
    password=None,
    tls_ca_file=None,
    tls_verify_hostname=None,
    server_settings=None
):
    if database is not None:
        resolved_config.set_database(*database)
    if user is not None:
        resolved_config.set_user(*user)
    if password is not None:
        resolved_config.set_password(*password)
    if tls_ca_file is not None:
        resolved_config.set_tls_ca_file(*tls_ca_file)
    if tls_verify_hostname is not None:
        resolved_config.set_tls_verify_hostname(*tls_verify_hostname)
    if server_settings is not None:
        resolved_config.add_server_settings(server_settings[0])

    compound_params = [dsn, instance_name, credentials_file, host or port]
    compound_params_count = len([p for p in compound_params if p is not None])

    if compound_params_count > 1:
        raise errors.ClientConnectionError(compound_error)

    if compound_params_count == 1:
        if dsn is not None or host is not None or port is not None:
            if port is not None:
                resolved_config.set_port(*port)
            if dsn is None:
                dsn = (
                    'edgedb://' + (_validate_host(host[0]) if host else ''),
                    host[1] if host is not None else port[1]
                )
            _parse_dsn_into_config(resolved_config, dsn)
        else:
            if credentials_file is None:
                if (
                    re.match(
                        '^[A-Za-z_][A-Za-z_0-9]*$',
                        instance_name[0]
                    ) is None
                ):
                    raise ValueError(
                        f'invalid instance name: "{instance_name[0]}"'
                    )
                credentials_file = (
                    credentials.get_credentials_path(instance_name[0]),
                    instance_name[1]
                )
            creds = credentials.read_credentials(credentials_file[0])

            source = credentials_file[1]

            resolved_config.set_host(creds.get('host'), source)
            resolved_config.set_port(creds.get('port'), source)
            resolved_config.set_database(creds.get('database'), source)
            resolved_config.set_user(creds.get('user'), source)
            resolved_config.set_password(creds.get('password'), source)
            resolved_config.set_tls_ca_data(creds.get('tls_cert_data'), source)
            resolved_config.set_tls_verify_hostname(
                creds.get('tls_verify_hostname'),
                source
            )

        return True

    return False


def find_edgedb_project_dir():
    dir = os.getcwd()
    dev = os.stat(dir).st_dev

    while True:
        toml = os.path.join(dir, 'edgedb.toml')
        if not os.path.isfile(toml):
            parent = os.path.basename(dir)
            if parent == dir:
                raise errors.ClientConnectionError(
                    f'no `edgedb.toml` found and '
                    f'no connection options specified'
                )
            parent_dev = os.stat(parent).st_dev
            if parent_dev != dev:
                raise errors.ClientConnectionError(
                    f'no `edgedb.toml` found and '
                    f'no connection options specified'
                    f'(stopped searching for `edgedb.toml` at file system'
                    f'boundary {dir!r})'
                )
            dir = parent
            dev = parent_dev
            continue
        return dir


def parse_connect_arguments(
    *, dsn, credentials_file, host, port,
    database, user, password,
    tls_ca_file, tls_verify_hostname,
    timeout, command_timeout, wait_until_available,
    server_settings
) -> typing.Tuple[ResolvedConnectConfig, ClientConfiguration]:

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

    connect_config = _parse_connect_dsn_and_args(
        dsn=dsn, credentials_file=credentials_file, host=host, port=port,
        database=database, user=user, password=password,
        tls_ca_file=tls_ca_file, tls_verify_hostname=tls_verify_hostname,
        server_settings=server_settings,
    )

    client_config = ClientConfiguration(
        connect_timeout=timeout,
        command_timeout=command_timeout,
        wait_until_available=wait_until_available or 0,
    )

    return connect_config, client_config


def check_alpn_protocol(ssl_obj):
    if ssl_obj.selected_alpn_protocol() != 'edgedb-binary':
        raise errors.ClientConnectionFailedError(
            "The server doesn't support the edgedb-binary protocol."
        )


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
