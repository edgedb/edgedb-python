import os
import pathlib
import typing
import json

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict

from . import platform


class RequiredCredentials(TypedDict, total=True):
    port: int
    user: str


class Credentials(RequiredCredentials, total=False):
    host: typing.Optional[str]
    password: typing.Optional[str]
    database: typing.Optional[str]
    tls_cert_data: typing.Optional[str]
    tls_verify_hostname: typing.Optional[bool]


def get_credentials_path(instance_name: str) -> pathlib.Path:
    return platform.search_config_dir("credentials", instance_name + ".json")


def read_credentials(path: os.PathLike) -> Credentials:
    try:
        with open(path, encoding='utf-8') as f:
            credentials = json.load(f)
        return validate_credentials(credentials)
    except Exception as e:
        raise RuntimeError(
            f"cannot read credentials at {path}"
        ) from e


def validate_credentials(data: dict) -> Credentials:
    port = data.get('port')
    if port is None:
        port = 5656
    if not isinstance(port, int) or port < 1 or port > 65535:
        raise ValueError("invalid `port` value")

    user = data.get('user')
    if user is None:
        raise ValueError("`user` key is required")
    if not isinstance(user, str):
        raise ValueError("`user` must be a string")

    result = {  # required keys
        "user": user,
        "port": port,
    }

    host = data.get('host')
    if host is not None:
        if not isinstance(host, str):
            raise ValueError("`host` must be a string")
        result['host'] = host

    database = data.get('database')
    if database is not None:
        if not isinstance(database, str):
            raise ValueError("`database` must be a string")
        result['database'] = database

    password = data.get('password')
    if password is not None:
        if not isinstance(password, str):
            raise ValueError("`password` must be a string")
        result['password'] = password

    cert_data = data.get('tls_cert_data')
    if cert_data is not None:
        if not isinstance(cert_data, str):
            raise ValueError("`tls_cert_data` must be a string")
        result['tls_cert_data'] = cert_data

    verify = data.get('tls_verify_hostname')
    if verify is not None:
        if not isinstance(verify, bool):
            raise ValueError("`tls_verify_hostname` must be a bool")
        result['tls_verify_hostname'] = verify

    return result
