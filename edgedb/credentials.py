import os
import typing
import json

try:
    from typing import TypedDict
except ImportError:
    from typing_extensions import TypedDict


class RequiredCredentials(TypedDict, total=True):
    port: int
    user: str


class Credentials(RequiredCredentials, total=False):
    host: typing.Optional[str]
    password: typing.Optional[str]
    database: typing.Optional[str]


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

    return result
