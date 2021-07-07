import os
import pathlib
import sys
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


if sys.platform == "darwin":
    def config_dir() -> pathlib.Path:
        return (
            pathlib.Path.home() / "Library" / "Application Support" / "edgedb"
        )
elif sys.platform == "win32":
    import ctypes
    from ctypes import windll

    def config_dir() -> pathlib.Path:
        path_buf = ctypes.create_unicode_buffer(255)
        csidl = 28  # CSIDL_LOCAL_APPDATA
        windll.shell32.SHGetFolderPathW(0, csidl, 0, 0, path_buf)
        return pathlib.Path(path_buf.value) / "EdgeDB" / "config"
else:
    def config_dir() -> pathlib.Path:
        xdg_conf_dir = pathlib.Path(os.environ.get("XDG_CONFIG_HOME", "."))
        if not xdg_conf_dir.is_absolute():
            xdg_conf_dir = pathlib.Path.home() / ".config"
        return xdg_conf_dir / "edgedb"


def get_credentials_path(instance_name: str) -> pathlib.Path:
    return config_dir() / "credentials" / (instance_name + ".json")


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
