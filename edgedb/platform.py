import os
import pathlib
import sys

if sys.platform == "darwin":
    def _config_dir() -> pathlib.Path:
        return (
            pathlib.Path.home() / "Library" / "Application Support" / "edgedb"
        )

    IS_WINDOWS = False

elif sys.platform == "win32":
    import ctypes
    from ctypes import windll

    def _config_dir() -> pathlib.Path:
        path_buf = ctypes.create_unicode_buffer(255)
        csidl = 28  # CSIDL_LOCAL_APPDATA
        windll.shell32.SHGetFolderPathW(0, csidl, 0, 0, path_buf)
        return pathlib.Path(path_buf.value) / "EdgeDB" / "config"

    IS_WINDOWS = True

else:
    def _config_dir() -> pathlib.Path:
        xdg_conf_dir = pathlib.Path(os.environ.get("XDG_CONFIG_HOME", "."))
        if not xdg_conf_dir.is_absolute():
            xdg_conf_dir = pathlib.Path.home() / ".config"
        return xdg_conf_dir / "edgedb"

    IS_WINDOWS = False


def config_dir() -> pathlib.Path:
    conf_dir = _config_dir()
    if not conf_dir.exists():
        conf_dir = pathlib.Path.home() / ".edgedb"
    return conf_dir
