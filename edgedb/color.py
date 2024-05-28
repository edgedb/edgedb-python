import os
import sys
import warnings

COLOR = None


class Color:
    HEADER = ""
    BLUE = ""
    CYAN = ""
    GREEN = ""
    WARNING = ""
    FAIL = ""
    ENDC = ""
    BOLD = ""
    UNDERLINE = ""


def get_color() -> Color:
    global COLOR

    if COLOR is None:
        COLOR = Color()
        if type(USE_COLOR) is bool:
            use_color = USE_COLOR
        else:
            try:
                use_color = USE_COLOR()
            except Exception:
                use_color = False
        if use_color:
            COLOR.HEADER = '\033[95m'
            COLOR.BLUE = '\033[94m'
            COLOR.CYAN = '\033[96m'
            COLOR.GREEN = '\033[92m'
            COLOR.WARNING = '\033[93m'
            COLOR.FAIL = '\033[91m'
            COLOR.ENDC = '\033[0m'
            COLOR.BOLD = '\033[1m'
            COLOR.UNDERLINE = '\033[4m'

    return COLOR


try:
    USE_COLOR = {
        "default": lambda: sys.stderr.isatty(),
        "auto": lambda: sys.stderr.isatty(),
        "enabled": True,
        "disabled": False,
    }[
        os.getenv("EDGEDB_COLOR_OUTPUT", "default")
    ]
except KeyError:
    warnings.warn(
        "EDGEDB_COLOR_OUTPUT can only be one of: "
        "default, auto, enabled or disabled",
        stacklevel=1,
    )
    USE_COLOR = False
