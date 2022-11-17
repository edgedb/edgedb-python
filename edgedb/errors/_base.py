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


import io
import os
import sys
import unicodedata

__all__ = (
    'EdgeDBError', 'EdgeDBMessage',
)


class Meta(type):

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)

        code = dct.get('_code')
        if code is not None:
            mcls._index[code] = cls

            # If it's a base class add it to the base class index
            b1, b2, b3, b4 = _decode(code)
            if b1 == 0 or b2 == 0 or b3 == 0 or b4 == 0:
                mcls._base_class_index[(b1, b2, b3, b4)] = cls

        return cls


class EdgeDBMessageMeta(Meta):

    _base_class_index = {}
    _index = {}


class EdgeDBMessage(Warning, metaclass=EdgeDBMessageMeta):

    _code = None

    def __init__(self, severity, message):
        super().__init__(message)
        self._severity = severity

    def get_severity(self):
        return self._severity

    def get_severity_name(self):
        return _severity_name(self._severity)

    def get_code(self):
        return self._code

    @staticmethod
    def _from_code(code, severity, message, *args, **kwargs):
        cls = _lookup_message_cls(code)
        exc = cls(severity, message, *args, **kwargs)
        exc._code = code
        return exc


class EdgeDBErrorMeta(Meta):

    _base_class_index = {}
    _index = {}


class EdgeDBError(Exception, metaclass=EdgeDBErrorMeta):

    _code = None
    _query = None
    tags = frozenset()

    def __init__(self, *args, **kwargs):
        self._attrs = {}
        super().__init__(*args, **kwargs)

    def has_tag(self, tag):
        return tag in self.tags

    @property
    def _position(self):
        # not a stable API method
        return int(self._read_str_field(FIELD_POSITION_START, -1))

    @property
    def _position_start(self):
        # not a stable API method
        return int(self._read_str_field(FIELD_CHARACTER_START, -1))

    @property
    def _position_end(self):
        # not a stable API method
        return int(self._read_str_field(FIELD_CHARACTER_END, -1))

    @property
    def _line(self):
        # not a stable API method
        return int(self._read_str_field(FIELD_LINE_START, -1))

    @property
    def _col(self):
        # not a stable API method
        return int(self._read_str_field(FIELD_COLUMN_START, -1))

    @property
    def _hint(self):
        # not a stable API method
        return self._read_str_field(FIELD_HINT)

    def _read_str_field(self, key, default=None):
        val = self._attrs.get(key)
        if val:
            return val.decode('utf-8')
        return default

    def get_code(self):
        return self._code

    def get_server_context(self):
        return self._read_str_field(FIELD_SERVER_TRACEBACK)

    @staticmethod
    def _from_code(code, *args, **kwargs):
        cls = _lookup_error_cls(code)
        exc = cls(*args, **kwargs)
        exc._code = code
        return exc

    def __str__(self):
        msg = super().__str__()
        if SHOW_HINT and self._query and self._position_start >= 0:
            return _format_error(
                msg,
                self._query,
                self._position_start,
                max(1, self._position_end - self._position_start),
                self._line if self._line > 0 else "?",
                self._col if self._col > 0 else "?",
                self._hint or "error",
            )
        else:
            return msg


def _lookup_cls(code: int, *, meta: type, default: type):
    try:
        return meta._index[code]
    except KeyError:
        pass

    b1, b2, b3, _ = _decode(code)

    try:
        return meta._base_class_index[(b1, b2, b3, 0)]
    except KeyError:
        pass
    try:
        return meta._base_class_index[(b1, b2, 0, 0)]
    except KeyError:
        pass
    try:
        return meta._base_class_index[(b1, 0, 0, 0)]
    except KeyError:
        pass

    return default


def _lookup_error_cls(code: int):
    return _lookup_cls(code, meta=EdgeDBErrorMeta, default=EdgeDBError)


def _lookup_message_cls(code: int):
    return _lookup_cls(code, meta=EdgeDBMessageMeta, default=EdgeDBMessage)


def _decode(code: int):
    return tuple(code.to_bytes(4, 'big'))


def _severity_name(severity):
    if severity <= EDGE_SEVERITY_DEBUG:
        return 'DEBUG'
    if severity <= EDGE_SEVERITY_INFO:
        return 'INFO'
    if severity <= EDGE_SEVERITY_NOTICE:
        return 'NOTICE'
    if severity <= EDGE_SEVERITY_WARNING:
        return 'WARNING'
    if severity <= EDGE_SEVERITY_ERROR:
        return 'ERROR'
    if severity <= EDGE_SEVERITY_FATAL:
        return 'FATAL'
    return 'PANIC'


def _format_error(msg, query, start, offset, line, col, hint):
    _init_colors()
    rv = io.StringIO()
    rv.write(f"{BOLD}{msg}{ENDC}{LINESEP}")
    lines = query.splitlines(keepends=True)
    num_len = len(str(len(lines)))
    rv.write(f"{BLUE}{'':>{num_len}} ┌─{ENDC} query:{line}:{col}{LINESEP}")
    rv.write(f"{BLUE}{'':>{num_len}} │ {ENDC}{LINESEP}")
    for num, line in enumerate(lines):
        length = len(line)
        line = line.rstrip()  # we'll use our own line separator
        if start >= length:
            # skip lines before the error
            start -= length
            continue

        if start >= 0:
            # Error starts in current line, write the line before the error
            first_half = repr(line[:start])[1:-1]
            line = line[start:]
            length -= start
            rv.write(f"{BLUE}{num + 1:>{num_len}} │   {ENDC}{first_half}")
            start = _unicode_width(first_half)
        else:
            # Multi-line error continues
            rv.write(f"{BLUE}{num + 1:>{num_len}} │ {FAIL}│ {ENDC}")

        if offset > length:
            # Error is ending beyond current line
            line = repr(line)[1:-1]
            rv.write(f"{FAIL}{line}{ENDC}{LINESEP}")
            if start >= 0:
                # Multi-line error starts
                rv.write(f"{BLUE}{'':>{num_len}} │ "
                         f"{FAIL}╭─{'─' * start}^{ENDC}{LINESEP}")
            offset -= length
            start = -1  # mark multi-line
        else:
            # Error is ending within current line
            first_half = repr(line[:offset])[1:-1]
            line = repr(line[offset:])[1:-1]
            rv.write(f"{FAIL}{first_half}{ENDC}{line}{LINESEP}")
            size = _unicode_width(first_half)
            if start >= 0:
                # Mark single-line error
                rv.write(f"{BLUE}{'':>{num_len}} │   {' ' * start}"
                         f"{FAIL}{'^' * size} {hint}{ENDC}")
            else:
                # End of multi-line error
                rv.write(f"{BLUE}{'':>{num_len}} │ "
                         f"{FAIL}╰─{'─' * (size - 1)}^ {hint}{ENDC}")
            break
    return rv.getvalue()


def _unicode_width(text):
    return sum(
        2 if unicodedata.east_asian_width(c) == "W" else 1
        for c in unicodedata.normalize("NFC", text)
    )


def _init_colors():
    global HEADER, BLUE, CYAN, GREEN, WARNING, FAIL, ENDC, BOLD, UNDERLINE
    global COLOR_INITIALIZED
    if COLOR_INITIALIZED:
        return
    COLOR_INITIALIZED = True
    try:
        use_color = os.getenv(
            "EDGEDB_PRETTY_ERROR", str(sys.stderr.isatty())
        ).lower() in ENV_ON_FLAGS
    except Exception:
        use_color = False
    if use_color:
        HEADER = '\033[95m'
        BLUE = '\033[94m'
        CYAN = '\033[96m'
        GREEN = '\033[92m'
        WARNING = '\033[93m'
        FAIL = '\033[91m'
        ENDC = '\033[0m'
        BOLD = '\033[1m'
        UNDERLINE = '\033[4m'
    else:
        HEADER = BLUE = CYAN = GREEN = WARNING = ""
        FAIL = ENDC = BOLD = UNDERLINE = ""


FIELD_HINT = 0x_00_01
FIELD_DETAILS = 0x_00_02
FIELD_SERVER_TRACEBACK = 0x_01_01

# XXX: Subject to be changed/deprecated.
FIELD_POSITION_START = 0x_FF_F1
FIELD_POSITION_END = 0x_FF_F2
FIELD_LINE_START = 0x_FF_F3
FIELD_COLUMN_START = 0x_FF_F4
FIELD_UTF16_COLUMN_START = 0x_FF_F5
FIELD_LINE_END = 0x_FF_F6
FIELD_COLUMN_END = 0x_FF_F7
FIELD_UTF16_COLUMN_END = 0x_FF_F8
FIELD_CHARACTER_START = 0x_FF_F9
FIELD_CHARACTER_END = 0x_FF_FA


EDGE_SEVERITY_DEBUG = 20
EDGE_SEVERITY_INFO = 40
EDGE_SEVERITY_NOTICE = 60
EDGE_SEVERITY_WARNING = 80
EDGE_SEVERITY_ERROR = 120
EDGE_SEVERITY_FATAL = 200
EDGE_SEVERITY_PANIC = 255


LINESEP = os.linesep
ENV_ON_FLAGS = {"1", "yes", "y", "true", "t", "on"}
SHOW_HINT = os.getenv("EDGEDB_ERROR_HINT", "1").lower() in ENV_ON_FLAGS
COLOR_INITIALIZED = False
