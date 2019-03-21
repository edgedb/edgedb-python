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


__all__ = (
    'EdgeDBError',
)


class EdgeDBErrorMeta(type):

    _base_class_index = {}
    _index = {}

    def __new__(mcls, name, bases, dct):
        cls = super().__new__(mcls, name, bases, dct)

        code = dct.get('_code')
        if code is not None:
            EdgeDBErrorMeta._index[code] = cls

            # If it's a base class add it to the base class index
            b1, b2, b3, b4 = _decode(code)
            if b1 == 0 or b2 == 0 or b3 == 0 or b4 == 0:
                EdgeDBErrorMeta._base_class_index[(b1, b2, b3, b4)] = cls

        return cls


class EdgeDBError(Exception, metaclass=EdgeDBErrorMeta):

    _code = None

    def __init__(self, *args, **kwargs):
        self._attrs = {}
        super().__init__(*args, **kwargs)

    @property
    def _position(self):
        # not a stable API method
        return int(self._attrs.get('P', -1))

    @property
    def _line(self):
        # not a stable API method
        return int(self._attrs.get('L', -1))

    @property
    def _col(self):
        # not a stable API method
        return int(self._attrs.get('C', -1))

    @property
    def _hint(self):
        # not a stable API method
        return self._attrs.get('H')

    def get_code(self):
        return self._code

    def get_server_context(self):
        return self._attrs.get('T')

    @staticmethod
    def _from_code(code, *args, **kwargs):
        cls = _lookup_error_cls(code)
        exc = cls(*args, **kwargs)
        exc._code = code
        return exc


def _lookup_error_cls(code: int):
    try:
        return EdgeDBErrorMeta._index[code]
    except KeyError:
        pass

    b1, b2, b3, _ = _decode(code)

    try:
        return EdgeDBErrorMeta._base_class_index[(b1, b2, b3, 0)]
    except KeyError:
        pass
    try:
        return EdgeDBErrorMeta._base_class_index[(b1, b2, 0, 0)]
    except KeyError:
        pass
    try:
        return EdgeDBErrorMeta._base_class_index[(b1, 0, 0, 0)]
    except KeyError:
        pass

    return EdgeDBError


def _decode(code: int):
    return tuple(code.to_bytes(4, 'big'))
