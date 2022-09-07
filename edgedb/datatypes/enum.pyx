#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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

import enum


cdef class EnumDescriptor:

    def __init__(self, object tid, tuple labels):
        self.tid = tid
        index = {}
        for i, l in enumerate(labels):
            index[l] = i
        self.index = index
        self.labels = labels

    cdef get_index(self, EnumValue v):
        return self.index[v.label]


cdef class EnumValue:

    def __init__(self, EnumDescriptor desc, str label):
        self.desc = desc
        self.label = label
        self.name = label.upper()

    cdef get_index(self):
        return self.desc.get_index(self)

    def __str__(self):
        return self.label

    def __repr__(self):
        return f'<edgedb.EnumValue {self.label!r}>'

    property __tid__:
        def __get__(self):
            return self.desc.tid

    property value:
        def __get__(self):
            return self.label

    def __eq__(self, other):
        if not isinstance(other, EnumValue):
            return NotImplemented
        if self.desc.tid != (<EnumValue>other).desc.tid:
            return NotImplemented
        return self.label == (<EnumValue>other).label

    def __ne__(self, other):
        if not isinstance(other, EnumValue):
            return NotImplemented
        if self.desc.tid != (<EnumValue>other).desc.tid:
            return NotImplemented
        return self.label != (<EnumValue>other).label

    def __lt__(self, other):
        if not isinstance(other, EnumValue):
            return NotImplemented
        if self.desc.tid != (<EnumValue>other).desc.tid:
            return NotImplemented
        return self.get_index() < (<EnumValue>other).get_index()

    def __gt__(self, other):
        if not isinstance(other, EnumValue):
            return NotImplemented
        if self.desc.tid != (<EnumValue>other).desc.tid:
            return NotImplemented
        return self.get_index() > (<EnumValue>other).get_index()

    def __le__(self, other):
        if not isinstance(other, EnumValue):
            return NotImplemented
        if self.desc.tid != (<EnumValue>other).desc.tid:
            return NotImplemented
        return self.get_index() <= (<EnumValue>other).get_index()

    def __ge__(self, other):
        if not isinstance(other, EnumValue):
            return NotImplemented
        if self.desc.tid != (<EnumValue>other).desc.tid:
            return NotImplemented
        return self.get_index() >= (<EnumValue>other).get_index()

    def __hash__(self):
        return hash((self.desc.tid, self.label))

    def __reduce__(self):
        return (
            _restore_enum_value,
            (self.desc.tid, self.desc.labels, self.label),
        )

    def __reduce_ex__(self, protocol):
        return self.__reduce__()

    def __dir__(self):
        return [
            '__class__',
            '__dir__',
            '__doc__',
            '__eq__',
            '__ge__',
            '__gt__',
            '__hash__',
            '__le__',
            '__lt__',
            '__ne__',
            '__reduce__',
            '__reduce_ex__',
            '__repr__',
            '__str__',
            '__tid__'
            'name',
            'value',
        ]


def _restore_enum_value(tid, labels, label):
    return EnumValue(EnumDescriptor(tid, labels), label)


EdgeType_SetMro(EnumValue, (EnumValue, enum.Enum, object))
