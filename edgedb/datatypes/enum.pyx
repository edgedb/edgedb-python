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

    cdef get_index(self):
        return self.desc.get_index(self)

    def __str__(self):
        return self.label

    def __repr__(self):
        return f'<EnumValue {self.label!r}>'

    property __tid__:
        def __get__(self):
            return self.desc.tid

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
