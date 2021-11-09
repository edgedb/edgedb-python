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


from libc.stdint cimport int64_t


DEF KiB = 1024;
DEF MiB = 1024 * KiB;
DEF GiB = 1024 * MiB;
DEF TiB = 1024 * GiB;
DEF PiB = 1024 * TiB;


@cython.final
cdef class ConfigMemory:

    def __init__(self, int64_t bytes):
        self.bytes = bytes

    def __eq__(self, other):
        if type(other) is not ConfigMemory:
            return NotImplemented

        return self.bytes == (<ConfigMemory>other).bytes

    def __hash__(self):
        return hash((RelativeDuration, self.bytes))

    def __repr__(self):
        return f'<edgedb.ConfigMemory "{self}">'

    @cython.cdivision(True)
    def __str__(self):
        cdef:
            int64_t bytes = self.bytes

        if bytes >= PiB and bytes % PiB == 0:
            return f'{bytes // PiB}PiB'
        if bytes >= TiB and bytes % TiB == 0:
            return f'{bytes // TiB}TiB'
        if bytes >= GiB and bytes % GiB == 0:
            return f'{bytes // GiB}GiB'
        if bytes >= MiB and bytes % MiB == 0:
            return f'{bytes // MiB}MiB'
        if bytes >= KiB and bytes % KiB == 0:
            return f'{bytes // KiB}KiB'
        return f'{bytes}B'

    @property
    def kibibytes(self):
        return self.bytes / KiB

    @property
    def mebibytes(self):
        return self.bytes / MiB

    @property
    def gibibytes(self):
        return self.bytes / GiB

    @property
    def tebibytes(self):
        return self.bytes / TiB

    @property
    def pebibytes(self):
        return self.bytes / PiB


cdef new_config_memory(int64_t bytes):
    cdef ConfigMemory mem = ConfigMemory.__new__(ConfigMemory)

    mem.bytes = bytes

    return mem
