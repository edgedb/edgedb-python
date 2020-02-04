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


@cython.final
cdef class EnumCodec(BaseCodec):

    cdef encode(self, WriteBuffer buf, object obj):
        if not isinstance(obj, (datatypes.EnumValue, str)):
            raise TypeError(
                f'a str or edgedb.EnumValue is expected as a valid '
                f'enum argument, got {type(obj).__name__}')
        pgproto.text_encode(DEFAULT_CODEC_CONTEXT, buf, str(obj))

    cdef decode(self, FRBuffer *buf):
        label = pgproto.text_decode(DEFAULT_CODEC_CONTEXT, buf)
        return datatypes.EnumValue(self.descriptor, label)

    @staticmethod
    cdef BaseCodec new(bytes tid, tuple enum_labels):
        cdef:
            EnumCodec codec

        codec = EnumCodec.__new__(EnumCodec)

        codec.tid = tid
        codec.name = 'Enum'
        codec.descriptor = datatypes.EnumDescriptor(
            pgproto.UUID(tid), enum_labels)

        return codec
