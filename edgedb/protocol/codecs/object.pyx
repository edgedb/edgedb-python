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
cdef class ObjectCodec(BaseNamedRecordCodec):

    cdef encode(self, WriteBuffer buf, object obj):
        raise NotImplementedError

    cdef decode(self, FRBuffer *buf):
        cdef:
            object result
            Py_ssize_t elem_count
            Py_ssize_t i
            int32_t elem_len
            BaseCodec elem_codec
            FRBuffer elem_buf

        elem_count = <Py_ssize_t><uint32_t>hton.unpack_int32(frb_read(buf, 4))

        if elem_count != len(self.fields_codecs):
            raise RuntimeError(
                f'cannot decode Object: expected {len(self.fields_codecs)} '
                f'elements, got {elem_count}')

        result = datatypes.object_new(self.descriptor)

        for i in range(elem_count):
            frb_read(buf, 4)  # reserved
            elem_len = hton.unpack_int32(frb_read(buf, 4))

            if elem_len == -1:
                elem = None
            else:
                elem_codec = <BaseCodec>self.fields_codecs[i]
                elem = elem_codec.decode(
                    frb_slice_from(&elem_buf, buf, elem_len))
                if frb_get_len(&elem_buf):
                    raise RuntimeError(
                        f'unexpected trailing data in buffer after '
                        f'object element decoding: {frb_get_len(&elem_buf)}')

            datatypes.object_set(result, i, elem)

        return result

    @staticmethod
    cdef BaseCodec new(bytes tid, tuple names, tuple flags, tuple codecs):
        cdef:
            ObjectCodec codec

        codec = ObjectCodec.__new__(ObjectCodec)

        codec.tid = tid
        codec.name = 'Object'
        codec.descriptor = datatypes.record_desc_new(names, flags)
        codec.fields_codecs = codecs

        return codec
