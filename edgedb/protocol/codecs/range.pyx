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


from edgedb.datatypes import range as range_mod


cdef uint8_t RANGE_EMPTY = 0x01
cdef uint8_t RANGE_LB_INC = 0x02
cdef uint8_t RANGE_UB_INC = 0x04
cdef uint8_t RANGE_LB_INF = 0x08
cdef uint8_t RANGE_UB_INF = 0x10


@cython.final
cdef class RangeCodec(BaseCodec):

    def __cinit__(self):
        self.sub_codec = None

    @staticmethod
    cdef BaseCodec new(bytes tid, BaseCodec sub_codec):
        cdef:
            RangeCodec codec

        codec = RangeCodec.__new__(RangeCodec)

        codec.tid = tid
        codec.name = 'Range'
        codec.sub_codec = sub_codec

        return codec

    cdef encode(self, WriteBuffer buf, object obj):
        cdef:
            uint8_t flags = 0
            WriteBuffer sub_data
            object lower = obj.lower
            object upper = obj.upper
            bint inc_lower = obj.inc_lower
            bint inc_upper = obj.inc_upper
            bint empty = obj.is_empty()

        if not isinstance(self.sub_codec, ScalarCodec):
            raise TypeError(
                'only scalar ranges are supported (got type {!r})'.format(
                    type(self.sub_codec).__name__
                )
            )

        if empty:
            flags |= RANGE_EMPTY
        else:
            if lower is None:
                flags |= RANGE_LB_INF
            elif inc_lower:
                flags |= RANGE_LB_INC
            if upper is None:
                flags |= RANGE_UB_INF
            elif inc_upper:
                flags |= RANGE_UB_INC

        sub_data = WriteBuffer.new()
        if lower is not None:
            try:
                self.sub_codec.encode(sub_data, lower)
            except TypeError as e:
                raise ValueError(
                    'invalid range lower bound: {}'.format(
                        e.args[0])) from None
        if upper is not None:
            try:
                self.sub_codec.encode(sub_data, upper)
            except TypeError as e:
                raise ValueError(
                    'invalid range upper bound: {}'.format(
                        e.args[0])) from None

        buf.write_int32(1 + sub_data.len())
        buf.write_byte(<int8_t>flags)
        buf.write_buffer(sub_data)

    cdef decode(self, FRBuffer *buf):
        cdef:
            uint8_t flags = <uint8_t>frb_read(buf, 1)[0]
            bint empty = (flags & RANGE_EMPTY) != 0
            bint inc_lower = (flags & RANGE_LB_INC) != 0
            bint inc_upper = (flags & RANGE_UB_INC) != 0
            bint has_lower = (flags & (RANGE_EMPTY | RANGE_LB_INF)) == 0
            bint has_upper = (flags & (RANGE_EMPTY | RANGE_UB_INF)) == 0
            object lower = None
            object upper = None
            int32_t sub_len
            FRBuffer sub_buf
            BaseCodec sub_codec = self.sub_codec

        if has_lower:
            sub_len = hton.unpack_int32(frb_read(buf, 4))
            if sub_len != -1:
                frb_slice_from(&sub_buf, buf, sub_len)
                lower = sub_codec.decode(&sub_buf)
                if frb_get_len(&sub_buf):
                    raise RuntimeError(
                        f'unexpected trailing data in buffer after '
                        f'range bound decoding: {frb_get_len(&sub_buf)}')

        if has_upper:
            sub_len = hton.unpack_int32(frb_read(buf, 4))
            if sub_len != -1:
                frb_slice_from(&sub_buf, buf, sub_len)
                upper = sub_codec.decode(&sub_buf)
                if frb_get_len(&sub_buf):
                    raise RuntimeError(
                        f'unexpected trailing data in buffer after '
                        f'range bound decoding: {frb_get_len(&sub_buf)}')

        return range_mod.Range(
            lower,
            upper,
            inc_lower=inc_lower,
            inc_upper=inc_upper,
            empty=empty,
        )

    cdef dump(self, int level = 0):
        return f'{level * " "}{self.name}\n{self.sub_codec.dump(level + 1)}'

    def make_type(self, describe_context):
        return describe.RangeType(
            desc_id=uuid.UUID(bytes=self.tid),
            name=None,
            value_type=self.sub_codec.make_type(describe_context),
        )
