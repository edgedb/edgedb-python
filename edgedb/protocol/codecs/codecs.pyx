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


import decimal
import uuid


include "./edb_types.pxi"


include "./base.pyx"
include "./scalar.pyx"
include "./tuple.pyx"
include "./namedtuple.pyx"
include "./object.pyx"
include "./array.pyx"
include "./set.pyx"
include "./enum.pyx"


DEF CTYPE_SET = 0
DEF CTYPE_SHAPE = 1
DEF CTYPE_BASE_SCALAR = 2
DEF CTYPE_SCALAR = 3
DEF CTYPE_TUPLE = 4
DEF CTYPE_NAMEDTUPLE = 5
DEF CTYPE_ARRAY = 6
DEF CTYPE_ENUM = 7

DEF _CODECS_BUILD_CACHE_SIZE = 200


cdef BaseCodec NULL_CODEC = NullCodec.__new__(NullCodec)
cdef BaseCodec EMPTY_TUPLE_CODEC = EmptyTupleCodec.__new__(EmptyTupleCodec)


cdef class CodecsRegistry:

    def __init__(self, *, cache_size=1000):
        self.codecs_build_cache = LRUMapping(maxsize=_CODECS_BUILD_CACHE_SIZE)
        self.codecs = LRUMapping(maxsize=cache_size)

    cdef BaseCodec _build_codec(self, FRBuffer *spec, list codecs_list):
        cdef:
            uint8_t t = <uint8_t>(frb_read(spec, 1)[0])
            bytes tid = frb_read(spec, 16)[:16]
            uint16_t els
            uint16_t i
            uint16_t str_len
            uint16_t pos
            int32_t dim_len
            BaseCodec res
            BaseCodec sub_codec


        res = self.codecs.get(tid, None)
        if res is None:
            res = self.codecs_build_cache.get(tid, None)
        if res is not None:
            # We have a codec for this "tid"; advance the buffer
            # so that we can process the next codec.

            if t == CTYPE_SET:
                frb_read(spec, 2)

            elif t == CTYPE_SHAPE:
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for i in range(els):
                    frb_read(spec, 1)
                    str_len = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                    # read the <str> (`str_len` bytes) and <pos> (2 bytes)
                    frb_read(spec, str_len + 2)

            elif t == CTYPE_BASE_SCALAR:
                pass

            elif t == CTYPE_SCALAR:
                frb_read(spec, 2)

            elif t == CTYPE_TUPLE:
                # tuple
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for i in range(els):
                    frb_read(spec, 2)

            elif t == CTYPE_NAMEDTUPLE:
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for i in range(els):
                    str_len = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                    frb_read(spec, str_len + 2)

            elif t == CTYPE_ARRAY:
                frb_read(spec, 2)
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                if els != 1:
                    raise NotImplementedError(
                        'cannot handle arrays with more than one dimension')
                # First dimension length.
                frb_read(spec, 4)

            elif t == CTYPE_ENUM:
                els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                for i in range(els):
                    str_len = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                    frb_read(spec, str_len)

            elif (t >= 0xf0 and t <= 0xff):
                # Ignore all type annotations.
                str_len = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                frb_read(spec, str_len)

            else:
                raise NotImplementedError(
                    f'no codec implementation for EdgeDB data class {t}')

            return res

        if t == CTYPE_SET:
            pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            sub_codec = <BaseCodec>codecs_list[pos]
            res = SetCodec.new(tid, sub_codec)

        elif t == CTYPE_SHAPE:
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            codecs = cpython.PyTuple_New(els)
            names = cpython.PyTuple_New(els)
            flags = cpython.PyTuple_New(els)
            for i in range(els):
                flag = <uint8_t>frb_read(spec, 1)[0]

                str_len = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))

                cpython.Py_INCREF(name)
                cpython.PyTuple_SetItem(names, i, name)

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

                cpython.Py_INCREF(flag)
                cpython.PyTuple_SetItem(flags, i, flag)

            res = ObjectCodec.new(tid, names, flags, codecs)

        elif t == CTYPE_BASE_SCALAR:
            res = <BaseCodec>BASE_SCALAR_CODECS[tid]

        elif t == CTYPE_SCALAR:
            pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            codec = codecs_list[pos]
            if type(codec) is not ScalarCodec:
                raise RuntimeError(
                    f'a scalar codec expected for base scalar type, '
                    f'got {type(codec).__name__}')
            res = (<ScalarCodec>codecs_list[pos]).derive(tid)

        elif t == CTYPE_TUPLE:
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            codecs = cpython.PyTuple_New(els)
            for i in range(els):
                pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

            res = TupleCodec.new(tid, codecs)

        elif t == CTYPE_NAMEDTUPLE:
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            codecs = cpython.PyTuple_New(els)
            names = cpython.PyTuple_New(els)
            for i in range(els):
                str_len = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)
                pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))

                cpython.Py_INCREF(name)
                cpython.PyTuple_SetItem(names, i, name)

                sub_codec = codecs_list[pos]
                cpython.Py_INCREF(sub_codec)
                cpython.PyTuple_SetItem(codecs, i, sub_codec)

            res = NamedTupleCodec.new(tid, names, codecs)

        elif t == CTYPE_ENUM:
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            names = cpython.PyTuple_New(els)
            for i in range(els):
                str_len = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
                name = cpythonx.PyUnicode_FromStringAndSize(
                    frb_read(spec, str_len), str_len)

                cpython.Py_INCREF(name)
                cpython.PyTuple_SetItem(names, i, name)

            res = EnumCodec.new(tid, names)

        elif t == CTYPE_ARRAY:
            pos = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            els = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            if els != 1:
                raise NotImplementedError(
                    'cannot handle arrays with more than one dimension')
            # First dimension length.
            dim_len = hton.unpack_int32(frb_read(spec, 4))
            sub_codec = <BaseCodec>codecs_list[pos]
            res = ArrayCodec.new(tid, sub_codec, dim_len)

        elif (t >= 0xf0 and t <= 0xff):
            # Ignore all type annotations.
            str_len = <uint16_t>hton.unpack_int16(frb_read(spec, 2))
            frb_read(spec, str_len)
            return

        else:
            raise NotImplementedError(
                f'no codec implementation for EdgeDB data class {t}')

        self.codecs_build_cache[tid] = res
        return res

    cdef has_codec(self, bytes type_id):
        return (
            type_id in self.codecs or
            type_id in {NULL_CODEC_ID, EMPTY_TUPLE_CODEC_ID}
        )

    cdef BaseCodec get_codec(self, bytes type_id):
        try:
            return <BaseCodec>self.codecs[type_id]
        except KeyError:
            pass

        if type_id == NULL_CODEC_ID:
            return NULL_CODEC

        if type_id == EMPTY_TUPLE_CODEC_ID:
            return EMPTY_TUPLE_CODEC

        raise LookupError

    cdef BaseCodec build_codec(self, bytes spec):
        cdef:
            FRBuffer buf
            BaseCodec res
            list codecs_list

        frb_init(
            &buf,
            cpython.PyBytes_AsString(spec),
            cpython.Py_SIZE(spec))

        codecs_list = []
        while frb_get_len(&buf):
            res = self._build_codec(&buf, codecs_list)
            if res is None:
                continue
            codecs_list.append(res)
            self.codecs[res.tid] = res

        return res


cdef dict BASE_SCALAR_CODECS = {}


cdef register_base_scalar_codec(
        str name,
        pgproto.encode_func encoder,
        pgproto.decode_func decoder):

    cdef:
        BaseCodec codec

    tid = TYPE_IDS.get(name)
    if tid is None:
        raise RuntimeError(f'cannot find known ID for type {name!r}')
    tid = tid.bytes

    if tid in BASE_SCALAR_CODECS:
        raise RuntimeError(f'base scalar codec for {id} is already registered')

    codec = ScalarCodec.new(tid, name, encoder, decoder)
    BASE_SCALAR_CODECS[tid] = codec


cdef time_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    if getattr(obj, 'tzinfo', None) is not None:
        raise TypeError(
            f'a naive time object (tzinfo is None) was expected')

    pgproto.time_encode(settings, buf, obj)


cdef date_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    # Python `datetime.date` object does not have a `tzinfo` attribute
    # nor it is timezone-aware.  But since we're accepting duck types
    # let's ensure it doesn't have tzinfo anyways.
    if getattr(obj, 'tzinfo', None) is not None:
        raise TypeError(
            f'a naive date object (tzinfo is None) was expected')

    pgproto.date_encode(settings, buf, obj)


cdef timestamp_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    if not cpython.datetime.PyDateTime_Check(obj):
        raise TypeError(
            f'a datetime.datetime object was expected, got {obj!r}')

    if getattr(obj, 'tzinfo', None) is not None:
        raise TypeError(
            f'a naive datetime object (tzinfo is None) was expected')

    pgproto.timestamp_encode(settings, buf, obj)


cdef timestamptz_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    if not cpython.datetime.PyDateTime_Check(obj):
        raise TypeError(
            f'a datetime.datetime object was expected')

    if getattr(obj, 'tzinfo', None) is None:
        raise TypeError(
            f'a timezone-aware datetime object (tzinfo is not None) '
            f'was expected')

    pgproto.timestamptz_encode(settings, buf, obj)


cdef duration_encode(pgproto.CodecContext settings, WriteBuffer buf,
                     object obj):

    cdef datatypes.Duration dur

    if type(obj) is not datatypes.Duration:
        raise TypeError(
            f'an edgedb.Duration object was expected')

    dur = <datatypes.Duration>obj
    buf.write_int32(16)
    buf.write_int64(dur.microseconds)
    buf.write_int32(dur.days)
    buf.write_int32(dur.months)


cdef duration_decode(pgproto.CodecContext settings, FRBuffer *buf):
    cdef:
        int32_t days
        int32_t months
        int64_t microseconds

    microseconds = hton.unpack_int64(frb_read(buf, 8))
    days = hton.unpack_int32(frb_read(buf, 4))
    months = hton.unpack_int32(frb_read(buf, 4))

    return datatypes.new_duration(microseconds, days, months)


cdef decimal_decode(pgproto.CodecContext settings, FRBuffer *buf):
    return pgproto.numeric_decode_binary_ex(settings, buf, True)


cdef bigint_encode(pgproto.CodecContext settings, WriteBuffer buf, obj):
    if type(obj) is not int and not isinstance(obj, int):
        raise TypeError(
            f'expected an int'
        )

    pgproto.numeric_encode_binary(settings, buf, obj)


cdef bigint_decode(pgproto.CodecContext settings, FRBuffer *buf):
    dec = pgproto.numeric_decode_binary(settings, buf)
    return int(dec)


cdef register_base_scalar_codecs():
    register_base_scalar_codec(
        'std::uuid',
        pgproto.uuid_encode,
        pgproto.uuid_decode)

    register_base_scalar_codec(
        'std::str',
        pgproto.text_encode,
        pgproto.text_decode)

    register_base_scalar_codec(
        'std::bytes',
        pgproto.bytea_encode,
        pgproto.bytea_decode)

    register_base_scalar_codec(
        'std::int16',
        pgproto.int2_encode,
        pgproto.int2_decode)

    register_base_scalar_codec(
        'std::int32',
        pgproto.int4_encode,
        pgproto.int4_decode)

    register_base_scalar_codec(
        'std::int64',
        pgproto.int8_encode,
        pgproto.int8_decode)

    register_base_scalar_codec(
        'std::float32',
        pgproto.float4_encode,
        pgproto.float4_decode)

    register_base_scalar_codec(
        'std::float64',
        pgproto.float8_encode,
        pgproto.float8_decode)

    register_base_scalar_codec(
        'std::decimal',
        pgproto.numeric_encode_binary,
        decimal_decode)

    register_base_scalar_codec(
        'std::bigint',
        bigint_encode,
        bigint_decode)

    register_base_scalar_codec(
        'std::bool',
        pgproto.bool_encode,
        pgproto.bool_decode)

    register_base_scalar_codec(
        'std::datetime',
        timestamptz_encode,
        pgproto.timestamptz_decode)

    register_base_scalar_codec(
        'std::local_datetime',
        timestamp_encode,
        pgproto.timestamp_decode)

    register_base_scalar_codec(
        'std::local_date',
        date_encode,
        pgproto.date_decode)

    register_base_scalar_codec(
        'std::local_time',
        time_encode,
        pgproto.time_decode)

    register_base_scalar_codec(
        'std::duration',
        duration_encode,
        duration_decode)

    register_base_scalar_codec(
        'std::json',
        pgproto.jsonb_encode,
        pgproto.jsonb_decode)


register_base_scalar_codecs()
