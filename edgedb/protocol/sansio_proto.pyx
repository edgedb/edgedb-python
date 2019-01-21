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


cimport cython
cimport cpython

import asyncio
import collections
import json
import time

from edgedb.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,

    FRBuffer,
    frb_init,
    frb_read,
    frb_read_all,
    frb_slice_from,
    frb_check,
    frb_set_len,
    frb_get_len,
)

from edgedb.pgproto cimport pgproto
from edgedb.pgproto cimport hton

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

from . cimport datatypes
from . cimport cpythonx

from edgedb import errors

include "./consts.pxi"
include "./lru.pyx"
include "./codecs/codecs.pyx"


cdef class QueryCache:

    def __init__(self, *, cache_size=1000):
        self.queries = LRUMapping(maxsize=cache_size)

    cdef get(self, str query, bint json_mode):
        return self.queries.get((query, json_mode), None)

    cdef set(self, str query, bint json_mode, int32_t parse_flags,
             BaseCodec in_type, BaseCodec out_type):
        assert in_type is not None
        assert out_type is not None
        self.queries[(query, json_mode)] = (parse_flags, in_type, out_type)


cdef class SansIOProtocol:

    def __init__(self, addr, con_params):
        self.buffer = ReadBuffer()

        self.addr = addr
        self.con_params = con_params

        self.connected = False
        self.backend_secret = None

        self.xact_status = TRANS_UNKNOWN

    def is_in_transaction(self):
        return self.xact_status in (TRANS_INTRANS, TRANS_INERROR)

    cdef encode_args(self, BaseCodec in_dc, WriteBuffer buf, args, kwargs):
        if args and kwargs:
            raise RuntimeError(
                'either positional or named arguments are supported; '
                'not both')

        if type(in_dc) is EmptyTupleCodec:
            if args:
                raise RuntimeError('expected no positional arguments')
            if kwargs:
                raise RuntimeError('expected no named arguments')
            in_dc.encode(buf, ())
            return

        if kwargs:
            if type(in_dc) is not NamedTupleCodec:
                raise RuntimeError(
                    'expected positional arguments, got named arguments')

            (<NamedTupleCodec>in_dc).encode_kwargs(buf, kwargs)

        else:
            if type(in_dc) is not TupleCodec:
                raise RuntimeError(
                    'expected named arguments, got positional arguments')
            in_dc.encode(buf, args)

    cdef parse_describe_type_message(self, CodecsRegistry reg):
        assert self.buffer.get_message_type() == b'T'

        cdef:
            bytes type_id
            int16_t type_size
            bytes type_data
            int32_t parse_flags

        try:
            parse_flags = self.buffer.read_int32()

            type_id = self.buffer.read_bytes(16)
            type_size = self.buffer.read_int16()
            type_data = self.buffer.read_bytes(type_size)

            if reg.has_codec(type_id):
                in_dc = reg.get_codec(type_id)
            else:
                in_dc = reg.build_codec(type_data)

            type_id = self.buffer.read_bytes(16)
            type_size = self.buffer.read_int16()
            type_data = self.buffer.read_bytes(type_size)

            if reg.has_codec(type_id):
                out_dc = reg.get_codec(type_id)
            else:
                out_dc = reg.build_codec(type_data)
        finally:
            self.buffer.finish_message()

        return parse_flags, in_dc, out_dc

    cdef parse_data_messages(self, BaseCodec out_dc, result):
        cdef:
            ReadBuffer buf = self.buffer

            decode_row_method decoder = <decode_row_method>out_dc.decode
            pgproto.try_consume_message_method try_consume_message = \
                <pgproto.try_consume_message_method>buf.try_consume_message
            pgproto.take_message_type_method take_message_type = \
                <pgproto.take_message_type_method>buf.take_message_type

            const char* cbuf
            ssize_t cbuf_len
            object row

            FRBuffer _rbuf
            FRBuffer *rbuf = &_rbuf

        if PG_DEBUG:
            if buf.get_message_type() != b'D':
                raise RuntimeError('first message is not "D"')

            if not datatypes.EdgeSet_Check(result):
                raise RuntimeError(
                    f'result is not an edgedb.Set, but {result!r}')

        while take_message_type(buf, b'D'):
            cbuf = try_consume_message(buf, &cbuf_len)
            if cbuf == NULL:
                mem = buf.consume_message()
                cbuf = cpython.PyBytes_AS_STRING(mem)
                cbuf_len = cpython.PyBytes_GET_SIZE(mem)

            if PG_DEBUG:
                frb_init(rbuf, cbuf, cbuf_len)

                flen = hton.unpack_int16(frb_read(rbuf, 2))
                if flen != 1:
                    raise RuntimeError(
                        f'invalid number of columns: expected 1 got {flen}')

                buflen = hton.unpack_int32(frb_read(rbuf, 4))
                if frb_get_len(rbuf) != buflen:
                    raise RuntimeError('invalid buffer length')
            else:
                # EdgeDB returns rows with one column; Postgres' rows
                # are encoded as follows:
                #   2 bytes - int16 - number of columns
                #   4 bytes - int32 - every column is prefixed with its length
                # so we want to skip first 6 bytes:
                frb_init(rbuf, cbuf + 6, cbuf_len - 6)

            row = decoder(out_dc, rbuf)
            datatypes.EdgeSet_AppendItem(result, row)

    cdef parse_sync_message(self):
        cdef char status

        assert self.buffer.get_message_type() == b'Z'

        status = self.buffer.read_byte()

        if status == b'I':
            self.xact_status = TRANS_IDLE
        elif status == b'T':
            self.xact_status = TRANS_INTRANS
        elif status == b'E':
            self.xact_status = TRANS_INERROR
        else:
            self.xact_status = TRANS_UNKNOWN

        self.buffer.finish_message()

    cdef parse_error_message(self):
        cdef:
            uint32_t code
            str msg

        assert self.buffer.get_message_type() == b'E'

        code = <uint32_t>self.buffer.read_int32()
        msg = self.buffer.read_utf8()
        attrs = {}

        k = self.buffer.read_byte()
        while k != 0:
            v = self.buffer.read_utf8()
            attrs[chr(k)] = v
            k = self.buffer.read_byte()

        exc = errors.EdgeDBError._from_code(code, msg)
        exc._attrs = attrs
        return exc


## etc

cdef bytes SYNC_MESSAGE = bytes(WriteBuffer.new_message(b'S').end_message())
cdef bytes FLUSH_MESSAGE = bytes(WriteBuffer.new_message(b'H').end_message())
