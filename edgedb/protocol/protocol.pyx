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
import types

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
from edgedb.pgproto.pgproto import UUID

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

from . cimport datatypes
from . cimport cpythonx

from edgedb import errors

include "./consts.pxi"
include "./lru.pyx"
include "./codecs/codecs.pyx"


cdef class QueryCodecsCache:

    def __init__(self, *, cache_size=1000):
        self.queries = LRUMapping(maxsize=cache_size)

    cdef get(self, str query, bint json_mode):
        return self.queries.get((query, json_mode), None)

    cdef set(self, str query, bint json_mode,
             bint has_na_cardinality, BaseCodec in_type, BaseCodec out_type):
        assert in_type is not None
        assert out_type is not None
        self.queries[(query, json_mode)] = (
            has_na_cardinality, in_type, out_type
        )


cdef class SansIOProtocol:

    def __init__(self, con_params):
        self.buffer = ReadBuffer()

        self.con_params = con_params

        self.connected = False
        self.backend_secret = None

        self.xact_status = TRANS_UNKNOWN

        self.server_settings = {}
        self.reset_status()

    cdef reset_status(self):
        self.last_status = None
        self.last_details = None

    def get_settings(self):
        return types.MappingProxyType(self.server_settings)

    def is_in_transaction(self):
        return self.xact_status in (TRANS_INTRANS, TRANS_INERROR)

    cpdef abort(self):
        raise NotImplementedError

    cdef write(self, WriteBuffer buf):
        raise NotImplementedError

    async def wait_for_message(self):
        raise NotImplementedError

    async def wait_for_connect(self):
        raise NotImplementedError

    async def _parse(self, CodecsRegistry reg, str query, bint json_mode,
                     bint expect_one):
        cdef:
            WriteBuffer buf
            char mtype
            BaseCodec in_dc = None
            BaseCodec out_dc = None
            int16_t type_size
            bytes in_type_id
            bytes out_type_id
            bytes cardinality

        if not self.connected:
            raise RuntimeError('not connected')

        buf = WriteBuffer.new_message(b'P')
        buf.write_byte(b'j' if json_mode else b'b')
        buf.write_byte(b'o' if expect_one else b'm')
        buf.write_utf8('')  # stmt_name
        buf.write_utf8(query)
        buf.end_message()
        buf.write_bytes(SYNC_MESSAGE)
        self.write(buf)

        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'1':
                    cardinality = self.buffer.read_byte()
                    in_type_id = self.buffer.read_bytes(16)
                    out_type_id = self.buffer.read_bytes(16)

                elif mtype == b'E':
                    # ErrorResponse
                    exc = self.parse_error_message()
                    exc = self.amend_parse_error(
                        exc, json_mode, expect_one)

                elif mtype == b'Z':
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()
            finally:
                self.buffer.finish_message()

        if exc is not None:
            raise exc

        if reg.has_codec(in_type_id):
            in_dc = reg.get_codec(in_type_id)
        if reg.has_codec(out_type_id):
            out_dc = reg.get_codec(out_type_id)

        if in_dc is None or out_dc is None:
            buf = WriteBuffer.new_message(b'D')
            buf.write_byte(b'T')
            buf.write_utf8('')  # stmt_name
            buf.end_message()
            buf.write_bytes(SYNC_MESSAGE)
            self.write(buf)

            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                mtype = self.buffer.get_message_type()

                try:
                    if mtype == b'T':
                        cardinality, in_dc, out_dc = \
                            self.parse_describe_type_message(reg)

                    elif mtype == b'E':
                        # ErrorResponse
                        exc = self.parse_error_message()
                        exc = self.amend_parse_error(
                            exc, json_mode, expect_one)

                    elif mtype == b'Z':
                        self.parse_sync_message()
                        break

                    else:
                        self.fallthrough()

                finally:
                    self.buffer.finish_message()

        if exc is not None:
            raise exc

        if expect_one and cardinality == b'n':  # cardinality==N/A
            methname = 'fetchone_json' if json_mode else 'fetchone'
            raise errors.InterfaceError(
                f'query cannot be executed with {methname}() as it '
                f'does not return any data')

        return cardinality, in_dc, out_dc

    async def _execute(self, BaseCodec in_dc, BaseCodec out_dc, args, kwargs):
        cdef:
            WriteBuffer packet
            WriteBuffer buf
            char mtype

        if not self.connected:
            raise RuntimeError('not connected')
        self.reset_status()

        packet = WriteBuffer.new()

        buf = WriteBuffer.new_message(b'E')
        buf.write_utf8('')  # stmt_name
        self.encode_args(in_dc, buf, args, kwargs)
        packet.write_buffer(buf.end_message())

        packet.write_bytes(SYNC_MESSAGE)
        self.write(packet)

        result = datatypes.EdgeSet_New(0)

        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'D':
                    if exc is None:
                        try:
                            self.parse_data_messages(out_dc, result)
                        except Exception as ex:
                            # An error during data decoding.  We need to
                            # handle this as gracefully as possible:
                            # * save the exception to raise it once SYNC is
                            #   received;
                            # * ignore all 'D' messages for this query.
                            exc = ex
                            # Take care of a partially consumed 'D' message
                            # (if any).
                            if self.buffer.take_message():
                                if self.buffer.get_message_type() == b'D':
                                    self.buffer.discard_message()
                                else:
                                    self.buffer.put_message()
                    else:
                        self.buffer.discard_message()

                elif mtype == b'C':  # CommandComplete
                    self.parse_command_complete_message()

                elif mtype == b'E':
                    # ErrorResponse
                    exc = self.parse_error_message()

                elif mtype == b'Z':
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

        if exc is not None:
            raise exc

        return result

    async def _opportunistic_execute(self, CodecsRegistry reg,
                                     QueryCodecsCache qc,
                                     bint json_mode,
                                     bint expect_one,
                                     BaseCodec in_dc, BaseCodec out_dc,
                                     str query, args, kwargs):
        cdef:
            WriteBuffer packet
            WriteBuffer buf
            char mtype
            bint re_exec
            object result
            bytes new_cardinality = None

        buf = WriteBuffer.new_message(b'O')
        buf.write_byte(b'j' if json_mode else b'b')
        buf.write_byte(b'o' if expect_one else b'm')
        buf.write_utf8(query)
        buf.write_bytes(in_dc.get_tid())
        buf.write_bytes(out_dc.get_tid())
        self.encode_args(in_dc, buf, args, kwargs)
        buf.end_message()

        packet = WriteBuffer.new()
        packet.write_buffer(buf)
        packet.write_bytes(SYNC_MESSAGE)
        self.write(packet)

        result = datatypes.EdgeSet_New(0)
        re_exec = False
        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'T':
                    # our in/out type spec is out-dated
                    new_cardinality, in_dc, out_dc = \
                        self.parse_describe_type_message(reg)
                    qc.set(
                        query, json_mode,
                        new_cardinality == b'n', in_dc, out_dc)
                    re_exec = True

                elif mtype == b'D':
                    assert not re_exec
                    self.parse_data_messages(out_dc, result)

                elif mtype == b'C':  # CommandComplete
                    self.parse_command_complete_message()

                elif mtype == b'E':
                    # ErrorResponse
                    exc = self.parse_error_message()
                    exc = self.amend_parse_error(
                        exc, json_mode, expect_one)

                elif mtype == b'Z':
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

        if exc is not None:
            raise exc

        if re_exec:
            assert new_cardinality is not None
            if expect_one and new_cardinality == b'n':  # cardinality==N/A
                methname = 'fetchone_json' if json_mode else 'fetchone'
                raise errors.InterfaceError(
                    f'query cannot be executed with {methname}() as it '
                    f'does not return any data')
            return await self._execute(in_dc, out_dc, args, kwargs)
        else:
            return result

    async def simple_query(self, str query):
        cdef:
            WriteBuffer buf
            char mtype

        if not self.connected:
            raise RuntimeError('not connected')
        self.reset_status()

        buf = WriteBuffer.new_message(b'Q')
        buf.write_utf8(query)
        self.write(buf.end_message())

        exc = None

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'C':
                    # CommandComplete
                    self.parse_command_complete_message()

                elif mtype == b'E':
                    # ErrorResponse
                    exc = self.parse_error_message()

                elif mtype == b'Z':
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

        if exc is not None:
            raise exc

    async def execute_anonymous(self, bint expect_one,
                                bint json_mode,
                                CodecsRegistry reg, QueryCodecsCache qc,
                                str query, args, kwargs):
        cdef:
            BaseCodec in_dc
            BaseCodec out_dc
            bint cached

        if not self.connected:
            raise RuntimeError('not connected')
        self.reset_status()

        cached = True
        codecs = qc.get(query, json_mode)
        if codecs is None:
            cached = False

            codecs = await self._parse(reg, query, json_mode, expect_one)

            cardinality = codecs[0]
            in_dc = <BaseCodec>codecs[1]
            out_dc = <BaseCodec>codecs[2]

            if not cached:
                qc.set(query, json_mode, cardinality == b'n', in_dc, out_dc)

            ret = await self._execute(in_dc, out_dc, args, kwargs)

        else:
            has_na_cardinality = codecs[0]
            in_dc = <BaseCodec>codecs[1]
            out_dc = <BaseCodec>codecs[2]

            if expect_one and has_na_cardinality:
                methname = 'fetchone_json' if json_mode else 'fetchone'
                raise errors.InterfaceError(
                    f'query cannot be executed with {methname}() as it '
                    f'does not return any data')

            ret = await self._opportunistic_execute(
                reg, qc, json_mode, expect_one,
                in_dc, out_dc, query, args, kwargs)

        if expect_one:
            if ret:
                return ret[0]
            else:
                methname = 'fetchone_json' if json_mode else 'fetchone'
                raise errors.NoDataError(
                    f'query executed via {methname}() returned no data')
        else:
            if ret:
                if json_mode:
                    return ret[0]
                else:
                    return ret
            else:
                if json_mode:
                    return '[]'
                else:
                    return ret

    async def connect(self):
        cdef:
            WriteBuffer ver_buf
            WriteBuffer msg_buf
            WriteBuffer buf
            char mtype
            int32_t status

        await self.wait_for_connect()

        if self.connected:
            raise RuntimeError('already connected')

        # protocol version
        ver_buf = WriteBuffer()
        ver_buf.write_int16(1)
        ver_buf.write_int16(0)

        msg_buf = WriteBuffer.new_message(b'0')
        msg_buf.write_utf8(self.con_params.user or '')
        msg_buf.write_utf8(self.con_params.password or '')
        msg_buf.write_utf8(self.con_params.database or '')
        msg_buf.end_message()

        buf = WriteBuffer()
        buf.write_buffer(ver_buf)
        buf.write_buffer(msg_buf)
        self.write(buf)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == b'Y':
                self.buffer.discard_message()

            elif mtype == b'R':
                # Authentication...
                status = self.buffer.read_int32()
                if status != 0:
                    self.abort()
                    raise RuntimeError(
                        f'unsupported authentication method requested by the '
                        f'server: {status}')

            elif mtype == b'K':
                # BackendKeyData
                self.backend_secret = self.buffer.read_int32()

            elif mtype == b'E':
                # ErrorResponse
                raise self.parse_error_message()

            elif mtype == b'Z':
                # ReadyForQuery
                self.parse_sync_message()
                if self.xact_status == TRANS_IDLE:
                    self.connected = True
                    return
                else:
                    raise RuntimeError('non-idle status after connect')

            else:
                self.fallthrough()

            self.buffer.finish_message()

    cdef fallthrough(self):
        cdef:
            char mtype = self.buffer.get_message_type()

        if mtype == b'S':
            name = self.buffer.read_utf8()
            val = self.buffer.read_utf8()
            self.buffer.finish_message()
            self.server_settings[name] = val
            return

        # TODO:
        # * handle Notice and ServerStatus messages here

        raise RuntimeError(
            f'unexpected message type {chr(mtype)!r}')

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
            bytes cardinality

        try:
            cardinality = self.buffer.read_byte()

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

        return cardinality, in_dc, out_dc

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

    cdef parse_command_complete_message(self):
        assert self.buffer.get_message_type() == b'C'
        self.last_status = self.buffer.read_null_str()
        self.last_details = self.buffer.read_null_str()
        self.buffer.finish_message()

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

    cdef amend_parse_error(self, exc, bint json_mode, bint expect_one):
        if expect_one and exc.get_code() == result_cardinality_mismatch_code:
            methname = 'fetchone_json' if json_mode else 'fetchone'
            new_exc = errors.InterfaceError(
                f'query cannot be executed with {methname}() as it '
                f'returns a multiset')
            new_exc.__cause__ = exc
            exc = new_exc

        return exc

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

cdef result_cardinality_mismatch_code = \
    errors.ResultCardinalityMismatchError._code

cdef bytes SYNC_MESSAGE = bytes(WriteBuffer.new_message(b'S').end_message())
cdef bytes FLUSH_MESSAGE = bytes(WriteBuffer.new_message(b'H').end_message())


## Other exports

_RecordDescriptor = datatypes.EdgeRecordDesc_InitType()
Tuple = datatypes.EdgeTuple_InitType()
NamedTuple = datatypes.EdgeNamedTuple_InitType()
Object = datatypes.EdgeObject_InitType()
Set = datatypes.EdgeSet_InitType()
Array = datatypes.EdgeArray_InitType()
Link = datatypes.EdgeLink_InitType()
LinkSet = datatypes.EdgeLinkSet_InitType()


_EDGE_POINTER_IS_IMPLICIT = datatypes.EDGE_POINTER_IS_IMPLICIT
_EDGE_POINTER_IS_LINKPROP = datatypes.EDGE_POINTER_IS_LINKPROP
_EDGE_POINTER_IS_LINK = datatypes.EDGE_POINTER_IS_LINK


def get_object_descriptor(obj):
    return datatypes.EdgeObject_GetRecordDesc(obj)


def create_object_factory(**pointers):
    flags = ()
    names = ()
    for pname, ptype in pointers.items():
        names += (pname,)

        if not isinstance(ptype, set):
            ptype = {ptype}

        flag = 0
        for pt in ptype:
            if pt == 'link':
                flag |= datatypes.EDGE_POINTER_IS_LINK
            elif pt == 'property':
                pass
            elif pt == 'link-property':
                flag |= datatypes.EDGE_POINTER_IS_LINKPROP
            elif pt == 'implicit':
                flag |= datatypes.EDGE_POINTER_IS_IMPLICIT
            else:
                raise ValueError(f'unknown pointer type {pt}')

        flags += (flag,)

    desc = datatypes.EdgeRecordDesc_New(names, flags)
    size = len(pointers)

    def factory(*items):
        if len(items) != size:
            raise ValueError

        o = datatypes.EdgeObject_New(desc)
        for i in range(size):
            datatypes.EdgeObject_SetItem(o, i, items[i])

        return o

    return factory
