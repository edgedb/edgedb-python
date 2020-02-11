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
cimport cpython.datetime

import asyncio
import collections
import datetime
import json
import time
import types
import weakref

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

from edgedb.pgproto import pgproto
from edgedb.pgproto cimport pgproto
from edgedb.pgproto cimport hton
from edgedb.pgproto.pgproto import UUID

from libc.stdint cimport int8_t, uint8_t, int16_t, uint16_t, \
                         int32_t, uint32_t, int64_t, uint64_t, \
                         UINT32_MAX

from edgedb.datatypes cimport datatypes
from . cimport cpythonx

from edgedb import errors
from edgedb import scram


include "./consts.pxi"
include "./lru.pyx"
include "./codecs/codecs.pyx"


cpython.datetime.import_datetime()


_FETCHONE_METHOD = {
    IoFormat.JSON: 'fetchone_json',
    IoFormat.JSON_ELEMENTS: '_fetchall_json_elements',
    IoFormat.BINARY: 'fetchone',
}


cdef class QueryCodecsCache:

    def __init__(self, *, cache_size=1000):
        self.queries = LRUMapping(maxsize=cache_size)

    cdef get(self, str query, IoFormat io_format):
        return self.queries.get((query, io_format), None)

    cdef set(self, str query, IoFormat io_format,
             bint has_na_cardinality, BaseCodec in_type, BaseCodec out_type):
        assert in_type is not None
        assert out_type is not None
        self.queries[(query, io_format)] = (
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
        self.protocol_version = (PROTO_VER_MAJOR, PROTO_VER_MINOR_MIN)

    cdef reset_status(self):
        self.last_status = None
        self.last_details = None

    def get_settings(self):
        return types.MappingProxyType(self.server_settings)

    def is_in_transaction(self):
        return self.xact_status in (TRANS_INTRANS, TRANS_INERROR)

    def set_connection(self, con):
        self.con = weakref.ref(con)

    cpdef abort(self):
        raise NotImplementedError

    cdef write(self, WriteBuffer buf):
        raise NotImplementedError

    async def wait_for_message(self):
        raise NotImplementedError

    async def try_recv_eagerly(self):
        # If there's data in the socket try reading it into the
        # buffer.  Needed for blocking-io connections.
        raise NotImplementedError

    async def wait_for_connect(self):
        raise NotImplementedError

    cdef inline reject_headers(self):
        # We don't send any headers and thus we don't expect any
        # headers from the server.
        cdef int16_t nheaders = self.buffer.read_int16()
        if nheaders != 0:
            raise errors.BinaryProtocolError('unexpected headers')

    cdef write_headers(self, buf: WriteBuffer, headers: dict):
        buf.write_int16(len(headers))
        for k, v in headers.items():
            buf.write_int16(<int16_t><uint16_t>k)
            buf.write_len_prefixed_utf8(str(v))

    async def _parse(
        self,
        query: str,
        *,
        reg: CodecsRegistry,
        io_format: IoFormat=IoFormat.BINARY,
        expect_one: bint=False,
        implicit_limit: int=0,
    ):
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

        buf = WriteBuffer.new_message(PREPARE_MSG)
        if implicit_limit:
            self.write_headers(
                buf,
                {QUERY_OPT_IMPLICIT_LIMIT: implicit_limit},
            )
        else:
            buf.write_int16(0)  # no headers
        buf.write_byte(io_format)
        buf.write_byte(CARDINALITY_ONE if expect_one else CARDINALITY_MANY)
        buf.write_len_prefixed_bytes(b'')  # stmt_name
        buf.write_len_prefixed_utf8(query)
        buf.end_message()
        buf.write_bytes(SYNC_MESSAGE)
        self.write(buf)

        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == PREPARE_COMPLETE_MSG:
                    self.reject_headers()
                    cardinality = self.buffer.read_byte()
                    in_type_id = self.buffer.read_bytes(16)
                    out_type_id = self.buffer.read_bytes(16)

                elif mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    exc = self._amend_parse_error(
                        exc, io_format, expect_one)

                elif mtype == READY_FOR_COMMAND_MSG:
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
            buf = WriteBuffer.new_message(DESCRIBE_STMT_MSG)
            buf.write_int16(0)  # no headers
            buf.write_byte(DESCRIBE_ASPECT_DATA)
            buf.write_len_prefixed_bytes(b'')  # stmt_name
            buf.end_message()
            buf.write_bytes(SYNC_MESSAGE)
            self.write(buf)

            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                mtype = self.buffer.get_message_type()

                try:
                    if mtype == STMT_DATA_DESC_MSG:
                        cardinality, in_dc, out_dc = \
                            self.parse_describe_type_message(reg)

                    elif mtype == ERROR_RESPONSE_MSG:
                        exc = self.parse_error_message()
                        exc = self._amend_parse_error(
                            exc, io_format, expect_one)

                    elif mtype == READY_FOR_COMMAND_MSG:
                        self.parse_sync_message()
                        break

                    else:
                        self.fallthrough()

                finally:
                    self.buffer.finish_message()

        if exc is not None:
            raise exc

        if expect_one and cardinality == CARDINALITY_NOT_APPLICABLE:
            methname = _FETCHONE_METHOD[io_format]
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

        buf = WriteBuffer.new_message(EXECUTE_MSG)
        buf.write_int16(0)  # no headers
        buf.write_len_prefixed_bytes(b'')  # stmt_name
        self.encode_args(in_dc, buf, args, kwargs)
        packet.write_buffer(buf.end_message())

        packet.write_bytes(SYNC_MESSAGE)
        self.write(packet)

        result = datatypes.set_new(0)

        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == DATA_MSG:
                    if exc is None:
                        try:
                            self.parse_data_messages(out_dc, result)
                        except Exception as ex:
                            # An error during data decoding.  We need to
                            # handle this as gracefully as possible:
                            # * save the exception to raise it once SYNC is
                            #   received;
                            # * ignore all 'D' messages for this query.
                            exc = errors.ClientError(
                                'unable to decode data to Python objects')
                            exc.__cause__ = ex
                            # Take care of a partially consumed 'D' message
                            # and the ones yet unparsed.
                            while self.buffer.take_message_type(DATA_MSG):
                                self.buffer.discard_message()
                    else:
                        self.buffer.discard_message()

                elif mtype == COMMAND_COMPLETE_MSG:
                    self.parse_command_complete_message()

                elif mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()

                elif mtype == READY_FOR_COMMAND_MSG:
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

        if exc is not None:
            raise exc

        return result

    async def _optimistic_execute(
        self,
        *,
        query: str,
        args,
        kwargs,
        reg: CodecsRegistry,
        qc: QueryCodecsCache,
        io_format: object,
        expect_one: bint,
        implicit_limit: int,
        in_dc: BaseCodec,
        out_dc: BaseCodec,
    ):
        cdef:
            WriteBuffer packet
            WriteBuffer buf
            char mtype
            bint re_exec
            object result
            bytes new_cardinality = None

        buf = WriteBuffer.new_message(OPTIMISTIC_EXECUTE_MSG)
        if implicit_limit:
            self.write_headers(
                buf,
                {QUERY_OPT_IMPLICIT_LIMIT: implicit_limit},
            )
        else:
            buf.write_int16(0)  # no headers
        buf.write_byte(io_format)
        buf.write_byte(CARDINALITY_ONE if expect_one else CARDINALITY_MANY)
        buf.write_len_prefixed_utf8(query)
        buf.write_bytes(in_dc.get_tid())
        buf.write_bytes(out_dc.get_tid())
        self.encode_args(in_dc, buf, args, kwargs)
        buf.end_message()

        packet = WriteBuffer.new()
        packet.write_buffer(buf)
        packet.write_bytes(SYNC_MESSAGE)
        self.write(packet)

        result = datatypes.set_new(0)
        re_exec = False
        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == STMT_DATA_DESC_MSG:
                    # our in/out type spec is out-dated
                    new_cardinality, in_dc, out_dc = \
                        self.parse_describe_type_message(reg)
                    qc.set(
                        query, io_format,
                        new_cardinality == CARDINALITY_NOT_APPLICABLE,
                        in_dc, out_dc)
                    re_exec = True

                elif mtype == DATA_MSG:
                    assert not re_exec
                    if exc is None:
                        try:
                            self.parse_data_messages(out_dc, result)
                        except Exception as ex:
                            # An error during data decoding.  We need to
                            # handle this as gracefully as possible:
                            # * save the exception to raise it once SYNC is
                            #   received;
                            # * ignore all 'D' messages for this query.
                            exc = errors.ClientError(
                                'unable to decode data to Python objects')
                            exc.__cause__ = ex
                            # Take care of a partially consumed 'D' message
                            # and the ones yet unparsed.
                            while self.buffer.take_message_type(DATA_MSG):
                                self.buffer.discard_message()
                    else:
                        self.buffer.discard_message()

                elif mtype == COMMAND_COMPLETE_MSG:
                    self.parse_command_complete_message()

                elif mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    exc = self._amend_parse_error(
                        exc, io_format, expect_one)

                elif mtype == READY_FOR_COMMAND_MSG:
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
            if expect_one and new_cardinality == CARDINALITY_NOT_APPLICABLE:
                methname = _FETCHONE_METHOD[io_format]
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

        buf = WriteBuffer.new_message(EXECUTE_SCRIPT_MSG)
        buf.write_int16(0)  # no headers
        buf.write_len_prefixed_utf8(query)
        self.write(buf.end_message())

        exc = None

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == COMMAND_COMPLETE_MSG:
                    self.parse_command_complete_message()

                elif mtype == ERROR_RESPONSE_MSG:
                    # ErrorResponse
                    exc = self.parse_error_message()

                elif mtype == READY_FOR_COMMAND_MSG:
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

        if exc is not None:
            raise exc

    async def execute_anonymous(
        self,
        *,
        query: str,
        args,
        kwargs,
        reg: CodecsRegistry,
        qc: QueryCodecsCache,
        io_format: object,
        expect_one: bint = False,
        implicit_limit: int = 0,
    ):
        cdef:
            BaseCodec in_dc
            BaseCodec out_dc
            bint cached

        if not self.connected:
            raise RuntimeError('not connected')
        self.reset_status()

        cached = True
        codecs = qc.get(query, io_format)
        if codecs is None:
            cached = False

            codecs = await self._parse(
                query,
                reg=reg,
                io_format=io_format,
                expect_one=expect_one,
                implicit_limit=implicit_limit,
            )

            cardinality = codecs[0]
            in_dc = <BaseCodec>codecs[1]
            out_dc = <BaseCodec>codecs[2]

            if not cached:
                qc.set(query, io_format,
                    cardinality == CARDINALITY_NOT_APPLICABLE, in_dc, out_dc)

            ret = await self._execute(in_dc, out_dc, args, kwargs)

        else:
            has_na_cardinality = codecs[0]
            in_dc = <BaseCodec>codecs[1]
            out_dc = <BaseCodec>codecs[2]

            if expect_one and has_na_cardinality:
                methname = _FETCHONE_METHOD[io_format]
                raise errors.InterfaceError(
                    f'query cannot be executed with {methname}() as it '
                    f'does not return any data')

            ret = await self._optimistic_execute(
                query=query,
                args=args,
                kwargs=kwargs,
                reg=reg,
                qc=qc,
                io_format=io_format,
                expect_one=expect_one,
                implicit_limit=implicit_limit,
                in_dc=in_dc,
                out_dc=out_dc,
            )

        if expect_one:
            if ret:
                return ret[0]
            else:
                methname = _FETCHONE_METHOD[io_format]
                raise errors.NoDataError(
                    f'query executed via {methname}() returned no data')
        else:
            if ret:
                if io_format == IoFormat.JSON:
                    return ret[0]
                else:
                    return ret
            else:
                if io_format == IoFormat.JSON:
                    return '[]'
                else:
                    return ret

    async def dump(self, header_callback, block_callback):
        cdef:
            WriteBuffer buf
            char mtype

        if not self.connected:
            raise RuntimeError('not connected')

        self.reset_status()

        buf = WriteBuffer.new_message(DUMP_MSG)
        buf.write_int16(0)  # no headers
        buf.end_message()
        buf.write_bytes(SYNC_MESSAGE)
        self.write(buf)

        header_received = False
        data_received = False
        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == DUMP_BLOCK_MSG:
                    if not header_received:
                        raise RuntimeError('data block before header block')
                    data_received = True

                    await block_callback(
                        self.buffer.consume_message()
                    )

                elif mtype == DUMP_HEADER_BLOCK_MSG:
                    if header_received:
                        raise RuntimeError('more than one header block')
                    if data_received:
                        raise RuntimeError('header block after data block')

                    header_received = True

                    await header_callback(
                        self.buffer.consume_message()
                    )

                elif mtype == ERROR_RESPONSE_MSG:
                    # ErrorResponse
                    exc = self.parse_error_message()

                elif mtype == COMMAND_COMPLETE_MSG:
                    pass

                elif mtype == READY_FOR_COMMAND_MSG:
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()

            finally:
                self.buffer.finish_message()

        if not header_received:
            raise RuntimeError('header block was not received')

        if exc is not None:
            raise exc

    async def _sync(self):
        cdef char mtype
        self.write(WriteBuffer.new_message(SYNC_MSG).end_message())
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == READY_FOR_COMMAND_MSG:
                self.parse_sync_message()
                break
            else:
                self.fallthrough()

    async def restore(self, bytes header, data_gen):
        cdef:
            WriteBuffer buf
            char mtype

        if not self.connected:
            raise RuntimeError('not connected')

        self.reset_status()

        buf = WriteBuffer.new_message(RESTORE_MSG)
        buf.write_int16(0)  # no headers
        buf.write_int16(1)  # -j level
        buf.write_bytes(header)
        buf.end_message()
        self.write(buf)

        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == RESTORE_READY_MSG:
                    self.reject_headers()
                    self.buffer.read_int16()  # discard -j level
                    break
                elif mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    break
                else:
                    self.fallthrough()
            finally:
                self.buffer.finish_message()

        if exc is not None:
            await self._sync()
            raise exc

        # TODO: flow-control is missing here. For now this is fine
        # since this method is only called on a blocking connection.

        async for data in data_gen:
            buf = WriteBuffer.new_message(DUMP_BLOCK_MSG)
            buf.write_bytes(data)
            self.write(buf.end_message())

            if not self.buffer.take_message():
                await self.try_recv_eagerly()
            if self.buffer.take_message():
                # Check if we received an error.
                mtype = self.buffer.get_message_type()
                if mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    self.buffer.finish_message()
                    break
                else:
                    self.fallthrough()

        if exc is not None:
            await self._sync()
            raise exc

        self.write(WriteBuffer.new_message(RESTORE_EOF_MSG).end_message())

        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == COMMAND_COMPLETE_MSG:
                    break
                elif mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    break
                else:
                    self.fallthrough()
            finally:
                self.buffer.finish_message()

        if exc is not None:
            await self._sync()
            raise exc

    def terminate(self):
        try:
            self.write(WriteBuffer.new_message(TERMINATE_MSG).end_message())
        except ConnectionError:
            pass

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
        handshake_buf = WriteBuffer.new_message(CLIENT_HANDSHAKE_MSG)
        handshake_buf.write_int16(PROTO_VER_MAJOR)
        handshake_buf.write_int16(PROTO_VER_MINOR)

        # params
        params = {
            'user': self.con_params.user,
            'database': self.con_params.database,
        }
        handshake_buf.write_int16(len(params))
        for k, v in params.items():
            handshake_buf.write_len_prefixed_utf8(k)
            handshake_buf.write_len_prefixed_utf8(v)

        # no extensions requested
        handshake_buf.write_int16(0)
        handshake_buf.end_message()

        self.write(handshake_buf)

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            if mtype == SERVER_HANDSHAKE_MSG:
                # Server responded with ServerHandshake, this
                # means protocol negotiation.
                major = self.buffer.read_int16()
                minor = self.buffer.read_int16()
                self.parse_headers()
                self.buffer.finish_message()

                if major != PROTO_VER_MAJOR or minor < PROTO_VER_MINOR_MIN:
                    raise errors.ClientConnectionError(
                        f'the server requested an unsupported version of '
                        f'the protocol: {major}.{minor}'
                    )

                self.protocol_version = (PROTO_VER_MAJOR, minor)

            elif mtype == AUTH_REQUEST_MSG:
                # Authentication...
                status = self.buffer.read_int32()
                if status == AuthenticationStatuses.AUTH_OK:
                    pass
                elif status == AuthenticationStatuses.AUTH_SASL:
                    await self._auth_sasl()
                else:
                    self.abort()
                    raise RuntimeError(
                        f'unsupported authentication method requested by the '
                        f'server: {status}')

            elif mtype == SERVER_KEY_DATA_MSG:
                self.backend_secret = self.buffer.read_bytes(32)

            elif mtype == ERROR_RESPONSE_MSG:
                raise self.parse_error_message()

            elif mtype == READY_FOR_COMMAND_MSG:
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

    async def _auth_sasl(self):
        num_methods = self.buffer.read_int32()
        if num_methods <= 0:
            raise RuntimeError(
                'the server requested SASL authentication but did not '
                'offer any methods')

        methods = []
        for i in range(num_methods):
            method = self.buffer.read_len_prefixed_bytes()
            methods.append(method)

        self.buffer.finish_message()

        for method in methods:
            if method == b'SCRAM-SHA-256':
                break
        else:
            raise RuntimeError(
                f'the server offered the following SASL authentication '
                f'methods: {", ".join(methods)}, neither are supported.')

        client_nonce = scram.generate_nonce()
        client_first, client_first_bare = scram.build_client_first_message(
            client_nonce, self.con_params.user)

        msg_buf = WriteBuffer.new_message(AUTH_INITIAL_RESPONSE_MSG)
        msg_buf.write_len_prefixed_bytes(b'SCRAM-SHA-256')
        msg_buf.write_len_prefixed_utf8(client_first)
        msg_buf.end_message()
        self.write(msg_buf)

        if not self.buffer.take_message():
            await self.wait_for_message()
        mtype = self.buffer.get_message_type()

        if mtype == ERROR_RESPONSE_MSG:
            # ErrorResponse
            exc = self.parse_error_message()
            self.buffer.finish_message()
            raise exc

        elif mtype != AUTH_REQUEST_MSG:
            raise RuntimeError(
                f'expected SASLContinue from the server, received {mtype}')

        status = self.buffer.read_int32()
        if status != AuthenticationStatuses.AUTH_SASL_CONTINUE:
            raise RuntimeError(
                f'expected SASLContinue from the server, received {status}')

        server_first = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()

        server_nonce, salt, itercount = (
            scram.parse_server_first_message(server_first))

        client_final, expected_server_sig = scram.build_client_final_message(
            self.con_params.password or '',
            salt,
            itercount,
            client_first_bare.encode('utf-8'),
            server_first,
            server_nonce)

        msg_buf = WriteBuffer.new_message(AUTH_RESPONSE_MSG)
        msg_buf.write_len_prefixed_utf8(client_final)
        msg_buf.end_message()
        self.write(msg_buf)

        if not self.buffer.take_message():
            await self.wait_for_message()
        mtype = self.buffer.get_message_type()

        if mtype == ERROR_RESPONSE_MSG:
            exc = self.parse_error_message()
            self.buffer.finish_message()
            raise exc
        elif mtype != AUTH_REQUEST_MSG:
            raise RuntimeError(
                f'expected SASLFinal from the server, received {mtype}')

        status = self.buffer.read_int32()
        if status != AuthenticationStatuses.AUTH_SASL_FINAL:
            raise RuntimeError(
                f'expected SASLFinal from the server, received {status}')

        server_final = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()
        server_sig = scram.parse_server_final_message(server_final)

        if server_sig != expected_server_sig:
            raise RuntimeError(
                f'server SCRAM proof does not match')

    cdef fallthrough(self):
        cdef:
            char mtype = self.buffer.get_message_type()

        if mtype == PARAMETER_STATUS_MSG:
            name = self.buffer.read_len_prefixed_utf8()
            val = self.buffer.read_len_prefixed_utf8()
            self.buffer.finish_message()
            self.server_settings[name] = val

        elif mtype == LOG_MSG:
            severity = <uint8_t>self.buffer.read_byte()
            code = <uint32_t>self.buffer.read_int32()
            message = self.buffer.read_len_prefixed_utf8()
            # Ignore any headers: not yet specified for log messages.
            self.parse_headers()
            self.buffer.finish_message()

            msg = errors.EdgeDBMessage._from_code(code, severity, message)
            con = self.con()
            if con is not None:
                con._on_log_message(msg)

        else:
            self.abort()

            raise errors.ProtocolError(
                f'unexpected message type {chr(mtype)!r}')

    cdef encode_args(self, BaseCodec in_dc, WriteBuffer buf, args, kwargs):
        cdef:
             WriteBuffer tmp = WriteBuffer.new()

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
        assert self.buffer.get_message_type() == COMMAND_DATA_DESC_MSG

        cdef:
            bytes type_id
            bytes cardinality

        self.reject_headers()

        try:
            cardinality = self.buffer.read_byte()

            type_id = self.buffer.read_bytes(16)
            type_data = self.buffer.read_len_prefixed_bytes()

            if reg.has_codec(type_id):
                in_dc = reg.get_codec(type_id)
            else:
                in_dc = reg.build_codec(type_data)

            type_id = self.buffer.read_bytes(16)
            type_data = self.buffer.read_len_prefixed_bytes()

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
            if buf.get_message_type() != DATA_MSG:
                raise RuntimeError('first message is not "DataMsg"')

            if not datatypes.set_check(result):
                raise RuntimeError(
                    f'result is not an edgedb.Set, but {result!r}')

        while take_message_type(buf, DATA_MSG):
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
            datatypes.set_append(result, row)

            if frb_get_len(rbuf):
                raise RuntimeError(
                    f'unexpected trailing data in buffer after '
                    f'data message decoding: {frb_get_len(rbuf)}')

    cdef parse_command_complete_message(self):
        assert self.buffer.get_message_type() == COMMAND_COMPLETE_MSG
        self.reject_headers()
        self.last_status = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()

    cdef parse_sync_message(self):
        cdef char status

        assert self.buffer.get_message_type() == READY_FOR_COMMAND_MSG

        self.reject_headers()

        status = self.buffer.read_byte()

        if status == TRANS_STATUS_IDLE:
            self.xact_status = TRANS_IDLE
        elif status == TRANS_STATUS_INTRANS:
            self.xact_status = TRANS_INTRANS
        elif status == TRANS_STATUS_ERROR:
            self.xact_status = TRANS_INERROR
        else:
            self.xact_status = TRANS_UNKNOWN

        self.buffer.finish_message()

    cdef _amend_parse_error(self, exc, IoFormat io_format, bint expect_one):
        if expect_one and exc.get_code() == result_cardinality_mismatch_code:
            methname = _FETCHONE_METHOD[io_format]
            new_exc = errors.InterfaceError(
                f'query cannot be executed with {methname}() as it '
                f'returns a multiset')
            new_exc.__cause__ = exc
            exc = new_exc

        return exc

    cdef dict parse_headers(self):
        cdef:
            dict attrs
            uint16_t num_fields
            uint16_t key
            bytes value

        attrs = {}
        num_fields = <uint16_t>self.buffer.read_int16()
        while num_fields:
            key = <uint16_t>self.buffer.read_int16()
            value = self.buffer.read_len_prefixed_bytes()
            attrs[key] = value
            num_fields -= 1
        return attrs

    cdef parse_error_message(self):
        cdef:
            uint32_t code
            int16_t num_fields
            uint8_t severity
            str msg

        assert self.buffer.get_message_type() == ERROR_RESPONSE_MSG

        severity = <uint8_t>self.buffer.read_byte()
        code = <uint32_t>self.buffer.read_int32()
        msg = self.buffer.read_len_prefixed_utf8()
        attrs = self.parse_headers()

        exc = errors.EdgeDBError._from_code(code, msg)
        exc._attrs = attrs
        return exc


## etc

cdef result_cardinality_mismatch_code = \
    errors.ResultCardinalityMismatchError._code

cdef bytes SYNC_MESSAGE = bytes(
    WriteBuffer.new_message(SYNC_MSG).end_message())
cdef bytes FLUSH_MESSAGE = bytes(
    WriteBuffer.new_message(FLUSH_MSG).end_message())
