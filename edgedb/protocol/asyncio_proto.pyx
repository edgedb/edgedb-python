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


include "./sansio_proto.pyx"


cdef class AsyncIOProtocol(SansIOProtocol):

    def __init__(self, addr, con_params, loop):
        SansIOProtocol.__init__(self, addr, con_params)

        self.loop = loop
        self.transport = None
        self.connected_fut = loop.create_future()

        self.msg_waiter = None

    async def _parse(self, CodecsRegistry reg, str query, bint json_mode):
        cdef:
            WriteBuffer buf
            char mtype
            BaseCodec in_dc = None
            BaseCodec out_dc = None
            int16_t type_size
            int32_t parse_flags
            bytes in_type_id
            bytes out_type_id

        if not self.connected:
            raise RuntimeError('not connected')

        buf = WriteBuffer.new_message(b'P')
        buf.write_byte(b'j' if json_mode else b'b')
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
                    parse_flags = self.buffer.read_int32()
                    in_type_id = self.buffer.read_bytes(16)
                    out_type_id = self.buffer.read_bytes(16)

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
                        parse_flags, in_dc, out_dc = \
                            self.parse_describe_type_message(reg)

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

        return parse_flags, in_dc, out_dc

    async def _execute(self, BaseCodec in_dc, BaseCodec out_dc, args, kwargs):
        cdef:
            WriteBuffer packet
            WriteBuffer buf
            char mtype

        if not self.connected:
            raise RuntimeError('not connected')

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
                    self.buffer.discard_message()

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
                                     QueryCache qc,
                                     bint json_mode,
                                     int32_t parse_flags,
                                     BaseCodec in_dc, BaseCodec out_dc,
                                     str query, args, kwargs):
        cdef:
            WriteBuffer packet
            WriteBuffer buf
            char mtype
            bint re_exec
            object result

        buf = WriteBuffer.new_message(b'O')
        buf.write_byte(b'j' if json_mode else b'b')
        buf.write_utf8(query)
        buf.write_int32(parse_flags)
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
                    parse_flags, in_dc, out_dc = \
                        self.parse_describe_type_message(reg)
                    qc.set(query, json_mode, parse_flags, in_dc, out_dc)
                    re_exec = True

                elif mtype == b'D':
                    assert not re_exec
                    self.parse_data_messages(out_dc, result)

                elif mtype == b'C':  # CommandComplete
                    self.buffer.discard_message()

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

        if re_exec:
            return await self._execute(in_dc, out_dc, args, kwargs)
        else:
            return result

    async def legacy(self, str query, bint graphql):
        cdef:
            WriteBuffer buf
            char mtype

        if not self.connected:
            raise RuntimeError('not connected')
        if self.transport is None:
            raise RuntimeError('no transport object in legacy()')

        buf = WriteBuffer.new_message(b'L')
        if graphql:
            buf.write_byte(b'g')
        else:
            buf.write_byte(b'e')
        buf.write_utf8(query)
        self.write(buf.end_message())

        exc = json_data = None

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == b'L':
                    # Legacy message data row
                    json_data = self.buffer.consume_message()

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

        return json.loads(json_data.decode('utf-8'))

    async def simple_query(self, str query):
        cdef:
            WriteBuffer buf
            char mtype

        if not self.connected:
            raise RuntimeError('not connected')
        if self.transport is None:
            raise RuntimeError('no transport object in simple_query()')

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
                    self.buffer.discard_message()

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

    async def execute_anonymous(self, bint fetchval_mode,
                                bint json_mode,
                                CodecsRegistry reg, QueryCache qc,
                                str query, args, kwargs):
        cdef:
            BaseCodec in_dc
            BaseCodec out_dc
            bint cached
            int32_t parse_flags

        if not self.connected:
            raise RuntimeError('not connected')
        if self.transport is None:
            raise RuntimeError('no transport object in legacy()')

        cached = True
        codecs = qc.get(query, json_mode)
        if codecs is None:
            cached = False

            codecs = await self._parse(reg, query, json_mode)

            parse_flags = <int32_t>codecs[0]
            if fetchval_mode and not (
                    parse_flags & PARSE_SINGLETON_RESULT):
                raise errors.InterfaceError(
                    f'cannot execute {query!r} in fetchval(): '
                    f'the result set can be a multiset')

            in_dc = <BaseCodec>codecs[1]
            out_dc = <BaseCodec>codecs[2]

            if not cached:
                qc.set(query, json_mode, parse_flags, in_dc, out_dc)

            ret = await self._execute(in_dc, out_dc, args, kwargs)

        else:
            parse_flags = <int32_t>codecs[0]
            in_dc = <BaseCodec>codecs[1]
            out_dc = <BaseCodec>codecs[2]

            if fetchval_mode and not (
                    parse_flags & PARSE_SINGLETON_RESULT):
                raise errors.InterfaceError(
                    f'cannot execute {query!r} in fetchval(): '
                    f'the result set can be a multiset')

            ret = await self._opportunistic_execute(
                reg, qc, json_mode, parse_flags,
                in_dc, out_dc, query, args, kwargs)

        if json_mode:
            return ret[0] if ret else '[]'

        if fetchval_mode:
            return ret[0] if ret else None
        else:
            return ret

    async def connect(self):
        cdef:
            WriteBuffer ver_buf
            WriteBuffer msg_buf
            WriteBuffer buf
            char mtype
            int32_t status

        if self.connected_fut is not None:
            await self.connected_fut
        if self.connected:
            raise RuntimeError('already connected')
        if self.transport is None:
            raise RuntimeError('no transport object in connect()')

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

        # TODO:
        # * handle Notice and ServerStatus messages here

        raise RuntimeError(
            f'unexpected message type {chr(mtype)!r}')

    ## Private APIs and implementation details

    cpdef abort(self):
        self.connected = False
        if self.transport is not None:
            self.transport.close()
            self.transport = None

    cdef write(self, WriteBuffer buf):
        self.transport.write(memoryview(buf))

    async def wait_for_message(self):
        if self.buffer.take_message():
            return
        self.msg_waiter = self.loop.create_future()
        await self.msg_waiter

    def connection_made(self, transport):
        if self.transport is not None:
            raise RuntimeError('connection_made: invalid connection status')
        self.transport = transport
        self.connected_fut.set_result(True)
        self.connected_fut = None

    def connection_lost(self, exc):
        self.connected = False

        if self.connected_fut is not None and not self.connected_fut.done():
            self.connected_fut.set_exception(ConnectionAbortedError())
            return

        if self.msg_waiter is not None and not self.msg_waiter.done():
            self.msg_waiter.set_exception(ConnectionAbortedError())
            self.msg_waiter = None

        self.transport = None

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        self.buffer.feed_data(data)

        if (self.msg_waiter is not None and
                self.buffer.take_message() and
                not self.msg_waiter.done()):
            self.msg_waiter.set_result(True)
            self.msg_waiter = None

    def eof_received(self):
        pass


## Other exports

_RecordDescriptor = datatypes.EdgeRecordDesc_InitType()
Tuple = datatypes.EdgeTuple_InitType()
NamedTuple = datatypes.EdgeNamedTuple_InitType()
Object = datatypes.EdgeObject_InitType()
Set = datatypes.EdgeSet_InitType()
Array = datatypes.EdgeArray_InitType()


_EDGE_POINTER_IS_IMPLICIT = datatypes.EDGE_POINTER_IS_IMPLICIT
_EDGE_POINTER_IS_LINKPROP = datatypes.EDGE_POINTER_IS_LINKPROP


def _create_object_factory(tuple pointers, frozenset linkprops):
    flags = ()
    for name in pointers:
        if name in linkprops:
            flags += (datatypes.EDGE_POINTER_IS_LINKPROP,)
        else:
            flags += (0,)

    desc = datatypes.EdgeRecordDesc_New(pointers, flags)
    size = len(pointers)

    def factory(*items):
        if len(items) != size:
            raise ValueError

        o = datatypes.EdgeObject_New(desc)
        for i in range(size):
            datatypes.EdgeObject_SetItem(o, i, items[i])

        return o

    return factory
