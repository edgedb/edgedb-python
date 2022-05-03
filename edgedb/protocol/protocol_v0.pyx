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

cdef class SansIOProtocolBackwardsCompatible(SansIOProtocol):
    async def _legacy_parse(
        self,
        query: str,
        *,
        reg: CodecsRegistry,
        io_format: IoFormat=IoFormat.BINARY,
        expect_one: bint=False,
        required_one: bool=False,
        implicit_limit: int=0,
        inline_typenames: bool=False,
        inline_typeids: bool=False,
        allow_capabilities: typing.Optional[int] = None,
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
        self.write_execute_headers(
            buf, implicit_limit, inline_typenames, inline_typeids,
            ALL_CAPABILITIES if allow_capabilities is None
            else allow_capabilities)
        buf.write_byte(io_format)
        buf.write_byte(CARDINALITY_ONE if expect_one else CARDINALITY_MANY)
        buf.write_len_prefixed_bytes(b'')  # stmt_name
        buf.write_len_prefixed_utf8(query)
        buf.end_message()
        buf.write_bytes(SYNC_MESSAGE)
        self.write(buf)

        attrs = None
        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == PREPARE_COMPLETE_MSG:
                    attrs = self.parse_headers()
                    cardinality = self.buffer.read_byte()
                    if self.protocol_version >= (0, 14):
                        in_dc, out_dc = self.parse_type_data(reg)
                    else:
                        in_type_id = self.buffer.read_bytes(16)
                        out_type_id = self.buffer.read_bytes(16)

                elif mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    exc = self._amend_parse_error(
                        exc, io_format, expect_one, required_one)

                elif mtype == READY_FOR_COMMAND_MSG:
                    self.parse_sync_message()
                    break

                else:
                    self.fallthrough()
            finally:
                self.buffer.finish_message()

        if exc is not None:
            raise exc

        if self.protocol_version < (0, 14):
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
                            cardinality, in_dc, out_dc, _ = \
                                self.parse_describe_type_message(reg)

                        elif mtype == ERROR_RESPONSE_MSG:
                            exc = self.parse_error_message()
                            exc = self._amend_parse_error(
                                exc, io_format, expect_one, required_one)

                        elif mtype == READY_FOR_COMMAND_MSG:
                            self.parse_sync_message()
                            break

                        else:
                            self.fallthrough()

                    finally:
                        self.buffer.finish_message()

            if exc is not None:
                raise exc

        if required_one and cardinality == CARDINALITY_NOT_APPLICABLE:
            methname = _QUERY_SINGLE_METHOD[required_one][io_format]
            raise errors.InterfaceError(
                f'query cannot be executed with {methname}() as it '
                f'does not return any data')

        return cardinality, in_dc, out_dc, attrs
