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


from edgedb import enums


DEF QUERY_OPT_IMPLICIT_LIMIT = 0xFF01
DEF QUERY_OPT_INLINE_TYPENAMES = 0xFF02
DEF QUERY_OPT_INLINE_TYPEIDS = 0xFF03
DEF QUERY_OPT_ALLOW_CAPABILITIES = 0xFF04

DEF SERVER_HEADER_CAPABILITIES = 0x1001


cdef class SansIOProtocolBackwardsCompatible(SansIOProtocol):
    async def _legacy_parse(
        self,
        query: str,
        *,
        reg: CodecsRegistry,
        output_format: OutputFormat=OutputFormat.BINARY,
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
        self.legacy_write_execute_headers(
            buf, implicit_limit, inline_typenames, inline_typeids,
            ALL_CAPABILITIES if allow_capabilities is None
            else allow_capabilities)
        buf.write_byte(output_format)
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
                    attrs = self.legacy_parse_headers()
                    cardinality = self.buffer.read_byte()
                    if self.protocol_version >= (0, 14):
                        in_dc, out_dc = self.parse_type_data(reg)
                    else:
                        in_type_id = self.buffer.read_bytes(16)
                        out_type_id = self.buffer.read_bytes(16)

                elif mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    exc = self._amend_parse_error(
                        exc, output_format, expect_one, required_one)

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
                                self.parse_legacy_describe_type_message(reg)

                        elif mtype == ERROR_RESPONSE_MSG:
                            exc = self.parse_error_message()
                            exc = self._amend_parse_error(
                                exc, output_format, expect_one, required_one)

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
            methname = _QUERY_SINGLE_METHOD[required_one][output_format]
            raise errors.InterfaceError(
                f'query cannot be executed with {methname}() as it '
                f'does not return any data')

        return cardinality, in_dc, out_dc, attrs

    async def _legacy_execute(
        self, BaseCodec in_dc, BaseCodec out_dc, args, kwargs
    ):
        cdef:
            WriteBuffer packet
            WriteBuffer buf
            char mtype

        self.ensure_connected()
        self.reset_status()

        packet = WriteBuffer.new()

        buf = WriteBuffer.new_message(LEGACY_EXECUTE_MSG)
        buf.write_int16(0)  # no headers
        buf.write_len_prefixed_bytes(b'')  # stmt_name
        self.encode_args(in_dc, buf, args, kwargs)
        packet.write_buffer(buf.end_message())

        packet.write_bytes(SYNC_MESSAGE)
        self.write(packet)

        result = []

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
                    self.parse_legacy_command_complete_message()

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

    async def _legacy_optimistic_execute(
        self,
        *,
        query: str,
        args,
        kwargs,
        reg: CodecsRegistry,
        qc: QueryCodecsCache,
        output_format: object,
        expect_one: bint,
        required_one: bint,
        implicit_limit: int,
        inline_typenames: bint,
        inline_typeids: bint,
        allow_capabilities: typing.Optional[int] = None,
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

        buf = WriteBuffer.new_message(EXECUTE_MSG)
        self.legacy_write_execute_headers(
            buf, implicit_limit, inline_typenames, inline_typeids,
            ALL_CAPABILITIES if allow_capabilities is None
            else allow_capabilities)
        buf.write_byte(output_format)
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

        result = []
        re_exec = False
        exc = None
        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == STMT_DATA_DESC_MSG:
                    # our in/out type spec is out-dated
                    new_cardinality, in_dc, out_dc, headers = \
                        self.parse_legacy_describe_type_message(reg)

                    capabilities = headers.get(SERVER_HEADER_CAPABILITIES)
                    if capabilities is not None:
                        capabilities = int.from_bytes(capabilities, 'big')

                    qc.set(
                        query,
                        output_format,
                        implicit_limit,
                        inline_typenames,
                        inline_typeids,
                        expect_one,
                        new_cardinality == CARDINALITY_NOT_APPLICABLE,
                        in_dc, out_dc, capabilities)
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
                    self.parse_legacy_command_complete_message()

                elif mtype == ERROR_RESPONSE_MSG:
                    exc = self.parse_error_message()
                    exc = self._amend_parse_error(
                        exc, output_format, expect_one, required_one)

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
            if required_one and new_cardinality == CARDINALITY_NOT_APPLICABLE:
                methname = _QUERY_SINGLE_METHOD[required_one][output_format]
                raise errors.InterfaceError(
                    f'query cannot be executed with {methname}() as it '
                    f'does not return any data')
            return await self._legacy_execute(in_dc, out_dc, args, kwargs)
        else:
            return result

    async def legacy_execute_anonymous(
        self,
        *,
        query: str,
        args,
        kwargs,
        reg: CodecsRegistry,
        qc: QueryCodecsCache,
        output_format: object,
        expect_one: bint = False,
        required_one: bool = False,
        implicit_limit: int = 0,
        inline_typenames: bool = False,
        inline_typeids: bool = False,
        allow_capabilities: enums.Capability = enums.Capability.ALL,
    ):
        cdef:
            BaseCodec in_dc
            BaseCodec out_dc

        self.ensure_connected()
        self.reset_status()

        codecs = qc.get(
            query,
            output_format,
            implicit_limit,
            inline_typenames,
            inline_typeids,
            expect_one)
        if codecs is None:
            codecs = await self._legacy_parse(
                query,
                reg=reg,
                output_format=output_format,
                expect_one=expect_one,
                required_one=required_one,
                implicit_limit=implicit_limit,
                inline_typenames=inline_typenames,
                inline_typeids=inline_typeids,
                allow_capabilities=allow_capabilities,
            )

            cardinality = codecs[0]
            in_dc = <BaseCodec>codecs[1]
            out_dc = <BaseCodec>codecs[2]
            headers = <BaseCodec>codecs[3]

            capabilities = None
            if headers:
                capabilities = headers.get(SERVER_HEADER_CAPABILITIES)
                if capabilities is not None:
                    capabilities = int.from_bytes(capabilities, 'big')

            qc.set(
                query,
                output_format,
                implicit_limit,
                inline_typenames,
                inline_typeids,
                expect_one,
                cardinality == CARDINALITY_NOT_APPLICABLE,
                in_dc,
                out_dc,
                capabilities,
                )

            ret = await self._legacy_execute(in_dc, out_dc, args, kwargs)

        else:
            has_na_cardinality = codecs[0]
            in_dc = <BaseCodec>codecs[1]
            out_dc = <BaseCodec>codecs[2]

            if required_one and has_na_cardinality:
                methname = _QUERY_SINGLE_METHOD[required_one][output_format]
                raise errors.InterfaceError(
                    f'query cannot be executed with {methname}() as it '
                    f'does not return any data')

            ret = await self._legacy_optimistic_execute(
                query=query,
                args=args,
                kwargs=kwargs,
                reg=reg,
                qc=qc,
                output_format=output_format,
                expect_one=expect_one,
                required_one=required_one,
                implicit_limit=implicit_limit,
                inline_typenames=inline_typenames,
                inline_typeids=inline_typeids,
                allow_capabilities=allow_capabilities,
                in_dc=in_dc,
                out_dc=out_dc,
            )

        if expect_one:
            if ret or not required_one:
                if ret:
                    return ret[0]
                else:
                    if output_format == OutputFormat.JSON:
                        return 'null'
                    else:
                        return None
            else:
                methname = _QUERY_SINGLE_METHOD[required_one][output_format]
                raise errors.NoDataError(
                    f'query executed via {methname}() returned no data')
        else:
            if ret:
                if output_format == OutputFormat.JSON:
                    return ret[0]
                else:
                    return ret
            else:
                if output_format == OutputFormat.JSON:
                    return '[]'
                else:
                    return ret

    async def legacy_simple_query(
        self, str query, capabilities: enums.Capability
    ):
        cdef:
            WriteBuffer buf
            char mtype

        self.ensure_connected()
        self.reset_status()


        buf = WriteBuffer.new_message(EXECUTE_SCRIPT_MSG)
        cap_bytes = cpython.PyBytes_FromStringAndSize(NULL, sizeof(uint64_t))
        hton.pack_int64(
            cpython.PyBytes_AsString(cap_bytes),
            <int64_t><uint64_t>capabilities,
        )
        self.legacy_write_headers(
            buf,
            {QUERY_OPT_ALLOW_CAPABILITIES: cap_bytes},
        )
        buf.write_len_prefixed_utf8(query)
        self.write(buf.end_message())

        exc = None

        while True:
            if not self.buffer.take_message():
                await self.wait_for_message()
            mtype = self.buffer.get_message_type()

            try:
                if mtype == COMMAND_COMPLETE_MSG:
                    self.parse_legacy_command_complete_message()

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

    cdef parse_legacy_describe_type_message(self, CodecsRegistry reg):
        assert self.buffer.get_message_type() == COMMAND_DATA_DESC_MSG

        cdef:
            bytes cardinality

        headers = self.legacy_parse_headers()

        try:
            cardinality = self.buffer.read_byte()

            in_dc, out_dc = self.parse_type_data(reg)
        finally:
            self.buffer.finish_message()

        return cardinality, in_dc, out_dc, headers

    cdef parse_legacy_command_complete_message(self):
        assert self.buffer.get_message_type() == COMMAND_COMPLETE_MSG
        headers = self.legacy_parse_headers()
        capabilities = headers.get(SERVER_HEADER_CAPABILITIES)
        if capabilities is not None:
            self.last_capabilities = enums.Capability(
                int.from_bytes(capabilities, 'big'))
        else:
            self.last_capabilities = None
        self.last_status = self.buffer.read_len_prefixed_bytes()
        self.buffer.finish_message()

    cdef legacy_write_headers(self, buf: WriteBuffer, headers: dict):
        buf.write_int16(len(headers))
        for k, v in headers.items():
            buf.write_int16(<int16_t><uint16_t>k)
            if isinstance(v, bytes):
                buf.write_len_prefixed_bytes(v)
            else:
                buf.write_len_prefixed_utf8(str(v))

    cdef legacy_write_execute_headers(
        self,
        WriteBuffer buf,
        int implicit_limit,
        bint inline_typenames,
        bint inline_typeids,
        uint64_t allow_capabilities,
    ):
        cdef bytes val
        if (
            implicit_limit or
            inline_typenames or inline_typeids or
            allow_capabilities != ALL_CAPABILITIES
        ):
            headers = {}
            if implicit_limit:
                headers[QUERY_OPT_IMPLICIT_LIMIT] = implicit_limit
            if inline_typenames:
                headers[QUERY_OPT_INLINE_TYPENAMES] = True
            if inline_typeids:
                headers[QUERY_OPT_INLINE_TYPEIDS] = True
            if allow_capabilities != ALL_CAPABILITIES:
                val = cpython.PyBytes_FromStringAndSize(NULL, sizeof(uint64_t))
                hton.pack_int64(
                    cpython.PyBytes_AsString(val),
                    <int64_t><uint64_t>allow_capabilities
                )
                headers[QUERY_OPT_ALLOW_CAPABILITIES] = val
            self.legacy_write_headers(buf, headers)
        else:
            buf.write_int16(0)  # no headers

    cdef dict legacy_parse_headers(self):
        cdef:
            dict attrs
            uint16_t num_fields
            uint16_t key
            bytes value

        attrs = {}
        num_fields = <uint16_t> self.buffer.read_int16()
        while num_fields:
            key = <uint16_t> self.buffer.read_int16()
            value = self.buffer.read_len_prefixed_bytes()
            attrs[key] = value
            num_fields -= 1
        return attrs
