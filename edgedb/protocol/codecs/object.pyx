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

    cdef encode_args(self, WriteBuffer buf, dict obj):
        cdef:
            WriteBuffer elem_data
            Py_ssize_t objlen
            Py_ssize_t i
            BaseCodec sub_codec
            descriptor = (<BaseNamedRecordCodec>self).descriptor

        self._check_encoder()

        objlen = len(obj)
        if objlen != len(self.fields_codecs):
            raise self._make_missing_args_error_message(obj)

        elem_data = WriteBuffer.new()
        for i in range(objlen):
            name = datatypes.record_desc_pointer_name(descriptor, i)
            try:
                arg = obj[name]
            except KeyError:
                raise self._make_missing_args_error_message(obj) from None

            card = datatypes.record_desc_pointer_card(descriptor, i)

            elem_data.write_int32(0)  # reserved bytes
            if arg is None:
                if card in {datatypes.EdgeFieldCardinality.ONE,
                            datatypes.EdgeFieldCardinality.AT_LEAST_ONE}:
                    raise errors.InvalidArgumentError(
                        f'argument ${name} is required, but received None'
                    )
                elem_data.write_int32(-1)
            else:
                sub_codec = <BaseCodec>(self.fields_codecs[i])
                try:
                    sub_codec.encode(elem_data, arg)
                except (TypeError, ValueError) as e:
                    value_repr = repr(arg)
                    if len(value_repr) > 40:
                        value_repr = value_repr[:40] + '...'
                    raise errors.InvalidArgumentError(
                        'invalid input for query argument'
                        f' ${name}: {value_repr} ({e})') from e

        buf.write_int32(4 + elem_data.len())  # buffer length
        buf.write_int32(<int32_t><uint32_t>objlen)
        buf.write_buffer(elem_data)

    def _make_missing_args_error_message(self, args):
        cdef descriptor = (<BaseNamedRecordCodec>self).descriptor

        required_args = set()

        for i in range(len(self.fields_codecs)):
            name = datatypes.record_desc_pointer_name(descriptor, i)
            required_args.add(name)

        passed_args = set(args.keys())
        missed_args = required_args - passed_args
        extra_args = passed_args - required_args

        error_message = f'expected {required_args} arguments'
        error_message += f', got {passed_args}'

        missed_args = set(required_args) - set(passed_args)
        if missed_args:
            error_message += f', missed {missed_args}'

        extra_args = set(passed_args) - set(required_args)
        if extra_args:
            error_message += f', extra {extra_args}'

        return errors.QueryArgumentError(error_message)

    cdef decode(self, FRBuffer *buf):
        cdef:
            object result
            Py_ssize_t elem_count
            Py_ssize_t i
            int32_t elem_len
            BaseCodec elem_codec
            FRBuffer elem_buf
            tuple fields_codecs = (<BaseRecordCodec>self).fields_codecs
            descriptor = (<BaseNamedRecordCodec>self).descriptor

        elem_count = <Py_ssize_t><uint32_t>hton.unpack_int32(frb_read(buf, 4))

        if elem_count != len(fields_codecs):
            raise RuntimeError(
                f'cannot decode Object: expected {len(fields_codecs)} '
                f'elements, got {elem_count}')

        result = datatypes.object_new(descriptor)

        for i in range(elem_count):
            frb_read(buf, 4)  # reserved
            elem_len = hton.unpack_int32(frb_read(buf, 4))

            if elem_len == -1:
                elem = None
            else:
                elem_codec = <BaseCodec>fields_codecs[i]
                elem = elem_codec.decode(
                    frb_slice_from(&elem_buf, buf, elem_len))
                if frb_get_len(&elem_buf):
                    raise RuntimeError(
                        f'unexpected trailing data in buffer after '
                        f'object element decoding: {frb_get_len(&elem_buf)}')

            datatypes.object_set(result, i, elem)

        return result

    @staticmethod
    cdef BaseCodec new(bytes tid, tuple names, tuple flags, tuple cards,
                       tuple codecs):
        cdef:
            ObjectCodec codec

        codec = ObjectCodec.__new__(ObjectCodec)

        codec.tid = tid
        codec.name = 'Object'
        codec.descriptor = datatypes.record_desc_new(names, flags, cards)
        codec.fields_codecs = codecs

        return codec
