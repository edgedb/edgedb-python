#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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
cdef class SparseObjectCodec(BaseNamedRecordCodec):

    cdef encode(self, WriteBuffer buf, object obj):
        cdef:
            WriteBuffer elem_data
            Py_ssize_t objlen = 0
            Py_ssize_t i
            BaseCodec sub_codec
            descriptor = (<BaseNamedRecordCodec>self).descriptor

        elem_data = WriteBuffer.new()
        for name, arg in obj.items():
            if arg is not None:
                try:
                    i = datatypes.input_shape_get_pos(descriptor, name)
                except LookupError:
                    raise self._make_unknown_args_error_message(obj) from None
                objlen += 1
                elem_data.write_int32(i)

                sub_codec = <BaseCodec>(self.fields_codecs[i])
                try:
                    sub_codec.encode(elem_data, arg)
                except (TypeError, ValueError) as e:
                    value_repr = repr(arg)
                    if len(value_repr) > 40:
                        value_repr = value_repr[:40] + '...'
                    raise errors.InvalidArgumentError(
                        'invalid input for session argument '
                        f' {name} := {value_repr} ({e})') from e

        buf.write_int32(4 + elem_data.len())  # buffer length
        buf.write_int32(<int32_t><uint32_t>objlen)
        buf.write_buffer(elem_data)

    def _make_unknown_args_error_message(self, args):
        cdef descriptor = (<BaseNamedRecordCodec>self).descriptor

        acceptable_args = set()

        for i in range(len(self.fields_codecs)):
            name = datatypes.input_shape_pointer_name(descriptor, i)
            acceptable_args.add(name)

        passed_args = set(args.keys())
        error_message = f'acceptable {acceptable_args} arguments'

        passed_args_repr = repr(passed_args) if passed_args else 'nothing'
        error_message += f', got {passed_args_repr}'

        extra_args = set(passed_args) - set(acceptable_args)
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

        result = datatypes.sparse_object_new(descriptor)

        for i in range(len(fields_codecs)):
            datatypes.sparse_object_set(result, i, None)

        for _ in range(elem_count):
            i = <uint32_t>hton.unpack_int32(frb_read(buf, 4))
            elem_len = hton.unpack_int32(frb_read(buf, 4))

            if elem_len == -1:
                continue

            elem_codec = <BaseCodec>fields_codecs[i]
            elem = elem_codec.decode(
                frb_slice_from(&elem_buf, buf, elem_len))
            if frb_get_len(&elem_buf):
                raise RuntimeError(
                    f'unexpected trailing data in buffer after '
                    f'object element decoding: {frb_get_len(&elem_buf)}')
            cpython.Py_DECREF(None)
            datatypes.sparse_object_set(result, i, elem)

        return result

    @staticmethod
    cdef BaseCodec new(bytes tid, tuple names, tuple codecs):
        cdef:
            SparseObjectCodec codec

        codec = SparseObjectCodec.__new__(SparseObjectCodec)

        codec.tid = tid
        codec.name = 'SparseObject'
        codec.descriptor = datatypes.input_shape_new(names)
        codec.fields_codecs = codecs

        return codec
