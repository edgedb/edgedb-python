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


from __future__ import annotations

import dataclasses
import enum
import io
import typing
import uuid

from . import binwrapper


CTYPE_SET = b"\x00"
CTYPE_SHAPE = b"\x01"
CTYPE_BASE_SCALAR = b"\x02"
CTYPE_SCALAR = b"\x03"
CTYPE_TUPLE = b"\x04"
CTYPE_NAMEDTUPLE = b"\x05"
CTYPE_ARRAY = b"\x06"
CTYPE_ENUM = b"\x07"
CTYPE_INPUT_SHAPE = b"\x08"
CTYPE_RANGE = b"\x09"
CTYPE_ANNO_TYPENAME = b"\xff"


BASE_SCALAR_TYPES = {
    uuid.UUID("00000000-0000-0000-0000-000000000100"): "std::uuid",
    uuid.UUID("00000000-0000-0000-0000-000000000101"): "string",  # std::str
    uuid.UUID("00000000-0000-0000-0000-000000000102"): "std::bytes",
    uuid.UUID("00000000-0000-0000-0000-000000000103"): "integer",  # std::int16
    uuid.UUID("00000000-0000-0000-0000-000000000104"): "integer",  # std::int32
    uuid.UUID("00000000-0000-0000-0000-000000000105"): "integer",  # std::int64
    uuid.UUID(
        "00000000-0000-0000-0000-000000000106"
    ): "number",  # std::float32
    uuid.UUID(
        "00000000-0000-0000-0000-000000000107"
    ): "number",  # std::float64
    uuid.UUID("00000000-0000-0000-0000-000000000108"): "std::decimal",
    uuid.UUID("00000000-0000-0000-0000-000000000109"): "boolean",  # std::bool
    uuid.UUID("00000000-0000-0000-0000-00000000010a"): "std::datetime",
    uuid.UUID("00000000-0000-0000-0000-00000000010e"): "std::duration",
    uuid.UUID("00000000-0000-0000-0000-00000000010f"): "string",  # std::json
    uuid.UUID(
        "00000000-0000-0000-0000-000000000110"
    ): "integer",  # std::bigint
    uuid.UUID("00000000-0000-0000-0000-00000000010b"): "cal::local_datetime",
    uuid.UUID("00000000-0000-0000-0000-00000000010c"): "cal::local_date",
    uuid.UUID("00000000-0000-0000-0000-00000000010d"): "cal::local_time",
    uuid.UUID(
        "00000000-0000-0000-0000-000000000111"
    ): "cal::relative_duration",
    uuid.UUID("00000000-0000-0000-0000-000000000112"): "cal::date_duration",
    uuid.UUID("00000000-0000-0000-0000-000000000130"): "cfg::memory",
}


class Cardinality(enum.Enum):
    # Cardinality isn't applicable for the query:
    # * the query is a command like CONFIGURE that
    #   does not return any data;
    # * the query is composed of multiple queries.
    NO_RESULT = 0x6E

    # Cardinality is 1 or 0
    AT_MOST_ONE = 0x6F

    # Cardinality is 1
    ONE = 0x41

    # Cardinality is >= 0
    MANY = 0x6D

    # Cardinality is >= 1
    AT_LEAST_ONE = 0x4D


def _parse(
    desc: binwrapper.BinWrapper,
    codecs_list: typing.List[TypeDesc],
) -> typing.Optional[TypeDesc]:
    t = desc.read_bytes(1)
    tid = uuid.UUID(bytes=desc.read_bytes(16))

    if t == CTYPE_SET:
        pos = desc.read_ui16()
        return SetDesc(tid=tid, subtype=codecs_list[pos])

    elif t == CTYPE_SHAPE or t == CTYPE_INPUT_SHAPE:
        els = desc.read_ui16()
        fields = {}
        flags = {}
        cardinalities = {}
        fields_list = []
        for idx in range(els):
            flag = desc.read_ui32()
            cardinality = Cardinality(desc.read_bytes(1)[0])
            name = desc.read_len32_prefixed_bytes().decode()
            pos = desc.read_ui16()
            codec = codecs_list[pos]
            if t == CTYPE_INPUT_SHAPE:
                fields_list.append((name, codec))
                fields[name] = idx, codec
            else:
                fields[name] = codec
            flags[name] = flag
            if cardinality:
                cardinalities[name] = cardinality
        args = dict(
            tid=tid,
            flags=flags,
            fields=fields,
            cardinalities=cardinalities,
        )
        if t == CTYPE_SHAPE:
            return ShapeDesc(**args)
        else:
            return InputShapeDesc(fields_list=fields_list, **args)

    elif t == CTYPE_BASE_SCALAR:
        return BaseScalarDesc(tid=tid)

    elif t == CTYPE_SCALAR:
        pos = desc.read_ui16()
        return ScalarDesc(tid=tid, subtype=codecs_list[pos])

    elif t == CTYPE_TUPLE:
        els = desc.read_ui16()
        fields = []
        for _ in range(els):
            pos = desc.read_ui16()
            fields.append(codecs_list[pos])
        return TupleDesc(tid=tid, fields=fields)

    elif t == CTYPE_NAMEDTUPLE:
        els = desc.read_ui16()
        fields = {}
        for _ in range(els):
            name = desc.read_len32_prefixed_bytes().decode()
            pos = desc.read_ui16()
            fields[name] = codecs_list[pos]
        return NamedTupleDesc(tid=tid, fields=fields)

    elif t == CTYPE_ENUM:
        els = desc.read_ui16()
        names = []
        for _ in range(els):
            name = desc.read_len32_prefixed_bytes().decode()
            names.append(name)
        return EnumDesc(tid=tid, names=names)

    elif t == CTYPE_ARRAY:
        pos = desc.read_ui16()
        els = desc.read_ui16()
        if els != 1:
            raise NotImplementedError(
                "cannot handle arrays with more than one dimension"
            )
        dim_len = desc.read_i32()
        return ArrayDesc(tid=tid, dim_len=dim_len, subtype=codecs_list[pos])

    elif t == CTYPE_RANGE:
        pos = desc.read_ui16()
        return RangeDesc(tid=tid, inner=codecs_list[pos])

    elif t[0] >= 0x80 and t[0] <= 0xFF:
        # Ignore all type annotations.
        desc.read_len32_prefixed_bytes()
        return None

    else:
        raise NotImplementedError(
            f"no codec implementation for EdgeDB data class {t}"
        )


def parse(typedesc: bytes) -> TypeDesc:
    buf = io.BytesIO(typedesc)
    wrapped = binwrapper.BinWrapper(buf)
    codecs_list = []
    while buf.tell() < len(typedesc):
        desc = _parse(wrapped, codecs_list)
        if desc is not None:
            codecs_list.append(desc)
    return codecs_list[-1]


def describe(
    in_desc: typing.Optional[TypeDesc],
    out_desc: typing.Optional[TypeDesc],
    name: str,
    cardinality: bytes,
) -> typing.Dict[str, typing.Any]:
    defs = {}
    result = {}
    rv = {
        "$schema": "https://json-schema.org/draft/2020-12/schema",
        "definitions": defs,
        "type": "object",
        "properties": result,
    }
    reg = {}
    if out_desc:
        out_schema = out_desc.describe(name, defs, reg)
    else:
        out_schema = {"type": "null"}
    if in_desc:
        result["input"] = in_desc.describe_args(defs, reg)
    else:
        result["input"] = {"type": "null"}
    cardinality = Cardinality(cardinality[0])
    if cardinality == Cardinality.MANY:
        result["output"] = {
            "type": "array",
            "items": out_schema,
        }
    else:
        result["output"] = out_schema
    return rv


@dataclasses.dataclass(frozen=True)
class TypeDesc:
    tid: uuid.UUID

    def describe(
        self,
        name: str,
        defs: typing.Dict[str, typing.Any],
        reg: typing.Dict[uuid.UUID, typing.Dict[str, typing.Any]],
    ) -> typing.Dict[str, typing.Any]:
        raise NotImplementedError

    def describe_arg(
        self,
        name: str,
        defs: typing.Dict[str, typing.Any],
        reg: typing.Dict[uuid.UUID, typing.Dict[str, typing.Any]],
    ) -> typing.Dict[str, typing.Any]:
        return self.describe(name, defs, reg)

    @staticmethod
    def find_name(name, defs: typing.Dict[str, typing.Any]):
        if name in defs:
            for i in range(64):
                new_name = f"{name}_{i}"
                if new_name not in defs:
                    name = new_name
                    break
        return name


@dataclasses.dataclass(frozen=True)
class SetDesc(TypeDesc):
    subtype: TypeDesc
    impl: typing.ClassVar[type] = frozenset

    def describe(
        self,
        name: str,
        defs: typing.Dict[str, typing.Any],
        reg: typing.Dict[uuid.UUID, typing.Dict[str, typing.Any]],
    ) -> typing.Dict[str, typing.Any]:
        return {
            "type": "array",
            "items": self.subtype.describe(name, defs, reg),
        }


@dataclasses.dataclass(frozen=True)
class ShapeDesc(TypeDesc):
    fields: typing.Dict[str, TypeDesc]
    flags: typing.Dict[str, int]
    cardinalities: typing.Dict[str, Cardinality]

    def describe(
        self,
        name: str,
        defs: typing.Dict[str, typing.Any],
        reg: typing.Dict[uuid.UUID, typing.Dict[str, typing.Any]],
    ) -> typing.Dict[str, typing.Any]:
        if self.tid in reg:
            return reg[self.tid]
        name = self.find_name(name, defs)
        props = {}
        defs[name] = {
            "type": "object",
            "properties": props,
        }
        reg[self.tid] = rv = {"$ref": f"#/definitions/{name}"}
        for name, desc in self.fields.items():
            if self.cardinalities[name] == Cardinality.MANY:
                props[name] = {
                    "type": "array",
                    "items": desc.describe(name, defs, reg),
                }
            else:
                props[name] = desc.describe(name, defs, reg)
        return rv

    def describe_arg(
        self,
        name: str,
        defs: typing.Dict[str, typing.Any],
        reg: typing.Dict[uuid.UUID, typing.Dict[str, typing.Any]],
    ) -> typing.Dict[str, typing.Any]:
        return {"type": "object"}

    def describe_args(
        self,
        defs: typing.Dict[str, typing.Any],
        reg: typing.Dict[uuid.UUID, typing.Dict[str, typing.Any]],
    ) -> typing.Dict[str, typing.Any]:
        props = {}
        for name, desc in self.fields.items():
            try:
                new_name = int(name)
                sub_name = f"input{name}"
            except ValueError:
                new_name = name
                sub_name = name
            if self.cardinalities[name] == Cardinality.MANY:
                props[new_name] = {
                    "type": "array",
                    "items": desc.describe_arg(sub_name, defs, reg),
                }
            else:
                props[new_name] = desc.describe_arg(sub_name, defs, reg)
        return {
            "type": "object",
            "properties": props,
        }


@dataclasses.dataclass(frozen=True)
class ScalarDesc(TypeDesc):
    subtype: TypeDesc

    def describe(
        self,
        name: str,
        defs: typing.Dict[str, typing.Any],
        reg: typing.Dict[uuid.UUID, typing.Dict[str, typing.Any]],
    ) -> typing.Dict[str, typing.Any]:
        if self.tid in reg:
            return reg[self.tid]
        name = self.find_name(name, defs)
        defs[name] = self.subtype.describe(name, defs, reg)
        reg[self.tid] = rv = {"$ref": f"#/definitions/{name}"}
        return rv


@dataclasses.dataclass(frozen=True)
class BaseScalarDesc(TypeDesc):
    def describe(
        self,
        name: str,
        defs: typing.Dict[str, typing.Any],
        reg: typing.Dict[uuid.UUID, typing.Dict[str, typing.Any]],
    ) -> typing.Dict[str, typing.Any]:
        return {"type": BASE_SCALAR_TYPES[self.tid]}


@dataclasses.dataclass(frozen=True)
class NamedTupleDesc(TypeDesc):
    fields: typing.Dict[str, TypeDesc]

    def describe(
        self,
        name: str,
        defs: typing.Dict[str, typing.Any],
        reg: typing.Dict[uuid.UUID, typing.Dict[str, typing.Any]],
    ) -> typing.Dict[str, typing.Any]:
        if self.tid in reg:
            return reg[self.tid]
        name = self.find_name(name, defs)
        props = {}
        defs[name] = {
            "type": "object",
            "properties": props,
        }
        reg[self.tid] = rv = {"$ref": f"#/definitions/{name}"}
        for name, desc in self.fields.items():
            props[name] = desc.describe(name, defs, reg)
        return rv


@dataclasses.dataclass(frozen=True)
class TupleDesc(TypeDesc):
    fields: typing.List[TypeDesc]

    def describe(
        self,
        name: str,
        defs: typing.Dict[str, typing.Any],
        reg: typing.Dict[uuid.UUID, typing.Dict[str, typing.Any]],
    ) -> typing.Dict[str, typing.Any]:
        name = self.find_name(name, defs)
        props = {}
        for i, desc in enumerate(self.fields):
            props[i] = desc.describe(f"{name}{i}", defs, reg)
        return {
            "type": "object",
            "properties": props,
        }


@dataclasses.dataclass(frozen=True)
class EnumDesc(TypeDesc):
    names: typing.List[str]

    def describe(
        self,
        name: str,
        defs: typing.Dict[str, typing.Any],
        reg: typing.Dict[uuid.UUID, typing.Dict[str, typing.Any]],
    ) -> typing.Dict[str, typing.Any]:
        if self.tid in reg:
            return reg[self.tid]
        name = self.find_name(name, defs)
        defs[name] = {
            "type": "string",
            "enum": self.names,
        }
        reg[self.tid] = rv = {"$ref": f"#/definitions/{name}"}
        return rv


@dataclasses.dataclass(frozen=True)
class ArrayDesc(SetDesc):
    dim_len: int
    impl: typing.ClassVar[type] = list


@dataclasses.dataclass(frozen=True)
class RangeDesc(TypeDesc):
    inner: TypeDesc

    def describe(
        self,
        name: str,
        defs: typing.Dict[str, typing.Any],
        reg: typing.Dict[uuid.UUID, typing.Dict[str, typing.Any]],
    ) -> typing.Dict[str, typing.Any]:
        if self.tid in reg:
            return reg[self.tid]
        name = self.find_name(name, defs)
        reg[self.tid] = rv = {"$ref": f"#/definitions/{name}"}
        defs[name] = {
            "type": "object",
            "properties": {
                "inc_lower": {"type": "boolean"},
                "inc_upper": {"type": "boolean"},
                "lower": self.inner.describe(f"{name}_in", defs, reg),
                "upper": self.inner.describe(f"{name}_in", defs, reg),
            },
        }
        return rv


@dataclasses.dataclass(frozen=True)
class InputShapeDesc(ShapeDesc):
    fields: typing.Dict[str, typing.Tuple[int, TypeDesc]]
    fields_list: typing.List[typing.Tuple[str, TypeDesc]]
