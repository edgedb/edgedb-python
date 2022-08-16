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

import argparse
import pathlib
import sys
import textwrap
import typing

import edgedb
from edgedb import blocking_client

from . import sertypes


TYPE_MAPPING = {
    "null": "None",
    "string": "str",
    "number": "float",
    "integer": "int",
    "boolean": "bool",
    "std::uuid": "uuid.UUID",
    "std::bytes": "bytes",
    "std::decimal": "decimal.Decimal",
    "std::datetime": "datetime.datetime",
    "std::duration": "datetime.timedelta",
    "cal::local_date": "datetime.date",
    "cal::local_time": "datetime.time",
    "cal::local_datetime": "datetime.datetime",
    "cal::relative_duration": "edgedb.RelativeDuration",
    "cal::date_duration": "edgedb.DateDuration",
    "cfg::memory": "edgedb.ConfigMemory",
}

TYPE_IMPORTS = {
    "std::uuid": "uuid",
    "std::decimal": "decimal",
    "std::datetime": "datetime",
    "std::duration": "datetime",
    "cal::local_date": "datetime",
    "cal::local_time": "datetime",
    "cal::local_datetime": "datetime",
}


class ParseConnection(blocking_client.BlockingIOConnection):
    async def parse(self, query: str):
        return await self._protocol.raw_parse(query)


class ParseClient(edgedb.Client):
    async def _parse(self, query: str):
        con = await self._impl.acquire()
        try:
            return await con.parse(query)
        finally:
            await self._impl.release(con)

    def parse(self, query: str):
        return self._iter_coroutine(self._parse(query))


class DirGenerator:
    def __init__(self, args: argparse.Namespace):
        self._force = args.force
        self._client = ParseClient(
            connection_class=ParseConnection, max_concurrency=1
        )
        with pathlib.Path(__file__).with_name(
            "async_query.py.template" if args.asyncio else "query.py.template"
        ).open() as f:
            self._template = f.read()

    def __enter__(self):
        self._client.ensure_connected()
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        self._client.close()

    def generate_dir(self, dir_: pathlib.Path):
        for file_or_dir in dir_.iterdir():
            file_or_dir = file_or_dir.resolve()
            if not file_or_dir.exists():
                continue
            if file_or_dir.is_dir():
                self.generate_dir(file_or_dir)
            elif file_or_dir.suffix.lower() == ".edgeql":
                if not file_or_dir.match("dbschema/migrations/*.edgeql"):
                    self.generate_file(file_or_dir)

    def generate_file(self, source: pathlib.Path):
        stem = source.stem
        target = source.with_stem(source.stem + "_edgeql").with_suffix(".py")
        if (
            not self._force
            and target.exists()
            and target.stat().st_mtime > source.stat().st_mtime
        ):
            return

        print(f"Generating {target}", file=sys.stderr)
        with source.open() as f:
            query = textwrap.indent(f.read().strip(), " " * 8).lstrip()

        # Parse and build JSON schema
        cardinality, in_dc, out_dc, capabilities = self._client.parse(query)
        in_desc = sertypes.parse(in_dc) if in_dc else None
        out_desc = sertypes.parse(out_dc) if out_dc else None
        schema = sertypes.describe(in_desc, out_desc, stem, cardinality)

        # Generate code from schema
        gen = Generator(schema)
        in_schema = schema["properties"]["input"]
        if in_schema["type"] == "null":
            in_type_args = in_type_call = ""
        elif isinstance(list(in_schema["properties"])[0], int):
            # positional args
            in_type = {
                f"p{k}": gen.generate(v)
                for k, v in sorted(
                    in_schema["properties"].items(),
                )
            }
            in_type_args = textwrap.indent(
                "".join(f"{k}: {v},\n" for k, v in in_type.items()), " " * 4
            )
            in_type_call = textwrap.indent(
                "".join(f"{k},\n" for k in in_type), " " * 8
            )
        else:
            # keyword args
            in_type = {
                k: gen.generate(v) for k, v in in_schema["properties"].items()
            }
            in_type_args = textwrap.indent(
                "".join(f"{k}: {v},\n" for k, v in in_type.items()), " " * 4
            )
            in_type_call = textwrap.indent(
                "".join(f"{k}={k},\n" for k in in_type), " " * 8
            )
        out_schema = schema["properties"]["output"]
        out_type = " -> " + gen.generate(out_schema)

        with target.open("w") as f:
            f.write(
                self._template.format(
                    query=query,
                    stem=stem,
                    gen=gen,
                    in_type_args=in_type_args,
                    in_type_call=in_type_call,
                    out_type=out_type,
                )
            )


class Generator:
    def __init__(self, schema: typing.Dict[str, typing.Any]):
        self.schema = schema
        self._ids = {}
        self._imports = {"edgedb"}
        self.defs = {}
        self._aliases = {}
        for k, j in schema["definitions"].items():
            if j["type"] == "object":
                self._imports.add("dataclasses")
                fields = []
                for name, sub_json in j["properties"].items():
                    fields.append(f"{name}: {self.generate(sub_json)}")
                self.defs[k] = (
                    textwrap.dedent(
                        f"""
                    @dataclasses.dataclass
                    class {self.get_id(k)}:
                    {{fields}}

                        @classmethod
                        def __get_validators__(cls):
                            return []
                """
                    )
                    .strip()
                    .format(fields=textwrap.indent("\n".join(fields), "    "))
                )
            elif "enum" in j:
                raise NotImplementedError(f"Enum is not supported")
            else:
                self._aliases[k] = f"{self.get_id(k)} = {self.generate(j)}"

    def get_id(self, name: str) -> str:
        if name in self._ids:
            return self._ids[name]
        new_name = name.title().replace("_", "")
        if new_name in self._ids.values():
            new_name = name.title()
        self._ids[name] = new_name
        return new_name

    @property
    def imports(self):
        return "\n".join(f"import {m}" for m in sorted(self._imports))

    @property
    def definitions(self):
        return "".join(f"{d}\n\n\n" for _, d in sorted(self.defs.items()))

    @property
    def aliases(self):
        if self._aliases:
            return (
                "\n".join(a for _, a in sorted(self._aliases.items()))
                + "\n\n\n"
            )
        else:
            return ""

    def generate(self, json_schema) -> str:
        if "type" in json_schema:
            type_ = json_schema["type"]
            if type_ in TYPE_MAPPING:
                if type_ in TYPE_IMPORTS:
                    self._imports.add(TYPE_IMPORTS[type_])
                return TYPE_MAPPING[type_]
            elif type_ == "array":
                self._imports.add("typing")
                return (
                    f"typing.Sequence[{self.generate(json_schema['items'])}]"
                )
            elif type_ == "object" and "properties" in json_schema:
                # tuple
                self._imports.add("typing")
                content = ", ".join(
                    self.generate(v)
                    for _, v in sorted(
                        json_schema["properties"].items(),
                        key=lambda x: int(x[0]),
                    )
                )
                return f"typing.Tuple[{content}]"
            elif type_ == "object":
                return "edgedb.Object"
            raise NotImplementedError(f"Type {type_} is not supported")
        else:
            prefix = "#/definitions/"
            assert json_schema["$ref"].startswith(prefix)
            return self.get_id(json_schema["$ref"][len(prefix) :])
