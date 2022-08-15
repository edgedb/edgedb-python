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

import edgedb


class Generator:
    def __init__(self, args: argparse.Namespace):
        self._force = args.force
        self._client = edgedb.create_client()
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
                self.generate_file(file_or_dir)

    def generate_file(self, source: pathlib.Path):
        target = source.with_suffix("_edgeql.py")
        if (
            not self._force
            and target.exists()
            and target.stat().st_mtime > source.stat().st_mtime
        ):
            return
        print(f"Generating {target}", file=sys.stderr)
        with source.open() as f:
            content = textwrap.indent(f.read().strip(), " " * 8).lstrip()
        with target.open("w") as f:
            f.write(self._template.format(content=content))
