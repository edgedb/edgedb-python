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
import getpass
import pathlib
import sys

import edgedb
from edgedb.con_utils import find_edgedb_project_dir


FILE_MODE_OUTPUT_FILE = "generated_edgeql.py"


def _get_conn_args(args: argparse.Namespace):
    if args.password_from_stdin:
        if args.password:
            print(
                "--password and --password-from-stdin are "
                "mutually exclusive",
                file=sys.stderr,
            )
            sys.exit(22)
        if sys.stdin.isatty():
            password = getpass.getpass()
        else:
            password = sys.stdin.read().strip()
    else:
        password = args.password
    if args.dsn and args.instance:
        print("--dsn and --instance are mutually exclusive", file=sys.stderr)
        sys.exit(22)
    return dict(
        dsn=args.dsn or args.instance,
        credentials_file=args.credentials_file,
        host=args.host,
        port=args.port,
        database=args.database,
        user=args.user,
        password=password,
        tls_ca_file=args.tls_ca_file,
        tls_security=args.tls_security,
    )


class Generator:
    def __init__(self, args: argparse.Namespace):
        try:
            self._project_dir = pathlib.Path(find_edgedb_project_dir())
        except edgedb.ClientConnectionError:
            print(
                "Cannot find edgedb.toml: codegen must be run under an EdgeDB project dir"
            )
            sys.exit(2)
        print(f"Found EdgeDB project: {self._project_dir}", file=sys.stderr)
        self._client = edgedb.create_client(**_get_conn_args(args))
        self._file_mode = args.file
        self._method_names = set()
        self._describe_results = []
        self._output = []

    def run(self):
        try:
            self._client.ensure_connected()
        except edgedb.EdgeDBError as e:
            print(f"Failed to connect to EdgeDB instance: {e}")
            sys.exit(61)
        with self._client:
            self._process_dir(self._project_dir)
        if self._file_mode:
            self._generate_single_file()
        else:
            self._generate_files()

    def _process_dir(self, dir_: pathlib.Path):
        for file_or_dir in dir_.iterdir():
            file_or_dir = file_or_dir.resolve()
            if not file_or_dir.exists():
                continue
            if file_or_dir.is_dir():
                if file_or_dir.relative_to(self._project_dir) != pathlib.Path(
                    "dbschema/migrations"
                ):
                    self._process_dir(file_or_dir)
            elif file_or_dir.suffix.lower() == ".edgeql":
                self._process_file(file_or_dir)

    def _process_file(self, source: pathlib.Path):
        print(f"Processing {source}", file=sys.stderr)
        with source.open() as f:
            query = f.read()
        name = source.stem
        if self._file_mode:
            if name in self._method_names:
                print(f"Conflict method names: {name}", file=sys.stderr)
                sys.exit(17)
            self._method_names.add(name)
        dr = self._client.describe(query)
        self._describe_results.append((name, source, dr))

    def _generate_files(self):
        for name, source, dr in self._describe_results:
            target = source.with_stem(f"{name}_edgeql").with_suffix(".py")
            print(f"Generating {target}", file=sys.stderr)
            content = self._generate(name, dr)
            with target.open("w") as f:
                f.write(content)

    def _generate_single_file(self):
        target = self._project_dir / FILE_MODE_OUTPUT_FILE
        print(f"Generating {target}", file=sys.stderr)
        for name, _, dr in self._describe_results:
            self._output.append(self._generate(name, dr))
        with target.open("w") as f:
            f.writelines(self._output)

    def _generate(self, name: str, dr: edgedb.DescribeResult) -> str:
        return f"{name}: {dr}\n"
