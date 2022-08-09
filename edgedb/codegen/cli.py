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

from . import generator


parser = argparse.ArgumentParser(
    description="Generate Python code for .edgeql files."
)
parser.add_argument(
    "file_or_dir",
    metavar="PATH",
    nargs="?",
    type=pathlib.Path,
    help=(
        "Path to an .edgeql file, or a directory that contains .edgeql "
        "files (subdirectories included). Default: current directory"
    ),
)
parser.add_argument(
    "-f",
    "--force",
    action="store_true",
    help="Force generate all .edgeql files, ignore timestamps",
)


def main():
    args = parser.parse_args()
    with generator.Generator(args) as gen:
        if args.file_or_dir is None:
            gen.generate_dir(pathlib.Path.cwd())
        else:
            file_or_dir = args.file_or_dir.resolve()
            if file_or_dir.is_dir():
                gen.generate_dir(file_or_dir)
            else:
                gen.generate_file(file_or_dir)
