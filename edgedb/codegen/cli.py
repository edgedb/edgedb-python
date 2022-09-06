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

from . import generator


parser = argparse.ArgumentParser(
    description="Generate Python code for .edgeql files."
)
parser.add_argument("--dsn")
parser.add_argument("--credentials_file", metavar="PATH")
parser.add_argument("-I", "--instance", metavar="NAME")
parser.add_argument("-H", "--host")
parser.add_argument("-P", "--port")
parser.add_argument("-d", "--database", metavar="NAME")
parser.add_argument("-u", "--user")
parser.add_argument("--password")
parser.add_argument("--password-from-stdin", action="store_true")
parser.add_argument("--tls-ca-file", metavar="PATH")
parser.add_argument(
    "--tls-security",
    choices=["default", "strict", "no_host_verification", "insecure"],
)
parser.add_argument("--file", action="store_true")


def main():
    args = parser.parse_args()
    generator.Generator(args).run()
