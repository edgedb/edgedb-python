#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2021-present MagicStack Inc. and the EdgeDB authors.
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

import enum


class Capability(enum.IntFlag):
    NONE = 0
    MODIFICATIONS = 1 << 0
    SESSION_CONFIG = 1 << 1
    TRANSACTION = 1 << 2
    DDL = 1 << 3
    PERSISTENT_CONFIG = 1 << 4

    ALL = 0xFFFF_FFFF_FFFF_FFFF
    EXECUTE = ALL & ~TRANSACTION & ~SESSION_CONFIG
    LEGACY_EXECUTE = ALL & ~TRANSACTION


class CompilationFlag(enum.IntFlag):
    INJECT_OUTPUT_TYPE_IDS = 1 << 0
    INJECT_OUTPUT_TYPE_NAMES = 1 << 1
    INJECT_OUTPUT_OBJECT_IDS = 1 << 2


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

    def is_single(self) -> bool:
        return self in {Cardinality.AT_MOST_ONE, Cardinality.ONE}

    def is_multi(self) -> bool:
        return self in {Cardinality.AT_LEAST_ONE, Cardinality.MANY}


class ElementKind(enum.Enum):
    LINK = 1
    PROPERTY = 2
    LINK_PROPERTY = 3
