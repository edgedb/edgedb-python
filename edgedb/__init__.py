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


# flake8: noqa

from .errors import *

from edgedb.datatypes.datatypes import Tuple, NamedTuple, EnumValue
from edgedb.datatypes.datatypes import Set, Object, Array, Link, LinkSet
from edgedb.datatypes.datatypes import Duration

from .asyncio_con import async_connect
from .asyncio_pool import create_async_pool
from .blocking_con import connect


__all__ = (
    'async_connect', 'connect', 'create_async_pool',
    'EnumValue', 'Tuple', 'NamedTuple', 'Set',
    'Object', 'Array', 'Link', 'LinkSet',
) + errors.__all__
