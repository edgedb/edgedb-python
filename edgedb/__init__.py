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
from .asyncio_pool import create_async_pool, AsyncIOPool
from .blocking_con import connect
from .transaction import Transaction, AsyncIOTransaction


__all__ = (
    'async_connect', 'connect', 'create_async_pool', 'AsyncIOPool',
    'EnumValue', 'Tuple', 'NamedTuple', 'Set',
    'Object', 'Array', 'Link', 'LinkSet',
    'Transaction', 'AsyncIOTransaction',
) + errors.__all__


# The rules of changing __version__:
#
#    In a release revision, __version__ must be set to 'x.y.z',
#    and the release revision tagged with the 'vx.y.z' tag.
#    For example, release 0.15.0 should have
#    __version__ set to '0.15.0', and tagged with 'v0.15.0'.
#
#    In between releases, __version__ must be set to
#    'x.y+1.0.dev0', so revisions between 0.15.0 and
#    0.16.0 should have __version__ set to '0.16.0.dev0' in
#    the source.
#
#    Source and wheel distributions built from development
#    snapshots will automatically include the git revision
#    in __version__, for example: '0.16.0.dev0+ge06ad03'

__version__ = '0.6.0'
