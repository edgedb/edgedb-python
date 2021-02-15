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

import asyncio
import sys


if sys.version_info < (3, 7):
    # Workaround for https://bugs.python.org/issue37658
    import functools

    async def _cancel_and_wait(fut):
        def _release_waiter(waiter, *args):
            if not waiter.done():
                waiter.set_result(None)

        waiter = asyncio.get_event_loop().create_future()
        cb = functools.partial(_release_waiter, waiter)
        fut.add_done_callback(cb)

        try:
            fut.cancel()
            await waiter
        finally:
            fut.remove_done_callback(cb)

    async def wait_for(fut, timeout):
        fut = asyncio.ensure_future(fut)

        try:
            return await asyncio.wait_for(fut, timeout)
        except (asyncio.CancelledError, asyncio.TimeoutError):
            if fut.done():
                return fut.result()
            else:
                await _cancel_and_wait(fut)
                raise

else:
    wait_for = asyncio.wait_for
