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

from edgedb.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,
)

from . cimport protocol


cdef class AsyncIOProtocol(protocol.SansIOProtocol):

    def __init__(self, con_params, loop):
        protocol.SansIOProtocol.__init__(self, con_params)

        self.loop = loop
        self.transport = None
        self.connected_fut = loop.create_future()

        self.msg_waiter = None

    cpdef abort(self):
        self.terminate()
        self.connected = False
        if self.transport is not None:
            self.transport.close()
            self.transport = None

    cdef write(self, WriteBuffer buf):
        if self.transport is None:
            raise ConnectionAbortedError
        self.transport.write(memoryview(buf))

    async def wait_for_message(self):
        if self.buffer.take_message():
            return

        while True:
            try:
                self.msg_waiter = self.loop.create_future()
                await self.msg_waiter
                return
            except asyncio.CancelledError:
                # TODO: A proper cancellation requires server/protocol
                # support, which isn't yet available.  Therefore,
                # we're disabling asyncio cancellation completely
                # until we can implement it properly.
                pass

    async def try_recv_eagerly(self):
        pass

    async def wait_for_connect(self):
        if self.connected_fut is not None:
            await self.connected_fut

    def connection_made(self, transport):
        if self.transport is not None:
            raise RuntimeError('connection_made: invalid connection status')
        self.transport = transport
        self.connected_fut.set_result(True)
        self.connected_fut = None

    def connection_lost(self, exc):
        self.connected = False

        if self.connected_fut is not None and not self.connected_fut.done():
            self.connected_fut.set_exception(ConnectionAbortedError())
            return

        if self.msg_waiter is not None and not self.msg_waiter.done():
            self.msg_waiter.set_exception(ConnectionAbortedError())
            self.msg_waiter = None

        self.transport = None

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def data_received(self, data):
        self.buffer.feed_data(data)

        if (self.msg_waiter is not None and
                self.buffer.take_message() and
                not self.msg_waiter.done()):
            self.msg_waiter.set_result(True)
            self.msg_waiter = None

    def eof_received(self):
        pass
