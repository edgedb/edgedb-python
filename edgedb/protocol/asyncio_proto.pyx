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
        self.disconnected_fut = None

        self.msg_waiter = None

    cpdef abort(self):
        self.connected = False
        self.terminate()
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

        try:
            self.msg_waiter = self.loop.create_future()
            await self.msg_waiter
            return
        except asyncio.CancelledError:
            # TODO: A proper cancellation requires server/protocol
            # support, which isn't yet available.  Therefore,
            # we're disabling asyncio cancellation completely
            # until we can implement it properly.
            try:
                self.cancelled = True
                self.abort()
            finally:
                raise

    async def try_recv_eagerly(self):
        pass

    async def wait_for_connect(self):
        if self.connected_fut is not None:
            await self.connected_fut

    async def wait_for_disconnect(self):
        if not self.connected:
            return
        else:
            self.disconnected_fut = self.loop.create_future()
            try:
                await self.disconnected_fut
            except ConnectionError:
                pass
            finally:
                self.disconnected_fut = None

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

        if (
            self.disconnected_fut is not None
            and not self.disconnected_fut.done()
        ):
            self.disconnected_fut.set_exception(ConnectionResetError())

        if self.msg_waiter is not None and not self.msg_waiter.done():
            self.msg_waiter.set_exception(ConnectionResetError())
            self.msg_waiter = None

        self.transport = None

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def eof_received(self):
        pass

    def data_received(self, data):
        self.buffer.feed_data(data)

        if (self.msg_waiter is not None and
                self.buffer.take_message() and
                not self.msg_waiter.done()):
            self.msg_waiter.set_result(True)
            self.msg_waiter = None


cdef class AsyncIOUpgradeProtocol(protocol.HttpUpgradeProtocol):

    def __init__(self, loop):
        protocol.HttpUpgradeProtocol.__init__(self)
        self.waiter = loop.create_future()
        self.connected_fut = loop.create_future()
        self.transport = None

    def connection_made(self, transport):
        self.transport = transport
        self.connected_fut.set_result(True)
        self.connected_fut = None

    def data_received(self, data):
        if self.transport is None or self.waiter.done():
            raise RuntimeError(
                "Unexpected data received in AsyncIOUpgradeProtocol"
            )

        try:
            self.feed_data(data)
        except Exception as e:
            self.transport.pause_reading()
            self.waiter.set_exception(e)
        else:
            if self.message_completed:
                self.transport.pause_reading()
                self.waiter.set_result(True)

    def pause_writing(self):
        pass

    def resume_writing(self):
        pass

    def eof_received(self):
        # TODO: feed EOF to the parser
        pass

    def connection_lost(self, exc):
        self.transport = None
        if self.connected_fut is not None and not self.connected_fut.done():
            self.connected_fut.set_exception(ConnectionAbortedError())
        if not self.waiter.done():
            self.waiter.set_exception(ConnectionResetError())

    def abort(self):
        if self.transport is not None:
            self.transport.close()
            self.transport = None

    async def wait_for_response(self):
        try:
            await self.waiter
        except asyncio.CancelledError:
            try:
                self.abort()
            finally:
                raise

    def _upgrade(self, factory, remaining_data):
        transport, self.transport = self.transport, None
        if transport is not None:
            proto = factory()
            transport.set_protocol(proto)
            proto.connection_made(transport)
            if remaining_data:
                proto.data_received(remaining_data)
            transport.resume_reading()
            return proto

    async def wait_for_connect(self):
        if self.connected_fut is not None:
            await self.connected_fut

    cdef write(self, data):
        if self.transport is None:
            raise ConnectionAbortedError
        self.transport.write(data)
