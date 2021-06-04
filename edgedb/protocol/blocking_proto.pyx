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


from edgedb.pgproto.pgproto cimport (
    WriteBuffer,
    ReadBuffer,
)


from . cimport protocol


DEF RECV_BUF = 65536


cdef _iter_coroutine(coro):
    try:
        coro.send(None)
    except StopIteration as ex:
        if ex.args:
            result = ex.args[0]
        else:
            result = None
    finally:
        coro.close()
    return result


cdef class BlockingIOProtocol(protocol.SansIOProtocol):

    def __init__(self, con_params, sock):
        protocol.SansIOProtocol.__init__(self, con_params)
        self.sock = sock

    cpdef abort(self):
        self.terminate()
        self.connected = False
        if self.sock is not None:
            self.sock.close()
            self.sock = None

    cdef write(self, WriteBuffer buf):
        self.sock.send(buf)

    async def wait_for_message(self):
        while not self.buffer.take_message():
            data = self.sock.recv(RECV_BUF)
            if not data:
                raise ConnectionAbortedError
            self.buffer.feed_data(data)

    async def try_recv_eagerly(self):
        if self.buffer.take_message():
            return

        self.sock.settimeout(0)  # Make non-blocking.
        try:
            while not self.buffer.take_message():
                data = self.sock.recv(RECV_BUF)
                if not data:
                    raise ConnectionAbortedError
                self.buffer.feed_data(data)
        except BlockingIOError:
            # No data in the socket net buffer.
            return
        finally:
            self.sock.settimeout(None)

    async def wait_for_connect(self):
        return True

    def sync_connect(self):
        return _iter_coroutine(self.connect())

    def sync_execute_anonymous(self, *args, **kwargs):
        result, _headers = _iter_coroutine(
            self.execute_anonymous(*args, **kwargs),
        )
        # don't expose headers to blocking client for now
        return result

    def sync_simple_query(self, *args, **kwargs):
        return _iter_coroutine(self.simple_query(*args, **kwargs))

    def sync_dump(self, *, header_callback, block_callback):
        async def header_wrapper(data):
            header_callback(data)
        async def block_wrapper(data):
            block_callback(data)
        return _iter_coroutine(self.dump(header_wrapper, block_wrapper))

    def sync_restore(self, *, header, data_gen):
        async def wrapper():
            while True:
                try:
                    block = next(data_gen)
                except StopIteration:
                    return
                yield block

        return _iter_coroutine(self.restore(header, wrapper()))


cdef class BlockingIOUpgradeProtocol(protocol.HttpUpgradeProtocol):

    def __init__(self, sock):
        protocol.HttpUpgradeProtocol.__init__(self)
        self.sock = sock

    cdef write(self, data):
        self.sock.send(data)

    async def wait_for_response(self):
        while not self.message_completed:
            data = self.sock.recv(RECV_BUF)
            if not data:
                raise ConnectionAbortedError
            self.feed_data(data)

    def abort(self):
        if self.sock is not None:
            self.sock.close()
            self.sock = None

    def sync_upgrade(self, factory):
        return _iter_coroutine(self.upgrade(factory))

    def _upgrade(self, factory, remaining_data):
        proto = factory()
        if remaining_data:
            proto.buffer.feed_data(remaining_data)
        return proto

    async def wait_for_connect(self):
        pass
