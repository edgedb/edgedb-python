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

from .. import con_utils
from .. import errors
from . cimport protocol


DEF RECV_BUF = 65536


cdef class BlockingIOProtocol(protocol.SansIOProtocolBackwardsCompatible):

    def __init__(self, con_params, sock):
        protocol.SansIOProtocolBackwardsCompatible.__init__(self, con_params)
        self.sock = sock

    cpdef abort(self):
        self.terminate()
        self._disconnect()

    cdef _disconnect(self):
        self.connected = False
        if self.sock is not None:
            self.sock.close()
            self.sock = None

    cdef write(self, WriteBuffer buf):
        try:
            self.sock.send(buf)
        except OSError as e:
            self._disconnect()
            raise con_utils.wrap_error(e) from e

    async def wait_for_message(self):
        while not self.buffer.take_message():
            try:
                data = self.sock.recv(RECV_BUF)
            except OSError as e:
                self._disconnect()
                raise con_utils.wrap_error(e) from e
            if not data:
                self._disconnect()
                raise errors.ClientConnectionClosedError()
            self.buffer.feed_data(data)

    async def try_recv_eagerly(self):
        if self.buffer.take_message():
            return

        self.sock.settimeout(0)  # Make non-blocking.
        try:
            while not self.buffer.take_message():
                data = self.sock.recv(RECV_BUF)
                if not data:
                    self._disconnect()
                    raise errors.ClientConnectionClosedError()
                self.buffer.feed_data(data)
        except BlockingIOError:
            # No data in the socket net buffer.
            return
        except OSError as e:
            self._disconnect()
            raise con_utils.wrap_error(e) from e
        finally:
            self.sock.settimeout(None)

    async def wait_for_connect(self):
        return True

    async def wait_for_disconnect(self):
        if self.cancelled or not self.connected:
            return
        try:
            while True:
                if not self.buffer.take_message():
                    await self.wait_for_message()
                self.fallthrough()
        except errors.ClientConnectionClosedError:
            pass
