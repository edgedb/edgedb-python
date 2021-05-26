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


import errno
import random
import socket


def find_available_port(port_range=(49152, 65535), max_tries=1000):
    low, high = port_range

    port = low
    try_no = 0

    while try_no < max_tries:
        try_no += 1
        port = random.randint(low, high)
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            sock.bind(('localhost', port))
        except socket.error as e:
            if e.errno == errno.EADDRINUSE:
                continue
        finally:
            sock.close()

        break
    else:
        port = None

    return port
