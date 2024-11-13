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


DEF _MAXINT32 = 2**31 - 1

DEF CLIENT_HANDSHAKE_MSG = b'V'
DEF SERVER_HANDSHAKE_MSG = b'v'
DEF SERVER_KEY_DATA_MSG = b'K'
DEF ERROR_RESPONSE_MSG = b'E'
DEF READY_FOR_COMMAND_MSG = b'Z'
DEF SYNC_MSG = b'S'
DEF FLUSH_MSG = b'H'
DEF COMMAND_COMPLETE_MSG = b'C'
DEF DATA_MSG = b'D'
DEF COMMAND_DATA_DESC_MSG = b'T'
DEF LOG_MSG = b'L'
DEF PARAMETER_STATUS_MSG = b'S'
DEF AUTH_REQUEST_MSG = b'R'
DEF AUTH_INITIAL_RESPONSE_MSG = b'p'
DEF AUTH_RESPONSE_MSG = b'r'
DEF PREPARE_MSG = b'P'
DEF PREPARE_COMPLETE_MSG = b'1'
DEF DESCRIBE_STMT_MSG = b'D'
DEF STMT_DATA_DESC_MSG = b'T'
DEF STATE_DATA_DESC_MSG = b's'
DEF LEGACY_EXECUTE_MSG = b'E'
DEF EXECUTE_MSG = b'O'
DEF EXECUTE_SCRIPT_MSG = b'Q'
DEF TERMINATE_MSG = b'X'

DEF DUMP_MSG = b'>'
DEF DUMP_HEADER_BLOCK_MSG = b'@'
DEF DUMP_BLOCK_MSG = b'='
DEF RESTORE_MSG = b'<'
DEF RESTORE_READY_MSG = b'+'
DEF RESTORE_EOF_MSG = b'.'

DEF CARDINALITY_ONE = b'o'
DEF CARDINALITY_MANY = b'm'
DEF CARDINALITY_NOT_APPLICABLE = b'n'

DEF DESCRIBE_ASPECT_DATA = b'T'

DEF TRANS_STATUS_IDLE = b'I'
DEF TRANS_STATUS_INTRANS = b'T'
DEF TRANS_STATUS_ERROR = b'E'

DEF PROTO_VER_MAJOR = 3
DEF PROTO_VER_MINOR = 0

DEF MIN_PROTO_VER_MAJOR = 0
DEF MIN_PROTO_VER_MINOR = 13
