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


cimport cython
cimport cpython


include "./relative_duration.pxd"
include "./enum.pxd"


cdef extern from "datatypes.h":

    int EDGE_POINTER_IS_IMPLICIT
    int EDGE_POINTER_IS_LINKPROP
    int EDGE_POINTER_IS_LINK

    object EdgeRecordDesc_InitType()
    object EdgeRecordDesc_New(object, object)
    object EdgeRecordDesc_PointerName(object, Py_ssize_t pos)

    object EdgeTuple_InitType()
    object EdgeTuple_New(Py_ssize_t)
    int EdgeTuple_SetItem(object, Py_ssize_t, object) except -1

    object EdgeNamedTuple_InitType()
    object EdgeNamedTuple_New(object)
    int EdgeNamedTuple_SetItem(object, Py_ssize_t, object) except -1

    object EdgeObject_InitType()
    object EdgeObject_New(object);
    int EdgeObject_SetItem(object, Py_ssize_t, object) except -1
    object EdgeObject_GetRecordDesc(object)

    bint EdgeSet_Check(object)
    object EdgeSet_InitType()
    object EdgeSet_New(Py_ssize_t);
    int EdgeSet_SetItem(object, Py_ssize_t, object) except -1
    int EdgeSet_AppendItem(object, object) except -1

    object EdgeArray_InitType()
    object EdgeArray_New(Py_ssize_t);
    int EdgeArray_SetItem(object, Py_ssize_t, object) except -1

    object EdgeLink_InitType()

    object EdgeLinkSet_InitType()


cdef record_desc_new(object names, object flags)
cdef record_desc_pointer_name(object desc, Py_ssize_t pos)
cdef tuple_new(Py_ssize_t size)
cdef tuple_set(object tuple, Py_ssize_t pos, object elem)
cdef namedtuple_new(object desc)
cdef namedtuple_set(object tuple, Py_ssize_t pos, object elem)
cdef object_new(object desc)
cdef object_set(object tuple, Py_ssize_t pos, object elem)
cdef bint set_check(object set)
cdef set_new(Py_ssize_t size)
cdef set_set(object set, Py_ssize_t pos, object elem)
cdef set_append(object set, object elem)
cdef array_new(Py_ssize_t size)
cdef array_set(object array, Py_ssize_t pos, object elem)
