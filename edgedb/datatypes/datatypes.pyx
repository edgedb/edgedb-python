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

include "./relative_duration.pyx"
include "./enum.pyx"


_RecordDescriptor = EdgeRecordDesc_InitType()
Tuple = EdgeTuple_InitType()
NamedTuple = EdgeNamedTuple_InitType()
Object = EdgeObject_InitType()
Set = EdgeSet_InitType()
Array = EdgeArray_InitType()
Link = EdgeLink_InitType()
LinkSet = EdgeLinkSet_InitType()


_EDGE_POINTER_IS_IMPLICIT = EDGE_POINTER_IS_IMPLICIT
_EDGE_POINTER_IS_LINKPROP = EDGE_POINTER_IS_LINKPROP
_EDGE_POINTER_IS_LINK = EDGE_POINTER_IS_LINK


def get_object_descriptor(obj):
    return EdgeObject_GetRecordDesc(obj)


def create_object_factory(**pointers):
    flags = ()
    names = ()
    for pname, ptype in pointers.items():
        names += (pname,)

        if not isinstance(ptype, set):
            ptype = {ptype}

        flag = 0
        for pt in ptype:
            if pt == 'link':
                flag |= EDGE_POINTER_IS_LINK
            elif pt == 'property':
                pass
            elif pt == 'link-property':
                flag |= EDGE_POINTER_IS_LINKPROP
            elif pt == 'implicit':
                flag |= EDGE_POINTER_IS_IMPLICIT
            else:
                raise ValueError(f'unknown pointer type {pt}')

        flags += (flag,)

    desc = EdgeRecordDesc_New(names, flags)
    size = len(pointers)

    def factory(*items):
        if len(items) != size:
            raise ValueError

        o = EdgeObject_New(desc)
        for i in range(size):
            EdgeObject_SetItem(o, i, items[i])

        return o

    return factory


cdef record_desc_new(object names, object flags):
    return EdgeRecordDesc_New(names, flags)


cdef record_desc_pointer_name(object desc, Py_ssize_t pos):
    return EdgeRecordDesc_PointerName(desc, pos)


cdef tuple_new(Py_ssize_t size):
    return EdgeTuple_New(size)


cdef tuple_set(object tuple, Py_ssize_t pos, object elem):
    EdgeTuple_SetItem(tuple, pos, elem)


cdef namedtuple_new(object desc):
    return EdgeNamedTuple_New(desc)


cdef namedtuple_set(object tuple, Py_ssize_t pos, object elem):
    EdgeNamedTuple_SetItem(tuple, pos, elem)


cdef object_new(object desc):
    return EdgeObject_New(desc)


cdef object_set(object obj, Py_ssize_t pos, object elem):
    EdgeObject_SetItem(obj, pos, elem)


cdef bint set_check(object set):
    return EdgeSet_Check(set)


cdef set_new(Py_ssize_t size):
    return EdgeSet_New(size)


cdef set_set(object set, Py_ssize_t pos, object elem):
    EdgeSet_SetItem(set, pos, elem)


cdef set_append(object set, object elem):
    EdgeSet_AppendItem(set, elem)


cdef array_new(Py_ssize_t size):
    return EdgeArray_New(size)


cdef array_set(object array, Py_ssize_t pos, object elem):
    EdgeArray_SetItem(array, pos, elem)
