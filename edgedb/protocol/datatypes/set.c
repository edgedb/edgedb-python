/*
* This source file is part of the EdgeDB open source project.
*
* Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
*     http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/


#include "datatypes.h"
#include "internal.h"


static int init_type_called = 0;
static Py_hash_t base_hash = -1;


PyObject *
EdgeSet_New(Py_ssize_t size)
{
    assert(init_type_called);

    PyObject *l = PyList_New(size);
    if (l == NULL) {
        return NULL;
    }

    EdgeSetObject *o = PyObject_GC_New(EdgeSetObject, &EdgeSet_Type);
    if (o == NULL) {
        Py_DECREF(l);
        return NULL;
    }

    o->els = l;
    o->cached_hash = -1;

    PyObject_GC_Track(o);
    return (PyObject *)o;
}


int
EdgeSet_SetItem(PyObject *ob, Py_ssize_t pos, PyObject *el)
{
    assert(EdgeSet_Check(ob));
    EdgeSetObject *o = (EdgeSetObject *)ob;
    Py_INCREF(el);
    return PyList_SetItem(o->els, pos, el);
}


int
EdgeSet_AppendItem(PyObject *ob, PyObject *el)
{
    assert(EdgeSet_Check(ob));
    EdgeSetObject *o = (EdgeSetObject *)ob;
    return PyList_Append(o->els, el);
}


static PyObject *
set_tp_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    if (args == NULL ||
            PyTuple_Size(args) != 1 ||
            (kwds != NULL && PyDict_Size(kwds)))
    {
        PyErr_SetString(
            PyExc_TypeError,
            "edgedb.Set accepts only one positional argument");
        return NULL;
    }

    EdgeSetObject *o = (EdgeSetObject *)EdgeSet_New(0);
    if (o == NULL) {
        return NULL;
    }

    PyObject *res = _PyList_Extend((PyListObject *)o->els,
                                   PyTuple_GET_ITEM(args, 0));
    if (res == NULL) {
        Py_DECREF(o);
        return NULL;
    }
    Py_DECREF(res);

    return (PyObject *)o;
}


static int
set_traverse(EdgeSetObject *o, visitproc visit, void *arg)
{
    Py_VISIT(o->els);
    return 0;
}


static void
set_dealloc(EdgeSetObject *o)
{
    PyObject_GC_UnTrack(o);
    Py_TRASHCAN_SAFE_BEGIN(o)
    o->cached_hash = -1;
    Py_CLEAR(o->els);
    Py_TRASHCAN_SAFE_END(o)
    Py_TYPE(o)->tp_free((PyObject *)o);
}


static Py_hash_t
set_hash(EdgeSetObject *o)
{
    if (o->cached_hash == -1) {
        o->cached_hash = _EdgeGeneric_HashWithBase(
            base_hash,
            _PyList_ITEMS(o->els),
            PyList_GET_SIZE(o->els));
    }
    return o->cached_hash;
}


static Py_ssize_t
set_length(EdgeSetObject *o)
{
    return PyList_GET_SIZE(o->els);
}


static PyObject *
set_getitem(EdgeSetObject *o, Py_ssize_t i)
{
    if (i < 0 || i >= PyList_GET_SIZE(o->els)) {
        PyErr_SetString(PyExc_IndexError, "edgedb.Set index out of range");
        return NULL;
    }
    PyObject *val = PyList_GetItem(o->els, i);
    Py_INCREF(val);
    return val;
}


static PyObject *
set_repr(EdgeSetObject *o)
{
    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;

    if (_PyUnicodeWriter_WriteASCIIString(&writer, "Set{", 4) < 0) {
        goto error;
    }

    if (_EdgeGeneric_RenderValues(&writer, (PyObject *)o,
                                  _PyList_ITEMS(o->els),
                                  PyList_GET_SIZE(o->els)) < 0)
    {
        goto error;
    }

    if (_PyUnicodeWriter_WriteChar(&writer, '}') < 0) {
        goto error;
    }

    return _PyUnicodeWriter_Finish(&writer);

error:
    _PyUnicodeWriter_Dealloc(&writer);
    return NULL;
}



static PySequenceMethods set_as_sequence = {
    .sq_length = (lenfunc)set_length,
    .sq_item = (ssizeargfunc)set_getitem,
};


PyTypeObject EdgeSet_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "edgedb.Set",
    .tp_basicsize = sizeof(EdgeSetObject),
    .tp_dealloc = (destructor)set_dealloc,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)set_traverse,
    .tp_new = set_tp_new,
    .tp_hash = (hashfunc)set_hash,
    .tp_as_sequence = &set_as_sequence,
    .tp_repr = (reprfunc)set_repr,
    .tp_free = PyObject_GC_Del,
};


PyObject *
EdgeSet_InitType(void)
{
    if (PyType_Ready(&EdgeSet_Type) < 0) {
        return NULL;
    }

    base_hash = _EdgeGeneric_HashString("edgedb.Set");
    if (base_hash == -1) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeSet_Type;
}
