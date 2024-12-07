#include "pythoncapi_compat.h"

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
#include "freelist.h"
#include "internal.h"


static int init_type_called = 0;

EDGE_SETUP_FREELIST(
    EDGE_RECORD,
    EdgeRecord,
    EDGE_RECORD_FREELIST_MAXSAVE,
    EDGE_RECORD_FREELIST_SIZE)


#define EdgeRecord_GET_ITEM(op, i) \
    (((EdgeRecord *)(op))->ob_item[i])
#define EdgeRecord_SET_ITEM(op, i, v) \
    (((EdgeRecord *)(op))->ob_item[i] = v)


PyObject *
EdgeRecord_New(PyObject *desc)
{
    assert(init_type_called);

    if (desc == NULL || !EdgeRecordDesc_Check(desc)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    Py_ssize_t size = EdgeRecordDesc_GetSize(desc);

    if (size > EDGE_MAX_TUPLE_SIZE) {
        PyErr_Format(
            PyExc_ValueError,
            "Cannot create Object with more than %d elements",
            EDGE_MAX_TUPLE_SIZE);
        return NULL;
    }

    EdgeRecord *o = NULL;
    EDGE_NEW_WITH_FREELIST(EDGE_RECORD, EdgeRecord,
                           &EdgeRecord_Type, o, size);
    assert(o != NULL);
    assert(Py_SIZE(o) == size);
    assert(EdgeRecord_Check(o));

    o->weakreflist = NULL;

    Py_INCREF(desc);
    o->desc = desc;

    o->cached_hash = -1;

    PyObject_GC_Track(o);
    return (PyObject *)o;
}


PyObject *
EdgeRecord_GetRecordDesc(PyObject *o)
{
    if (!EdgeRecord_Check(o)) {
        PyErr_Format(
            PyExc_TypeError,
            "an instance of edgedb.Object expected");
        return NULL;
    }

    PyObject *desc = ((EdgeRecord *)o)->desc;
    Py_INCREF(desc);
    return desc;
}


int
EdgeRecord_SetItem(PyObject *ob, Py_ssize_t i, PyObject *el)
{
    assert(EdgeRecord_Check(ob));
    EdgeRecord *o = (EdgeRecord *)ob;
    assert(i >= 0);
    assert(i < Py_SIZE(o));
    Py_INCREF(el);
    EdgeRecord_SET_ITEM(o, i, el);
    return 0;
}


PyObject *
EdgeRecord_GetItem(PyObject *ob, Py_ssize_t i)
{
    assert(EdgeRecord_Check(ob));
    EdgeRecord *o = (EdgeRecord *)ob;
    if (i < 0 || i >= Py_SIZE(o)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    PyObject *el = EdgeRecord_GET_ITEM(o, i);
    Py_INCREF(el);
    return el;
}


static void
record_dealloc(EdgeRecord *o)
{
    PyObject_GC_UnTrack(o);
    if (o->weakreflist != NULL) {
        PyObject_ClearWeakRefs((PyObject*)o);
    }
    Py_CLEAR(o->desc);
    o->cached_hash = -1;
    Py_TRASHCAN_BEGIN(o, record_dealloc);
    EDGE_DEALLOC_WITH_FREELIST(EDGE_RECORD, EdgeRecord, o);
    Py_TRASHCAN_END(o);
}


static int
record_traverse(EdgeRecord *o, visitproc visit, void *arg)
{
    Py_VISIT(o->desc);

    Py_ssize_t i;
    for (i = Py_SIZE(o); --i >= 0;) {
        if (o->ob_item[i] != NULL) {
            Py_VISIT(o->ob_item[i]);
        }
    }
    return 0;
}


static PyObject *
record_getitem(EdgeRecord *o, PyObject *name)
{
    Py_RETURN_NONE;
}


static PyObject *
record_repr(EdgeRecord *o)
{
    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;

    if (_PyUnicodeWriter_WriteASCIIString(&writer, "Object{", 7) < 0) {
        goto error;
    }

    if (_EdgeGeneric_RenderItems(&writer,
                                 (PyObject *)o, o->desc,
                                 o->ob_item, Py_SIZE(o), 1, 0) < 0)
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


static PyMappingMethods record_as_mapping = {
    .mp_subscript = (binaryfunc)record_getitem,
};


PyTypeObject EdgeRecord_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.Record",
    .tp_basicsize = sizeof(EdgeRecord) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)record_dealloc,
    .tp_as_mapping = &record_as_mapping,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)record_traverse,
    .tp_free = PyObject_GC_Del,
    .tp_repr = (reprfunc)record_repr,
    .tp_weaklistoffset = offsetof(EdgeRecord, weakreflist),
};


PyObject *
EdgeRecord_InitType(void)
{
    if (PyType_Ready(&EdgeRecord_Type) < 0) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeRecord_Type;
}
