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
static Py_hash_t base_hash = -1;


EDGE_SETUP_FREELIST(
    EDGE_ARRAY,
    EdgeArrayObject,
    EDGE_ARRAY_FREELIST_MAXSAVE,
    EDGE_ARRAY_FREELIST_SIZE)


#define EdgeArray_GET_ITEM(op, i) \
    (((EdgeArrayObject *)(op))->ob_item[i])
#define EdgeArray_SET_ITEM(op, i, v) \
    (((EdgeArrayObject *)(op))->ob_item[i] = v)


PyObject *
EdgeArray_New(Py_ssize_t size)
{
    assert(init_type_called);

    EdgeArrayObject *obj = NULL;

    EDGE_NEW_WITH_FREELIST(EDGE_ARRAY, EdgeArrayObject,
                           &EdgeArray_Type, obj, size)
    assert(obj != NULL);
    assert(EdgeArray_Check(obj));
    assert(Py_SIZE(obj) == size);

    obj->cached_hash = -1;
    obj->weakreflist = NULL;

    PyObject_GC_Track(obj);
    return (PyObject *)obj;
}


int
EdgeArray_SetItem(PyObject *ob, Py_ssize_t i, PyObject *el)
{
    assert(EdgeArray_Check(ob));
    EdgeArrayObject *o = (EdgeArrayObject *)ob;
    assert(i >= 0);
    assert(i < Py_SIZE(o));
    Py_INCREF(el);
    EdgeArray_SET_ITEM(o, i, el);
    return 0;
}


static void
array_dealloc(EdgeArrayObject *o)
{
    o->cached_hash = -1;
    PyObject_GC_UnTrack(o);
    if (o->weakreflist != NULL) {
        PyObject_ClearWeakRefs((PyObject*)o);
    }
    Py_TRASHCAN_SAFE_BEGIN(o)
    EDGE_DEALLOC_WITH_FREELIST(EDGE_ARRAY, EdgeArrayObject, o);
    Py_TRASHCAN_SAFE_END(o)
}


static Py_hash_t
array_hash(EdgeArrayObject *o)
{
     if (o->cached_hash == -1) {
        o->cached_hash = _EdgeGeneric_HashWithBase(
            base_hash, o->ob_item, Py_SIZE(o));
    }
    return o->cached_hash;
}


static int
array_traverse(EdgeArrayObject *o, visitproc visit, void *arg)
{
    Py_ssize_t i;
    for (i = Py_SIZE(o); --i >= 0;) {
        if (o->ob_item[i] != NULL) {
            Py_VISIT(o->ob_item[i]);
        }
    }
    return 0;
}


static PyObject *
array_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    PyObject *iterable = NULL;
    EdgeArrayObject *o;

    if (type != &EdgeArray_Type) {
        PyErr_BadInternalCall();
        return NULL;
    }

    if (!_Edge_NoKeywords("edgedb.Array", kwargs) ||
            !PyArg_UnpackTuple(args, "edgedb.Array", 0, 1, &iterable))
    {
        return NULL;
    }

    if (iterable == NULL) {
        return EdgeArray_New(0);
    }

    PyObject *tup = PySequence_Tuple(iterable);
    if (tup == NULL) {
        return NULL;
    }

    o = (EdgeArrayObject *)EdgeArray_New(Py_SIZE(tup));
    if (o == NULL) {
        Py_DECREF(tup);
        return NULL;
    }

    for (Py_ssize_t i = 0; i < Py_SIZE(tup); i++) {
        PyObject *el = PyTuple_GET_ITEM(tup, i);
        Py_INCREF(el);
        EdgeArray_SET_ITEM(o, i, el);
    }
    Py_DECREF(tup);
    return (PyObject *)o;
}


static Py_ssize_t
array_length(EdgeArrayObject *o)
{
    return Py_SIZE(o);
}


static PyObject *
array_getitem(EdgeArrayObject *o, Py_ssize_t i)
{
    if (i < 0 || i >= Py_SIZE(o)) {
        PyErr_SetString(PyExc_IndexError, "array index out of range");
        return NULL;
    }
    PyObject *el = EdgeArray_GET_ITEM(o, i);
    Py_INCREF(el);
    return el;
}


static PyObject *
array_richcompare(EdgeArrayObject *v, PyObject *w, int op)
{
    if (EdgeArray_Check(w)) {
        return _EdgeGeneric_RichCompareValues(
            v->ob_item, Py_SIZE(v),
            ((EdgeArrayObject *)w)->ob_item, Py_SIZE(w),
            op);
    }

    if (PyList_CheckExact(w)) {
        return _EdgeGeneric_RichCompareValues(
            v->ob_item, Py_SIZE(v),
            _PyList_ITEMS(w), Py_SIZE(w),
            op);
    }

    Py_RETURN_NOTIMPLEMENTED;
}


static PyObject *
array_repr(EdgeArrayObject *o)
{
    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;

    if (_PyUnicodeWriter_WriteChar(&writer, '[') < 0) {
        goto error;
    }

    if (_EdgeGeneric_RenderValues(&writer,
                                  (PyObject *)o, o->ob_item, Py_SIZE(o)) < 0)
    {
        goto error;
    }

    if (_PyUnicodeWriter_WriteChar(&writer, ']') < 0) {
        goto error;
    }

    return _PyUnicodeWriter_Finish(&writer);

error:
    _PyUnicodeWriter_Dealloc(&writer);
    return NULL;
}


static PySequenceMethods array_as_sequence = {
    .sq_length = (lenfunc)array_length,
    .sq_item = (ssizeargfunc)array_getitem,
};


PyTypeObject EdgeArray_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.Array",
    .tp_basicsize = sizeof(EdgeArrayObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)array_dealloc,
    .tp_as_sequence = &array_as_sequence,
    .tp_hash = (hashfunc)array_hash,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)array_traverse,
    .tp_new = array_new,
    .tp_richcompare = (richcmpfunc)array_richcompare,
    .tp_free = PyObject_GC_Del,
    .tp_repr = (reprfunc)array_repr,
    .tp_weaklistoffset = offsetof(EdgeArrayObject, weakreflist),
};


PyObject *
EdgeArray_InitType(void)
{
    if (PyType_Ready(&EdgeArray_Type) < 0) {
        return NULL;
    }

    base_hash = _EdgeGeneric_HashString("edgedb.Array");
    if (base_hash == -1) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeArray_Type;
}
