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


EDGE_SETUP_FREELIST(
    EDGE_TUPLE,
    EdgeTupleObject,
    EDGE_TUPLE_FREELIST_MAXSAVE,
    EDGE_TUPLE_FREELIST_SIZE)


#define EdgeTuple_GET_ITEM(op, i) \
    (((EdgeTupleObject *)(op))->ob_item[i])
#define EdgeTuple_SET_ITEM(op, i, v) \
    (((EdgeTupleObject *)(op))->ob_item[i] = v)


static int init_type_called = 0;


PyObject *
EdgeTuple_New(Py_ssize_t size)
{
    assert(init_type_called);

    if (size > EDGE_MAX_TUPLE_SIZE) {
        PyErr_Format(
            PyExc_ValueError,
            "Cannot create Tuple with more than %d elements",
            EDGE_MAX_TUPLE_SIZE);
        return NULL;
    }

    EdgeTupleObject *obj = NULL;

    EDGE_NEW_WITH_FREELIST(EDGE_TUPLE, EdgeTupleObject,
                           &EdgeTuple_Type, obj, size)
    assert(obj != NULL);
    assert(EdgeTuple_Check(obj));
    assert(Py_SIZE(obj) == size);

    obj->weakreflist = NULL;

    PyObject_GC_Track(obj);
    return (PyObject *)obj;
}


int
EdgeTuple_SetItem(PyObject *ob, Py_ssize_t i, PyObject *el)
{
    assert(EdgeTuple_Check(ob));
    EdgeTupleObject *o = (EdgeTupleObject *)ob;
    assert(i >= 0);
    assert(i < Py_SIZE(o));
    Py_INCREF(el);
    EdgeTuple_SET_ITEM(o, i, el);
    return 0;
}


static void
tuple_dealloc(EdgeTupleObject *o)
{
    PyObject_GC_UnTrack(o);
    if (o->weakreflist != NULL) {
        PyObject_ClearWeakRefs((PyObject*)o);
    }
    Py_TRASHCAN_SAFE_BEGIN(o)
    EDGE_DEALLOC_WITH_FREELIST(EDGE_TUPLE, EdgeTupleObject, o);
    Py_TRASHCAN_SAFE_END(o)
}


static Py_hash_t
tuple_hash(EdgeTupleObject *o)
{
    return _EdgeGeneric_Hash(o->ob_item, Py_SIZE(o));
}


static int
tuple_traverse(EdgeTupleObject *o, visitproc visit, void *arg)
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
tuple_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    PyObject *iterable = NULL;
    EdgeTupleObject *o;

    if (type != &EdgeTuple_Type) {
        PyErr_BadInternalCall();
        return NULL;
    }

    if (!_Edge_NoKeywords("edgedb.Tuple", kwargs) ||
            !PyArg_UnpackTuple(args, "edgedb.Tuple", 0, 1, &iterable))
    {
        return NULL;
    }

    if (iterable == NULL) {
        return EdgeTuple_New(0);
    }

    PyObject *tup = PySequence_Tuple(iterable);
    if (tup == NULL) {
        return NULL;
    }

    o = (EdgeTupleObject *)EdgeTuple_New(Py_SIZE(tup));
    if (o == NULL) {
        Py_DECREF(tup);
        return NULL;
    }

    for (Py_ssize_t i = 0; i < Py_SIZE(tup); i++) {
        PyObject *el = PyTuple_GET_ITEM(tup, i);
        Py_INCREF(el);
        EdgeTuple_SET_ITEM(o, i, el);
    }
    Py_DECREF(tup);
    return (PyObject *)o;
}


static Py_ssize_t
tuple_length(EdgeTupleObject *o)
{
    return Py_SIZE(o);
}


static PyObject *
tuple_getitem(EdgeTupleObject *o, Py_ssize_t i)
{
    if (i < 0 || i >= Py_SIZE(o)) {
        PyErr_SetString(PyExc_IndexError, "tuple index out of range");
        return NULL;
    }
    PyObject *el = EdgeTuple_GET_ITEM(o, i);
    Py_INCREF(el);
    return el;
}

static PyObject *
tuple_getslice(EdgeTupleObject *o, Py_ssize_t start, Py_ssize_t stop, Py_ssize_t step) {
    Py_ssize_t i, size = Py_SIZE(o);
    Py_ssize_t slicelength = PySlice_AdjustIndices(size, &start, &stop, step);
    size_t cur;

    if (slicelength <= 0) {
        return EdgeTuple_New(0);
    }

    if (start == 0 && step == 1 && slicelength == size) {
        Py_INCREF(o);
        return o;
    }

    PyObject *result = EdgeTuple_New(slicelength);
    for (cur = start, i = 0; i < slicelength; cur += step, i++) {
        PyObject *el = EdgeTuple_GET_ITEM(o, cur);
        Py_INCREF(el);
        EdgeTuple_SET_ITEM(result, i, el);
    }

    return result;
}

static PyObject *
tuple_getsubscript(EdgeTupleObject *o, PyObject *key)
{
    if (PySlice_Check(key)) {
        Py_ssize_t start, stop, step;
        if (PySlice_Unpack(key, &start, &stop, &step)) {
            PyErr_SetString(PyExc_RuntimeError, "invalid slice");
            return NULL;
        };

        return tuple_getslice(o, start, stop, step);
    }

    if (PyIndex_Check(key)) {
        Py_ssize_t val = PyNumber_AsSsize_t(key, PyExc_IndexError);
        if (val == -1 && PyErr_Occurred()) {
            return NULL;
        }

        return tuple_getitem(o, val);
    }

    PyErr_Format(
        PyExc_TypeError,
        "tuple indices must be integer or slices, not %.200s",
        Py_TYPE(key)->tp_name);
    return NULL;
}

static PyObject *
tuple_concat(EdgeTupleObject *self, PyObject *other) {
    Py_ssize_t size;
    Py_ssize_t i;
    PyObject **src, **dst;
    EdgeTupleObject *np;

    if (Py_TYPE(other) != &EdgeTuple_Type) {
        PyErr_Format(
            PyExc_TypeError,
            "can only concatenate edgedb.Tuple (not \"%.200s\") to edgedb.Tuple",
            Py_TYPE(other)->tp_name
        );
        return NULL;
    }

    if (Py_SIZE(self) == 0) {
        Py_INCREF(other);
        return other;
    }

    EdgeTupleObject *o = (EdgeTupleObject *)other;

    if (Py_SIZE(o) == 0) {
        Py_INCREF(self);
        return (PyObject *)self;
    }

    assert((size_t)Py_SIZE(self) + (size_t)Py_SIZE(o) < PY_SSIZE_T_MAX);
    size = Py_SIZE(self) + Py_SIZE(o);
    if (size == 0) {
        return EdgeTuple_New(0);
    }

    np = EdgeTuple_New(size);
    if (np == NULL) {
        return NULL;
    }

    src = self->ob_item;
    dst = np->ob_item;
    for (i = 0; i < Py_SIZE(self); i++) {
        PyObject *v = src[i];
        Py_INCREF(v);
        dst[i] = v;
    }

    src = o->ob_item;
    dst = np->ob_item + Py_SIZE(self);
    for (i = 0; i < Py_SIZE(o); i++) {
        PyObject *v = src[i];
        Py_INCREF(v);
        dst[i] = v;
    }

    return (PyObject *)np;
}

static PyObject *
tuple_richcompare(EdgeTupleObject *v, PyObject *w, int op)
{
    if (EdgeTuple_Check(w)) {
        return _EdgeGeneric_RichCompareValues(
            v->ob_item, Py_SIZE(v),
            ((EdgeTupleObject *)w)->ob_item, Py_SIZE(w),
            op);
    }

    if (PyTuple_CheckExact(w)) {
        return _EdgeGeneric_RichCompareValues(
            v->ob_item, Py_SIZE(v),
            ((PyTupleObject *)w)->ob_item, Py_SIZE(w),
            op);
    }

    Py_RETURN_NOTIMPLEMENTED;
}


static PyObject *
tuple_repr(EdgeTupleObject *o)
{
    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;

    if (_PyUnicodeWriter_WriteChar(&writer, '(') < 0) {
        goto error;
    }

    if (_EdgeGeneric_RenderValues(&writer,
                                  (PyObject *)o, o->ob_item, Py_SIZE(o)) < 0)
    {
        goto error;
    }

    if (_PyUnicodeWriter_WriteChar(&writer, ')') < 0) {
        goto error;
    }

    return _PyUnicodeWriter_Finish(&writer);

error:
    _PyUnicodeWriter_Dealloc(&writer);
    return NULL;
}


static PySequenceMethods tuple_as_sequence = {
    .sq_length = (lenfunc)tuple_length,
    .sq_item = (ssizeargfunc)tuple_getitem,
    .sq_concat = (binaryfunc)tuple_concat,
};

static PyMappingMethods tuple_as_map = {
    .mp_subscript = (ssizeargfunc)tuple_getsubscript,
};


PyTypeObject EdgeTuple_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.Tuple",
    .tp_basicsize = sizeof(EdgeTupleObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)tuple_dealloc,
    .tp_as_sequence = &tuple_as_sequence,
    .tp_as_mapping = &tuple_as_map,
    .tp_hash = (hashfunc)tuple_hash,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)tuple_traverse,
    .tp_new = tuple_new,
    .tp_richcompare = (richcmpfunc)tuple_richcompare,
    .tp_free = PyObject_GC_Del,
    .tp_repr = (reprfunc)tuple_repr,
    .tp_weaklistoffset = offsetof(EdgeTupleObject, weakreflist),
};


PyObject *
EdgeTuple_InitType(void)
{
    if (PyType_Ready(&EdgeTuple_Type) < 0) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeTuple_Type;
}
