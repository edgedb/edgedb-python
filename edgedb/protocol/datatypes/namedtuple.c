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
    EDGE_NAMED_TUPLE,
    EdgeNamedTupleObject,
    EDGE_NAMEDTUPLE_FREELIST_MAXSAVE,
    EDGE_NAMEDTUPLE_FREELIST_SIZE)


#define EdgeNamedTuple_GET_ITEM(op, i) \
    (((EdgeNamedTupleObject *)(op))->ob_item[i])
#define EdgeNamedTuple_SET_ITEM(op, i, v) \
    (((EdgeNamedTupleObject *)(op))->ob_item[i] = v)


static int init_type_called = 0;


PyObject *
EdgeNamedTuple_New(PyObject *desc)
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
            "Cannot create NamedTuple with more than %d elements",
            EDGE_MAX_TUPLE_SIZE);
        return NULL;
    }

    EdgeNamedTupleObject *nt = NULL;
    EDGE_NEW_WITH_FREELIST(EDGE_NAMED_TUPLE, EdgeNamedTupleObject,
                           &EdgeNamedTuple_Type, nt, size);
    assert(nt != NULL);
    assert(EdgeNamedTuple_Check(nt));
    assert(Py_SIZE(nt) == size);

    nt->weakreflist = NULL;

    Py_INCREF(desc);
    nt->desc = desc;

    PyObject_GC_Track(nt);
    return (PyObject *)nt;
}


int
EdgeNamedTuple_SetItem(PyObject *ob, Py_ssize_t i, PyObject *el)
{
    assert(EdgeNamedTuple_Check(ob));
    EdgeNamedTupleObject *o = (EdgeNamedTupleObject *)ob;
    assert(i >= 0);
    assert(i < Py_SIZE(o));
    Py_INCREF(el);
    EdgeNamedTuple_SET_ITEM(o, i, el);
    return 0;
}


static void
namedtuple_dealloc(EdgeNamedTupleObject *o)
{
    PyObject_GC_UnTrack(o);
    if (o->weakreflist != NULL) {
        PyObject_ClearWeakRefs((PyObject*)o);
    }
    Py_CLEAR(o->desc);
    Py_TRASHCAN_SAFE_BEGIN(o)
    EDGE_DEALLOC_WITH_FREELIST(EDGE_NAMED_TUPLE, EdgeNamedTupleObject, o);
    Py_TRASHCAN_SAFE_END(o)
}


static Py_hash_t
namedtuple_hash(EdgeNamedTupleObject *v)
{
    return _EdgeGeneric_Hash(v->ob_item, Py_SIZE(v));
}


static int
namedtuple_traverse(EdgeNamedTupleObject *o, visitproc visit, void *arg)
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
namedtuple_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    EdgeNamedTupleObject *o = NULL;
    PyObject *keys_tup = NULL;
    PyObject *kwargs_iter = NULL;
    PyObject *desc = NULL;

    if (type != &EdgeNamedTuple_Type) {
        PyErr_BadInternalCall();
        goto fail;
    }

    if (args != NULL && PyTuple_GET_SIZE(args) > 0) {
        PyErr_BadInternalCall();
        goto fail;
    }

    if (kwargs == NULL ||
            !PyDict_CheckExact(kwargs) ||
            PyDict_Size(kwargs) == 0)
    {
        PyErr_SetString(
            PyExc_ValueError,
            "edgedb.NamedTuple requires at least one field/value");
        goto fail;
    }

    Py_ssize_t size = PyDict_Size(kwargs);
    assert(size);

    keys_tup = PyTuple_New(size);
    if (keys_tup == NULL) {
        goto fail;
    }

    kwargs_iter = PyObject_GetIter(kwargs);
    if (kwargs_iter == NULL) {
        goto fail;
    }

    for (Py_ssize_t i = 0; i < size; i++) {
        PyObject *key = PyIter_Next(kwargs_iter);
        if (key == NULL) {
            if (PyErr_Occurred()) {
                goto fail;
            }
            else {
                PyErr_BadInternalCall();
                goto fail;
            }
        }

        PyTuple_SET_ITEM(keys_tup, i, key);
    }
    Py_CLEAR(kwargs_iter);

    desc = EdgeRecordDesc_New(keys_tup, NULL);
    if (desc == NULL) {
        goto fail;
    }

    o = (EdgeNamedTupleObject *)EdgeNamedTuple_New(desc);
    if (o == NULL) {
        goto fail;
    }
    Py_CLEAR(desc);

    for (Py_ssize_t i = 0; i < size; i++) {
        PyObject *key = PyTuple_GET_ITEM(keys_tup, i);  /* borrowed */
        PyObject *val = PyDict_GetItem(kwargs, key);  /* borrowed */
        if (val == NULL) {
            if (PyErr_Occurred()) {
                goto fail;
            }
            else {
                PyErr_BadInternalCall();
                goto fail;
            }
        }
        Py_INCREF(val);
        EdgeNamedTuple_SET_ITEM(o, i, val);
    }
    Py_CLEAR(keys_tup);

    return (PyObject *)o;

fail:
    Py_CLEAR(kwargs_iter);
    Py_CLEAR(keys_tup);
    Py_CLEAR(desc);
    Py_CLEAR(o);
    return NULL;
}


static PyObject *
namedtuple_getattr(EdgeNamedTupleObject *o, PyObject *name)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup(
        (PyObject *)o->desc, name, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_NOT_FOUND:
            return PyObject_GenericGetAttr((PyObject *)o, name);

        case L_LINK:
        case L_LINKPROP:
            /* shouldn't be possible for namedtuples */
            PyErr_BadInternalCall();
            return NULL;

        case L_PROPERTY: {
            PyObject *val = EdgeNamedTuple_GET_ITEM(o, pos);
            Py_INCREF(val);
            return val;
        }

        default:
            abort();
    }
}


static Py_ssize_t
namedtuple_length(EdgeNamedTupleObject *o)
{
    return Py_SIZE(o);
}


static PyObject *
namedtuple_getitem(EdgeNamedTupleObject *o, Py_ssize_t i)
{
    if (i < 0 || i >= Py_SIZE(o)) {
        PyErr_SetString(PyExc_IndexError, "namedtuple index out of range");
        return NULL;
    }
    PyObject *el = EdgeNamedTuple_GET_ITEM(o, i);
    Py_INCREF(el);
    return el;
}


static PyObject *
namedtuple_richcompare(EdgeNamedTupleObject *v,
                       PyObject *w, int op)
{
    if (EdgeNamedTuple_Check(w)) {
        return _EdgeGeneric_RichCompareValues(
            v->ob_item, Py_SIZE(v),
            ((EdgeNamedTupleObject *)w)->ob_item, Py_SIZE(w),
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
namedtuple_repr(EdgeNamedTupleObject *o)
{
    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;

    if (_PyUnicodeWriter_WriteChar(&writer, '(') < 0) {
        goto error;
    }

    if (_EdgeGeneric_RenderItems(&writer,
                                 (PyObject *)o, o->desc,
                                 o->ob_item, Py_SIZE(o), 0, 0) < 0)
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


static PyObject *
namedtuple_dir(EdgeNamedTupleObject *o, PyObject *args)
{
    return EdgeRecordDesc_List(
        o->desc,
        0xFF,
        0xFF);
}


static PyMethodDef namedtuple_methods[] = {
    {"__dir__", (PyCFunction)namedtuple_dir, METH_NOARGS, NULL},
    {NULL, NULL}
};


static PySequenceMethods namedtuple_as_sequence = {
    .sq_length = (lenfunc)namedtuple_length,
    .sq_item = (ssizeargfunc)namedtuple_getitem,
};


PyTypeObject EdgeNamedTuple_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.NamedTuple",
    .tp_basicsize = sizeof(EdgeNamedTupleObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_methods = namedtuple_methods,
    .tp_dealloc = (destructor)namedtuple_dealloc,
    .tp_as_sequence = &namedtuple_as_sequence,
    .tp_hash = (hashfunc)namedtuple_hash,
    .tp_getattro = (getattrofunc)namedtuple_getattr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_richcompare = (richcmpfunc)namedtuple_richcompare,
    .tp_traverse = (traverseproc)namedtuple_traverse,
    .tp_new = namedtuple_new,
    .tp_free = PyObject_GC_Del,
    .tp_repr = (reprfunc)namedtuple_repr,
    .tp_weaklistoffset = offsetof(EdgeNamedTupleObject, weakreflist),
};


PyObject *
EdgeNamedTuple_InitType(void)
{
    if (PyType_Ready(&EdgeNamedTuple_Type) < 0) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeNamedTuple_Type;
}
