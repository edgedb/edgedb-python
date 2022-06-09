/*
* This source file is part of the EdgeDB open source project.
*
* Copyright 2022-present MagicStack Inc. and the EdgeDB authors.
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
    EDGE_SPARSE_OBJECT,
    EdgeSparseObject,
    EDGE_SPARSE_OBJECT_FREELIST_MAXSAVE,
    EDGE_SPARSE_OBJECT_FREELIST_SIZE)


#define EdgeSparseObject_GET_ITEM(op, i) \
    (((EdgeSparseObject *)(op))->ob_item[i])
#define EdgeSparseObject_SET_ITEM(op, i, v) \
    (((EdgeSparseObject *)(op))->ob_item[i] = v)


PyObject *
EdgeSparseObject_New(PyObject *desc)
{
    assert(init_type_called);

    if (desc == NULL || !EdgeInputShape_Check(desc)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    Py_ssize_t size = EdgeInputShape_GetSize(desc);

    if (size > EDGE_MAX_TUPLE_SIZE) {
        PyErr_Format(
            PyExc_ValueError,
            "Cannot create Object with more than %d elements",
            EDGE_MAX_TUPLE_SIZE);
        return NULL;
    }

    EdgeSparseObject *o = NULL;
    EDGE_NEW_WITH_FREELIST(EDGE_SPARSE_OBJECT, EdgeSparseObject,
                           &EdgeSparseObject_Type, o, size);
    assert(o != NULL);
    assert(Py_SIZE(o) == size);
    assert(EdgeSparseObject_Check(o));

    o->weakreflist = NULL;

    Py_INCREF(desc);
    o->desc = desc;

    o->cached_hash = -1;

    PyObject_GC_Track(o);
    return (PyObject *)o;
}


PyObject *
EdgeSparseObject_GetInputShape(PyObject *o)
{
    if (!EdgeSparseObject_Check(o)) {
        PyErr_Format(
            PyExc_TypeError,
            "an instance of edgedb.Object expected");
        return NULL;
    }

    PyObject *desc = ((EdgeSparseObject *)o)->desc;
    Py_INCREF(desc);
    return desc;
}


int
EdgeSparseObject_SetItem(PyObject *ob, Py_ssize_t i, PyObject *el)
{
    assert(EdgeSparseObject_Check(ob));
    EdgeSparseObject *o = (EdgeSparseObject *)ob;
    assert(i >= 0);
    assert(i < Py_SIZE(o));
    Py_INCREF(el);
    EdgeSparseObject_SET_ITEM(o, i, el);
    return 0;
}


PyObject *
EdgeSparseObject_GetItem(PyObject *ob, Py_ssize_t i)
{
    assert(EdgeSparseObject_Check(ob));
    EdgeSparseObject *o = (EdgeSparseObject *)ob;
    if (i < 0 || i >= Py_SIZE(o)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    PyObject *el = EdgeSparseObject_GET_ITEM(o, i);
    Py_INCREF(el);
    return el;
}


static void
sparse_object_dealloc(EdgeSparseObject *o)
{
    PyObject_GC_UnTrack(o);
    if (o->weakreflist != NULL) {
        PyObject_ClearWeakRefs((PyObject*)o);
    }
    Py_CLEAR(o->desc);
    o->cached_hash = -1;
    Py_TRASHCAN_SAFE_BEGIN(o)
    EDGE_DEALLOC_WITH_FREELIST(EDGE_SPARSE_OBJECT, EdgeSparseObject, o);
    Py_TRASHCAN_SAFE_END(o)
}


static int
sparse_object_traverse(EdgeSparseObject *o, visitproc visit, void *arg)
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


static Py_hash_t
sparse_object_hash(EdgeSparseObject *o)
{
    if (o->cached_hash == -1) {
        o->cached_hash = _EdgeGeneric_HashWithBase(
            base_hash, o->ob_item, Py_SIZE(o));
    }
    return o->cached_hash;
}


static PyObject *
sparse_object_getattr(EdgeSparseObject *o, PyObject *name)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeInputShape_Lookup(
        (PyObject *)o->desc, name, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_NOT_FOUND:
            return PyObject_GenericGetAttr((PyObject *)o, name);

        case L_PROPERTY: {
            PyObject *val = EdgeSparseObject_GET_ITEM(o, pos);
            Py_INCREF(val);
            return val;
        }

        default:
            abort();
    }
}

static PyObject *
sparse_object_getitem(EdgeSparseObject *o, PyObject *name)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeInputShape_Lookup(
        (PyObject *)o->desc, name, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_PROPERTY:
            PyErr_Format(
                PyExc_TypeError,
                "property %R should be accessed via dot notation",
                name);
            return NULL;

        case L_NOT_FOUND:
            PyErr_Format(
                PyExc_KeyError,
                "link %R does not exist",
                name);
            return NULL;

        default:
            abort();
    }

}


static PyObject *
sparse_object_repr(EdgeSparseObject *o)
{
    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;

    if (_PyUnicodeWriter_WriteASCIIString(&writer, "SparseObject{", 13) < 0) {
        goto error;
    }

    if (_EdgeGeneric_RenderSparseItems(&writer,
                                       (PyObject *)o, o->desc,
                                       o->ob_item, Py_SIZE(o)) < 0)
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


static PyObject *
sparse_object_dir(EdgeSparseObject *o, PyObject *args)
{
    return EdgeInputShape_List(o->desc);
}


static PyMethodDef sparse_object_methods[] = {
    {"__dir__", (PyCFunction)sparse_object_dir, METH_NOARGS, NULL},
    {NULL, NULL}
};


static PyMappingMethods sparse_object_as_mapping = {
    .mp_subscript = (binaryfunc)sparse_object_getitem,
};


PyTypeObject EdgeSparseObject_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.SparseObject",
    .tp_basicsize = sizeof(EdgeSparseObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)sparse_object_dealloc,
    .tp_hash = (hashfunc)sparse_object_hash,
    .tp_methods = sparse_object_methods,
    .tp_as_mapping = &sparse_object_as_mapping,
    .tp_getattro = (getattrofunc)sparse_object_getattr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)sparse_object_traverse,
    .tp_free = PyObject_GC_Del,
    .tp_repr = (reprfunc)sparse_object_repr,
    .tp_weaklistoffset = offsetof(EdgeSparseObject, weakreflist),
};


PyObject *
EdgeSparseObject_InitType(void)
{
    if (PyType_Ready(&EdgeSparseObject_Type) < 0) {
        return NULL;
    }

    base_hash = _EdgeGeneric_HashString("edgedb.SparseObject");
    if (base_hash == -1) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeSparseObject_Type;
}
