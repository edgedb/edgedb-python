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
    PyTupleObject,
    EDGE_NAMEDTUPLE_FREELIST_MAXSAVE,
    EDGE_NAMEDTUPLE_FREELIST_SIZE)


#define EdgeNamedTuple_Type_DESC(type) \
    *(PyObject **)(((char *)type) + Py_TYPE(type)->tp_basicsize)


static int init_type_called = 0;


PyObject *
EdgeNamedTuple_New(PyObject *type)
{
    assert(init_type_called);

    PyObject *desc = EdgeNamedTuple_Type_DESC(type);
    if (desc == NULL || !EdgeRecordDesc_Check(desc)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    Py_ssize_t size = EdgeRecordDesc_GetSize(desc);
    if (size < 0) {
        return NULL;
    }

    if (size > EDGE_MAX_TUPLE_SIZE) {
        PyErr_Format(
            PyExc_ValueError,
            "Cannot create NamedTuple with more than %d elements",
            EDGE_MAX_TUPLE_SIZE);
        return NULL;
    }

    PyTupleObject *nt = NULL;
    EDGE_NEW_WITH_FREELIST(EDGE_NAMED_TUPLE, PyTupleObject, type, nt, size);
    assert(nt != NULL);
    if (Py_TYPE(nt) != type) {
        Py_DECREF(Py_TYPE(nt));
        Py_INCREF(type);
        Py_TYPE(nt) = type;
    }
    assert(Py_SIZE(nt) == size);

    PyObject_GC_Track(nt);
    return (PyObject *)nt;
}


static void
namedtuple_dealloc(PyTupleObject *o)
{
    PyObject_GC_UnTrack(o);
    Py_TRASHCAN_SAFE_BEGIN(o)
    EDGE_DEALLOC_WITH_FREELIST(EDGE_NAMED_TUPLE, PyTupleObject, o);
    Py_TRASHCAN_SAFE_END(o)
}


static int
namedtuple_traverse(PyTupleObject *o, visitproc visit, void *arg)
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
namedtuple_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    PyTupleObject *o = NULL;
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

    desc = EdgeRecordDesc_New(keys_tup, NULL, NULL);
    if (desc == NULL) {
        goto fail;
    }

    type = EdgeNamedTuple_Type_New(desc);
    o = (PyTupleObject *)EdgeNamedTuple_New(type);
    Py_CLEAR(type);

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
        PyTuple_SET_ITEM(o, i, val);
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
namedtuple_derived_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    PyTupleObject *o = (PyTupleObject *)EdgeNamedTuple_New(type);
    if (o == NULL) {
        goto fail;
    }

    PyObject *desc = EdgeNamedTuple_Type_DESC(type);
    Py_ssize_t size = EdgeRecordDesc_GetSize(desc);
    if (size < 0) {
        goto fail;
    }
    Py_ssize_t args_size = 0;
    PyObject *val;

    if (args != NULL) {
        args_size = PyTuple_GET_SIZE(args);
        if (args_size > size) {
            PyErr_Format(
                PyExc_ValueError,
                "edgedb.NamedTuple only needs %zd arguments, %zd given",
                size, args_size);
            goto fail;
        }
        for (Py_ssize_t i = 0; i < args_size; i++) {
            val = PyTuple_GET_ITEM(args, i);
            Py_INCREF(val);
            PyTuple_SET_ITEM(o, i, val);
        }
    }
    if (kwargs == NULL || !PyDict_CheckExact(kwargs)) {
        if (size == args_size) {
            return (PyObject *)o;
        } else {
            PyErr_Format(
                PyExc_ValueError,
                "edgedb.NamedTuple requires %zd arguments, %zd given",
                size, args_size);
            goto fail;
        }
    }
    if (PyDict_Size(kwargs) > size - args_size) {
        PyErr_SetString(
            PyExc_ValueError,
            "edgedb.NamedTuple got extra keyword arguments");
        goto fail;
    }
    for (Py_ssize_t i = args_size; i < size; i++) {
        PyObject *key = EdgeRecordDesc_PointerName(desc, i);
        if (key == NULL) {
            goto fail;
        }
        PyObject *val = PyDict_GetItem(kwargs, key);
        if (val == NULL) {
            if (PyErr_Occurred()) {
                Py_CLEAR(key);
                goto fail;
            } else {
                PyErr_Format(
                    PyExc_ValueError,
                    "edgedb.NamedTuple missing required argument: %U",
                    key);
                Py_CLEAR(key);
                goto fail;
            }
        }
        Py_CLEAR(key);
        Py_INCREF(val);
        PyTuple_SET_ITEM(o, i, val);
    }
    return (PyObject *)o;
fail:
    Py_CLEAR(o);
    return NULL;
}


static PyObject *
namedtuple_getattr(PyTupleObject *o, PyObject *name)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup(
        EdgeNamedTuple_Type_DESC(Py_TYPE(o)), name, &pos);
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
            PyObject *val = PyTuple_GET_ITEM(o, pos);
            Py_INCREF(val);
            return val;
        }

        default:
            abort();
    }
}


static PyObject *
namedtuple_repr(PyTupleObject *o)
{
    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;

    if (_PyUnicodeWriter_WriteChar(&writer, '(') < 0) {
        goto error;
    }

    if (_EdgeGeneric_RenderItems(&writer,
                                 (PyObject *)o,
                                 EdgeNamedTuple_Type_DESC(Py_TYPE(o)),
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
namedtuple_dir(PyTupleObject *o, PyObject *args)
{
    return EdgeRecordDesc_List(
        EdgeNamedTuple_Type_DESC(Py_TYPE(o)),
        0xFF,
        0xFF);
}


static PyMethodDef namedtuple_methods[] = {
    {"__dir__", (PyCFunction)namedtuple_dir, METH_NOARGS, NULL},
    {NULL, NULL}
};


static PyType_Slot namedtuple_slots[] = {
    {Py_tp_repr, (reprfunc)namedtuple_repr},
    {Py_tp_methods, namedtuple_methods},
    {Py_tp_getattro, (getattrofunc)namedtuple_getattr},
    {Py_tp_traverse, (traverseproc)namedtuple_traverse},
    {Py_tp_dealloc, (destructor)namedtuple_dealloc},
    {Py_tp_new, namedtuple_derived_new},
    {0, 0},
};


static PyType_Spec namedtuple_spec = {
    "edgedb.DerivedNamedTuple",
    sizeof(PyTupleObject) - sizeof(PyObject *),
    sizeof(PyObject *),
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    namedtuple_slots,
};


PyTypeObject EdgeNamedTuple_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.NamedTuple",
    .tp_basicsize = sizeof(PyTupleObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = namedtuple_new,
};


PyObject *
EdgeNamedTuple_Type_New(PyObject *desc)
{
    assert(init_type_called);

    if (desc == NULL || !EdgeRecordDesc_Check(desc)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    PyObject *type;
    PyObject *rv;

    type = PyType_FromSpecWithBases(
        &namedtuple_spec, PyTuple_Pack(1, &EdgeNamedTuple_Type)
    );
    if (type == NULL) {
        return NULL;
    }

    // Over-allocate the new type object to store a quick pointer to desc.
    PyObject_GC_UnTrack(type);  // needed by PyObject_GC_Resize
    // PyObject_GC_Resize() increases the size by type->ob_type->tp_itemsize,
    // which is sizeof(PyMemberDef) in PyType_Type, but we only need PyObject*,
    // so make sure we have enough space to store the pointer.
    Py_ssize_t size = Py_SIZE(type);
    assert(Py_TYPE(type)->tp_itemsize > sizeof(PyObject *));
    rv = PyObject_GC_Resize(PyObject, type, size  + 1);
    if (rv == NULL) {
        PyObject_GC_Del(type);
        return NULL;
    }
    Py_SIZE(rv) = size;
    EdgeNamedTuple_Type_DESC(rv) = desc;
    // desc is also stored in tp_dict for refcount.
    PyDict_SetItemString(((PyTypeObject *)rv)->tp_dict, "__desc__", desc);

    // store `_fields` for collections.namedtuple duc-typing
    size = EdgeRecordDesc_GetSize(desc);
    PyTupleObject *fields = PyTuple_New(size);
    if (fields == NULL) {
        goto fail;
    }
    for (Py_ssize_t i = 0; i < size; i++) {
        PyObject *name = EdgeRecordDesc_PointerName(desc, i);
        if (name == NULL) {
            Py_CLEAR(fields);
            goto fail;
        }
        PyTuple_SET_ITEM(fields, i, name);
    }
    PyDict_SetItemString(((PyTypeObject *)rv)->tp_dict, "_fields", fields);

    PyObject_GC_Track(rv);
    return rv;

fail:
    PyObject_GC_Del(rv);
    return NULL;
}


PyObject *
EdgeNamedTuple_InitType(void)
{
    EdgeNamedTuple_Type.tp_base = &PyTuple_Type;

    if (PyType_Ready(&EdgeNamedTuple_Type) < 0) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeNamedTuple_Type;
}
