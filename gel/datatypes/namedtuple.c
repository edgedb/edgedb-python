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
#include "structmember.h"


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
    assert(PyType_Check(type));

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
    // Expand the new_with_freelist because we need to incref the type
    if (
        _EDGE_NAMED_TUPLE_FL_MAX_SAVE_SIZE &&
        size < _EDGE_NAMED_TUPLE_FL_MAX_SAVE_SIZE &&
        (nt = _EDGE_NAMED_TUPLE_FL[size]) != NULL
    ) {
        if (size == 0) {
            Py_INCREF(nt);
        } else {
            _EDGE_NAMED_TUPLE_FL[size] = (PyTupleObject *) nt->ob_item[0];
            _EDGE_NAMED_TUPLE_FL_NUM_FREE[size]--;
            _Py_NewReference((PyObject *)nt);
            Py_INCREF(type);
            Py_SET_TYPE(nt, (PyTypeObject*)type);
        }
    } else {
        if (
            (size_t)size > (
                (size_t)PY_SSIZE_T_MAX - sizeof(PyTupleObject *) - sizeof(PyObject *)
            ) / sizeof(PyObject *)
        ) {
            PyErr_NoMemory();
            return NULL;
        }
        nt = PyObject_GC_NewVar(PyTupleObject, (PyTypeObject*)type, size);
        if (nt == NULL) {
            return NULL;
        }
    }
    assert(nt != NULL);
    assert(Py_SIZE(nt) == size);

    for (Py_ssize_t i = 0; i < size; i++) {
        nt->ob_item[i] = NULL;
    }
    PyObject_GC_Track(nt);
    return (PyObject *)nt;
}


static void
namedtuple_dealloc(PyTupleObject *o)
{
    PyTypeObject *tp;
    PyObject_GC_UnTrack(o);
    CPy_TRASHCAN_BEGIN(o, namedtuple_dealloc)
    tp = Py_TYPE(o);
    EDGE_DEALLOC_WITH_FREELIST(EDGE_NAMED_TUPLE, PyTupleObject, o);
    Py_DECREF(tp);
    CPy_TRASHCAN_END(o)
}


static int
namedtuple_traverse(PyTupleObject *o, visitproc visit, void *arg)
{
#if PY_VERSION_HEX >= 0x03090000
    // This was not needed before Python 3.9 (Python issue 35810 and 40217)
    Py_VISIT(Py_TYPE(o));
#endif
    for (Py_ssize_t i = Py_SIZE(o); --i >= 0;) {
        if (o->ob_item[i] != NULL) {
            Py_VISIT(o->ob_item[i]);
        }
    }
    return 0;
}


static PyObject *
namedtuple_derived_new(PyTypeObject *type, PyObject *args, PyObject *kwargs);


static PyObject *
namedtuple_new(PyTypeObject *type, PyObject *args, PyObject *kwargs) {
    PyTupleObject *o = NULL;
    PyObject *keys_tup = NULL;
    PyObject *kwargs_iter = NULL;
    PyObject *desc = NULL;

    if (type != &EdgeNamedTuple_Type) {
        return namedtuple_derived_new(type, args, kwargs);
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
            "gel.NamedTuple requires at least one field/value");
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

    PyObject *new_type = EdgeNamedTuple_Type_New(desc);
    o = (PyTupleObject *)EdgeNamedTuple_New(new_type);
    Py_CLEAR(new_type);  // the type is now referenced by the object

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
    PyTupleObject *o = (PyTupleObject *)EdgeNamedTuple_New((PyObject*)type);
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
                "gel.NamedTuple only needs %zd arguments, %zd given",
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
                "gel.NamedTuple requires %zd arguments, %zd given",
                size, args_size);
            goto fail;
        }
    }
    if (PyDict_Size(kwargs) > size - args_size) {
        PyErr_SetString(
            PyExc_ValueError,
            "gel.NamedTuple got extra keyword arguments");
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
                    "gel.NamedTuple missing required argument: %U",
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
                                 o->ob_item, Py_SIZE(o),
                                 EDGE_RENDER_NAMES) < 0)
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


// This is not a property on namedtuple objects, we are only using this member
// to allocate additional space on the **type object** to store a fast-access
// pointer to the `desc`. It's not visible to users with a Py_SIZE hack below.
static PyMemberDef namedtuple_members[] = {
    {"__desc__", T_OBJECT_EX, 0, READONLY},
    {NULL}  /* Sentinel */
};


static PyType_Slot namedtuple_slots[] = {
    {Py_tp_traverse, (traverseproc)namedtuple_traverse},
    {Py_tp_dealloc, (destructor)namedtuple_dealloc},
    {Py_tp_members, namedtuple_members},
    {0, 0}
};


static PyType_Spec namedtuple_spec = {
    "gel.DerivedNamedTuple",
    sizeof(PyTupleObject) - sizeof(PyObject *),
    sizeof(PyObject *),
    Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    namedtuple_slots,
};


PyTypeObject EdgeNamedTuple_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "gel.NamedTuple",
    .tp_basicsize = sizeof(PyTupleObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_BASETYPE,
    .tp_new = namedtuple_new,
    .tp_repr = (reprfunc)namedtuple_repr,
    .tp_methods = namedtuple_methods,
    .tp_getattro = (getattrofunc)namedtuple_getattr,
};


PyObject *
EdgeNamedTuple_Type_New(PyObject *desc)
{
    assert(init_type_called);

    if (desc == NULL || !EdgeRecordDesc_Check(desc)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    PyTypeObject *rv = (PyTypeObject *)PyType_FromSpecWithBases(
        &namedtuple_spec, PyTuple_Pack(1, &EdgeNamedTuple_Type)
    );
    if (rv == NULL) {
        return NULL;
    }

    // The tp_members give the type object extra space to store `desc` pointer
    assert(Py_TYPE(rv)->tp_itemsize > sizeof(PyObject *));
    EdgeNamedTuple_Type_DESC(rv) = desc;  // store the fast-access pointer

    Py_SET_SIZE(rv, 0);  // hack the size so the member is not visible to user

    // desc is also stored in tp_dict for refcount.
    if (PyDict_SetItemString(rv->tp_dict, "__desc__", desc) < 0) {
        goto fail;
    }

    // store `_fields` for collections.namedtuple duck-typing
    Py_ssize_t size = EdgeRecordDesc_GetSize(desc);
    PyTupleObject *fields = (PyTupleObject *)PyTuple_New(size);
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
    if (PyDict_SetItemString(rv->tp_dict, "_fields", (PyObject*)fields) < 0) {
        goto fail;
    }

    return (PyObject*)rv;

fail:
    Py_DECREF(rv);
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
