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
PyObject* at_sign_ptr;


EDGE_SETUP_FREELIST(
    EDGE_OBJECT,
    EdgeObject,
    EDGE_OBJECT_FREELIST_MAXSAVE,
    EDGE_OBJECT_FREELIST_SIZE)


#define EdgeObject_GET_ITEM(op, i) \
    (((EdgeObject *)(op))->ob_item[i])
#define EdgeObject_SET_ITEM(op, i, v) \
    (((EdgeObject *)(op))->ob_item[i] = v)


PyObject *
EdgeObject_New(PyObject *desc)
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

    EdgeObject *o = NULL;
    EDGE_NEW_WITH_FREELIST(EDGE_OBJECT, EdgeObject,
                           &EdgeObject_Type, o, size);
    assert(o != NULL);
    assert(Py_SIZE(o) == size);
    assert(EdgeObject_Check(o));

    o->weakreflist = NULL;

    Py_INCREF(desc);
    o->desc = desc;

    o->cached_hash = -1;

    PyObject_GC_Track(o);
    return (PyObject *)o;
}


PyObject *
EdgeObject_GetRecordDesc(PyObject *o)
{
    if (!EdgeObject_Check(o)) {
        PyErr_Format(
            PyExc_TypeError,
            "an instance of gel.Object expected");
        return NULL;
    }

    PyObject *desc = ((EdgeObject *)o)->desc;
    Py_INCREF(desc);
    return desc;
}


int
EdgeObject_SetItem(PyObject *ob, Py_ssize_t i, PyObject *el)
{
    assert(EdgeObject_Check(ob));
    EdgeObject *o = (EdgeObject *)ob;
    assert(i >= 0);
    assert(i < Py_SIZE(o));
    Py_INCREF(el);
    EdgeObject_SET_ITEM(o, i, el);
    return 0;
}


PyObject *
EdgeObject_GetItem(PyObject *ob, Py_ssize_t i)
{
    assert(EdgeObject_Check(ob));
    EdgeObject *o = (EdgeObject *)ob;
    if (i < 0 || i >= Py_SIZE(o)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    PyObject *el = EdgeObject_GET_ITEM(o, i);
    Py_INCREF(el);
    return el;
}


static void
object_dealloc(EdgeObject *o)
{
    PyObject_GC_UnTrack(o);
    if (o->weakreflist != NULL) {
        PyObject_ClearWeakRefs((PyObject*)o);
    }
    Py_CLEAR(o->desc);
    o->cached_hash = -1;
    Py_TRASHCAN_BEGIN(o, object_dealloc);
    EDGE_DEALLOC_WITH_FREELIST(EDGE_OBJECT, EdgeObject, o);
    Py_TRASHCAN_END(o);
}


static int
object_traverse(EdgeObject *o, visitproc visit, void *arg)
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
object_getattr(EdgeObject *o, PyObject *name)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup(
        (PyObject *)o->desc, name, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_NOT_FOUND: {
            // Used in `dataclasses.as_dict()`
            if (
                PyUnicode_CompareWithASCIIString(
                    name, "__dataclass_fields__"
                ) == 0
            ) {
                return EdgeRecordDesc_GetDataclassFields((PyObject *)o->desc);
            }
            return PyObject_GenericGetAttr((PyObject *)o, name);
        }

        case L_LINKPROP:
        case L_LINK:
        case L_PROPERTY: {
            PyObject *val = EdgeObject_GET_ITEM(o, pos);
            Py_INCREF(val);
            return val;
        }

        default:
            abort();
    }
}

static PyObject *
object_getitem(EdgeObject *o, PyObject *name)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup(
        (PyObject *)o->desc, name, &pos
    );
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_PROPERTY:
            PyErr_Format(
                PyExc_TypeError,
                "property %R should be accessed via dot notation",
                name);
            return NULL;

        case L_LINKPROP: {
            PyObject *val = EdgeObject_GET_ITEM(o, pos);
            Py_INCREF(val);
            return val;
        }

        case L_NOT_FOUND: {
            int prefixed = 0;
            if (PyUnicode_Check(name)) {
                prefixed = PyUnicode_Tailmatch(
                    name, at_sign_ptr, 0, PY_SSIZE_T_MAX, -1
                );
                if (prefixed == -1) {
                    return NULL;
                }
            }
            if (prefixed) {
                PyErr_Format(
                    PyExc_KeyError,
                    "link property %R does not exist",
                    name);
            } else {
                PyErr_Format(
                    PyExc_TypeError,
                    "link property %R should be accessed with '@' prefix",
                    name);
            }
            return NULL;
        }

        case L_LINK: {
            PyErr_Format(
                PyExc_TypeError,
                "link %R should be accessed via dot notation",
                name);
            return NULL;
        }

        default:
            abort();
    }

}


static PyObject *
object_repr(EdgeObject *o)
{
    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;

    if (_PyUnicodeWriter_WriteASCIIString(&writer, "Object{", 7) < 0) {
        goto error;
    }

    if (_EdgeGeneric_RenderItems(
            &writer,
            (PyObject *)o, o->desc,
            o->ob_item, Py_SIZE(o),
            EDGE_RENDER_NAMES | EDGE_RENDER_LINK_PROPS) < 0)
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
object_dir(EdgeObject *o, PyObject *args)
{
    return EdgeRecordDesc_List(
        o->desc,
        0xFF,
        EDGE_POINTER_IS_LINKPROP);
}


static PyMethodDef object_methods[] = {
    {"__dir__", (PyCFunction)object_dir, METH_NOARGS, NULL},
    {NULL, NULL}
};


static PyMappingMethods object_as_mapping = {
    .mp_subscript = (binaryfunc)object_getitem,
};


PyTypeObject EdgeObject_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "gel.Object",
    .tp_basicsize = sizeof(EdgeObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)object_dealloc,
    .tp_methods = object_methods,
    .tp_as_mapping = &object_as_mapping,
    .tp_getattro = (getattrofunc)object_getattr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)object_traverse,
    .tp_free = PyObject_GC_Del,
    .tp_repr = (reprfunc)object_repr,
    .tp_weaklistoffset = offsetof(EdgeObject, weakreflist),
};


PyObject *
EdgeObject_InitType(void)
{
    if (PyType_Ready(&EdgeObject_Type) < 0) {
        return NULL;
    }

    // Pass the `dataclasses.is_dataclass(obj)` check - which then checks
    // `hasattr(type(obj), "__dataclass_fields__")`, the dict is always empty
    PyObject *default_fields = PyDict_New();
    if (default_fields == NULL) {
        return NULL;
    }
    PyDict_SetItemString(
        EdgeObject_Type.tp_dict, "__dataclass_fields__", default_fields
    );

    init_type_called = 1;
    return (PyObject *)&EdgeObject_Type;
}
