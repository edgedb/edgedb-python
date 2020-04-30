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

    if (EdgeRecordDesc_IDPos(desc) < 0) {
        PyErr_SetString(
            PyExc_ValueError,
            "Cannot create Object without 'id' field");
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
            "an instance of edgedb.Object expected");
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


PyObject *
EdgeObject_GetID(PyObject *ob)
{
    assert(EdgeObject_Check(ob));
    EdgeObject *o = (EdgeObject *)ob;
    Py_ssize_t i = EdgeRecordDesc_IDPos(o->desc);
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
    Py_TRASHCAN_SAFE_BEGIN(o)
    EDGE_DEALLOC_WITH_FREELIST(EDGE_OBJECT, EdgeObject, o);
    Py_TRASHCAN_SAFE_END(o)
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


static Py_hash_t
object_hash(EdgeObject *o)
{
    if (o->cached_hash == -1) {
        o->cached_hash = _EdgeGeneric_HashWithBase(
            base_hash, o->ob_item, Py_SIZE(o));
    }
    return o->cached_hash;
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

        case L_LINKPROP:
        case L_NOT_FOUND:
            return PyObject_GenericGetAttr((PyObject *)o, name);

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

        case L_LINKPROP:
        case L_NOT_FOUND:
            PyErr_Format(
                PyExc_KeyError,
                "link %R does not exist",
                name);
            return NULL;

        case L_LINK: {
            PyObject *val = EdgeObject_GET_ITEM(o, pos);

            if (EdgeSet_Check(val)) {
                return EdgeLinkSet_New(name, (PyObject *)o, val);
            }
            else if (val == Py_None) {
                Py_RETURN_NONE;
            }
            else {
                return EdgeLink_New(name, (PyObject *)o, val);
            }
        }

        default:
            abort();
    }

}


static PyObject *
object_richcompare(EdgeObject *v, EdgeObject *w, int op)
{
    if (!EdgeObject_Check(v) || !EdgeObject_Check(w)) {
        Py_RETURN_NOTIMPLEMENTED;
    }

    Py_ssize_t v_id_pos = EdgeRecordDesc_IDPos(v->desc);
    Py_ssize_t w_id_pos = EdgeRecordDesc_IDPos(w->desc);

    if (v_id_pos < 0 || w_id_pos < 0 ||
        v_id_pos >= Py_SIZE(v) || w_id_pos >= Py_SIZE(w))
    {
        PyErr_SetString(
            PyExc_TypeError, "invalid object ID field offset");
        return NULL;
    }

    PyObject *v_id = EdgeObject_GET_ITEM(v, v_id_pos);
    PyObject *w_id = EdgeObject_GET_ITEM(w, w_id_pos);

    Py_INCREF(v_id);
    Py_INCREF(w_id);
    PyObject *ret = PyObject_RichCompare(v_id, w_id, op);
    Py_DECREF(v_id);
    Py_DECREF(w_id);
    return ret;
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
    "edgedb.Object",
    .tp_basicsize = sizeof(EdgeObject) - sizeof(PyObject *),
    .tp_itemsize = sizeof(PyObject *),
    .tp_dealloc = (destructor)object_dealloc,
    .tp_hash = (hashfunc)object_hash,
    .tp_methods = object_methods,
    .tp_as_mapping = &object_as_mapping,
    .tp_getattro = (getattrofunc)object_getattr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_richcompare = (richcmpfunc)object_richcompare,
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

    base_hash = _EdgeGeneric_HashString("edgedb.Object");
    if (base_hash == -1) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeObject_Type;
}
