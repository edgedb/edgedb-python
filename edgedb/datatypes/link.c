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


PyObject *
EdgeLink_New(PyObject *name, PyObject *source, PyObject *target)
{
    assert(init_type_called);

    if (!EdgeObject_Check(source)) {
        PyErr_SetString(
            PyExc_TypeError,
            "cannot construct a Link object; source is expected "
            "to be an edgedb.Object");
        return NULL;
    }

    if (!EdgeObject_Check(target)) {
        PyErr_SetString(
            PyExc_TypeError,
            "cannot construct a Link object; target is expected "
            "to be an edgedb.Object");
        return NULL;
    }

    EdgeLinkObject *o = PyObject_GC_New(EdgeLinkObject, &EdgeLink_Type);
    if (o == NULL) {
        return NULL;
    }

    o->weakreflist = NULL;

    Py_INCREF(name);
    o->name = name;

    Py_INCREF(source);
    o->source = source;

    Py_INCREF(target);
    o->target = target;

    PyObject_GC_Track(o);
    return (PyObject *)o;
}


static int
link_clear(EdgeLinkObject *o)
{
    Py_CLEAR(o->name);
    Py_CLEAR(o->source);
    Py_CLEAR(o->target);
    return 0;
}


static int
link_traverse(EdgeLinkObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->name);
    Py_VISIT(self->source);
    Py_VISIT(self->target);
    return 0;
}


static void
link_dealloc(EdgeLinkObject *o)
{
    PyObject_GC_UnTrack(o);
    if (o->weakreflist != NULL) {
        PyObject_ClearWeakRefs((PyObject*)o);
    }
    (void)link_clear(o);
    Py_TYPE(o)->tp_free(o);
}


static Py_hash_t
link_hash(EdgeLinkObject *o)
{
    Py_hash_t hash = base_hash;

    Py_hash_t sub_hash = PyObject_Hash(o->source);
    if (sub_hash == -1) {
        return -1;
    }

    hash ^= sub_hash;

    sub_hash = PyObject_Hash(o->target);
    if (sub_hash == -1) {
        return -1;
    }

    hash ^= sub_hash;
    if (hash == -1) {
        hash = -2;
    }

    return hash;
}


static PyObject *
link_repr(EdgeLinkObject *o)
{
    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;

    if (_PyUnicodeWriter_WriteASCIIString(&writer, "Link(name=", 10) < 0)
    {
        goto error;
    }

    PyObject *sub_repr = _EdgeGeneric_RenderObject(o->name);
    if (sub_repr == NULL) {
        goto error;
    }
    if (_PyUnicodeWriter_WriteStr(&writer, sub_repr) < 0) {
        Py_DECREF(sub_repr);
        goto error;
    }
    Py_DECREF(sub_repr);

    if (_PyUnicodeWriter_WriteASCIIString(&writer, ", source_id=", 12) < 0)
    {
        goto error;
    }

    PyObject *source_id = EdgeObject_GetID(o->source);
    if (source_id == NULL) {
        goto error;
    }
    sub_repr = _EdgeGeneric_RenderObject(source_id);
    Py_CLEAR(source_id);
    if (sub_repr == NULL) {
        goto error;
    }
    if (_PyUnicodeWriter_WriteStr(&writer, sub_repr) < 0) {
        Py_DECREF(sub_repr);
        goto error;
    }
    Py_DECREF(sub_repr);

    if (_PyUnicodeWriter_WriteASCIIString(&writer, ", target_id=", 12) < 0) {
        goto error;
    }

    PyObject *target_id = EdgeObject_GetID(o->target);
    if (target_id == NULL) {
        goto error;
    }
    sub_repr = _EdgeGeneric_RenderObject(target_id);
    Py_CLEAR(target_id);
    if (sub_repr == NULL) {
        goto error;
    }
    if (_PyUnicodeWriter_WriteStr(&writer, sub_repr) < 0) {
        Py_DECREF(sub_repr);
        goto error;
    }
    Py_DECREF(sub_repr);

    if (_PyUnicodeWriter_WriteChar(&writer, ')') < 0) {
        goto error;
    }

    return _PyUnicodeWriter_Finish(&writer);

error:
    _PyUnicodeWriter_Dealloc(&writer);
    return NULL;

}


static PyObject *
link_richcompare(EdgeLinkObject *v, EdgeLinkObject *w, int op)
{
    if (op != Py_EQ && op != Py_NE) {
        goto not_imp;
    }

    if (!EdgeLink_Check(w)) {
        goto not_imp;
    }

    int res;
    int is_eq = 1;

    is_eq = PyObject_RichCompareBool(v->name, w->name, Py_EQ);
    if (is_eq == -1) {
        goto error;
    }
    if (is_eq == 0) {
        goto done;
    }

    is_eq = PyObject_RichCompareBool(v->source, w->source, Py_EQ);
    if (is_eq == -1) {
        goto error;
    }
    if (is_eq == 0) {
        goto done;
    }

    is_eq = PyObject_RichCompareBool(v->target, w->target, Py_EQ);
    if (is_eq == -1) {
        goto error;
    }

done:
    res = is_eq;
    if (op == Py_NE) {
        res = !res;
    }

    if (res) {
        Py_RETURN_TRUE;
    }
    else {
        Py_RETURN_FALSE;
    }

not_imp:
    Py_RETURN_NOTIMPLEMENTED;

error:
    return NULL;
}


static PyObject *
link_getattr(EdgeLinkObject *o, PyObject *name)
{
    if (PyUnicode_CompareWithASCIIString(name, "source") == 0) {
        Py_INCREF(o->source);
        return o->source;
    }

    if (PyUnicode_CompareWithASCIIString(name, "target") == 0) {
        Py_INCREF(o->target);
        return o->target;
    }

    EdgeObject *target = (EdgeObject *)(o->target);
    assert(EdgeObject_Check(target));

    PyObject *desc = target->desc;
    assert(EdgeRecordDesc_Check(desc));

    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup(desc, name, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_LINK:
        case L_PROPERTY:
        case L_NOT_FOUND:
            goto not_found;

        case L_LINKPROP:
            return EdgeObject_GetItem((PyObject *)target, pos);

        default:
            abort();
    }

not_found:
    return PyObject_GenericGetAttr((PyObject *)o, name);
}


static PyObject *
link_dir(EdgeLinkObject *o, PyObject *args)
{
    EdgeObject *target = (EdgeObject *)(o->target);
    assert(EdgeObject_Check(target));

    PyObject *ret = EdgeRecordDesc_List(
        target->desc,
        EDGE_POINTER_IS_LINKPROP,
        0);

    if (ret == NULL) {
        return NULL;
    }

    PyObject *str = PyUnicode_FromString("source");
    if (str == NULL) {
        Py_DECREF(ret);
        return NULL;
    }
    if (PyList_Append(ret, str)) {
        Py_DECREF(str);
        Py_DECREF(ret);
        return NULL;
    }
    Py_DECREF(str);

    str = PyUnicode_FromString("target");
    if (str == NULL) {
        Py_DECREF(ret);
        return NULL;
    }
    if (PyList_Append(ret, str)) {
        Py_DECREF(str);
        Py_DECREF(ret);
        return NULL;
    }
    Py_DECREF(str);

    return ret;
}


static PyMethodDef link_methods[] = {
    {"__dir__", (PyCFunction)link_dir, METH_NOARGS, NULL},
    {NULL, NULL}
};


PyTypeObject EdgeLink_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.Link",
    sizeof(EdgeLinkObject),
    .tp_methods = link_methods,
    .tp_dealloc = (destructor)link_dealloc,
    .tp_hash = (hashfunc)link_hash,
    .tp_getattro = (getattrofunc)link_getattr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_richcompare = (richcmpfunc)link_richcompare,
    .tp_clear = (inquiry)link_clear,
    .tp_traverse = (traverseproc)link_traverse,
    .tp_free = PyObject_GC_Del,
    .tp_repr = (reprfunc)link_repr,
    .tp_weaklistoffset = offsetof(EdgeLinkObject, weakreflist),
};


PyObject *
EdgeLink_InitType(void)
{
    if (PyType_Ready(&EdgeLink_Type) < 0) {
        return NULL;
    }

    base_hash = _EdgeGeneric_HashString("edgedb.Link");
    if (base_hash == -1) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeLink_Type;
}
