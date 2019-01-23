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
EdgeLinkSet_New(PyObject *name, PyObject *source, PyObject *targets)
{
    assert(init_type_called);

    if (!EdgeObject_Check(source)) {
        PyErr_SetString(
            PyExc_TypeError,
            "cannot construct a Link object; source is expected "
            "to be an edgedb.Object");
        return NULL;
    }

    if (!EdgeSet_Check(targets)) {
        PyErr_SetString(
            PyExc_TypeError,
            "cannot construct a Link object; targets is expected "
            "to be an edgedb.Set");
        return NULL;
    }

    EdgeLinkSetObject *o = PyObject_GC_New(
        EdgeLinkSetObject, &EdgeLinkSet_Type);
    if (o == NULL) {
        return NULL;
    }

    o->weakreflist = NULL;

    Py_INCREF(name);
    o->name = name;

    Py_INCREF(source);
    o->source = source;

    Py_INCREF(targets);
    o->targets = targets;

    PyObject_GC_Track(o);
    return (PyObject *)o;
}


static Py_hash_t
linkset_hash(EdgeLinkSetObject *o)
{
    Py_hash_t hash = base_hash;

    Py_hash_t sub_hash = PyObject_Hash(o->source);
    if (sub_hash == -1) {
        return -1;
    }

    hash ^= sub_hash;

    sub_hash = PyObject_Hash(o->targets);
    if (sub_hash == -1) {
        return -1;
    }

    hash ^= sub_hash;
    if (hash == -1) {
        hash = -2;
    }

    return hash;
}


static int
linkset_clear(EdgeLinkSetObject *o)
{
    Py_CLEAR(o->name);
    Py_CLEAR(o->source);
    Py_CLEAR(o->targets);
    return 0;
}


static int
linkset_traverse(EdgeLinkSetObject *self, visitproc visit, void *arg)
{
    Py_VISIT(self->name);
    Py_VISIT(self->source);
    Py_VISIT(self->targets);
    return 0;
}


static void
linkset_dealloc(EdgeLinkSetObject *o)
{
    PyObject_GC_UnTrack(o);
    if (o->weakreflist != NULL) {
        PyObject_ClearWeakRefs((PyObject*)o);
    }
    (void)linkset_clear(o);
    Py_TYPE(o)->tp_free(o);
}


static PyObject *
linkset_repr(EdgeLinkSetObject *o)
{
    _PyUnicodeWriter writer;
    _PyUnicodeWriter_Init(&writer);
    writer.overallocate = 1;

    if (_PyUnicodeWriter_WriteASCIIString(&writer, "LinkSet(name=", 13) < 0)
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

    if (_PyUnicodeWriter_WriteASCIIString(&writer, ", source_id=", 12) < 0) {
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

    if (_PyUnicodeWriter_WriteASCIIString(&writer, ", target_ids={", 14) < 0) {
        goto error;
    }

    for (Py_ssize_t i = 0; i < EdgeSet_Len(o->targets); i++) {
        PyObject *el = EdgeSet_GetItem(o->targets, i);
        PyObject *item_repr = NULL;
        if (EdgeObject_Check(el)) {
            /* should always be the case */
            PyObject *id = EdgeObject_GetID(el);
            Py_DECREF(el);
            if (id == NULL) {
                goto error;
            }
            item_repr = _EdgeGeneric_RenderObject(id);
            Py_DECREF(id);
        }
        else {
            item_repr = _EdgeGeneric_RenderObject(el);
            Py_DECREF(el);
        }

        if (item_repr == NULL) {
            goto error;
        }

        if (_PyUnicodeWriter_WriteStr(&writer, item_repr) < 0) {
            Py_DECREF(item_repr);
            goto error;
        }
        Py_DECREF(item_repr);

        if (i < EdgeSet_Len(o->targets) - 1) {
            if (_PyUnicodeWriter_WriteASCIIString(&writer, ", ", 2) < 0) {
                goto error;
            }
        }
    }

    if (_PyUnicodeWriter_WriteASCIIString(&writer, "})", 2) < 0) {
        goto error;
    }

    return _PyUnicodeWriter_Finish(&writer);

error:
    _PyUnicodeWriter_Dealloc(&writer);
    return NULL;

}


static PyObject *
linkset_richcompare(EdgeLinkSetObject *v, EdgeLinkSetObject *w, int op)
{
    if (op != Py_EQ && op != Py_NE) {
        goto not_imp;
    }

    if (!EdgeLinkSet_Check(w)) {
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

    is_eq = PyObject_RichCompareBool(v->targets, w->targets, Py_EQ);
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


static Py_ssize_t
linkset_length(EdgeLinkSetObject *o)
{
    assert(EdgeSet_Check(o->targets));
    return EdgeSet_Len(o->targets);
}


static PyObject *
linkset_getitem(EdgeLinkSetObject *o, Py_ssize_t i)
{
    PyObject *target = EdgeSet_GetItem(o->targets, i);
    if (target == NULL) {
        return NULL;
    }

    PyObject *link = EdgeLink_New(o->name, o->source, target);
    Py_DECREF(target);
    return link;
}


static PySequenceMethods linkset_as_sequence = {
    .sq_length = (lenfunc)linkset_length,
    .sq_item = (ssizeargfunc)linkset_getitem,
};


PyTypeObject EdgeLinkSet_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    "edgedb.LinkSet",
    sizeof(EdgeLinkSetObject),
    .tp_as_sequence = &linkset_as_sequence,
    .tp_dealloc = (destructor)linkset_dealloc,
    .tp_hash = (hashfunc)linkset_hash,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_richcompare = (richcmpfunc)linkset_richcompare,
    .tp_clear = (inquiry)linkset_clear,
    .tp_traverse = (traverseproc)linkset_traverse,
    .tp_free = PyObject_GC_Del,
    .tp_repr = (reprfunc)linkset_repr,
    .tp_weaklistoffset = offsetof(EdgeLinkSetObject, weakreflist),
};


PyObject *
EdgeLinkSet_InitType(void)
{
    if (PyType_Ready(&EdgeLinkSet_Type) < 0) {
        return NULL;
    }

    base_hash = _EdgeGeneric_HashString("edgedb.LinkSet");
    if (base_hash == -1) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeLinkSet_Type;
}
