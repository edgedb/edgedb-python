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
#include "internal.h"


static int init_type_called = 0;


static void
record_desc_dealloc(EdgeRecordDescObject *o)
{
    PyObject_GC_UnTrack(o);
    Py_CLEAR(o->index);
    Py_CLEAR(o->names);
    Py_CLEAR(o->get_dataclass_fields_func);
    PyMem_RawFree(o->descs);
    PyObject_GC_Del(o);
}


static int
record_desc_traverse(EdgeRecordDescObject *o, visitproc visit, void *arg)
{
    Py_VISIT(o->index);
    Py_VISIT(o->names);
    return 0;
}

static PyObject *
record_desc_tp_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    if (args == NULL ||
            PyTuple_Size(args) < 1 ||
            PyTuple_Size(args) > 3 ||
            (kwds != NULL && PyDict_Size(kwds)))
    {
        PyErr_SetString(
            PyExc_TypeError,
            "RecordDescriptor accepts one to three positional arguments");
        return NULL;
    }

    return EdgeRecordDesc_New(
        PyTuple_GET_ITEM(args, 0),
        PyTuple_Size(args) >= 2 ? PyTuple_GET_ITEM(args, 1) : NULL,
        PyTuple_Size(args) >= 3 ? PyTuple_GET_ITEM(args, 2) : NULL
    );
}


static PyObject *
record_desc_is_linkprop(EdgeRecordDescObject *o, PyObject *arg)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup((PyObject *)o, arg, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_NOT_FOUND:
            PyErr_SetObject(PyExc_LookupError, arg);
            return NULL;

        case L_LINKPROP:
            Py_RETURN_TRUE;

        case L_LINK:
        case L_PROPERTY:
            Py_RETURN_FALSE;

        default:
            abort();
    }
}


static PyObject *
record_desc_is_link(EdgeRecordDescObject *o, PyObject *arg)
{
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup((PyObject *)o, arg, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_NOT_FOUND:
            PyErr_SetObject(PyExc_LookupError, arg);
            return NULL;

        case L_LINK:
            Py_RETURN_TRUE;

        case L_LINKPROP:
        case L_PROPERTY:
            Py_RETURN_FALSE;

        default:
            abort();
    }
}


static PyObject *
record_desc_get_pos(EdgeRecordDescObject *o, PyObject *arg) {
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup((PyObject *)o, arg, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_NOT_FOUND:
            PyErr_SetObject(PyExc_LookupError, arg);
            return NULL;

        case L_LINK:
        case L_LINKPROP:
        case L_PROPERTY:
            return PyLong_FromLong((long)pos);

        default:
            abort();
    }
}


static PyObject *
record_desc_is_implicit(EdgeRecordDescObject *o, PyObject *arg) {
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeRecordDesc_Lookup((PyObject *)o, arg, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_NOT_FOUND:
            PyErr_SetObject(PyExc_LookupError, arg);
            return NULL;

        case L_LINK:
        case L_LINKPROP:
        case L_PROPERTY:
            if (o->descs[pos].flags & EDGE_POINTER_IS_IMPLICIT) {
                Py_RETURN_TRUE;
            }
            else {
                Py_RETURN_FALSE;
            }

        default:
            abort();
    }
}


static PyObject *
record_desc_dir(EdgeRecordDescObject *o, PyObject *args)
{
    PyObject *names = o->names;
    Py_INCREF(names);
    return names;
}


static PyObject *
record_set_dataclass_fields_func(EdgeRecordDescObject *o, PyObject *arg)
{
    Py_CLEAR(o->get_dataclass_fields_func);
    o->get_dataclass_fields_func = arg;
    Py_INCREF(arg);
    Py_RETURN_NONE;
}


static PyMethodDef record_desc_methods[] = {
    {"is_linkprop", (PyCFunction)record_desc_is_linkprop, METH_O, NULL},
    {"is_link", (PyCFunction)record_desc_is_link, METH_O, NULL},
    {"is_implicit", (PyCFunction)record_desc_is_implicit, METH_O, NULL},
    {"get_pos", (PyCFunction)record_desc_get_pos, METH_O, NULL},
    {"__dir__", (PyCFunction)record_desc_dir, METH_NOARGS, NULL},
    {"set_dataclass_fields_func",
     (PyCFunction)record_set_dataclass_fields_func, METH_O, NULL},
    {NULL, NULL}
};


PyTypeObject EdgeRecordDesc_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "gel.RecordDescriptor",
    .tp_basicsize = sizeof(EdgeRecordDescObject),
    .tp_dealloc = (destructor)record_desc_dealloc,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)record_desc_traverse,
    .tp_new = record_desc_tp_new,
    .tp_methods = record_desc_methods,
};


PyObject *
EdgeRecordDesc_New(PyObject *names, PyObject *flags, PyObject *cards)
{
    EdgeRecordDescObject *o;

    assert(init_type_called);

    if (!names || !PyTuple_CheckExact(names)) {
        PyErr_SetString(
            PyExc_TypeError,
            "RecordDescriptor requires a tuple as its first argument");
        return NULL;
    }

    if (Py_SIZE(names) > EDGE_MAX_TUPLE_SIZE) {
        PyErr_Format(
            PyExc_ValueError,
            "EdgeDB does not supports tuples with more than %d elements",
            EDGE_MAX_TUPLE_SIZE);
        return NULL;
    }

    Py_ssize_t size = Py_SIZE(names);

    if (flags != NULL) {
        if (!PyTuple_CheckExact(flags)) {
            PyErr_SetString(
                PyExc_TypeError,
                "RecordDescriptor requires a tuple as its second argument");
            return NULL;
        }
        if (Py_SIZE(flags) != size) {
            PyErr_SetString(
                PyExc_TypeError,
                "RecordDescriptor the flags tuple to be the same "
                "length as the names tuple");
            return NULL;
        }
    }

    if (cards != NULL) {
        if (!PyTuple_CheckExact(cards)) {
            PyErr_SetString(
                PyExc_TypeError,
                "RecordDescriptor requires a tuple as its third argument");
            return NULL;
        }
        if (Py_SIZE(cards) != size) {
            PyErr_SetString(
                PyExc_TypeError,
                "RecordDescriptor the cards tuple to be the same "
                "length as the names tuple");
            return NULL;
        }
    }

    Py_ssize_t idpos = -1;

    PyObject *index = PyDict_New();
    if (index == NULL) {
        return NULL;
    }

    EdgeRecordFieldDesc *descs = (EdgeRecordFieldDesc *)PyMem_RawCalloc(
        (size_t)size, sizeof(EdgeRecordFieldDesc));
    if (descs == NULL) {
        PyErr_NoMemory();
        return NULL;
    }

    for (Py_ssize_t i = 0; i < size; i++) {
        PyObject *key = PyTuple_GET_ITEM(names, i);  /* borrowed */
        if (!PyUnicode_CheckExact(key)) {
            PyErr_SetString(
                PyExc_ValueError,
                "RecordDescriptor received a non-str key");
            goto fail;
        }

        if (flags != NULL) {
            if (PyUnicode_CompareWithASCIIString(key, "id") == 0) {
                idpos = i;
            }

            PyObject *flag = PyTuple_GET_ITEM(flags, i);
            int32_t flag_long = (int32_t)PyLong_AsLong(flag);
            if (flag_long == -1 && PyErr_Occurred()) {
                goto fail;
            }
            descs[i].flags = (uint32_t)flag_long;
        }

        if (cards != NULL) {
            if (PyUnicode_CompareWithASCIIString(key, "id") == 0) {
                idpos = i;
            }

            PyObject *card = PyTuple_GET_ITEM(cards, i);
            int32_t card_long = (int32_t)PyLong_AsLong(card);
            if (card_long == -1 && PyErr_Occurred()) {
                goto fail;
            }

            EdgeFieldCardinality cast_card = UNKNOWN;
            switch (card_long) {
                case 0x6e: cast_card = NO_RESULT; break;
                case 0x6f: cast_card = AT_MOST_ONE; break;
                case 0x41: cast_card = ONE; break;
                case 0x6d: cast_card = MANY; break;
                case 0x4d: cast_card = AT_LEAST_ONE; break;
                default: {
                    PyErr_Format(PyExc_OverflowError,
                                "invalid cardinality %d", card_long);
                    goto fail;
                }
            }
            descs[i].cardinality = cast_card;
        }

        PyObject *num = PyLong_FromLong(i);
        if (num == NULL) {
            Py_DECREF(index);
            goto fail;
        }

        if (PyDict_SetItem(index, key, num)) {
            Py_DECREF(index);
            Py_DECREF(num);
            goto fail;
        }

        Py_DECREF(num);
    }

    o = PyObject_GC_New(EdgeRecordDescObject, &EdgeRecordDesc_Type);
    if (o == NULL) {
        Py_DECREF(index);
        goto fail;
    }

    o->descs = descs;

    o->index = index;

    Py_INCREF(names);
    o->names = names;

    o->size = size;
    o->idpos = idpos;
    o->get_dataclass_fields_func = NULL;

    PyObject_GC_Track(o);
    return (PyObject *)o;

fail:
    PyMem_RawFree(descs);
    return NULL;
}


edge_attr_lookup_t
EdgeRecordDesc_Lookup(PyObject *ob, PyObject *key, Py_ssize_t *pos)
{
    if (!EdgeRecordDesc_Check(ob)) {
        PyErr_BadInternalCall();
        return L_ERROR;
    }

    EdgeRecordDescObject *d = (EdgeRecordDescObject *)ob;

    PyObject *res = PyDict_GetItem(d->index, key);  /* borrowed */
    if (res == NULL) {
        if (PyErr_Occurred()) {
            return L_ERROR;
        }
        else {
            return L_NOT_FOUND;
        }
    }

    assert(PyLong_CheckExact(res));
    long res_long = PyLong_AsLong(res);
    if (res_long < 0) {
        assert(PyErr_Occurred());
        return L_ERROR;
    }
    assert(res_long < d->size);
    *pos = res_long;

    if (d->descs[res_long].flags & EDGE_POINTER_IS_LINKPROP) {
        return L_LINKPROP;
    }
    else if (d->descs[res_long].flags & EDGE_POINTER_IS_LINK) {
        return L_LINK;
    }
    else {
        return L_PROPERTY;
    }
}


PyObject *
EdgeRecordDesc_PointerName(PyObject *ob, Py_ssize_t pos)
{
    if (!EdgeRecordDesc_Check(ob)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    EdgeRecordDescObject *o = (EdgeRecordDescObject *)ob;
    PyObject * key = PyTuple_GetItem(o->names, pos);
    if (key == NULL) {
        return NULL;
    }
    Py_INCREF(key);
    return key;
}


Py_ssize_t
EdgeRecordDesc_IDPos(PyObject *ob)
{
    if (!EdgeRecordDesc_Check(ob)) {
        PyErr_BadInternalCall();
        return -1;
    }
    return ((EdgeRecordDescObject *)ob)->idpos;
}


EdgeFieldCardinality
EdgeRecordDesc_PointerCardinality(PyObject *ob, Py_ssize_t pos)
{
    if (!EdgeRecordDesc_Check(ob)) {
        PyErr_BadInternalCall();
        return -1;
    }
    EdgeRecordDescObject *o = (EdgeRecordDescObject *)ob;
    if (pos < 0 || pos >= o->size) {
        PyErr_SetNone(PyExc_IndexError);
        return -1;
    }
    return o->descs[pos].cardinality;
}


int
EdgeRecordDesc_PointerIsLinkProp(PyObject *ob, Py_ssize_t pos)
{
    if (!EdgeRecordDesc_Check(ob)) {
        PyErr_BadInternalCall();
        return -1;
    }
    EdgeRecordDescObject *o = (EdgeRecordDescObject *)ob;
    if (pos < 0 || pos >= o->size) {
        PyErr_SetNone(PyExc_IndexError);
        return -1;
    }
    return o->descs[pos].flags & EDGE_POINTER_IS_LINKPROP;
}

int
EdgeRecordDesc_PointerIsLink(PyObject *ob, Py_ssize_t pos)
{
    if (!EdgeRecordDesc_Check(ob)) {
        PyErr_BadInternalCall();
        return -1;
    }
    EdgeRecordDescObject *o = (EdgeRecordDescObject *)ob;
    if (pos < 0 || pos >= o->size) {
        PyErr_SetNone(PyExc_IndexError);
        return -1;
    }
    return o->descs[pos].flags & EDGE_POINTER_IS_LINK;
}

int
EdgeRecordDesc_PointerIsImplicit(PyObject *ob, Py_ssize_t pos)
{
    if (!EdgeRecordDesc_Check(ob)) {
        PyErr_BadInternalCall();
        return -1;
    }
    EdgeRecordDescObject *o = (EdgeRecordDescObject *)ob;
    if (pos < 0 || pos >= o->size) {
        PyErr_SetNone(PyExc_IndexError);
        return -1;
    }
    return o->descs[pos].flags & EDGE_POINTER_IS_IMPLICIT;
}

Py_ssize_t
EdgeRecordDesc_GetSize(PyObject *ob)
{
    assert(ob != NULL);
    if (!EdgeRecordDesc_Check(ob)) {
        PyErr_BadInternalCall();
        return -1;
    }
    EdgeRecordDescObject *o = (EdgeRecordDescObject *)ob;
    return o->size;
}


PyObject *
EdgeRecordDesc_List(PyObject *ob, uint8_t include_mask, uint8_t exclude_mask)
{
    if (!EdgeRecordDesc_Check(ob)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    EdgeRecordDescObject *o = (EdgeRecordDescObject *)ob;

    PyObject *ret = PyList_New(0);
    if (ret == NULL) {
        return NULL;
    }

    for (Py_ssize_t i = 0; i < o->size; i++) {
        if ((include_mask == 0xFF || (o->descs[i].flags & include_mask)) &&
                (exclude_mask == 0 || !(o->descs[i].flags & exclude_mask)))
        {
            PyObject *name = PyTuple_GetItem(o->names, i);
            if (name == NULL) {
                Py_DECREF(ret);
                return NULL;
            }
            if (PyList_Append(ret, name)) {
                Py_DECREF(ret);
                return NULL;
            }
        }
    }

    return ret;
}


PyObject *
EdgeRecordDesc_GetDataclassFields(PyObject *ob)
{
    if (!EdgeRecordDesc_Check(ob)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    EdgeRecordDescObject *o = (EdgeRecordDescObject *)ob;

// bpo-37194 added PyObject_CallNoArgs() to Python 3.9.0a1
#if PY_VERSION_HEX < 0x030900A1
    return PyObject_CallFunctionObjArgs(o->get_dataclass_fields_func, NULL);
#else
    return PyObject_CallNoArgs(o->get_dataclass_fields_func);
#endif
}


PyObject *
EdgeRecordDesc_InitType(void)
{
    if (PyType_Ready(&EdgeRecordDesc_Type) < 0) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeRecordDesc_Type;
}
