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
#include "internal.h"


static int init_type_called = 0;


static void
input_shape_dealloc(EdgeInputShapeObject *o)
{
    PyObject_GC_UnTrack(o);
    Py_CLEAR(o->index);
    Py_CLEAR(o->names);
    PyObject_GC_Del(o);
}


static int
input_shape_traverse(EdgeInputShapeObject *o, visitproc visit, void *arg)
{
    Py_VISIT(o->index);
    Py_VISIT(o->names);
    return 0;
}

static PyObject *
input_shape_tp_new(PyTypeObject *type, PyObject *args, PyObject *kwds)
{
    if (args == NULL ||
            PyTuple_Size(args) != 1 ||
            (kwds != NULL && PyDict_Size(kwds)))
    {
        PyErr_SetString(
            PyExc_TypeError,
            "InputShape accepts exactly one positional argument");
        return NULL;
    }

    return EdgeInputShape_New(PyTuple_GET_ITEM(args, 0));
}


static PyObject *
input_shape_get_pos(EdgeInputShapeObject *o, PyObject *arg) {
    Py_ssize_t pos;
    edge_attr_lookup_t ret = EdgeInputShape_Lookup((PyObject *)o, arg, &pos);
    switch (ret) {
        case L_ERROR:
            return NULL;

        case L_NOT_FOUND:
            PyErr_SetObject(PyExc_LookupError, arg);
            return NULL;

        case L_PROPERTY:
            return PyLong_FromLong((long)pos);

        default:
            abort();
    }
}


static PyObject *
input_shape_dir(EdgeInputShapeObject *o, PyObject *args)
{
    PyObject *names = o->names;
    Py_INCREF(names);
    return names;
}


static PyMethodDef input_shape_methods[] = {
    {"get_pos", (PyCFunction)input_shape_get_pos, METH_O, NULL},
    {"__dir__", (PyCFunction)input_shape_dir, METH_NOARGS, NULL},
    {NULL, NULL}
};


PyTypeObject EdgeInputShape_Type = {
    PyVarObject_HEAD_INIT(NULL, 0)
    .tp_name = "edgedb.InputShape",
    .tp_basicsize = sizeof(EdgeInputShapeObject),
    .tp_dealloc = (destructor)input_shape_dealloc,
    .tp_getattro = PyObject_GenericGetAttr,
    .tp_flags = Py_TPFLAGS_DEFAULT | Py_TPFLAGS_HAVE_GC,
    .tp_traverse = (traverseproc)input_shape_traverse,
    .tp_new = input_shape_tp_new,
    .tp_methods = input_shape_methods,
};


PyObject *
EdgeInputShape_New(PyObject *names)
{
    EdgeInputShapeObject *o;

    assert(init_type_called);

    if (!names || !PyTuple_CheckExact(names)) {
        PyErr_SetString(
            PyExc_TypeError,
            "InputShape requires a tuple as its first argument");
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

    PyObject *index = PyDict_New();
    if (index == NULL) {
        return NULL;
    }

    for (Py_ssize_t i = 0; i < size; i++) {
        PyObject *key = PyTuple_GET_ITEM(names, i);  /* borrowed */
        if (!PyUnicode_CheckExact(key)) {
            PyErr_SetString(
                PyExc_ValueError,
                "InputShape received a non-str key");
            return NULL;
        }

        PyObject *num = PyLong_FromLong(i);
        if (num == NULL) {
            Py_DECREF(index);
            return NULL;
        }

        if (PyDict_SetItem(index, key, num)) {
            Py_DECREF(index);
            Py_DECREF(num);
            return NULL;
        }

        Py_DECREF(num);
    }

    o = PyObject_GC_New(EdgeInputShapeObject, &EdgeInputShape_Type);
    if (o == NULL) {
        Py_DECREF(index);
        return NULL;
    }

    o->index = index;

    Py_INCREF(names);
    o->names = names;

    o->size = size;

    PyObject_GC_Track(o);
    return (PyObject *)o;
}


edge_attr_lookup_t
EdgeInputShape_Lookup(PyObject *ob, PyObject *key, Py_ssize_t *pos)
{
    if (!EdgeInputShape_Check(ob)) {
        PyErr_BadInternalCall();
        return L_ERROR;
    }

    EdgeInputShapeObject *d = (EdgeInputShapeObject *)ob;

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

    return L_PROPERTY;
}


PyObject *
EdgeInputShape_PointerName(PyObject *ob, Py_ssize_t pos)
{
    if (!EdgeInputShape_Check(ob)) {
        PyErr_BadInternalCall();
        return NULL;
    }
    EdgeInputShapeObject *o = (EdgeInputShapeObject *)ob;
    PyObject * key = PyTuple_GetItem(o->names, pos);
    if (key == NULL) {
        return NULL;
    }
    Py_INCREF(key);
    return key;
}


Py_ssize_t
EdgeInputShape_GetSize(PyObject *ob)
{
    assert(ob != NULL);
    if (!EdgeInputShape_Check(ob)) {
        PyErr_BadInternalCall();
        return -1;
    }
    EdgeInputShapeObject *o = (EdgeInputShapeObject *)ob;
    return o->size;
}


PyObject *
EdgeInputShape_List(PyObject *ob)
{
    if (!EdgeInputShape_Check(ob)) {
        PyErr_BadInternalCall();
        return NULL;
    }

    EdgeInputShapeObject *o = (EdgeInputShapeObject *)ob;

    PyObject *ret = PyList_New(o->size);
    if (ret == NULL) {
        return NULL;
    }

    for (Py_ssize_t i = 0; i < o->size; i++) {
        PyObject *name = PyTuple_GetItem(o->names, i);
        if (name == NULL) {
            Py_DECREF(ret);
            return NULL;
        }

        Py_INCREF(name);
        if (PyList_SetItem(ret, i, name)) {
            Py_DECREF(ret);
            return NULL;
        }
    }

    return ret;
}


PyObject *
EdgeInputShape_InitType(void)
{
    if (PyType_Ready(&EdgeInputShape_Type) < 0) {
        return NULL;
    }

    init_type_called = 1;
    return (PyObject *)&EdgeInputShape_Type;
}
