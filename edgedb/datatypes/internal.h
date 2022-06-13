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


#ifndef EDGE_INTERNAL_H
#define EDGE_INTERNAL_H


#include <stddef.h> /* For offsetof */

#include <Python.h>

#include "datatypes.h"


#define _Edge_IsContainer(o)                                \
    (EdgeTuple_Check(o) ||                                  \
     EdgeNamedTuple_Check(o) ||                             \
     EdgeObject_Check(o) ||                                 \
     EdgeSet_Check(o) ||                                    \
     EdgeArray_Check(o))


int _Edge_NoKeywords(const char *, PyObject *);

Py_hash_t _EdgeGeneric_Hash(PyObject **, Py_ssize_t);
Py_hash_t _EdgeGeneric_HashWithBase(Py_hash_t, PyObject **, Py_ssize_t);
Py_hash_t _EdgeGeneric_HashString(const char *);

PyObject * _EdgeGeneric_RenderObject(PyObject *obj);

int _EdgeGeneric_RenderValues(
    _PyUnicodeWriter *, PyObject *, PyObject **, Py_ssize_t);

int _EdgeGeneric_RenderItems(_PyUnicodeWriter *,
                             PyObject *, PyObject *,
                             PyObject **, Py_ssize_t, int, int);

PyObject * _EdgeGeneric_RichCompareValues(PyObject **, Py_ssize_t,
                                          PyObject **, Py_ssize_t,
                                          int);


#ifndef _PyList_CAST
#  define _PyList_CAST(op) (assert(PyList_Check(op)), (PyListObject *)(op))
#endif

#ifndef _PyList_ITEMS
#  define _PyList_ITEMS(op) (_PyList_CAST(op)->ob_item)
#endif

#endif
