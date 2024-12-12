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


int _Edge_NoKeywords(const char *, PyObject *);

Py_hash_t _EdgeGeneric_HashString(const char *);

PyObject * _EdgeGeneric_RenderObject(PyObject *obj);

int _EdgeGeneric_RenderValues(
    _PyUnicodeWriter *, PyObject *, PyObject **, Py_ssize_t);

PyObject * _EdgeGeneric_RichCompareValues(PyObject **, Py_ssize_t,
                                          PyObject **, Py_ssize_t,
                                          int);


#define EDGE_RENDER_NAMES       0x1
#define EDGE_RENDER_LINK_PROPS  0x2
#define EDGE_RENDER_IMPLICIT    0x4
#define EDGE_RENDER_DEFAULT     0

int _EdgeGeneric_RenderItems(_PyUnicodeWriter *,
                             PyObject *, PyObject *,
                             PyObject **, Py_ssize_t,
                             int);

#ifndef _PyList_CAST
#  define _PyList_CAST(op) (assert(PyList_Check(op)), (PyListObject *)(op))
#endif

#ifndef _PyList_ITEMS
#  define _PyList_ITEMS(op) (_PyList_CAST(op)->ob_item)
#endif

#if PY_MAJOR_VERSION >= 3 && PY_MINOR_VERSION >= 8
#  define CPy_TRASHCAN_BEGIN(op, dealloc) Py_TRASHCAN_BEGIN(op, dealloc)
#  define CPy_TRASHCAN_END(op) Py_TRASHCAN_END
#else
#  define CPy_TRASHCAN_BEGIN(op, dealloc) Py_TRASHCAN_SAFE_BEGIN(op)
#  define CPy_TRASHCAN_END(op) Py_TRASHCAN_SAFE_END(op)
#endif

#endif
