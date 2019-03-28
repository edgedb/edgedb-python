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


#ifndef EDGE_DATATYPES_H
#define EDGE_DATATYPES_H


#include <stdint.h>
#include "Python.h"


#define EDGE_MAX_TUPLE_SIZE         (0x4000 - 1)

#define EDGE_POINTER_IS_IMPLICIT    (1 << 0)
#define EDGE_POINTER_IS_LINKPROP    (1 << 1)
#define EDGE_POINTER_IS_LINK        (1 << 2)


/* === edgedb.RecordDesc ==================================== */

extern PyTypeObject EdgeRecordDesc_Type;

#define EdgeRecordDesc_Check(d) (Py_TYPE(d) == &EdgeRecordDesc_Type)

typedef struct {
    PyObject_HEAD
    PyObject *index;
    PyObject *names;
    uint8_t *posbits;
    Py_ssize_t idpos;
    Py_ssize_t size;
} EdgeRecordDescObject;

typedef enum {
    L_ERROR,
    L_NOT_FOUND,
    L_LINKPROP,
    L_PROPERTY,
    L_LINK
} edge_attr_lookup_t;

PyObject * EdgeRecordDesc_InitType(void);
PyObject * EdgeRecordDesc_New(PyObject *, PyObject *);
PyObject * EdgeRecordDesc_PointerName(PyObject *, Py_ssize_t);
Py_ssize_t EdgeRecordDesc_IDPos(PyObject *ob);

int EdgeRecordDesc_PointerIsLinkProp(PyObject *, Py_ssize_t);
int EdgeRecordDesc_PointerIsLink(PyObject *, Py_ssize_t);
int EdgeRecordDesc_PointerIsImplicit(PyObject *, Py_ssize_t);

Py_ssize_t EdgeRecordDesc_GetSize(PyObject *);
edge_attr_lookup_t EdgeRecordDesc_Lookup(PyObject *, PyObject *, Py_ssize_t *);
PyObject * EdgeRecordDesc_List(PyObject *, uint8_t, uint8_t);



/* === edgedb.Tuple ========================================= */

#define EDGE_TUPLE_FREELIST_SIZE 500
#define EDGE_TUPLE_FREELIST_MAXSAVE 20

extern PyTypeObject EdgeTuple_Type;

#define EdgeTuple_Check(d) (Py_TYPE(d) == &EdgeTuple_Type)

typedef struct {
    PyObject_VAR_HEAD
    PyObject *weakreflist;
    PyObject *ob_item[1];
} EdgeTupleObject;

PyObject * EdgeTuple_InitType(void);
PyObject * EdgeTuple_New(Py_ssize_t size);
int EdgeTuple_SetItem(PyObject *, Py_ssize_t, PyObject *);



/* === edgedb.NamedTuple ==================================== */

#define EDGE_NAMEDTUPLE_FREELIST_SIZE 500
#define EDGE_NAMEDTUPLE_FREELIST_MAXSAVE 20

extern PyTypeObject EdgeNamedTuple_Type;

#define EdgeNamedTuple_Check(d) (Py_TYPE(d) == &EdgeNamedTuple_Type)

typedef struct {
    PyObject_VAR_HEAD
    PyObject *desc;
    PyObject *weakreflist;
    PyObject *ob_item[1];
} EdgeNamedTupleObject;

PyObject * EdgeNamedTuple_InitType(void);
PyObject * EdgeNamedTuple_New(PyObject *);
int EdgeNamedTuple_SetItem(PyObject *, Py_ssize_t, PyObject *);


/* === edgedb.Object ======================================== */

#define EDGE_OBJECT_FREELIST_SIZE 2000
#define EDGE_OBJECT_FREELIST_MAXSAVE 20

extern PyTypeObject EdgeObject_Type;

#define EdgeObject_Check(d) (Py_TYPE(d) == &EdgeObject_Type)

typedef struct {
    PyObject_VAR_HEAD
    PyObject *weakreflist;
    PyObject *desc;
    Py_hash_t cached_hash;
    PyObject *ob_item[1];
} EdgeObject;

PyObject * EdgeObject_InitType(void);
PyObject * EdgeObject_New(PyObject *);
PyObject * EdgeObject_GetRecordDesc(PyObject *);

int EdgeObject_SetItem(PyObject *, Py_ssize_t, PyObject *);
PyObject * EdgeObject_GetItem(PyObject *, Py_ssize_t);

PyObject * EdgeObject_GetID(PyObject *ob);


/* === edgedb.Set =========================================== */

extern PyTypeObject EdgeSet_Type;

#define EdgeSet_Check(d) (Py_TYPE(d) == &EdgeSet_Type)

typedef struct {
    PyObject_HEAD
    Py_hash_t cached_hash;
    PyObject *weakreflist;
    PyObject *els;
} EdgeSetObject;

PyObject * EdgeSet_InitType(void);
PyObject * EdgeSet_New(Py_ssize_t);

int EdgeSet_SetItem(PyObject *, Py_ssize_t, PyObject *);
PyObject * EdgeSet_GetItem(PyObject *, Py_ssize_t);

int EdgeSet_AppendItem(PyObject *, PyObject *);
Py_ssize_t EdgeSet_Len(PyObject *);


/* === edgedb.Array ========================================= */

#define EDGE_ARRAY_FREELIST_SIZE 500
#define EDGE_ARRAY_FREELIST_MAXSAVE 10

extern PyTypeObject EdgeArray_Type;

#define EdgeArray_Check(d) (Py_TYPE(d) == &EdgeArray_Type)

typedef struct {
    PyObject_VAR_HEAD
    PyObject *weakreflist;
    Py_hash_t cached_hash;
    PyObject *ob_item[1];
} EdgeArrayObject;

PyObject * EdgeArray_InitType(void);
PyObject * EdgeArray_New(Py_ssize_t size);
int EdgeArray_SetItem(PyObject *, Py_ssize_t, PyObject *);


/* === edgedb.Link ========================================== */

extern PyTypeObject EdgeLink_Type;

#define EdgeLink_Check(d) (Py_TYPE(d) == &EdgeLink_Type)

typedef struct {
    PyObject_VAR_HEAD
    PyObject *weakreflist;
    PyObject *name;
    PyObject *source;
    PyObject *target;
} EdgeLinkObject;

PyObject * EdgeLink_InitType(void);
PyObject * EdgeLink_New(PyObject *, PyObject *, PyObject *);


/* === edgedb.LinkSet ======================================= */

extern PyTypeObject EdgeLinkSet_Type;

#define EdgeLinkSet_Check(d) (Py_TYPE(d) == &EdgeLinkSet_Type)

typedef struct {
    PyObject_VAR_HEAD
    PyObject *weakreflist;
    PyObject *name;
    PyObject *source;
    PyObject *targets;
} EdgeLinkSetObject;

PyObject * EdgeLinkSet_InitType(void);
PyObject * EdgeLinkSet_New(PyObject *, PyObject *, PyObject *);

#endif
