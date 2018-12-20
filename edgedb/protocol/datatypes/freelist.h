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


#ifndef EDGE_FREELIST_H
#define EDGE_FREELIST_H


#include "Python.h"


#define EDGE_SETUP_FREELIST(NAME, TYPEDEF, max_save_size, max_free_list)    \
    static const Py_ssize_t _ ## NAME ## _FL_MAX_SAVE_SIZE = max_save_size; \
    static const Py_ssize_t _ ## NAME ## _FL_MAX_FREE_LIST = max_free_list; \
    static TYPEDEF * _ ## NAME ## _FL[max_save_size];                       \
    static int _ ## NAME ## _FL_NUM_FREE[max_save_size];


/* Must call PyObject_GC_Track *after* this macro */
#define EDGE_NEW_WITH_FREELIST(NAME, TYPEDEF, TYPE, obj, size)              \
    {                                                                       \
        TYPEDEF *_o;                                                        \
        Py_ssize_t i;                                                       \
                                                                            \
        if (size < 0) {                                                     \
            PyErr_BadInternalCall();                                        \
            return NULL;                                                    \
        }                                                                   \
                                                                            \
        if (_ ## NAME ## _FL_MAX_SAVE_SIZE &&                               \
                size < _ ## NAME ## _FL_MAX_SAVE_SIZE &&                    \
                (_o = _ ## NAME ## _FL[size]) != NULL)                      \
        {                                                                   \
            if (size == 0) {                                                \
                Py_INCREF(_o);                                              \
            }                                                               \
            else {                                                          \
                _ ## NAME ## _FL[size] = (TYPEDEF *) _o->ob_item[0];        \
                _ ## NAME ## _FL_NUM_FREE[size]--;                          \
                _Py_NewReference((PyObject *)_o);                           \
            }                                                               \
        }                                                                   \
        else {                                                              \
            if ((size_t)size > (                                            \
                    (size_t)PY_SSIZE_T_MAX - sizeof(TYPEDEF) -              \
                     sizeof(PyObject *)) / sizeof(PyObject *))              \
            {                                                               \
                PyErr_NoMemory();                                           \
                return NULL;                                                \
            }                                                               \
                                                                            \
            _o = PyObject_GC_NewVar(TYPEDEF, TYPE, size);                   \
            if (_o == NULL) {                                               \
                return NULL;                                                \
            }                                                               \
        }                                                                   \
                                                                            \
        for (i = 0; i < size; i++) {                                        \
            _o->ob_item[i] = NULL;                                          \
        }                                                                   \
                                                                            \
        obj = _o;                                                           \
    }


/* Must call PyObject_GC_UnTrack *before* this macro */
#define EDGE_DEALLOC_WITH_FREELIST(NAME, TYPEDEF, obj)                      \
    {                                                                       \
        TYPEDEF *_o = obj;                                                  \
        Py_ssize_t i;                                                       \
        Py_ssize_t len = Py_SIZE(_o);                                       \
        if (len > 0) {                                                      \
            i = len;                                                        \
            while (--i >= 0) {                                              \
                Py_CLEAR(_o->ob_item[i]);                                   \
            }                                                               \
                                                                            \
            if (_ ## NAME ## _FL_MAX_SAVE_SIZE &&                           \
                    len < _ ## NAME ## _FL_MAX_SAVE_SIZE &&                 \
                    _ ## NAME ## _FL_NUM_FREE[len] <                        \
                        _ ## NAME ## _FL_MAX_FREE_LIST)                     \
            {                                                               \
                _o->ob_item[0] = (PyObject *) _ ## NAME ## _FL[len];        \
                _ ## NAME ## _FL_NUM_FREE[len]++;                           \
                _ ## NAME ## _FL[len] = _o;                                 \
                goto _done;                                                 \
            }                                                               \
        }                                                                   \
        Py_TYPE(_o)->tp_free((PyObject *)_o);                               \
    _done: ;                                                                \
    }


#endif
