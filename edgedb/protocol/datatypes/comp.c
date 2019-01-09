/*
* This source file is part of the EdgeDB open source project.
*
* Copyright 2019-present MagicStack Inc. and the EdgeDB authors.
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


#include "internal.h"


PyObject *
_EdgeGeneric_RichCompareValues(PyObject **left_items, Py_ssize_t left_len,
                               PyObject **right_items, Py_ssize_t right_len,
                               int op)
{
    Py_ssize_t i;

    if (left_len != right_len && (op == Py_EQ || op == Py_NE)) {
        if (op == Py_EQ) {
            Py_RETURN_FALSE;
        }
        else {
            Py_RETURN_TRUE;
        }
    }

    for (i = 0; i < left_len && i < right_len; i++) {
        int ret = PyObject_RichCompareBool(
            left_items[i], right_items[i], Py_EQ);

        if (ret < 0) {
            return NULL;
        }

        if (ret == 0) {
            /* first element that differs is found */
            break;
        }
    }

    if (i >= left_len || i >= right_len) {
        /* Same logic as for Python tuple comparison */
        switch (op) {
            case Py_EQ:
                if (left_len == right_len) Py_RETURN_TRUE;
                Py_RETURN_FALSE;
            case Py_NE:
                if (left_len != right_len) Py_RETURN_TRUE;
                Py_RETURN_FALSE;
            case Py_LT:
                if (left_len < right_len) Py_RETURN_TRUE;
                Py_RETURN_FALSE;
            case Py_GT:
                if (left_len > right_len) Py_RETURN_TRUE;
                Py_RETURN_FALSE;
            case Py_LE:
                if (left_len <= right_len) Py_RETURN_TRUE;
                Py_RETURN_FALSE;
            case Py_GE:
                if (left_len >= right_len) Py_RETURN_TRUE;
                Py_RETURN_FALSE;
            default:
                abort();
        }
    }

    if (op == Py_EQ) {
        Py_RETURN_FALSE;
    }
    if (op == Py_NE) {
        Py_RETURN_TRUE;
    }

    return PyObject_RichCompare(left_items[i], right_items[i], op);
}
