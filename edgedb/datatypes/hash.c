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
*
* Portions Copyright 2001-2018 Python Software Foundation.
* See also https://github.com/python/cpython/blob/master/LICENSE.
*/

#include "Python.h"

#include "datatypes.h"


Py_hash_t
_EdgeGeneric_HashString(const char *s)
{
    PyObject *o = PyUnicode_FromString(s);
    if (o == NULL) {
        return -1;
    }

    Py_hash_t res = PyObject_Hash(o);
    Py_DECREF(o);
    return res;
}
