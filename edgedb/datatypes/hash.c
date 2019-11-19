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


#if PY_VERSION_HEX >= 0x03080000

#if SIZEOF_PY_UHASH_T > 4
#define _PyHASH_XXPRIME_1 ((Py_uhash_t)11400714785074694791ULL)
#define _PyHASH_XXPRIME_2 ((Py_uhash_t)14029467366897019727ULL)
#define _PyHASH_XXPRIME_5 ((Py_uhash_t)2870177450012600261ULL)
#define _PyHASH_XXROTATE(x) ((x << 31) | (x >> 33))  /* Rotate left 31 bits */
#else
#define _PyHASH_XXPRIME_1 ((Py_uhash_t)2654435761UL)
#define _PyHASH_XXPRIME_2 ((Py_uhash_t)2246822519UL)
#define _PyHASH_XXPRIME_5 ((Py_uhash_t)374761393UL)
#define _PyHASH_XXROTATE(x) ((x << 13) | (x >> 19))  /* Rotate left 13 bits */
#endif

Py_hash_t
_EdgeGeneric_Hash(PyObject **els, Py_ssize_t len)
{
    /* Python's tuple hash algorithm.  Hashes of edgedb.Tuple and
       edgedb.NamedTuple must be equal to hashes of Python't tuples
       with the same elements */

    Py_ssize_t i;
    Py_uhash_t acc = _PyHASH_XXPRIME_5;
    for (i = 0; i < len; i++) {
        Py_uhash_t lane = PyObject_Hash(els[i]);
        if (lane == (Py_uhash_t)-1) {
            return -1;
        }
        acc += lane * _PyHASH_XXPRIME_2;
        acc = _PyHASH_XXROTATE(acc);
        acc *= _PyHASH_XXPRIME_1;
    }

    /* Add input length, mangled to keep the historical value of hash(()). */
    acc += len ^ (_PyHASH_XXPRIME_5 ^ 3527539UL);

    if (acc == (Py_uhash_t)-1) {
        return 1546275796;
    }
    return (Py_hash_t)acc;
}

#else

Py_hash_t
_EdgeGeneric_Hash(PyObject **els, Py_ssize_t len)
{
    /* Python's tuple hash algorithm.  Hashes of edgedb.Tuple and
       edgedb.NamedTuple must be equal to hashes of Python't tuples
       with the same elements */

    Py_uhash_t x;  /* Unsigned for defined overflow behavior. */
    PyObject **p = els;
    Py_hash_t y;
    Py_uhash_t mult;

    mult = _PyHASH_MULTIPLIER;
    x = 0x345678UL;
    while (--len >= 0) {
        y = PyObject_Hash(*p++);
        if (y == -1) {
            return -1;
        }
        x = (x ^ (Py_uhash_t)y) * mult;
        /* the cast might truncate len; that doesn't change hash stability */
        mult += (Py_uhash_t)(82520UL + (size_t)len + (size_t)len);
    }
    x += 97531UL;
    if (x == (Py_uhash_t)-1) {
        x = (Py_uhash_t)-2;
    }
    return (Py_hash_t)x;
}

#endif


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


Py_hash_t
_EdgeGeneric_HashWithBase(Py_hash_t base_hash, PyObject **els, Py_ssize_t len)
{
    /* Roughly equivalent to calling `hash((base_hash, *els))` in Python */

    assert(base_hash != -1);

    Py_hash_t els_hash = _EdgeGeneric_Hash(els, len);
    if (els_hash == -1) {
        return -1;
    }

    Py_uhash_t x = 0x345678UL;
    Py_uhash_t mult = _PyHASH_MULTIPLIER;

    x = (x ^ (Py_uhash_t)base_hash) * mult;
    mult += (Py_uhash_t)(82520UL + (size_t)4);
    x = (x ^ (Py_uhash_t)els_hash) * mult;
    x += 97531UL;

    Py_hash_t res = (Py_hash_t)x;
    if (res == -1) {
        res = -2;
    }
    return res;
}
