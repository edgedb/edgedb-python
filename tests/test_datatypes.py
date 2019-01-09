#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2016-present MagicStack Inc. and the EdgeDB authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#


import unittest


import edgedb
from edgedb.protocol import aprotocol as private


class TestRecordDesc(unittest.TestCase):

    def test_recorddesc_1(self):
        with self.assertRaisesRegex(TypeError, 'one to two positional'):
            private._RecordDescriptor()

        with self.assertRaisesRegex(TypeError, 'one to two positional'):
            private._RecordDescriptor(t=1)

        with self.assertRaisesRegex(TypeError, 'requires a tuple'):
            private._RecordDescriptor(1)

        with self.assertRaisesRegex(TypeError, 'requires a tuple'):
            private._RecordDescriptor(('a',), 1)

        with self.assertRaisesRegex(TypeError,
                                    'the same length as the names tuple'):
            private._RecordDescriptor(('a',), ())

        private._RecordDescriptor(('a', 'b'))

        with self.assertRaisesRegex(ValueError, f'more than {0x4000-1}'):
            private._RecordDescriptor(('a',) * 20000)

    def test_recorddesc_2(self):
        rd = private._RecordDescriptor(
            ('a', 'b'), (private._EDGE_POINTER_IS_LINKPROP, 0))

        self.assertEqual(rd.get_pos('a'), 0)
        self.assertEqual(rd.get_pos('b'), 1)

        self.assertTrue(rd.is_linkprop('a'))
        self.assertFalse(rd.is_linkprop('b'))

        with self.assertRaises(LookupError):
            rd.get_pos('z')

        with self.assertRaises(LookupError):
            rd.is_linkprop('z')


class TestTuple(unittest.TestCase):

    def test_tuple_empty_1(self):
        t = edgedb.Tuple()
        self.assertEqual(len(t), 0)
        self.assertEqual(hash(t), hash(()))
        self.assertEqual(repr(t), '()')
        with self.assertRaisesRegex(IndexError, 'out of range'):
            t[0]

    def test_tuple_2(self):
        t = edgedb.Tuple((1, 'a'))
        self.assertEqual(len(t), 2)
        self.assertEqual(hash(t), hash((1, 'a')))

        self.assertEqual(repr(t), "(1, 'a')")

        self.assertEqual(t[0], 1)
        self.assertEqual(t[1], 'a')
        with self.assertRaisesRegex(IndexError, 'out of range'):
            t[2]

    def test_tuple_3(self):
        t = edgedb.Tuple((1, []))
        t[1].append(t)
        self.assertEqual(t[1], [t])

        self.assertEqual(repr(t), '(1, [(...)])')
        self.assertEqual(str(t), '(1, [(...)])')

    def test_tuple_4(self):
        with self.assertRaisesRegex(ValueError, f'more than {0x4000 - 1}'):
            edgedb.Tuple([1] * 20000)

    def test_tuple_freelist_1(self):
        l = []
        for i in range(5000):
            l.append(edgedb.Tuple((1,)))
        for t in l:
            self.assertEqual(t[0], 1)

    def test_tuple5(self):
        self.assertEqual(
            edgedb.Tuple([1, 2, 3]),
            edgedb.Tuple([1, 2, 3]))

        self.assertNotEqual(
            edgedb.Tuple([1, 2, 3]),
            edgedb.Tuple([1, 3, 2]))

        self.assertLess(
            edgedb.Tuple([1, 2, 3]),
            edgedb.Tuple([1, 3, 2]))

        self.assertEqual(
            edgedb.Tuple([]),
            edgedb.Tuple([]))

        self.assertEqual(
            edgedb.Tuple([1]),
            edgedb.Tuple([1]))

        self.assertGreaterEqual(
            edgedb.Tuple([1]),
            edgedb.Tuple([1]))

        self.assertNotEqual(
            edgedb.Tuple([1]),
            edgedb.Tuple([]))

        self.assertGreater(
            edgedb.Tuple([1]),
            edgedb.Tuple([]))

        self.assertNotEqual(
            edgedb.Tuple([1]),
            edgedb.Tuple([2]))

        self.assertLess(
            edgedb.Tuple([1]),
            edgedb.Tuple([2]))

        self.assertNotEqual(
            edgedb.Tuple([1, 2]),
            edgedb.Tuple([2, 2]))

        self.assertNotEqual(
            edgedb.Tuple([1, 1]),
            edgedb.Tuple([2, 2, 1]))


class TestNamedTuple(unittest.TestCase):

    def test_namedtuple_empty_1(self):
        with self.assertRaisesRegex(ValueError, 'at least one field'):
            edgedb.NamedTuple()

    def test_namedtuple_2(self):
        t = edgedb.NamedTuple(a=1)
        self.assertEqual(repr(t), "(a := 1)")

        t = edgedb.NamedTuple(a=1, b='a')

        self.assertEqual(repr(t), "(a := 1, b := 'a')")

        self.assertEqual(t[0], 1)
        self.assertEqual(t[1], 'a')
        with self.assertRaisesRegex(IndexError, 'out of range'):
            t[2]

        self.assertEqual(len(t), 2)
        self.assertEqual(hash(t), hash((1, 'a')))

        self.assertEqual(t.a, 1)
        self.assertEqual(t.b, 'a')

        with self.assertRaises(AttributeError):
            t.z

    def test_namedtuple_3(self):
        t = edgedb.NamedTuple(a=1, b=[])
        t.b.append(t)
        self.assertEqual(t.b, [t])

        self.assertEqual(repr(t), '(a := 1, b := [(...)])')
        self.assertEqual(str(t), '(a := 1, b := [(...)])')

    def test_namedtuple_4(self):
        t1 = edgedb.NamedTuple(a=1, b='aaaa')
        t2 = edgedb.Tuple((1, 'aaaa'))
        t3 = (1, 'aaaa')

        self.assertEqual(hash(t1), hash(t2))
        self.assertEqual(hash(t1), hash(t3))

    def test_namedtuple_5(self):
        self.assertEqual(
            edgedb.NamedTuple(a=1, b=2, c=3),
            edgedb.NamedTuple(x=1, y=2, z=3))

        self.assertNotEqual(
            edgedb.NamedTuple(a=1, b=2, c=3),
            edgedb.NamedTuple(a=1, c=3, b=2))

        self.assertLess(
            edgedb.NamedTuple(a=1, b=2, c=3),
            edgedb.NamedTuple(a=1, b=3, c=2))

        self.assertEqual(
            edgedb.NamedTuple(a=1),
            edgedb.NamedTuple(b=1))

        self.assertEqual(
            edgedb.NamedTuple(a=1),
            edgedb.NamedTuple(a=1))


class TestObject(unittest.TestCase):

    def test_object_1(self):
        f = private._create_object_factory(
            ('id', 'lb', 'c'), frozenset(['lb']))
        o = f(1, 2, 3)

        self.assertEqual(repr(o), 'Object{id := 1, @lb := 2, c := 3}')

        self.assertEqual(o.id, 1)
        self.assertEqual(o.c, 3)

        with self.assertRaises(AttributeError):
            o.lb

        with self.assertRaises(AttributeError):
            o.z

        with self.assertRaises(TypeError):
            len(o)

        with self.assertRaises(TypeError):
            o[0]

    def test_object_2(self):
        f = private._create_object_factory(
            ('id', 'lb', 'c'), frozenset(['lb']))
        o = f(1, 2, 3)

        self.assertEqual(hash(o), hash(f(1, 2, 3)))
        self.assertNotEqual(hash(o), hash(f(1, 2, 'aaaa')))
        self.assertNotEqual(hash(o), hash((1, 2, 3)))

    def test_object_3(self):
        f = private._create_object_factory(('id', 'c'), frozenset())
        o = f(1, [])

        o.c.append(o)
        self.assertEqual(repr(o), 'Object{id := 1, c := [Object{...}]}')

        with self.assertRaisesRegex(TypeError, 'unhashable'):
            hash(o)

    def test_object_4(self):
        f = private._create_object_factory(
            ('id', 'lb', 'c'), frozenset(['lb']))

        o1 = f(1, 'aa', 'ba')
        o2 = f(1, 'ab', 'bb')
        o3 = f(3, 'ac', 'bc')

        self.assertEqual(o1, o2)
        self.assertNotEqual(o1, o3)
        self.assertLess(o1, o3)
        self.assertGreater(o3, o2)

    def test_object_5(self):
        f = private._create_object_factory(
            ('a', 'lb', 'c'), frozenset(['lb']))
        with self.assertRaisesRegex(ValueError, "without 'id' field"):
            f(1, 2, 3)


class TestSet(unittest.TestCase):

    def test_set_1(self):
        s = edgedb.Set(())
        self.assertEqual(repr(s), 'Set{}')

        s = edgedb.Set((1, 2, [], 'a'))

        self.assertEqual(s[1], 2)
        self.assertEqual(s[2], [])
        self.assertEqual(len(s), 4)
        with self.assertRaises(IndexError):
            s[10]

        with self.assertRaises(TypeError):
            s[0] = 1

    def test_set_2(self):
        s = edgedb.Set((1, 2, 3000, 'a'))

        self.assertEqual(repr(s), "Set{1, 2, 3000, 'a'}")

        self.assertEqual(
            hash(s),
            hash(edgedb.Set((1, 2, sum([1000, 2000]), 'a'))))

        self.assertNotEqual(
            hash(s),
            hash((1, 2, 3000, 'a')))

    def test_set_3(self):
        s = edgedb.Set(())

        self.assertEqual(len(s), 0)
        self.assertEqual(hash(s), hash(edgedb.Set(())))
        self.assertNotEqual(hash(s), hash(()))

    def test_set_4(self):
        s = edgedb.Set(([],))
        s[0].append(s)
        self.assertEqual(repr(s), "Set{[Set{...}]}")

    def test_set_5(self):
        self.assertEqual(
            edgedb.Set([1, 2, 3]),
            edgedb.Set([3, 2, 1]))

        self.assertEqual(
            edgedb.Set([]),
            edgedb.Set([]))

        self.assertEqual(
            edgedb.Set([1]),
            edgedb.Set([1]))

        self.assertNotEqual(
            edgedb.Set([1]),
            edgedb.Set([]))

        self.assertNotEqual(
            edgedb.Set([1]),
            edgedb.Set([2]))

        self.assertNotEqual(
            edgedb.Set([1, 2]),
            edgedb.Set([2, 2]))

        self.assertNotEqual(
            edgedb.Set([1, 1, 2]),
            edgedb.Set([2, 2, 1]))

    def test_set_6(self):
        f = private._create_object_factory(
            ('id', 'lb', 'c'), frozenset(['lb']))

        o1 = f(1, 'aa', edgedb.Set([1, 2, 3]))
        o2 = f(1, 'ab', edgedb.Set([1, 2, 4]))
        o3 = f(3, 'ac', edgedb.Set([5, 5, 5, 5]))

        self.assertEqual(
            edgedb.Set([o1, o2, o3]),
            edgedb.Set([o2, o3, o1]))

        self.assertEqual(
            edgedb.Set([o1, o3]),
            edgedb.Set([o2, o3]))

        self.assertNotEqual(
            edgedb.Set([o1, o1]),
            edgedb.Set([o2, o3]))


class TestArray(unittest.TestCase):

    def test_array_empty_1(self):
        t = edgedb.Array()
        self.assertEqual(len(t), 0)
        self.assertNotEqual(hash(t), hash(()))
        with self.assertRaisesRegex(IndexError, 'out of range'):
            t[0]
        self.assertEqual(repr(t), "[]")

    def test_array_2(self):
        t = edgedb.Array((1, 'a'))

        self.assertEqual(repr(t), "[1, 'a']")
        self.assertEqual(str(t), "[1, 'a']")

        self.assertEqual(len(t), 2)
        self.assertEqual(hash(t), hash(edgedb.Array([1, 'a'])))
        self.assertNotEqual(hash(t), hash(edgedb.Array([10, 'ab'])))

        self.assertEqual(t[0], 1)
        self.assertEqual(t[1], 'a')
        with self.assertRaisesRegex(IndexError, 'out of range'):
            t[2]

    def test_array_3(self):
        t = edgedb.Array((1, []))
        t[1].append(t)
        self.assertEqual(t[1], [t])
        self.assertEqual(repr(t), '[1, [[...]]]')

    def test_array_4(self):
        self.assertEqual(
            edgedb.Array([1, 2, 3]),
            edgedb.Array([1, 2, 3]))

        self.assertNotEqual(
            edgedb.Array([1, 2, 3]),
            edgedb.Array([1, 3, 2]))

        self.assertLess(
            edgedb.Array([1, 2, 3]),
            edgedb.Array([1, 3, 2]))

        self.assertEqual(
            edgedb.Array([]),
            edgedb.Array([]))

        self.assertEqual(
            edgedb.Array([1]),
            edgedb.Array([1]))

        self.assertGreaterEqual(
            edgedb.Array([1]),
            edgedb.Array([1]))

        self.assertNotEqual(
            edgedb.Array([1]),
            edgedb.Array([]))

        self.assertGreater(
            edgedb.Array([1]),
            edgedb.Array([]))

        self.assertNotEqual(
            edgedb.Array([1]),
            edgedb.Array([2]))

        self.assertLess(
            edgedb.Array([1]),
            edgedb.Array([2]))

        self.assertNotEqual(
            edgedb.Array([1, 2]),
            edgedb.Array([2, 2]))

        self.assertNotEqual(
            edgedb.Array([1, 1]),
            edgedb.Array([2, 2, 1]))
