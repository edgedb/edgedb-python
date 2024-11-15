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


import dataclasses
import gc
import os
import random
import string
import unittest
import weakref

import edgedb
from edgedb.datatypes import datatypes as private
from edgedb import introspect


class TestRecordDesc(unittest.TestCase):

    def test_recorddesc_1(self):
        with self.assertRaisesRegex(TypeError, 'one to three positional'):
            private._RecordDescriptor()

        with self.assertRaisesRegex(TypeError, 'one to three positional'):
            private._RecordDescriptor(t=1)

        with self.assertRaisesRegex(TypeError, 'requires a tuple'):
            private._RecordDescriptor(1)

        with self.assertRaisesRegex(TypeError, 'requires a tuple'):
            private._RecordDescriptor(('a',), 1)

        with self.assertRaisesRegex(TypeError,
                                    'the same length as the names tuple'):
            private._RecordDescriptor(('a',), ())

        private._RecordDescriptor(('a', 'b'))

        with self.assertRaisesRegex(ValueError, f'more than {0x4000 - 1}'):
            private._RecordDescriptor(('a',) * 20000)

    def test_recorddesc_2(self):
        rd = private._RecordDescriptor(
            ('a', 'b', 'c'),
            (private._EDGE_POINTER_IS_LINKPROP,
             0,
             private._EDGE_POINTER_IS_LINK))

        self.assertEqual(rd.get_pos('a'), 0)
        self.assertEqual(rd.get_pos('b'), 1)
        self.assertEqual(rd.get_pos('c'), 2)

        self.assertTrue(rd.is_linkprop('a'))
        self.assertFalse(rd.is_linkprop('b'))
        self.assertFalse(rd.is_linkprop('c'))

        self.assertFalse(rd.is_link('a'))
        self.assertFalse(rd.is_link('b'))
        self.assertTrue(rd.is_link('c'))

        with self.assertRaises(LookupError):
            rd.get_pos('z')

        with self.assertRaises(LookupError):
            rd.is_linkprop('z')

    def test_recorddesc_3(self):
        f = private.create_object_factory(
            id={'property', 'implicit'},
            lb='link-property',
            c='property',
            d='link',
        )

        o = f(1, 2, 3, 4)

        desc = private.get_object_descriptor(o)
        self.assertEqual(set(dir(desc)), set(('id', '@lb', 'c', 'd')))

        self.assertTrue(desc.is_linkprop('@lb'))
        self.assertFalse(desc.is_linkprop('id'))
        self.assertFalse(desc.is_linkprop('c'))
        self.assertFalse(desc.is_linkprop('d'))

        self.assertFalse(desc.is_link('@lb'))
        self.assertFalse(desc.is_link('id'))
        self.assertFalse(desc.is_link('c'))
        self.assertTrue(desc.is_link('d'))

        self.assertFalse(desc.is_implicit('@lb'))
        self.assertTrue(desc.is_implicit('id'))
        self.assertFalse(desc.is_implicit('c'))
        self.assertFalse(desc.is_implicit('d'))

        self.assertEqual(desc.get_pos('@lb'), 1)
        self.assertEqual(desc.get_pos('id'), 0)
        self.assertEqual(desc.get_pos('c'), 2)
        self.assertEqual(desc.get_pos('d'), 3)

    def test_recorddesc_4(self):
        f = private.create_object_factory(
            id={'property', 'implicit'},
            lb='link-property',
            c='property',
            d='link',
        )

        o = f(1, 2, 3, 4)
        intro = introspect.introspect_object(o)

        self.assertEqual(
            intro.pointers,
            (
                ('id', introspect.ElementKind.PROPERTY, True),
                ('c', introspect.ElementKind.PROPERTY, False),
                ('d', introspect.ElementKind.LINK, False),
            )
        )

        # clear cache so that tests in refcount mode don't freak out.
        introspect._introspect_object_desc.cache_clear()


class TestTuple(unittest.TestCase):

    def test_tuple_empty_1(self):
        t = edgedb.Tuple()
        self.assertIsInstance(t, tuple)
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

    def test_tuple_freelist_1(self):
        lst = []
        for _ in range(5000):
            lst.append(edgedb.Tuple((1,)))
        for t in lst:
            self.assertEqual(t[0], 1)

    def test_tuple_5(self):
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

    def test_tuple_6(self):
        self.assertEqual(
            edgedb.Tuple([1, 2, 3]),
            (1, 2, 3))

        self.assertEqual(
            (1, 2, 3),
            edgedb.Tuple([1, 2, 3]))

        self.assertNotEqual(
            edgedb.Tuple([1, 2, 3]),
            (1, 3, 2))

        self.assertLess(
            edgedb.Tuple([1, 2, 3]),
            (1, 3, 2))

        self.assertEqual(
            edgedb.Tuple([]),
            ())

        self.assertEqual(
            edgedb.Tuple([1]),
            (1,))

        self.assertGreaterEqual(
            edgedb.Tuple([1]),
            (1,))

        self.assertNotEqual(
            edgedb.Tuple([1]),
            ())

        self.assertGreater(
            edgedb.Tuple([1]),
            ())

        self.assertNotEqual(
            edgedb.Tuple([1]),
            (2,))

        self.assertLess(
            edgedb.Tuple([1]),
            (2,))

        self.assertNotEqual(
            edgedb.Tuple([1, 2]),
            (2, 2))

        self.assertNotEqual(
            edgedb.Tuple([1, 1]),
            (2, 2, 1))

    def test_tuple_7(self):
        self.assertNotEqual(
            edgedb.Tuple([1, 2, 3]),
            123)


class TestNamedTuple(unittest.TestCase):

    def test_namedtuple_empty_1(self):
        with self.assertRaisesRegex(ValueError, 'at least one field'):
            edgedb.NamedTuple()

    def test_namedtuple_2(self):
        t = edgedb.NamedTuple(a=1)
        self.assertEqual(repr(t), "(a := 1)")

        t = edgedb.NamedTuple(a=1, b='a')

        self.assertEqual(set(dir(t)), {'a', 'b'})

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

    def test_namedtuple_6(self):
        self.assertEqual(
            edgedb.NamedTuple(a=1, b=2, c=3),
            (1, 2, 3))

        self.assertEqual(
            (1, 2, 3),
            edgedb.NamedTuple(a=1, b=2, c=3))

        self.assertNotEqual(
            edgedb.NamedTuple(a=1, b=2, c=3),
            (1, 3, 2))

        self.assertLess(
            edgedb.NamedTuple(a=1, b=2, c=3),
            (1, 3, 2))

        self.assertEqual(
            edgedb.NamedTuple(a=1),
            (1,))

        self.assertEqual(
            edgedb.NamedTuple(a=1),
            (1,))

    def test_namedtuple_7(self):
        self.assertNotEqual(
            edgedb.NamedTuple(a=1, b=2, c=3),
            1)

    def test_namedtuple_8(self):
        self.assertEqual(
            edgedb.NamedTuple(壹=1, 贰=2, 叄=3),
            (1, 2, 3))

    def test_namedtuple_memory(self):
        num = int(os.getenv("EDGEDB_PYTHON_TEST_NAMEDTUPLE_MEMORY", 100))

        def test():
            nt = []
            fix_tp = type(edgedb.NamedTuple(a=1, b=2))
            for _i in range(num):
                values = {}
                for _ in range(random.randint(9, 16)):
                    key = "".join(random.choices(string.ascii_letters, k=3))
                    value = random.randint(16384, 65536)
                    values[key] = value
                nt.append(edgedb.NamedTuple(**values))
                if random.random() > 0.5:
                    nt.append(
                        fix_tp(random.randint(10, 20), random.randint(20, 30))
                    )
                if len(nt) % random.randint(10, 20) == 0:
                    nt[:] = nt[random.randint(5, len(nt)):]

        gc.collect()
        gc.collect()
        gc.collect()
        gc_count = gc.get_count()
        test()
        gc.collect()
        gc.collect()
        gc.collect()
        self.assertEqual(gc.get_count(), gc_count)


class TestDerivedNamedTuple(unittest.TestCase):
    DerivedNamedTuple = type(edgedb.NamedTuple(a=1, b=2, c=3))

    def test_derived_namedtuple_1(self):
        self.assertEqual(
            (1, 2, 3),
            self.DerivedNamedTuple(a=1, b=2, c=3),
        )
        self.assertEqual(
            (1, 2, 3),
            self.DerivedNamedTuple(c=3, b=2, a=1),
        )
        self.assertEqual(
            (1, 2, 3),
            self.DerivedNamedTuple(1, c=3, b=2),
        )
        self.assertEqual(
            (1, 2, 3),
            self.DerivedNamedTuple(1, 2, 3),
        )

    def test_derived_namedtuple_2(self):
        with self.assertRaisesRegex(ValueError, "requires 3 arguments"):
            self.DerivedNamedTuple()

        with self.assertRaisesRegex(ValueError, "requires 3 arguments"):
            self.DerivedNamedTuple(1)

        with self.assertRaisesRegex(ValueError, "only needs 3 arguments"):
            self.DerivedNamedTuple(1, 2, 3, 4)

    def test_derived_namedtuple_3(self):
        with self.assertRaisesRegex(ValueError, "missing required argument"):
            self.DerivedNamedTuple(a=1)

        with self.assertRaisesRegex(ValueError, "missing required argument"):
            self.DerivedNamedTuple(b=2)

        with self.assertRaisesRegex(ValueError, "missing required argument"):
            self.DerivedNamedTuple(1, 2, d=4)

        with self.assertRaisesRegex(ValueError, "extra keyword arguments"):
            self.DerivedNamedTuple(1, 2, 3, d=4)

        with self.assertRaisesRegex(ValueError, "extra keyword arguments"):
            self.DerivedNamedTuple(1, 2, c=3, d=4)

    def test_derived_namedtuple_4(self):
        tp = type(edgedb.NamedTuple(x=42))
        tp(8)
        edgedb.NamedTuple(y=88)
        tp(16)
        tp_ref = weakref.ref(tp)
        gc.collect()
        self.assertIsNotNone(tp_ref())
        del tp
        gc.collect()
        self.assertIsNone(tp_ref())


class TestObject(unittest.TestCase):

    def test_object_1(self):
        f = private.create_object_factory(
            id='property',
            lb='link-property',
            c='property'
        )

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

        with self.assertRaises(TypeError):
            o['id']

        self.assertEqual(set(dir(o)), {'id', 'c'})

    def test_object_2(self):
        f = private.create_object_factory(
            id={'property', 'implicit'},
            lb='link-property',
            c='property'
        )

        o = f(1, 2, 3)

        self.assertEqual(repr(o), 'Object{@lb := 2, c := 3}')

        self.assertNotEqual(hash(o), hash(f(1, 2, 3)))
        self.assertNotEqual(hash(o), hash(f(1, 2, 'aaaa')))
        self.assertNotEqual(hash(o), hash((1, 2, 3)))

        self.assertEqual(set(dir(o)), {'id', 'c'})

    def test_object_3(self):
        f = private.create_object_factory(id='property', c='link')
        o = f(1, [])

        o.c.append(o)
        self.assertEqual(repr(o), 'Object{id := 1, c := [Object{...}]}')

    def test_object_4(self):
        f = private.create_object_factory(
            id={'property', 'implicit'},
            lb='link-property',
            c='property'
        )

        o1 = f(1, 'aa', 'ba')
        o2 = f(1, 'ab', 'bb')
        o3 = f(3, 'ac', 'bc')

        self.assertNotEqual(o1, o2)
        self.assertNotEqual(o1, o3)

    def test_object_5(self):
        f = private.create_object_factory(
            a='property',
            lb='link-property',
            c='property'
        )
        x = f(1, 2, 3)
        self.assertFalse(hasattr(x, 'id'))

    def test_object_6(self):
        User = private.create_object_factory(
            id='property',
            name='property',
        )

        u = User(1, 'user1')

        with self.assertRaisesRegex(TypeError,
                                    "property 'name' should be "
                                    "accessed via dot notation"):
            u['name']

    def test_object_links_1(self):
        O2 = private.create_object_factory(
            id='property',
            lb='link-property',
            c='property'
        )

        O1 = private.create_object_factory(
            id='property',
            o2s='link'
        )

        o2_1 = O2(1, 'linkprop o2 1', 3)
        o2_2 = O2(4, 'linkprop o2 2', 6)
        o1 = O1(2, edgedb.Set((o2_1, o2_2)))

        with self.assertRaisesRegex(TypeError,
                                    "link 'o2s' should be "
                                    "accessed via dot notation"):
            o1['o2s']

    def test_object_link_property_1(self):
        O2 = private.create_object_factory(
            id='property',
            lb='link-property',
            c='property'
        )

        O1 = private.create_object_factory(
            id='property',
            o2s='link'
        )

        o2_1 = O2(1, 'linkprop o2 1', 3)
        o2_2 = O2(4, 'linkprop o2 2', 6)
        o1 = O1(2, edgedb.Set((o2_1, o2_2)))

        o2s = o1.o2s
        self.assertEqual(len(o2s), 2)
        self.assertEqual(o2s, o1.o2s)
        self.assertEqual(
            repr(o2s),
            "[Object{id := 1, @lb := 'linkprop o2 1', c := 3},"
            " Object{id := 4, @lb := 'linkprop o2 2', c := 6}]"
        )

        self.assertEqual(o2s[0]['@lb'], 'linkprop o2 1')
        self.assertEqual(o2s[1]['@lb'], 'linkprop o2 2')
        self.assertEqual(getattr(o2s[0], '@lb'), 'linkprop o2 1')
        self.assertEqual(getattr(o2s[1], '@lb'), 'linkprop o2 2')

        with self.assertRaises(AttributeError):
            o2s[0].lb

        with self.assertRaises(AttributeError):
            getattr(o2s[0], "@lb2")

        with self.assertRaisesRegex(
            TypeError,
            "link property 'lb' should be accessed with '@' prefix",
        ):
            o2s[0]['lb']

        with self.assertRaisesRegex(
            TypeError, "property 'c' should be accessed via dot notation"
        ):
            o2s[0]['c']

        with self.assertRaisesRegex(
            KeyError, "link property '@c' does not exist"
        ):
            o2s[0]['@c']

    def test_object_dataclass_1(self):
        User = private.create_object_factory(
            id='property',
            name='property',
            tuple='property',
            namedtuple='property',
            linkprop="link-property",
        )

        u = User(
            1,
            'Bob',
            edgedb.Tuple((1, 2.0, '3')),
            edgedb.NamedTuple(a=1, b="Y"),
            123,
        )
        self.assertTrue(dataclasses.is_dataclass(u))
        self.assertEqual(
            dataclasses.asdict(u),
            {
                'id': 1,
                'name': 'Bob',
                'tuple': (1, 2.0, '3'),
                'namedtuple': (1, "Y"),
            },
        )


class TestSet(unittest.TestCase):

    def test_set_1(self):
        s = edgedb.Set(())
        self.assertEqual(repr(s), '[]')

        s = edgedb.Set((1, 2, [], 'a'))

        self.assertEqual(s[1], 2)
        self.assertEqual(s[2], [])
        self.assertEqual(len(s), 4)
        with self.assertRaises(IndexError):
            s[10]

    def test_set_2(self):
        s = edgedb.Set((1, 2, 3000, 'a'))

        self.assertEqual(repr(s), "[1, 2, 3000, 'a']")

    def test_set_3(self):
        s = edgedb.Set(())

        self.assertEqual(len(s), 0)

    def test_set_4(self):
        s = edgedb.Set(([],))
        s[0].append(s)
        self.assertEqual(repr(s), "[[[...]]]")

    def test_set_5(self):
        self.assertNotEqual(
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
        f = private.create_object_factory(
            id={'property', 'implicit'},
            lb='link-property',
            c='property'
        )

        o1 = f(1, 'aa', edgedb.Set([1, 2, 3]))
        o2 = f(1, 'ab', edgedb.Set([1, 2, 4]))
        o3 = f(3, 'ac', edgedb.Set([5, 5, 5, 5]))

        self.assertNotEqual(
            edgedb.Set([o1, o2, o3]),
            edgedb.Set([o2, o3, o1]))

        self.assertNotEqual(
            edgedb.Set([o1, o3]),
            edgedb.Set([o2, o3]))

        self.assertNotEqual(
            edgedb.Set([o1, o1]),
            edgedb.Set([o2, o3]))

    def test_set_7(self):
        self.assertEqual(
            edgedb.Set([1, 2, 3]),
            [1, 2, 3])

        self.assertNotEqual(
            edgedb.Set([1, 2, 3]),
            [3, 2, 1])

        self.assertNotEqual(
            edgedb.Set([1, 2, 3]),
            1)

    def test_set_8(self):
        s = edgedb.Set([1, 2, 3])
        si = iter(s)
        self.assertEqual(list(si), [1, 2, 3])


class TestArray(unittest.TestCase):

    def test_array_empty_1(self):
        t = edgedb.Array()
        self.assertEqual(len(t), 0)
        with self.assertRaisesRegex(IndexError, 'out of range'):
            t[0]
        self.assertEqual(repr(t), "[]")

    def test_array_2(self):
        t = edgedb.Array((1, 'a'))

        self.assertEqual(repr(t), "[1, 'a']")
        self.assertEqual(str(t), "[1, 'a']")

        self.assertEqual(len(t), 2)

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

    def test_array_5(self):
        self.assertEqual(
            edgedb.Array([1, 2, 3]),
            [1, 2, 3])

        self.assertEqual(
            [1, 2, 3],
            edgedb.Array([1, 2, 3]))

        self.assertNotEqual(
            [1, 2, 4],
            edgedb.Array([1, 2, 3]))

        self.assertNotEqual(
            edgedb.Array([1, 2, 3]),
            [1, 3, 2])

        self.assertLess(
            edgedb.Array([1, 2, 3]),
            [1, 3, 2])

        self.assertEqual(
            edgedb.Array([]),
            [])

        self.assertEqual(
            edgedb.Array([1]),
            [1])

        self.assertGreaterEqual(
            edgedb.Array([1]),
            [1])

        self.assertNotEqual(
            edgedb.Array([1]),
            [])

        self.assertGreater(
            edgedb.Array([1]),
            [])

        self.assertNotEqual(
            edgedb.Array([1]),
            [2])

        self.assertLess(
            edgedb.Array([1]),
            [2])

        self.assertNotEqual(
            edgedb.Array([1, 2]),
            [2, 2])

        self.assertNotEqual(
            edgedb.Array([1, 1]),
            [2, 2, 1])

    def test_array_6(self):
        self.assertNotEqual(
            edgedb.Array([1, 2, 3]),
            False)


class TestRange(unittest.TestCase):

    def test_range_empty_1(self):
        t = edgedb.Range(empty=True)
        self.assertEqual(t.lower, None)
        self.assertEqual(t.upper, None)
        self.assertFalse(t.inc_lower)
        self.assertFalse(t.inc_upper)
        self.assertTrue(t.is_empty())
        self.assertFalse(t)

        self.assertEqual(t, edgedb.Range(1, 1, empty=True))

        with self.assertRaisesRegex(ValueError, 'conflicting arguments'):
            edgedb.Range(1, 2, empty=True)

    def test_range_2(self):
        t = edgedb.Range(1, 2)
        self.assertEqual(repr(t), "<Range [1, 2]>")
        self.assertEqual(str(t), "<Range [1, 2]>")

        self.assertEqual(t.lower, 1)
        self.assertEqual(t.upper, 2)
        self.assertTrue(t.inc_lower)
        self.assertFalse(t.inc_upper)
        self.assertFalse(t.is_empty())
        self.assertTrue(t)

    def test_range_3(self):
        t = edgedb.Range(1)
        self.assertEqual(t.lower, 1)
        self.assertEqual(t.upper, None)
        self.assertTrue(t.inc_lower)
        self.assertFalse(t.inc_upper)
        self.assertFalse(t.is_empty())

        t = edgedb.Range(None, 1)
        self.assertEqual(t.lower, None)
        self.assertEqual(t.upper, 1)
        self.assertFalse(t.inc_lower)
        self.assertFalse(t.inc_upper)
        self.assertFalse(t.is_empty())

        t = edgedb.Range(None, None)
        self.assertEqual(t.lower, None)
        self.assertEqual(t.upper, None)
        self.assertFalse(t.inc_lower)
        self.assertFalse(t.inc_upper)
        self.assertFalse(t.is_empty())

    def test_range_4(self):
        for il in (False, True):
            for iu in (False, True):
                t = edgedb.Range(1, 2, inc_lower=il, inc_upper=iu)
                self.assertEqual(t.lower, 1)
                self.assertEqual(t.upper, 2)
                self.assertEqual(t.inc_lower, il)
                self.assertEqual(t.inc_upper, iu)
                self.assertFalse(t.is_empty())

    def test_range_5(self):
        # test hash
        self.assertEqual(
            {
                edgedb.Range(None, 2, inc_upper=True),
                edgedb.Range(1, 2),
                edgedb.Range(1, 2),
                edgedb.Range(1, 2),
                edgedb.Range(None, 2, inc_upper=True),
            },
            {
                edgedb.Range(1, 2),
                edgedb.Range(None, 2, inc_upper=True),
            }
        )


class TestMultiRange(unittest.TestCase):

    def test_multirange_empty_1(self):
        t = edgedb.MultiRange()
        self.assertEqual(len(t), 0)
        self.assertEqual(t, edgedb.MultiRange([]))

    def test_multirange_2(self):
        t = edgedb.MultiRange([
            edgedb.Range(1, 2),
            edgedb.Range(4),
        ])
        self.assertEqual(
            repr(t), "<MultiRange [<Range [1, 2]>, <Range [4, ]>]>")
        self.assertEqual(
            str(t), "<MultiRange [<Range [1, 2]>, <Range [4, ]>]>")

        self.assertEqual(
            t,
            edgedb.MultiRange([
                edgedb.Range(1, 2),
                edgedb.Range(4),
            ])
        )

    def test_multirange_3(self):
        ranges = [
            edgedb.Range(None, 0),
            edgedb.Range(1, 2),
            edgedb.Range(4),
        ]
        t = edgedb.MultiRange([
            edgedb.Range(None, 0),
            edgedb.Range(1, 2),
            edgedb.Range(4),
        ])

        for el, r in zip(t, ranges):
            self.assertEqual(el, r)

    def test_multirange_4(self):
        # test hash
        self.assertEqual(
            {
                edgedb.MultiRange([
                    edgedb.Range(1, 2),
                    edgedb.Range(4),
                ]),
                edgedb.MultiRange([edgedb.Range(None, 2, inc_upper=True)]),
                edgedb.MultiRange([
                    edgedb.Range(1, 2),
                    edgedb.Range(4),
                ]),
                edgedb.MultiRange([
                    edgedb.Range(1, 2),
                    edgedb.Range(4),
                ]),
                edgedb.MultiRange([edgedb.Range(None, 2, inc_upper=True)]),
            },
            {
                edgedb.MultiRange([edgedb.Range(None, 2, inc_upper=True)]),
                edgedb.MultiRange([
                    edgedb.Range(1, 2),
                    edgedb.Range(4),
                ]),
            }
        )

    def test_multirange_5(self):
        # test hash
        self.assertEqual(
            edgedb.MultiRange([
                edgedb.Range(None, 2, inc_upper=True),
                edgedb.Range(5, 9),
                edgedb.Range(5, 9),
                edgedb.Range(5, 9),
                edgedb.Range(None, 2, inc_upper=True),
            ]),
            edgedb.MultiRange([
                edgedb.Range(5, 9),
                edgedb.Range(None, 2, inc_upper=True),
            ]),
        )
