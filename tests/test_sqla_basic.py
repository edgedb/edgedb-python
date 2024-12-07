#
# This source file is part of the EdgeDB open source project.
#
# Copyright 2024-present MagicStack Inc. and the EdgeDB authors.
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

import os
import uuid

from sqlalchemy import create_engine, select
from sqlalchemy.orm import Session

from gel import _testbase as tb


class TestSQLABasic(tb.SQLATestCase):
    SCHEMA = os.path.join(os.path.dirname(__file__), 'dbsetup',
                          'base.esdl')

    SETUP = os.path.join(os.path.dirname(__file__), 'dbsetup',
                         'base.edgeql')

    SQLAPACKAGE = 'basemodels'

    @classmethod
    def setUpClass(cls):
        super().setUpClass()
        cls.engine = create_engine(cls.get_dsn_for_sqla())
        cls.sess = Session(cls.engine, autobegin=False)

        from basemodels import default
        cls.sm = default

    def setUp(self):
        super().setUp()

        if self.client.query_required_single('''
            select sys::get_version().major < 6
        '''):
            self.skipTest("Test needs SQL DML queries")

        self.sess.begin()

    def tearDown(self):
        super().tearDown()
        self.sess.rollback()

    def test_sqla_read_models_01(self):
        vals = {r.name for r in self.sess.query(self.sm.User).all()}
        self.assertEqual(
            vals, {'Alice', 'Billie', 'Cameron', 'Dana', 'Elsa', 'Zoe'})

        vals = {r.name for r in self.sess.query(self.sm.UserGroup).all()}
        self.assertEqual(
            vals, {'red', 'green', 'blue'})

        vals = {r.num for r in self.sess.query(self.sm.GameSession).all()}
        self.assertEqual(vals, {123, 456})

        vals = {r.body for r in self.sess.query(self.sm.Post).all()}
        self.assertEqual(
            vals, {'Hello', "I'm Alice", "I'm Cameron", '*magic stuff*'})

        # Read from the abstract type
        vals = {r.name for r in self.sess.query(self.sm.Named).all()}
        self.assertEqual(
            vals,
            {
                'Alice', 'Billie', 'Cameron', 'Dana', 'Elsa', 'Zoe',
                'red', 'green', 'blue',
            }
        )

    def test_sqla_read_models_02(self):
        # test single link and the one-to-many backlink
        # using load-on-demand

        res = self.sess.query(self.sm.Post).all()
        vals = {(p.author.name, p.body) for p in res}
        self.assertEqual(
            vals,
            {
                ('Alice', 'Hello'),
                ('Alice', "I'm Alice"),
                ('Cameron', "I'm Cameron"),
                ('Elsa', '*magic stuff*'),
            }
        )

        # use backlink
        res = self.sess.query(self.sm.User).order_by(self.sm.User.name).all()
        vals = [
            (u.name, {p.body for p in u.backlink_via_author})
            for u in res
        ]
        self.assertEqual(
            vals,
            [
                ('Alice', {'Hello', "I'm Alice"}),
                ('Billie', set()),
                ('Cameron', {"I'm Cameron"}),
                ('Dana', set()),
                ('Elsa', {'*magic stuff*'}),
                ('Zoe', set()),
            ]
        )

    def test_sqla_read_models_03(self):
        # test single link and the one-to-many backlink

        res = self.sess.execute(
            select(self.sm.Post, self.sm.User)
            .join(self.sm.Post.author)
        )
        vals = {(p.author.name, p.body) for (p, _) in res}
        self.assertEqual(
            vals,
            {
                ('Alice', 'Hello'),
                ('Alice', "I'm Alice"),
                ('Cameron', "I'm Cameron"),
                ('Elsa', '*magic stuff*'),
            }
        )

        # join via backlink
        res = self.sess.execute(
            select(self.sm.Post, self.sm.User)
            .join(self.sm.User.backlink_via_author)
            .order_by(self.sm.Post.body)
        )
        # We'll get a cross-product, so we need to jump through some hoops to
        # remove duplicates
        vals = {
            (u.name, tuple(p.body for p in u.backlink_via_author))
            for (_, u) in res
        }
        self.assertEqual(
            vals,
            {
                ('Alice', ('Hello', "I'm Alice")),
                ('Cameron', ("I'm Cameron",)),
                ('Elsa', ('*magic stuff*',)),
            }
        )

        # LEFT OUTER join via backlink
        res = self.sess.execute(
            select(self.sm.Post, self.sm.User)
            .join(self.sm.User.backlink_via_author, isouter=True)
            .order_by(self.sm.Post.body)
        )
        vals = {
            (u.name, tuple(p.body for p in u.backlink_via_author))
            for (p, u) in res
        }
        self.assertEqual(
            vals,
            {
                ('Alice', ('Hello', "I'm Alice")),
                ('Billie', ()),
                ('Cameron', ("I'm Cameron",)),
                ('Dana', ()),
                ('Elsa', ('*magic stuff*',)),
                ('Zoe', ()),
            }
        )

    def test_sqla_read_models_04(self):
        # test exclusive multi link and its backlink
        # using load-on-demand

        res = self.sess.query(
            self.sm.GameSession
        ).order_by(self.sm.GameSession.num).all()

        vals = [(g.num, {u.name for u in g.players}) for g in res]
        self.assertEqual(
            vals,
            [
                (123, {'Alice', 'Billie'}),
                (456, {'Dana'}),
            ]
        )

        # use backlink
        res = self.sess.query(self.sm.User).all()
        vals = {
            (u.name, tuple(g.num for g in u.backlink_via_players))
            for u in res
        }
        self.assertEqual(
            vals,
            {
                ('Alice', (123,)),
                ('Billie', (123,)),
                ('Cameron', ()),
                ('Dana', (456,)),
                ('Elsa', ()),
                ('Zoe', ()),
            }
        )

    def test_sqla_read_models_05(self):
        # test exclusive multi link and its backlink

        res = self.sess.execute(
            select(self.sm.GameSession, self.sm.User)
            .join(self.sm.GameSession.players)
        )
        # We'll get a cross-product, so we need to jump through some hoops to
        # remove duplicates
        vals = {
            (g.num, tuple(sorted(u.name for u in g.players)))
            for (g, _) in res
        }
        self.assertEqual(
            vals,
            {
                (123, ('Alice', 'Billie')),
                (456, ('Dana',)),
            }
        )

        # LEFT OUTER join via backlink
        res = self.sess.execute(
            select(self.sm.GameSession, self.sm.User)
            .join(self.sm.User.backlink_via_players, isouter=True)
        )
        vals = {
            (u.name, tuple(g.num for g in u.backlink_via_players))
            for (_, u) in res
        }
        self.assertEqual(
            vals,
            {
                ('Alice', (123,)),
                ('Billie', (123,)),
                ('Cameron', ()),
                ('Dana', (456,)),
                ('Elsa', ()),
                ('Zoe', ()),
            }
        )

    def test_sqla_read_models_06(self):
        # test multi link and its backlink
        # using load-on-demand

        res = self.sess.query(
            self.sm.UserGroup
        ).order_by(self.sm.UserGroup.name).all()

        vals = [(g.name, {u.name for u in g.users}) for g in res]
        self.assertEqual(
            vals,
            [
                ('blue', set()),
                ('green', {'Alice', 'Billie'}),
                ('red', {'Alice', 'Billie', 'Cameron', 'Dana', 'Elsa'}),
            ]
        )

        # use backlink
        res = self.sess.query(self.sm.User).order_by(self.sm.User.name).all()
        vals = [
            (u.name, {g.name for g in u.backlink_via_users})
            for u in res
        ]
        self.assertEqual(
            vals,
            [
                ('Alice', {'red', 'green'}),
                ('Billie', {'red', 'green'}),
                ('Cameron', {'red'}),
                ('Dana', {'red'}),
                ('Elsa', {'red'}),
                ('Zoe', set()),
            ]
        )

    def test_sqla_read_models_07(self):
        # test exclusive multi link and its backlink

        res = self.sess.execute(
            select(self.sm.UserGroup, self.sm.User)
            .join(self.sm.UserGroup.users, isouter=True)
            .order_by(self.sm.UserGroup.name)
        )
        # We'll get a cross-product, so we need to jump through some hoops to
        # remove duplicates
        vals = {
            (g.name, tuple(sorted(u.name for u in g.users)))
            for (g, _) in res
        }
        self.assertEqual(
            vals,
            {
                ('blue', ()),
                ('green', ('Alice', 'Billie')),
                ('red', ('Alice', 'Billie', 'Cameron', 'Dana', 'Elsa')),
            }
        )

        # LEFT OUTER join via backlink
        res = self.sess.execute(
            select(self.sm.UserGroup, self.sm.User)
            .join(self.sm.User.backlink_via_users, isouter=True)
        )
        vals = {
            (u.name, tuple(sorted(g.name for g in u.backlink_via_users)))
            for (_, u) in res
        }
        self.assertEqual(
            vals,
            {
                ('Alice', ('green', 'red')),
                ('Billie', ('green', 'red')),
                ('Cameron', ('red',)),
                ('Dana', ('red',)),
                ('Elsa', ('red',)),
                ('Zoe', ()),
            }
        )

    def test_sqla_create_models_01(self):
        vals = self.sess.query(self.sm.User).filter_by(name='Yvonne').all()
        self.assertEqual(list(vals), [])

        self.sess.add(self.sm.User(name='Yvonne'))
        self.sess.flush()

        user = self.sess.query(self.sm.User).filter_by(name='Yvonne').one()
        self.assertEqual(user.name, 'Yvonne')
        self.assertIsInstance(user.id, uuid.UUID)

    def test_sqla_create_models_02(self):
        cyan = self.sm.UserGroup(
            name='cyan',
            users=[
                self.sm.User(name='Yvonne'),
                self.sm.User(name='Xander'),
            ],
        )

        self.sess.add(cyan)
        self.sess.flush()

        for name in ['Yvonne', 'Xander']:
            user = self.sess.query(self.sm.User).filter_by(name=name).one()

            self.assertEqual(user.name, name)
            self.assertEqual(user.backlink_via_users[0].name, 'cyan')
            self.assertIsInstance(user.id, uuid.UUID)

    def test_sqla_create_models_03(self):
        user0 = self.sm.User(name='Yvonne')
        user1 = self.sm.User(name='Xander')
        cyan = self.sm.UserGroup(name='cyan')

        user0.backlink_via_users.append(cyan)
        user1.backlink_via_users.append(cyan)

        self.sess.add(cyan)
        self.sess.flush()

        for name in ['Yvonne', 'Xander']:
            user = self.sess.query(self.sm.User).filter_by(name=name).one()

            self.assertEqual(user.name, name)
            self.assertEqual(user.backlink_via_users[0].name, 'cyan')
            self.assertIsInstance(user.id, uuid.UUID)

    def test_sqla_create_models_04(self):
        user = self.sm.User(name='Yvonne')
        self.sm.Post(body='this is a test', author=user)
        self.sm.Post(body='also a test', author=user)

        self.sess.add(user)
        self.sess.flush()

        res = self.sess.execute(
            select(self.sm.Post)
            .join(self.sm.Post.author)
            .where(self.sm.User.name == 'Yvonne')
        )
        self.assertEqual(
            {p.body for (p,) in res},
            {'this is a test', 'also a test'},
        )

    def test_sqla_delete_models_01(self):
        user = self.sess.query(self.sm.User).filter_by(name='Zoe').one()
        self.assertEqual(user.name, 'Zoe')
        self.assertIsInstance(user.id, uuid.UUID)

        self.sess.delete(user)
        self.sess.flush()

        vals = self.sess.query(self.sm.User).filter_by(name='Zoe').all()
        self.assertEqual(list(vals), [])

    def test_sqla_delete_models_02(self):
        post = (
            self.sess.query(self.sm.Post)
            .join(self.sm.Post.author)
            .filter(self.sm.User.name == 'Elsa')
            .one()
        )
        user_id = post.author.id

        self.sess.delete(post)
        self.sess.flush()

        vals = (
            self.sess.query(self.sm.Post)
            .join(self.sm.Post.author)
            .filter(self.sm.User.name == 'Elsa')
            .all()
        )
        self.assertEqual(list(vals), [])

        user = self.sess.get(self.sm.User, user_id)
        self.assertEqual(user.name, 'Elsa')

    def test_sqla_delete_models_03(self):
        post = (
            self.sess.query(self.sm.Post)
            .join(self.sm.Post.author)
            .filter(self.sm.User.name == 'Elsa')
            .one()
        )
        user = post.author

        self.sess.delete(post)
        self.sess.delete(user)
        self.sess.flush()

        vals = (
            self.sess.query(self.sm.Post)
            .join(self.sm.Post.author)
            .filter(self.sm.User.name == 'Elsa')
            .all()
        )
        self.assertEqual(list(vals), [])

        vals = self.sess.query(self.sm.User).filter_by(name='Elsa').all()
        self.assertEqual(list(vals), [])

    def test_sqla_delete_models_04(self):
        group = self.sess.query(
            self.sm.UserGroup).filter_by(name='green').one()
        names = {u.name for u in group.users}

        self.sess.delete(group)
        self.sess.flush()

        vals = self.sess.query(
            self.sm.UserGroup).filter_by(name='green').all()
        self.assertEqual(list(vals), [])

        users = self.sess.query(self.sm.User).all()
        for name in names:
            self.assertIn(name, {u.name for u in users})

    def test_sqla_delete_models_05(self):
        group = self.sess.query(
            self.sm.UserGroup).filter_by(name='green').one()
        for u in group.users:
            if u.name == 'Billie':
                user = u
                break

        self.sess.delete(group)
        self.sess.delete(user)
        self.sess.flush()

        vals = self.sess.query(
            self.sm.UserGroup).filter_by(name='green').all()
        self.assertEqual(list(vals), [])

        users = self.sess.query(self.sm.User).all()
        self.assertNotIn('Billie', {u.name for u in users})

    def test_sqla_update_models_01(self):
        user = self.sess.query(self.sm.User).filter_by(name='Alice').one()
        self.assertEqual(user.name, 'Alice')
        self.assertIsInstance(user.id, uuid.UUID)

        user.name = 'Xander'
        self.sess.add(user)
        self.sess.flush()

        vals = self.sess.query(self.sm.User).filter_by(name='Alice').all()
        self.assertEqual(list(vals), [])
        other = self.sess.query(self.sm.User).filter_by(name='Xander').one()
        self.assertEqual(user, other)

    def test_sqla_update_models_02(self):
        red = self.sess.query(self.sm.UserGroup).filter_by(name='red').one()
        blue = self.sess.query(self.sm.UserGroup).filter_by(name='blue').one()
        user = self.sm.User(name='Yvonne')

        self.sess.add(user)
        red.users.append(user)
        blue.users.append(user)
        self.sess.flush()

        self.assertEqual(
            {g.name for g in user.backlink_via_users},
            {'red', 'blue'},
        )
        self.assertEqual(user.name, 'Yvonne')
        self.assertIsInstance(user.id, uuid.UUID)

        group = [g for g in user.backlink_via_users if g.name == 'red'][0]
        self.assertEqual(
            {u.name for u in group.users},
            {'Alice', 'Billie', 'Cameron', 'Dana', 'Elsa', 'Yvonne'},
        )

    def test_sqla_update_models_03(self):
        user0 = self.sess.query(self.sm.User).filter_by(name='Elsa').one()
        user1 = self.sess.query(self.sm.User).filter_by(name='Zoe').one()
        # Replace the author or a post
        post = user0.backlink_via_author[0]
        body = post.body
        post.author = user1

        self.sess.add(post)
        self.sess.flush()

        res = self.sess.execute(
            select(self.sm.Post)
            .join(self.sm.Post.author)
            .where(self.sm.User.name == 'Zoe')
        )
        self.assertEqual(
            {p.body for (p,) in res},
            {body},
        )

    def test_sqla_update_models_04(self):
        user = self.sess.query(self.sm.User).filter_by(name='Zoe').one()
        post = (
            self.sess.query(self.sm.Post)
            .join(self.sm.Post.author)
            .filter(self.sm.User.name == 'Elsa')
            .one()
        )
        # Replace the author or a post
        post_id = post.id
        post.author = user

        self.sess.add(post)
        self.sess.flush()

        post = self.sess.get(self.sm.Post, post_id)
        self.assertEqual(post.author.name, 'Zoe')
