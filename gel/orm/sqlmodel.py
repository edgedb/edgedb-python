import pathlib
import re
import warnings

from contextlib import contextmanager

from .introspection import get_sql_name, get_mod_and_name
from .introspection import GelORMWarning, FilePrinter


GEL_SCALAR_MAP = {
    'std::bool': 'bool',
    'std::str': 'str',
    'std::int16': 'int',
    'std::int32': 'int',
    'std::int64': 'int',
    'std::float32': 'float',
    'std::float64': 'float',
    'std::uuid': 'uuid.UUID',
}

CLEAN_RE = re.compile(r'[^A-Za-z0-9]+')

COMMENT = '''\
#
# Automatically generated from Gel schema.
#\
'''

MODELS_STUB = f'''\
{COMMENT}

import uuid

from sqlmodel import SQLModel, Field, Relationship
from sqlalchemy import Column, ForeignKey
'''


class ModelGenerator(FilePrinter):
    def __init__(self, *, outdir=None, basemodule=None):
        # set the output to be stdout by default, but this is generally
        # expected to be overridden by appropriate files in the `outdir`
        if outdir is not None:
            self.outdir = pathlib.Path(outdir)
        else:
            self.outdir = None

        self.basemodule = basemodule
        super().__init__()

    def init_dir(self, dirpath):
        if not dirpath:
            # nothing to initialize
            return

        path = pathlib.Path(dirpath).resolve()

        # ensure `path` directory exists
        if not path.exists():
            path.mkdir()
        elif not path.is_dir():
            raise NotADirectoryError(
                f'{path!r} exists, but it is not a directory')

        # ensure `path` directory contains `__init__.py`
        (path / '__init__.py').touch()

    @contextmanager
    def init_module(self, mod, modules):
        if any(m.startswith(f'{mod}::') for m in modules):
            # This is a prefix in another module, thus it is part of a nested
            # module structure.
            dirpath = mod.split('::')
            filename = '__init__.py'
        else:
            # This is a leaf module, so we just need to create a corresponding
            # <mod>.py file.
            *dirpath, filename = mod.split('::')
            filename = f'{filename}.py'

        # Along the dirpath we need to ensure that all packages are created
        path = self.outdir
        for el in dirpath:
            path = path / el
            self.init_dir(path)

        with open(path / filename, 'wt') as f:
            try:
                self.out = f
                self.write(f'{COMMENT}\n')
                relimport = '.' * len(dirpath)
                self.write(f'from {relimport}._tables import *')
                yield f
            finally:
                self.out = None

    def get_fk(self, mod, table, curmod):
        if mod == curmod:
            # No need for anything fancy within the same schema
            return f'foreign_key="{table}.id"'
        else:
            return f'foreign_key="{mod}.{table}.id"'

    def get_sqla_fk(self, mod, table, curmod):
        if mod == curmod:
            # No need for anything fancy within the same schema
            return f'ForeignKey("{table}.id")'
        else:
            return f'ForeignKey("{mod}.{table}.id")'

    def get_py_name(self, mod, name, curmod):
        if mod == curmod:
            # No need for anything fancy within the same module
            return f"'{name}'"
        else:
            mod = mod.replace('::', '.')
            return f"'{self.basemodule}.{mod}.{name}'"

    def spec_to_modules_dict(self, spec):
        modules = {
            mod: {} for mod in sorted(spec['modules'])
        }

        if len(spec['prop_objects']) > 0:
            warnings.warn(
                f"Skipping multi properties: SQLAlchemy reflection doesn't "
                f"support multi properties as they produce models without a "
                f"clear identity.",
                GelORMWarning,
            )

        for rec in spec['link_tables']:
            mod = rec['module']
            if 'link_tables' not in modules[mod]:
                modules[mod]['link_tables'] = []
            modules[mod]['link_tables'].append(rec)

        for lobj in spec['link_objects']:
            mod = lobj['module']
            if 'link_objects' not in modules[mod]:
                modules[mod]['link_objects'] = {}
            modules[mod]['link_objects'][lobj['name']] = lobj

        for rec in spec['object_types']:
            mod, name = get_mod_and_name(rec['name'])
            if 'object_types' not in modules[mod]:
                modules[mod]['object_types'] = {}
            modules[mod]['object_types'][name] = rec

        return modules

    def render_models(self, spec):
        # The modules dict will be populated with the respective types, link
        # tables, etc., since they will need to be put in their own files. We
        # sort the modules so that nested modules are initialized from root to
        # leaf.
        modules = self.spec_to_modules_dict(spec)

        # Initialize the base directory
        self.init_dir(self.outdir)

        with open(self.outdir / '_tables.py', 'wt') as f:
            self.out = f
            self.write(MODELS_STUB)

            for rec in spec['link_tables']:
                self.write()
                self.render_link_table(rec)

        if 'default' not in modules or len(modules) > 1:
            skipped = ', '.join([repr(m) for m in modules if m != 'default'])
            warnings.warn(
                f"Skipping modules {skipped}: SQLModel reflection doesn't "
                f"support multiple modules or non-default modules.",
                GelORMWarning,
            )

        with self.init_module('default', modules):
            maps = modules['default']
            if not maps:
                # skip apparently empty modules
                return

            for lobj in maps.get('link_objects', {}).values():
                self.write()
                self.render_link_object(lobj, modules)

            for rec in maps.get('object_types', {}).values():
                self.write()
                self.render_type(rec, modules)

    def render_link_table(self, spec):
        mod, source = get_mod_and_name(spec["source"])
        tmod, target = get_mod_and_name(spec["target"])
        s_fk = self.get_fk(mod, source, 'default')
        t_fk = self.get_fk(tmod, target, 'default')

        if mod != 'default' or tmod != 'default':
            skipped = ', '.join(
                [repr(m) for m in {mod, tmod} if m != 'default'])
            warnings.warn(
                f"Skipping modules {skipped}: SQLModel reflection doesn't "
                f"support multiple modules or non-default modules.",
                GelORMWarning,
            )
            return

        self.write()
        self.write(f'class {spec["name"]}(SQLModel, table=True):')
        self.indent()
        self.write(f'__tablename__ = {spec["table"]!r}')
        if mod != 'default':
            self.write(f'__table_args__ = {{"schema": {mod!r}}}')
        # We rely on Gel for maintaining integrity and various on delete
        # triggers, so the rows may be deleted in a different way from what
        # SQLAlchemy expects.
        self.write('__mapper_args__ = {"confirm_deleted_rows": False}')
        self.write()
        # source is in the same module as this table
        self.write(f'source: uuid.UUID = Field({s_fk}, primary_key=True)')
        self.write(f'target: uuid.UUID = Field({t_fk}, primary_key=True)')
        self.dedent()

    def render_link_object(self, spec, modules):
        mod = spec['module']
        name = spec['name']
        sql_name = spec['table']
        source_name, source_link = sql_name.split('.')

        if mod != 'default':
            warnings.warn(
                f"Skipping module {mod!r}: SQLModel reflection doesn't "
                f"support multiple modules or non-default modules.",
                GelORMWarning,
            )
            return

        self.write()
        self.write(f'class {name}(SQLModel, table=True):')
        self.indent()
        self.write(f'__tablename__ = {sql_name!r}')
        if mod != 'default':
            self.write(f'__table_args__ = {{"schema": {mod!r}}}')
        # We rely on Gel for maintaining integrity and various on delete
        # triggers, so the rows may be deleted in a different way from what
        # SQLAlchemy expects.
        self.write('__mapper_args__ = {"confirm_deleted_rows": False}')
        self.write()

        # No ids for these intermediate objects
        if spec['links']:
            self.write()
            self.write('# Links:')

            for link in spec['links']:
                lname = link['name']
                tmod, target = get_mod_and_name(link['target']['name'])

                if tmod != 'default':
                    warnings.warn(
                        f"Skipping module {tmod!r}: SQLModel reflection doesn't "
                        f"support multiple modules or non-default modules.",
                        GelORMWarning,
                    )
                    return

                fk = self.get_fk(tmod, target, mod)
                sqlafk = self.get_sqla_fk(tmod, target, mod)
                pyname = self.get_py_name(tmod, target, mod)
                self.write(f'{lname}_id: uuid.UUID = Field(sa_column=Column(')
                self.indent()
                self.write(f'{lname!r},')
                self.write(f'{sqlafk},')
                self.write(f'primary_key=True,')
                self.write(f'nullable=False,')
                self.dedent()
                self.write('))')

                if lname == 'source':
                    bklink = source_link
                else:
                    src = modules[mod]['object_types'][source_name]
                    bklink = src['backlink_renames'].get(
                        source_link,
                        f'back_to_{source_name}',
                    )

                self.write(
                    f'{lname}: {pyname} = '
                    f'Relationship(back_populates={bklink!r})'
                )

        if spec['properties']:
            self.write()
            self.write('# Properties:')

            for prop in spec['properties']:
                if prop['name'] != 'id':
                    self.render_prop(prop, mod, name, {})

        self.dedent()

    def render_type(self, spec, modules):
        # assume nice names for now
        mod, name = get_mod_and_name(spec['name'])
        sql_name = get_sql_name(spec['name'])

        if mod != 'default':
            warnings.warn(
                f"Skipping module {mod!r}: SQLModel reflection doesn't "
                f"support multiple modules or non-default modules.",
                GelORMWarning,
            )
            return

        self.write()
        self.write(f'class {name}(SQLModel, table=True):')
        self.indent()
        self.write(f'__tablename__ = {sql_name!r}')
        if mod != 'default':
            self.write(f'__table_args__ = {{"schema": {mod!r}}}')
        # We rely on Gel for maintaining integrity and various on delete
        # triggers, so the rows may be deleted in a different way from what
        # SQLAlchemy expects.
        self.write('__mapper_args__ = {"confirm_deleted_rows": False}')
        self.write()

        # Add two fields that all objects have
        self.write(f'id: uuid.UUID | None = Field(')
        self.indent()
        self.write(
            f"default=None, primary_key=True,")
        self.write(
            f"sa_column_kwargs=dict(server_default='uuid_generate_v4()'),")
        self.dedent()
        self.write(')')

        # This is maintained entirely by Gel, the server_default simply
        # indicates to SQLAlchemy that this value may be omitted.
        self.write(f'gel_type_id: uuid.UUID | None = Field(')
        self.indent()
        self.write(
            f"default=None,")
        self.write(
            f"sa_column=Column('__type__', server_default='PLACEHOLDER'),")
        self.dedent()
        self.write(')')

        if spec['properties']:
            self.write()
            self.write('# Properties:')

            for prop in spec['properties']:
                if prop['name'] != 'id':
                    self.render_prop(prop, mod, name, modules)

        if spec['links']:
            self.write()
            self.write('# Links:')

            for link in spec['links']:
                self.render_link(link, mod, name, modules)

        if spec['backlinks']:
            self.write()
            self.write('# Back-links:')

            for link in spec['backlinks']:
                self.render_backlink(link, mod, modules)

        self.dedent()

    def render_prop(self, spec, mod, parent, modules, *, is_pk=False):
        name = spec['name']
        nullable = not spec['required']
        cardinality = spec['cardinality']

        target = spec['target']['name']
        try:
            pytype = GEL_SCALAR_MAP[target]
        except KeyError:
            warnings.warn(
                f'Scalar type {target} is not supported',
                GelORMWarning,
            )
            # Skip rendering this one
            return

        if is_pk:
            # special case of a primary key property (should only happen to
            # 'target' in multi property table)
            self.write(
                f'{name}: {pytype} = Field(primary_key=True, nullable=False)'
            )
        elif cardinality == 'Many':
            # skip it
            return

        else:
            # plain property
            self.write(
                f'{name}: {pytype} = Field(nullable={nullable})'
            )

    def render_link(self, spec, mod, parent, modules):
        name = spec['name']
        nullable = not spec['required']
        tmod, target = get_mod_and_name(spec['target']['name'])
        source = modules[mod]['object_types'][parent]
        cardinality = spec['cardinality']
        bklink = source['backlink_renames'].get(name, f'back_to_{parent}')

        if tmod != 'default':
            warnings.warn(
                f"Skipping module {tmod!r}: SQLModel reflection doesn't "
                f"support multiple modules or non-default modules.",
                GelORMWarning,
            )
            return

        if spec.get('has_link_object'):
            # intermediate object will have the actual source and target
            # links, so the link here needs to be treated similar to a
            # back-link.
            linkobj = modules[mod]['link_objects'][f'{parent}_{name}_link']
            target = linkobj['name']
            tmod = linkobj['module']
            pyname = self.get_py_name(tmod, target, mod)

            if cardinality == 'One':
                self.write(
                    f'{name}: {pyname} = '
                    f"Relationship(back_populates='source')"
                )
            elif cardinality == 'Many':
                self.write(
                    f'{name}: list[{pyname}] = '
                    f"Relationship(back_populates='source')"
                )

            if cardinality == 'One':
                tmap = pyname
            elif cardinality == 'Many':
                tmap = f'list[{pyname}]'
            # We want the cascade to delete orphans here as the intermediate
            # objects represent links and must not exist without source.
            self.write(f'{name}: {tmap} = Relationship(')
            self.indent()
            self.write(f"back_populates='source',")
            self.write(f"cascade_delete=True,")
            self.dedent()
            self.write(')')

        else:
            fk = self.get_fk(tmod, target, mod)
            pyname = self.get_py_name(tmod, target, mod)

            if cardinality == 'One':
                self.write(
                    f'{name}_id: uuid.UUID = Field({fk}, nullable={nullable})'
                )
                self.write(
                    f'{name}: {pyname} = '
                    f'Relationship(back_populates={bklink!r})'
                )

            elif cardinality == 'Many':
                secondary = f'{parent}_{name}_table'
                self.write(
                    f'{name}: list[{pyname}] = Relationship('
                )
                self.indent()
                self.write(f'back_populates={bklink!r},')
                self.write(f'link_model={secondary},')
                self.dedent()
                self.write(')')

    def render_backlink(self, spec, mod, modules):
        name = spec['name']
        tmod, target = get_mod_and_name(spec['target']['name'])
        cardinality = spec['cardinality']
        exclusive = spec['exclusive']
        bklink = spec['fwname']

        if tmod != 'default':
            warnings.warn(
                f"Skipping module {tmod!r}: SQLModel reflection doesn't "
                f"support multiple modules or non-default modules.",
                GelORMWarning,
            )
            return

        if spec.get('has_link_object'):
            # intermediate object will have the actual source and target
            # links, so the link here needs to refer to the intermediate
            # object and 'target' as back-link.
            linkobj = modules[tmod]['link_objects'][f'{target}_{bklink}_link']
            target = linkobj['name']
            tmod = linkobj['module']
            pyname = self.get_py_name(tmod, target, mod)

            if cardinality == 'One':
                tmap = pyname
            elif cardinality == 'Many':
                tmap = f'list[{pyname}]'
            # We want the cascade to delete orphans here as the intermediate
            # objects represent links and must not exist without target.
            self.write(f'{name}: {tmap} = Relationship(')
            self.indent()
            self.write(f"back_populates='target',")
            self.write(f"cascade_delete=True,")
            self.dedent()
            self.write(')')

        else:
            pyname = self.get_py_name(tmod, target, mod)
            if exclusive:
                # This is a backlink from a single link. There is no link table
                # involved.
                if cardinality == 'One':
                    self.write(
                        f'{name}: {pyname} = '
                        f'Relationship(back_populates={bklink!r})'
                    )
                elif cardinality == 'Many':
                    self.write(
                        f'{name}: list[{pyname}] = '
                        f'Relationship(back_populates={bklink!r})'
                    )

            else:
                # This backlink involves a link table, so we still treat it as
                # a Many-to-Many.
                secondary = f'{target}_{bklink}_table'
                self.write(
                    f'{name}: list[{pyname}] = Relationship('
                )
                self.indent()
                self.write(f'back_populates={bklink!r},')
                self.write(f'link_model={secondary},')
                self.dedent()
                self.write(')')