import pathlib
import re

from ..introspection import get_mod_and_name, FilePrinter


GEL_SCALAR_MAP = {
    'std::uuid': 'UUIDField',
    'std::bigint': 'DecimalField',
    'std::bool': 'BooleanField',
    'std::bytes': 'BinaryField',
    'std::decimal': 'DecimalField',
    'std::float32': 'FloatField',
    'std::float64': 'FloatField',
    'std::int16': 'SmallIntegerField',
    'std::int32': 'IntegerField',
    'std::int64': 'BigIntegerField',
    'std::json': 'JSONField',
    'std::str': 'TextField',
    # Extreme caution is needed for datetime field, the TZ aware and naive
    # values are controlled in Django via settings (USE_TZ) and are mutually
    # exclusive in the same app under default circumstances.
    'std::datetime': 'DateTimeField',
    'cal::local_date': 'DateField',
    'cal::local_datetime': 'DateTimeField',
    'cal::local_time': 'TimeField',
    # all kinds of duration is not supported due to this error:
    # iso_8601 intervalstyle currently not supported
}

BASE_STUB = f'''\
#
# Automatically generated from Gel schema.
#
# This is based on the auto-generated Django model module, which has been
# updated to fit Gel schema more closely.
#

from django.db import models

class GelUUIDField(models.UUIDField):
    # This field must be treated as a auto-generated UUID.
    db_returning = True


class LTForeignKey(models.ForeignKey):
    # Linked tables need to return their source/target ForeignKeys.
    db_returning = True\
'''

GEL_META = f'''
class GelPGMeta:
    'This is a model reflected from Gel using Postgres protocol.'
'''

FK_RE = re.compile(r'''models\.ForeignKey\((.+?),''')
CLOSEPAR_RE = re.compile(r'\)(?=\s+#|$)')


class ModelClass(object):
    def __init__(self, name):
        self.name = name
        self.props = {}
        self.links = {}
        self.mlinks = {}
        self.meta = {'managed': False}
        self.backlinks = {}
        self.backlink_renames = {}

    @property
    def table(self):
        return self.meta['db_table'].strip("'")

    def get_backlink_name(self, name):
        return self.backlink_renames.get(name, f'backlink_via_{name}')


class ModelGenerator(FilePrinter):
    def __init__(self, *, out):
        super().__init__()
        # record the output file path
        self.outfile = pathlib.Path(out).resolve()

    def spec_to_modules_dict(self, spec):
        modules = {
            mod: {} for mod in sorted(spec['modules'])
        }

        for rec in spec['link_tables']:
            mod = rec['module']
            if 'link_tables' not in modules[mod]:
                modules[mod]['link_tables'] = {}
            modules[mod]['link_tables'][rec['table']] = rec

        for rec in spec['object_types']:
            mod, name = get_mod_and_name(rec['name'])
            if 'object_types' not in modules[mod]:
                modules[mod]['object_types'] = {}
            modules[mod]['object_types'][name] = rec

        return modules['default']

    def replace_foreignkey(self, fval, origtarget, newtarget, bkname=None):
        # Replace the reference with the string quoted
        # (because we don't check the order of definition)
        # name.
        fval = fval.replace(origtarget, repr(newtarget))

        if bkname:
            # Add a backlink reference
            fval = CLOSEPAR_RE.sub(f', related_name={bkname!r})', fval)

        return fval

    def build_models(self, maps):
        modmap = {}

        for name, rec in maps['object_types'].items():
            mod = ModelClass(name)
            mod.meta['db_table'] = repr(name)
            if 'backlink_renames' in rec:
                mod.backlink_renames = rec['backlink_renames']

            # copy backlink information
            for link in rec['backlinks']:
                mod.backlinks[link['name']] = link

            # process properties as fields
            for prop in rec['properties']:
                pname = prop['name']
                if pname == 'id':
                    continue

                mod.props[pname] = self.render_prop(prop)

            # process single links as fields
            for link in rec['links']:
                if link['cardinality'] != 'One':
                    # Multi links require link tables and are handled
                    # separately.
                    continue

                lname = link['name']
                bklink = mod.get_backlink_name(lname)
                mod.links[lname] = self.render_link(link, bklink)

            modmap[mod.name] = mod

        for table, rec in maps['link_tables'].items():
            source, fwname = table.split('.')
            mod = ModelClass(f'{source}{fwname.title()}')
            mod.meta['db_table'] = repr(table)
            mod.meta['unique_together'] = "(('source', 'target'),)"

            # Only have source and target
            _, target = get_mod_and_name(rec['target'])
            mod.links['source'] = (
                f"LTForeignKey({source!r}, models.DO_NOTHING, "
                f"db_column='source', primary_key=True)"
            )
            mod.links['target'] = (
                f"LTForeignKey({target!r}, models.DO_NOTHING, "
                f"db_column='target')"
            )

            # Update the source model with the corresponding
            # ManyToManyField.
            src = modmap[source]
            tgt = modmap[target]
            bkname = src.get_backlink_name(fwname)
            src.mlinks[fwname] = (
                f'models.ManyToManyField('
                f'{tgt.name!r}, '
                f'through={mod.name!r}, '
                f'through_fields=("source", "target"), '
                f'related_name={bkname!r})'
            )

            modmap[mod.name] = mod

        return modmap

    def render_prop(self, prop):
        if prop['required']:
            req = ''
        else:
            req = 'blank=True, null=True'

        target = prop['target']['name']
        try:
            ftype = GEL_SCALAR_MAP[target]
        except KeyError:
            raise RuntimeError(
                f'Scalar type {target} is not supported')

        return f'models.{ftype}({req})'

    def render_link(self, link, bklink=None):
        if link['required']:
            req = ''
        else:
            req = ', blank=True, null=True'

        _, target = get_mod_and_name(link['target']['name'])

        if bklink:
            bklink = f', related_name={bklink!r}'
        else:
            bklink = ''

        return (f'models.ForeignKey('
                f'{target!r}, models.DO_NOTHING{bklink}{req})')

    def render_models(self, spec):
        # Check that there is only "default" module
        mods = spec['modules']
        if mods[0] != 'default' or len(mods) > 1:
            raise RuntimeError(
                f"Django reflection doesn't support multiple modules or "
                f"non-default modules."
            )
        # Check that we don't have multiprops or link properties as they
        # produce models without `id` field and Django doesn't like that. It
        # causes Django to mistakenly use `source` as `id` and also attempt to
        # UPDATE `target` on link tables.
        if len(spec['prop_objects']) > 0:
            raise RuntimeError(
                f"Django reflection doesn't support multi properties as they "
                f"produce models without `id` field."
            )
        if len(spec['link_objects']) > 0:
            raise RuntimeError(
                f"Django reflection doesn't support link properties as they "
                f"produce models without `id` field."
            )

        maps = self.spec_to_modules_dict(spec)
        modmap = self.build_models(maps)

        with open(self.outfile, 'w+t') as f:
            self.out = f
            self.write(BASE_STUB)

            for mod in modmap.values():
                self.write()
                self.write()
                self.render_model_class(mod)

    def render_model_class(self, mod):
        self.write(f'class {mod.name}(models.Model):')
        self.indent()

        if '.' not in mod.table:
            # This is only valid for regular objects, not link tables.
            self.write(f"id = GelUUIDField(primary_key=True)")
            self.write(f"gel_type_id = models.UUIDField(db_column='__type__')")

        if mod.props:
            self.write()
            self.write(f'# properties as Fields')
            for name, val in mod.props.items():
                self.write(f'{name} = {val}')

        if mod.links:
            self.write()
            self.write(f'# links as ForeignKeys')
            for name, val in mod.links.items():
                self.write(f'{name} = {val}')

        if mod.mlinks:
            self.write()
            self.write(f'# multi links as ManyToManyFields')
            for name, val in mod.mlinks.items():
                self.write(f'{name} = {val}')

        if '.' not in mod.table:
            self.write(GEL_META)

        self.write('class Meta:')
        self.indent()
        for name, val in mod.meta.items():
            self.write(f'{name} = {val}')
        self.dedent()

        self.dedent()