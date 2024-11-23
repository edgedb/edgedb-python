import json
import re


INTRO_QUERY = '''
with module schema
select ObjectType {
    name,
    links: {
        name,
        readonly,
        required,
        cardinality,
        exclusive := exists (
            select .constraints
            filter .name = 'std::exclusive'
        ),
        target: {name},

        properties: {
            name,
            readonly,
            required,
            cardinality,
            exclusive := exists (
                select .constraints
                filter .name = 'std::exclusive'
            ),
            target: {name},
        },
    } filter .name != '__type__',
    properties: {
        name,
        readonly,
        required,
        cardinality,
        exclusive := exists (
            select .constraints
            filter .name = 'std::exclusive'
        ),
        target: {name},
    },
    backlinks := <array<str>>[],
}
filter
    not .builtin
    and
    not .internal
    and
    not re_test('^(std|cfg|sys|schema)::', .name);
'''

MODULE_QUERY = '''
with
    module schema,
    m := (select `Module` filter not .builtin)
select m.name;
'''

CLEAN_NAME = re.compile(r'^[A-Za-z_][A-Za-z0-9_]*$')


def get_sql_name(name):
    # Just remove the module name
    name = name.rsplit('::', 1)[-1]

    return name


def check_name(name):
    # Just remove module separators and check the rest
    name = name.replace('::', '')
    if not CLEAN_NAME.fullmatch(name):
        raise RuntimeError(
            f'Non-alphanumeric names are not supported: {name}')


def get_schema_json(client):
    types = json.loads(client.query_json(INTRO_QUERY))
    modules = json.loads(client.query_json(MODULE_QUERY))

    return _process_links(types, modules)


async def async_get_schema_json(client):
    types = json.loads(await client.query_json(INTRO_QUERY))
    modules = json.loads(client.query_json(MODULE_QUERY))

    return _process_links(types, modules)


def _process_links(types, modules):
    # Figure out all the backlinks, link tables, and links with link
    # properties that require their own intermediate objects.
    type_map = {}
    link_tables = []
    link_objects = []
    prop_objects = []

    for spec in types:
        check_name(spec['name'])
        type_map[spec['name']] = spec

        for prop in spec['properties']:
            check_name(prop['name'])

    for spec in types:
        mod = spec["name"].rsplit('::', 1)[0]
        sql_source = get_sql_name(spec["name"])

        for prop in spec['properties']:
            exclusive = prop['exclusive']
            cardinality = prop['cardinality']
            name = prop['name']
            check_name(name)
            sql_name = get_sql_name(name)

            if cardinality == 'Many':
                # Multi property will make its own "link table". But since it
                # doesn't link to any other object the link table itself must
                # be reflected as an object.
                pobj = {
                    'module': mod,
                    'name': f'{sql_source}_{sql_name}_prop',
                    'table': f'{sql_source}.{sql_name}',
                    'links': [{
                        'name': 'source',
                        'required': True,
                        'cardinality': 'One' if exclusive else 'Many',
                        'exclusive': cardinality == 'One',
                        'target': {'name': spec['name']},
                        'has_link_object': False,
                    }],
                    'properties': [{
                        'name': 'target',
                        'required': True,
                        'cardinality': 'One',
                        'exclusive': False,
                        'target': prop['target'],
                        'has_link_object': False,
                    }],
                }
                prop_objects.append(pobj)

        for link in spec['links']:
            if link['name'] != '__type__':
                target = link['target']['name']
                cardinality = link['cardinality']
                exclusive = link['exclusive']
                name = link['name']
                check_name(name)
                sql_name = get_sql_name(name)

                objtype = type_map[target]
                objtype['backlinks'].append({
                    'name': f'backlink_via_{sql_name}',
                    # flip cardinality and exclusivity
                    'cardinality': 'One' if exclusive else 'Many',
                    'exclusive': cardinality == 'One',
                    'target': {'name': spec['name']},
                    'has_link_object': False,
                })

                for prop in link['properties']:
                    check_name(prop['name'])

                link['has_link_object'] = False
                # Any link with properties should become its own intermediate
                # object, since ORMs generally don't have a special convenient
                # way of exposing this as just a link table.
                if len(link['properties']) > 2:
                    # more than just 'source' and 'target' properties
                    lobj = {
                        'module': mod,
                        'name': f'{sql_source}_{sql_name}_link',
                        'table': f'{sql_source}.{sql_name}',
                        'links': [],
                        'properties': [],
                    }
                    for prop in link['properties']:
                        if prop['name'] in {'source', 'target'}:
                            lobj['links'].append(prop)
                        else:
                            lobj['properties'].append(prop)

                    link_objects.append(lobj)
                    link['has_link_object'] = True
                    objtype['backlinks'][-1]['has_link_object'] = True

                elif cardinality == 'Many':
                    # Add a link table for One-to-Many and Many-to-Many
                    link_tables.append({
                        'module': mod,
                        'name': f'{sql_source}_{sql_name}_table',
                        'table': f'{sql_source}.{sql_name}',
                        'source': spec["name"],
                        'target': target,
                    })

    return {
        'modules': modules,
        'object_types': types,
        'link_tables': link_tables,
        'link_objects': link_objects,
        'prop_objects': prop_objects,
    }
