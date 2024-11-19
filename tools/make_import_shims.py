import os
import sys

MODS = sorted(['gel', 'gel._taskgroup', 'gel._version', 'gel.abstract', 'gel.ai', 'gel.ai.core', 'gel.ai.types', 'gel.asyncio_client', 'gel.base_client', 'gel.blocking_client', 'gel.codegen', 'gel.color', 'gel.con_utils', 'gel.credentials', 'gel.datatypes', 'gel.datatypes.datatypes', 'gel.datatypes.range', 'gel.describe', 'gel.enums', 'gel.errors', 'gel.errors._base', 'gel.errors.tags', 'gel.introspect', 'gel.options', 'gel.pgproto', 'gel.pgproto.pgproto', 'gel.pgproto.types', 'gel.platform', 'gel.protocol', 'gel.protocol.asyncio_proto', 'gel.protocol.blocking_proto', 'gel.protocol.protocol', 'gel.scram', 'gel.scram.saslprep', 'gel.transaction'])



def main():
    for mod in MODS:
        is_package = any(k.startswith(mod + '.') for k in MODS)

        nmod = 'edgedb' + mod[len('gel'):]
        slash_name = nmod.replace('.', '/')
        if is_package:
            os.mkdir(slash_name)
            fname = slash_name + '/__init__.py'
        else:
            fname = slash_name + '.py'

        # import * skips things not in __all__ or with underscores at
        # the start, so we have to do some nonsense.
        with open(fname, 'w') as f:
            f.write(f'''\
# Auto-generated shim
import {mod} as _mod
import sys as _sys
_cur = _sys.modules['{nmod}']
for _k in vars(_mod):
    if not _k.startswith('__') or _k in ('__all__', '__doc__'):
        setattr(_cur, _k, getattr(_mod, _k))
del _cur
del _sys
del _mod
del _k
''')

    with open('edgedb/py.typed', 'w') as f:
        pass


if __name__ == '__main__':
    main()
