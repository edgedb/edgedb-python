# Auto-generated shim
TYPE_CHECKING = False
if TYPE_CHECKING:
    from gel.codegen import *  # noqa
import gel.codegen as _mod
import sys as _sys
_cur = _sys.modules['edgedb.codegen']
for _k in vars(_mod):
    if not _k.startswith('__') or _k in ('__all__', '__doc__'):
        setattr(_cur, _k, getattr(_mod, _k))
del _cur
del _sys
del _mod
del _k
