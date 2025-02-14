# Auto-generated shim
TYPE_CHECKING = False
if TYPE_CHECKING:
    from gel.ai import *  # noqa
    create_ai = create_rag_client
    EdgeDBAI = RAGClient
    create_async_ai = create_async_rag_client
    AsyncEdgeDBAI = AsyncRAGClient
    AIOptions = RAGOptions
import gel.ai as _mod
import sys as _sys
_cur = _sys.modules['edgedb.ai']
for _k in vars(_mod):
    if not _k.startswith('__') or _k in ('__all__', '__doc__'):
        setattr(_cur, _k, getattr(_mod, _k))
setattr(_cur, 'create_ai', getattr(_mod, 'create_rag_client'))
setattr(_cur, 'EdgeDBAI', getattr(_mod, 'RAGClient'))
setattr(_cur, 'create_async_ai', getattr(_mod, 'create_async_rag_client'))
setattr(_cur, 'AsyncEdgeDBAI', getattr(_mod, 'AsyncRAGClient'))
setattr(_cur, 'AIOptions', getattr(_mod, 'RAGOptions'))
if hasattr(_cur, '__all__'):
    setattr(_cur, '__all__', getattr(_cur, '__all__') + [
        'create_ai', 'EdgeDBAI', 'create_async_ai', 'AsyncEdgeDBAI', 'AIOptions',
    ])
del _cur
del _sys
del _mod
del _k
