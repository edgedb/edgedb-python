import abc
import typing

from . import options
from .datatypes import datatypes
from .protocol import protocol

__all__ = (
    "QueryWithArgs",
    "QueryCache",
    "QueryOptions",
    "QueryContext",
    "Executor",
    "AsyncIOExecutor",
    "ReadOnlyExecutor",
    "AsyncIOReadOnlyExecutor",
)


class QueryWithArgs(typing.NamedTuple):
    query: str
    args: typing.Tuple
    kwargs: typing.Dict[str, typing.Any]


class QueryCache(typing.NamedTuple):
    codecs_registry: protocol.CodecsRegistry
    query_cache: protocol.QueryCodecsCache


class QueryOptions(typing.NamedTuple):
    io_format: protocol.IoFormat
    expect_one: bool
    required_one: bool


class QueryContext(typing.NamedTuple):
    query: QueryWithArgs
    cache: QueryCache
    query_options: QueryOptions
    retry_options: typing.Optional[options.RetryOptions]


_query_opts = QueryOptions(
    io_format=protocol.IoFormat.BINARY,
    expect_one=False,
    required_one=False,
)
_query_single_opts = QueryOptions(
    io_format=protocol.IoFormat.BINARY,
    expect_one=True,
    required_one=False,
)
_query_required_single_opts = QueryOptions(
    io_format=protocol.IoFormat.BINARY,
    expect_one=True,
    required_one=True,
)
_query_json_opts = QueryOptions(
    io_format=protocol.IoFormat.JSON,
    expect_one=False,
    required_one=False,
)
_query_single_json_opts = QueryOptions(
    io_format=protocol.IoFormat.JSON,
    expect_one=True,
    required_one=False,
)
_query_required_single_json_opts = QueryOptions(
    io_format=protocol.IoFormat.JSON,
    expect_one=True,
    required_one=True,
)


class BaseReadOnlyExecutor(abc.ABC):
    __slots__ = ()

    @abc.abstractmethod
    def _get_query_cache(self) -> QueryCache:
        ...

    def _get_retry_options(self) -> typing.Optional[options.RetryOptions]:
        return None


class ReadOnlyExecutor(BaseReadOnlyExecutor):
    """Subclasses can execute *at least* read-only queries"""

    __slots__ = ()

    @abc.abstractmethod
    def _query(self, query_context: QueryContext):
        ...

    def query(self, query: str, *args, **kwargs) -> datatypes.Set:
        return self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_opts,
            retry_options=self._get_retry_options(),
        ))

    def query_single(
        self, query: str, *args, **kwargs
    ) -> typing.Union[typing.Any, None]:
        return self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_single_opts,
            retry_options=self._get_retry_options(),
        ))

    def query_required_single(self, query: str, *args, **kwargs) -> typing.Any:
        return self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_required_single_opts,
            retry_options=self._get_retry_options(),
        ))

    def query_json(self, query: str, *args, **kwargs) -> str:
        return self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_json_opts,
            retry_options=self._get_retry_options(),
        ))

    def query_single_json(self, query: str, *args, **kwargs) -> str:
        return self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_single_json_opts,
            retry_options=self._get_retry_options(),
        ))

    def query_required_single_json(self, query: str, *args, **kwargs) -> str:
        return self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_required_single_json_opts,
            retry_options=self._get_retry_options(),
        ))

    # TODO(tailhook) add *args, **kwargs, when they are supported
    @abc.abstractmethod
    def execute(self, query: str) -> None:
        ...


class Executor(ReadOnlyExecutor):
    """Subclasses can execute both read-only and modification queries"""

    __slots__ = ()


class AsyncIOReadOnlyExecutor(BaseReadOnlyExecutor):
    """Subclasses can execute *at least* read-only queries"""

    __slots__ = ()

    @abc.abstractmethod
    async def _query(self, query_context: QueryContext):
        ...

    async def query(self, query: str, *args, **kwargs) -> datatypes.Set:
        return await self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_opts,
            retry_options=self._get_retry_options(),
        ))

    async def query_single(self, query: str, *args, **kwargs) -> typing.Any:
        return await self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_single_opts,
            retry_options=self._get_retry_options(),
        ))

    async def query_required_single(
        self,
        query: str,
        *args,
        **kwargs
    ) -> typing.Any:
        return await self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_required_single_opts,
            retry_options=self._get_retry_options(),
        ))

    async def query_json(self, query: str, *args, **kwargs) -> str:
        return await self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_json_opts,
            retry_options=self._get_retry_options(),
        ))

    async def query_single_json(self, query: str, *args, **kwargs) -> str:
        return await self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_single_json_opts,
            retry_options=self._get_retry_options(),
        ))

    async def query_required_single_json(
        self,
        query: str,
        *args,
        **kwargs
    ) -> str:
        return await self._query(QueryContext(
            query=QueryWithArgs(query, args, kwargs),
            cache=self._get_query_cache(),
            query_options=_query_required_single_json_opts,
            retry_options=self._get_retry_options(),
        ))

    # TODO(tailhook) add *args, **kwargs, when they are supported
    @abc.abstractmethod
    async def execute(self, query: str) -> None:
        ...


class AsyncIOExecutor(AsyncIOReadOnlyExecutor):
    """Subclasses can execute both read-only and modification queries"""

    __slots__ = ()
