import abc
import typing

from .datatypes import datatypes


__all__ = ('Executor', 'AsyncIOExecutor')


class Executor(abc.ABC):

    @abc.abstractmethod
    def query(self, query: str, *args, **kwargs) -> datatypes.Set:
        ...

    @abc.abstractmethod
    def query_one(self, query: str, *args, **kwargs) -> typing.Any:
        ...

    @abc.abstractmethod
    def query_json(self, query: str, *args, **kwargs) -> str:
        ...

    @abc.abstractmethod
    def query_one_json(self, query: str, *args, **kwargs) -> str:
        ...

    # TODO(tailhook) add *args, **kwargs, when they are supported
    @abc.abstractmethod
    def execute(self, query: str) -> None:
        ...


class AsyncIOExecutor(abc.ABC):

    @abc.abstractmethod
    async def query(self, query: str, *args, **kwargs) -> datatypes.Set:
        ...

    @abc.abstractmethod
    async def query_one(self, query: str, *args, **kwargs) -> typing.Any:
        ...

    @abc.abstractmethod
    async def query_json(self, query: str, *args, **kwargs) -> str:
        ...

    @abc.abstractmethod
    async def query_one_json(self, query: str, *args, **kwargs) -> str:
        ...

    # TODO(tailhook) add *args, **kwargs, when they are supported
    @abc.abstractmethod
    async def execute(self, query: str) -> None:
        ...
