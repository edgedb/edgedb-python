import asyncio
import random
import time

from . import errors
from . import transaction as _transaction


DEFAULT_MAX_ITERATIONS = 3


def default_backoff(attempt):
    return (2 ** attempt) * 0.1 + random.randrange(100) * 0.001


class AsyncIOIteration(_transaction.AsyncIOTransaction):
    def __init__(self, retry, owner, iteration):
        super().__init__(owner)
        self.__retry = retry
        self.__iteration = iteration

    async def start(self):
        if not self._managed:
            raise errors.InterfaceError(
                "Only managed retriable transactions are supported. "
                "Use `async with transaction:`"
            )
        await self._start(single_connect=self.__iteration != 0)

    async def __aexit__(self, extype, ex, tb):
        try:
            await super().__aexit__(extype, ex, tb)
        except errors.EdgeDBError as err:
            if ex is None:
                # On commit we don't know if commit is succeeded before the
                # database have received it or after it have been done but
                # network is dropped before we were able to receive a response
                # TODO(tailhook) retry on some errors
                raise err
            # If we were going to rollback, look at original error
            # to find out whether we want to retry, regardless of
            # the rollback error.
            # In this case we ignore rollback issue as original error is more
            # important, e.g. in case `CancelledError` it's important
            # to propagate it to cancel the whole task.
            # NOTE: rollback error is always swallowed, should we use
            # on_log_message for it?

        if (
            extype is not None and
            issubclass(extype, errors.EdgeDBError) and
            ex.has_tag(errors.SHOULD_RETRY)
        ):
            return self.__retry._retry(ex)


class BaseRetry:

    def __init__(self, owner):
        self._owner = owner
        self._iteration = 0
        self._done = False
        self._backoff = default_backoff
        self._max_iterations = DEFAULT_MAX_ITERATIONS

    def _retry(self, exc):
        self._last_exception = exc
        if self._iteration >= self._max_iterations:
            return False
        self._done = False
        return True


class AsyncIORetry(BaseRetry):

    def __aiter__(self):
        return self

    async def __anext__(self):
        # Note: when changing this code consider also
        # updating Retry.__next__.
        if self._done:
            raise StopAsyncIteration
        assert self._iteration + 1 < self._max_iterations, \
            f"Extra retry {self._iteration}/{self._max_iterations}"
        if self._iteration > 0:
            await asyncio.sleep(self._backoff(self._iteration))
        self._done = True
        iteration = AsyncIOIteration(self, self._owner, self._iteration)
        self._iteration += 1
        return iteration


class Retry(BaseRetry):

    def __iter__(self):
        return self

    def __next__(self):
        # Note: when changing this code consider also
        # updating AsyncIORetry.__anext__.
        if self._done:
            raise StopIteration
        assert self._iteration + 1 < self._max_iterations, \
            f"Extra retry {self._iteration}/{self._max_iterations}"
        if self._iteration > 0:
            time.sleep(self._backoff(self._iteration))
        self._done = True
        iteration = Iteration(self, self._owner, self._iteration)
        self._iteration += 1
        return iteration


class Iteration(_transaction.Transaction):
    def __init__(self, retry, owner, iteration):
        super().__init__(owner)
        self.__retry = retry
        self.__iteration = iteration

    def start(self):
        if not self._managed:
            raise errors.InterfaceError(
                "Only managed retriable transactions are supported. "
                "Use `with transaction:`"
            )
        self._start(single_connect=self.__iteration != 0)

    def __exit__(self, extype, ex, tb):
        try:
            super().__exit__(extype, ex, tb)
        except errors.EdgeDBError as err:
            if ex is None:
                # On commit we don't know if commit is succeeded before the
                # database have received it or after it have been done but
                # network is dropped before we were able to receive a response
                # TODO(tailhook) retry on some errors
                raise err
            # If we were going to rollback, look at original error
            # to find out whether we want to retry, regardless of
            # the rollback error.
            # In this case we ignore rollback issue as original error is more
            # important.

        if (
            extype is not None and
            issubclass(extype, errors.EdgeDBError) and
            ex.has_tag(errors.SHOULD_RETRY)
        ):
            return self.__retry._retry(ex)
