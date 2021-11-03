import asyncio
import time

from . import errors
from . import transaction as _transaction


class AsyncIOIteration(_transaction.BaseAsyncIOTransaction):
    def __init__(self, retry, owner, iteration):
        super().__init__(owner, retry._options.transaction_options)
        self.__retry = retry
        self.__iteration = iteration
        self.__started = False

    async def _ensure_transaction(self):
        if not self._managed:
            raise errors.InterfaceError(
                "Only managed retriable transactions are supported. "
                "Use `async with transaction:`"
            )
        if not self.__started:
            self.__started = True
            await self._start(single_connect=self.__iteration != 0)
            if self._pool is not None:
                # Having a pool means we just acquired the connection in
                # _start() - let's mark it as borrowed for transaction anyways
                # just in case the connection is somehow accessed separately.
                self._borrow()

    async def __aenter__(self):
        if self._managed:
            raise errors.InterfaceError(
                'cannot enter context: already in an `async with` block')
        self._managed = True
        if self._pool is None:
            # Borrow the connection for transaction now if it's not on a pool,
            # because that means we already have the connection now, and
            # further use of the connection like this should be prevented:
            #     async for tx in conn.transaction():
            #         async with tx:
            #             await conn.query("...")  # <- wrong use after borrow
            self._borrow()
        return self

    async def __aexit__(self, extype, ex, tb):
        self._managed = False
        if not self.__started:
            self._maybe_return()
            return False

        try:
            if extype is not None:
                await self._rollback()
            else:
                await self._commit()
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
        self._next_backoff = 0
        self._options = owner._options

    def _retry(self, exc):
        self._last_exception = exc
        rule = self._options.retry_options.get_rule_for_exception(exc)
        if self._iteration >= rule.attempts:
            return False
        self._done = False
        self._next_backoff = rule.backoff(self._iteration)
        return True


class AsyncIORetry(BaseRetry):

    def __aiter__(self):
        return self

    async def __anext__(self):
        # Note: when changing this code consider also
        # updating Retry.__next__.
        if self._done:
            raise StopAsyncIteration
        if self._next_backoff:
            await asyncio.sleep(self._next_backoff)
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
        if self._next_backoff:
            time.sleep(self._next_backoff)
        self._done = True
        iteration = Iteration(self, self._owner, self._iteration)
        self._iteration += 1
        return iteration


class Iteration(_transaction.BaseBlockingIOTransaction):
    def __init__(self, retry, owner, iteration):
        super().__init__(owner, retry._options.transaction_options)
        self.__retry = retry
        self.__iteration = iteration
        self.__started = False

    def _ensure_transaction(self):
        if not self._managed:
            raise errors.InterfaceError(
                "Only managed retriable transactions are supported. "
                "Use `with transaction:`"
            )
        if not self.__started:
            self.__started = True
            self._start(single_connect=self.__iteration != 0)

    def __enter__(self):
        if self._managed:
            raise errors.InterfaceError(
                'cannot enter context: already in a `with` block')
        self._managed = True
        self._borrow()
        return self

    def __exit__(self, extype, ex, tb):
        self._managed = False
        if not self.__started:
            self._maybe_return()
            return False

        try:
            if extype is not None:
                self._rollback()
            else:
                self._commit()
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
