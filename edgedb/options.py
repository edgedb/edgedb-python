import abc
import enum
import random
from collections import namedtuple

from . import errors


_RetryRule = namedtuple("_RetryRule", ["attempts", "backoff"])


def default_backoff(attempt):
    return (2 ** attempt) * 0.1 + random.randrange(100) * 0.001


class RetryCondition:
    """Specific condition to retry on for fine-grained control"""
    SerializationError = enum.auto()
    Deadlock = enum.auto()
    NetworkError = enum.auto()


class IsolationLevel:
    """Isolation level for transaction"""
    Serializable = enum.auto()
    RepeatableRead = enum.auto()


class RetryOptions:
    """An immutable class that contains rules for `retrying_transaction()`"""
    __slots__ = ['_default', '_overrides']

    def __init__(self, attempts: int, backoff):
        self._default = _RetryRule(attempts, backoff)
        self._overrides = None

    def with_rule(self, condition, attempts=None, backoff=None):
        default = self._default
        overrides = self._overrides
        if overrides is None:
            overrides = {}
        else:
            overrides = overrides.copy()
        overrides[condition] = _RetryRule(
            default.attempts if attempts is None else attempts,
            default.backoff if backoff is None else backoff,
        )
        result = RetryOptions.__new__(RetryOptions)
        result._default = default
        result._overrides = overrides
        return result

    @classmethod
    def defaults(cls):
        return cls(
            attempts=3,
            backoff=default_backoff,
        )

    def get_rule_for_exception(self, exception):
        default = self._default
        overrides = self._overrides
        res = default
        if overrides:
            if isinstance(exception, errors.TransactionSerializationError):
                res = overrides.get(RetryCondition.SerializationError, default)
            elif isinstance(exception, errors.TansactionDeadlockError):
                res = overrides.get(RetryCondition.Deadlock, default)
            elif isinstance(exception, errors.ClientError):
                res = overrides.get(RetryCondition.NetworkError, default)
        return res


class TransactionOptions:
    """Options for `raw_transaction()` an `retrying_transaction()`"""
    __slots__ = ['_isolation', '_readonly', '_deferrable']

    def __init__(
        self,
        isolation: IsolationLevel=IsolationLevel.RepeatableRead,
        readonly: bool = False,
        deferrable: bool = False,
    ):
        self._isolation = isolation
        self._readonly = readonly
        self._deferrable = deferrable

    @classmethod
    def defaults(cls):
        return cls()


class _OptionsMixin:
    def __init__(self, *args, **kwargs):
        self._options = _Options.defaults()
        super().__init__(*args, **kwargs)

    @abc.abstractmethod
    def _shallow_clone(self):
        pass


class _Options:
    """Internal class for storing connection options"""

    __slots__ = ['_retry_options', '_transaction_options']

    def __init__(
        self,
        retry_options: RetryOptions,
        transaction_options: TransactionOptions,
    ):
        self._retry_options = retry_options
        self._transaction_options = transaction_options

    @property
    def retry_options(self):
        return self._retry_options

    def with_retry_options(self, options: RetryOptions):
        return _Options(
            options,
            self._transaction_options,
        )

    def with_transaction_options(self, options: TransactionOptions):
        return _Options(
            options,
            self._transaction_options,
        )

    @classmethod
    def defaults(cls):
        return cls(
            RetryOptions.defaults(),
            TransactionOptions.defaults(),
        )
