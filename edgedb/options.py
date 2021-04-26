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
    TransactionConflict = enum.auto()
    NetworkError = enum.auto()


class IsolationLevel:
    """Isolation level for transaction"""
    Serializable = "SERIALIZABLE"
    RepeatableRead = "REPEATABLE READ"


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
            if isinstance(exception, errors.TransactionConflictError):
                res = overrides.get(RetryCondition.TransactionConflict, res)
            elif isinstance(exception, errors.ClientError):
                res = overrides.get(RetryCondition.NetworkError, res)
        return res


class TransactionOptions:
    """Options for `raw_transaction()` and `retrying_transaction()`"""
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

    def start_transaction_query(self):
        isolation = str(self._isolation)
        if self._readonly:
            mode = 'READ ONLY'
        else:
            mode = 'READ WRITE'

        if self._deferrable:
            defer = 'DEFERRABLE'
        else:
            defer = 'NOT DEFERRABLE'

        return f'START TRANSACTION ISOLATION {isolation}, {mode}, {defer};'

    def __repr__(self):
        return (
            f'<{self.__class__.__name__} '
            f'isolation:{self._isolation}, '
            f'readonly:{self._readonly}, '
            f'deferrable:{self._deferrable}>'
        )


class _OptionsMixin:
    def __init__(self, *args, **kwargs):
        self._options = _Options.defaults()
        super().__init__(*args, **kwargs)

    @abc.abstractmethod
    def _shallow_clone(self):
        pass

    def with_transaction_options(self, options: TransactionOptions = None):
        """Returns object with adjusted options for future transactions.

        :param options TransactionOptions:
            Object that encapsulates transaction options.

        This method returns a "shallow copy" of the current object
        with modified transaction options.

        Both ``self`` and returned object can be used after, but when using
        them transaction options applied will be different.

        Transaction options are are used by both
        ``raw_transaction`` and ``retrying_transaction``.
        """
        result = self._shallow_clone()
        result._options = self._options.with_transaction_options(options)
        return result

    def with_retry_options(self, options: RetryOptions=None):
        """Returns object with adjusted options for future retrying
        transactions.

        :param options RetryOptions:
            Object that encapsulates retry options.

        This method returns a "shallow copy" of the current object
        with modified transaction options.

        Both ``self`` and returned object can be used after, but when using
        them transaction options applied will be different.
        """

        result = self._shallow_clone()
        result._options = self._options.with_retry_options(options)
        return result


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

    @property
    def transaction_options(self):
        return self._transaction_options

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
