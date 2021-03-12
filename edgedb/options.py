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

    def with_transaction_options(
        self,
        options: TransactionOptions = None, *,
        isolation: IsolationLevel=None,
        readonly: bool = None,
        deferrable: bool = None,
    ):
        """Returns object with adjusted options for future transactions.

        :param options TransactionOptions:
            Options object to use. Either ``options`` or a combination of
            ``isolation``, ``readonly`` and ``deferrable`` parameters might be
            specified.

        :param isolation IsolationLevel:
            Isolation level to use for transactions.  If ``None``, original
            value is kept unchanged. Can't be used if ``options`` is provided.

        :param readonly bool:
            Whether transaction is readonly.  If ``None``, original value is
            kept unchanged. Can't be used if ``options`` is provided.

        This method returns a "shallow copy" of the current object
        with modified transaction options.

        Both ``self`` and returned object can be used after, but when using
        them transaction options applied will be different.

        Transaction options are are used by both
        ``raw_transaction`` and ``retrying_transaction``.
        """
        if options is None:
            o = self._options.transaction_options
            options = TransactionOptions(
                isolation=o._isolation if isolation is None else isolation,
                readonly=o._readonly if readonly is None else readonly,
                deferrable=o._deferrable if deferrable is None else deferrable,
            )
        elif (isolation is not None or
              readonly is not None or
              deferrable is not None):
            raise TypeError("Either options or isolation/readonly/deferrable "
                            "should be specified, not both.")

        result = self._shallow_clone()
        result._options = self._options.with_transaction_options(options)
        return result

    def with_retry_options(
        self,
        options: RetryOptions=None, *,
        condition: RetryCondition=None, attempts: int=None, backoff=None,
    ):
        """Returns object with adjusted options for future retrying
        transactions.

        :param options RetryOptions:
            Options object to use. Either ``options`` or a combination of
            ``condition``, ``attempts``, ``backoff`` parameters might be
            specified.

        :param condition RetryCondition:
            Specifies condition which rule will be modified for.
            If condition is ``None`` and either ``attempts`` or ``backoff``
            is not None, all rules are replaced.

            Can't be used if ``options`` is provided.

        :param attempts int:
            Number of attempts to perform retry on this condition.
            If ``attempts`` is not specified but ``backoff`` is, number of
            attempts are get from default rule.

            Can't be used if ``options`` is provided.

        :param backoff function:
            Function that returns amount to sleep between attempts given the
            iteration number. First retry (which is a second attempt)
            gets ``n=1``.  If ``attempts`` is not specified but ``backoff``
            is, number of attempts are get from default rule.

            Make sure to add some randomness to a backoff function.

            Can't be used if ``options`` is provided.

        Examples::

            # Set 4 attempts, but don't change backoff function
            con1 = con.with_retry(attempts=4)

            # Deadlock with twice delay comparing to default
            deadlock_fn = lambda n: (2 ** n) * 0.2 + randrange(100) * 0.002
            # deadlock_fn = lambda n: edgedb.default_backoff(n) * 2
            con2 = con.with_retry(RetryCondition.Deadlock, backoff=deadlock_fn)

        This method returns a "shallow copy" of the current object
        with modified transaction options.

        Both ``self`` and returned object can be used after, but when using
        them transaction options applied will be different.

        Transaction options are are used by both
        ``raw_transaction`` and ``retrying_transaction``.

        """
        if options is None:
            o = self._options.retry_options
            if condition is None:
                options = RetryOptions(
                    attempts=o._attempts if attempts is None else attempts,
                    backoff=o._backoff if backoff is None else backoff,
                )
            else:
                options = o.with_rule(
                    condition,
                    attempts=attempts,
                    backoff=backoff,
                )
        elif (condition is not None or
              attempts is not None or
              backoff is not None):
            raise TypeError("Either options or condition/attempts/backoff "
                            "should be specified, not both.")

        result = self._shallow_clone()
        result._options = self._options.with_retry(options)
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
