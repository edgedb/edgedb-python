import abc
import enum
import random
import typing
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


class RetryOptions:
    """An immutable class that contains rules for `transaction()`"""
    __slots__ = ['_default', '_overrides']

    def __init__(self, attempts: int, backoff=default_backoff):
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
    """Options for `transaction()`"""
    __slots__ = ['_isolation', '_readonly', '_deferrable']

    def __init__(
        self,
        isolation: IsolationLevel=IsolationLevel.Serializable,
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


class Session:
    __slots__ = ['_module', '_aliases', '_config', '_globals']

    def __init__(
        self,
        module: typing.Optional[str] = None,
        aliases: typing.Mapping[str, str] = None,
        config: typing.Mapping[str, typing.Any] = None,
        globals_: typing.Mapping[str, typing.Any] = None,
    ):
        self._module = module
        self._aliases = {} if aliases is None else dict(aliases)
        self._config = {} if config is None else dict(config)
        self._globals = {} if globals_ is None else dict(globals_)

    @classmethod
    def defaults(cls):
        return cls()

    def with_aliases(self, module=..., **aliases):
        new_aliases = self._aliases.copy()
        new_aliases.update(aliases)
        return Session(
            module=self._module if module is ... else module,
            aliases=new_aliases,
            config=self._config,
            globals_=self._globals,
        )

    def with_config(self, **config):
        new_config = self._config.copy()
        new_config.update(config)
        return Session(
            module=self._module,
            aliases=self._aliases,
            config=new_config,
            globals_=self._globals,
        )

    def with_globals(self, **globals_):
        new_globals = self._globals.copy()
        new_globals.update(globals_)
        return Session(
            module=self._module,
            aliases=self._aliases,
            config=self._config,
            globals_=new_globals,
        )

    def as_dict(self):
        rv = {}
        if self._module is not None:
            module = rv["module"] = self._module
        else:
            module = 'default'
        if self._aliases:
            rv["aliases"] = list(self._aliases.items())
        if self._config:
            rv["config"] = self._config
        if self._globals:
            rv["globals"] = g = {}
            for k, v in self._globals.items():
                parts = k.split("::")
                if len(parts) == 1:
                    g[f"{module}::{k}"] = v
                elif len(parts) == 2:
                    mod, glob = parts
                    mod = self._aliases.get(mod, mod)
                    g[f"{mod}::{glob}"] = v
                else:
                    raise errors.InvalidArgumentError(
                        f"Illegal global name: {k}"
                    )
        return rv


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

        Transaction options are are used by the ``transaction`` method.
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
        with modified retry options.

        Both ``self`` and returned object can be used after, but when using
        them transaction options applied will be different.
        """

        result = self._shallow_clone()
        result._options = self._options.with_retry_options(options)
        return result

    def with_session(self, session: Session):
        result = self._shallow_clone()
        result._options = self._options.with_session(session)
        return result

    def with_aliases(self, module=None, **aliases):
        result = self._shallow_clone()
        result._options = self._options.with_session(
            self._options.session.with_aliases(module=module, **aliases)
        )
        return result

    def with_config(self, **config):
        result = self._shallow_clone()
        result._options = self._options.with_session(
            self._options.session.with_config(**config)
        )
        return result

    def with_globals(self, **globals_):
        result = self._shallow_clone()
        result._options = self._options.with_session(
            self._options.session.with_globals(**globals_)
        )
        return result


class _Options:
    """Internal class for storing connection options"""

    __slots__ = ['_retry_options', '_transaction_options', '_session']

    def __init__(
        self,
        retry_options: RetryOptions,
        transaction_options: TransactionOptions,
        session: Session,
    ):
        self._retry_options = retry_options
        self._transaction_options = transaction_options
        self._session = session

    @property
    def retry_options(self):
        return self._retry_options

    @property
    def transaction_options(self):
        return self._transaction_options

    @property
    def session(self):
        return self._session

    def with_retry_options(self, options: RetryOptions):
        return _Options(
            options,
            self._transaction_options,
            self._session,
        )

    def with_transaction_options(self, options: TransactionOptions):
        return _Options(
            self._retry_options,
            options,
            self._session,
        )

    def with_session(self, session: Session):
        return _Options(
            self._retry_options,
            self._transaction_options,
            session,
        )

    @classmethod
    def defaults(cls):
        return cls(
            RetryOptions.defaults(),
            TransactionOptions.defaults(),
            Session.defaults(),
        )
