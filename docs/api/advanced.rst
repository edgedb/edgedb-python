.. _edgedb-python-advanced:

==============
Advanced Usage
==============

.. py:currentmodule:: edgedb


.. _edgedb-python-transaction-options:

Transaction Options
===================

Transactions can be customized with different options:

.. py:class:: TransactionOptions(isolation=IsolationLevel.Serializable, readonly=False, deferrable=False)

    :param IsolationLevel isolation: transaction isolation level
    :param bool readonly: if true the transaction will be readonly
    :param bool deferrable: if true the transaction will be deferrable

    .. py:method:: defaults()
        :classmethod:

        Returns the default :py:class:`TransactionOptions`.

.. py:class:: IsolationLevel

    Isolation level for transaction

    .. py:attribute:: Serializable

        Serializable isolation level

:py:class:`TransactionOptions` can be set on :py:class:`~edgedb.Client` or
:py:class:`~edgedb.AsyncIOClient` using one of these methods:

* :py:meth:`edgedb.Client.with_transaction_options`
* :py:meth:`edgedb.AsyncIOClient.with_transaction_options`

These methods return a "shallow copy" of the current client object with modified
transaction options. Both ``self`` and the returned object can be used, but
different transaction options will applied respectively.

Transaction options are used by the future calls to the method
:py:meth:`edgedb.Client.transaction` or :py:meth:`edgedb.AsyncIOClient.transaction`.


.. _edgedb-python-retry-options:

Retry Options
=============

Individual EdgeQL commands or whole transaction blocks are automatically retried on
retryable errors. By default, edgedb-python will try at most 3 times, with an
exponential backoff time interval starting from 100ms, plus a random hash under 100ms.

Retry rules can be granularly customized with different retry options:

.. py:class:: RetryOptions(attempts, backoff=default_backoff)

    :param int attempts: the default number of attempts
    :param Callable[[int], Union[float, int]] backoff: the default backoff function

    .. py:method:: with_rule(condition, attempts=None, backoff=None)

        Adds a backoff rule for a particular condition

        :param RetryCondition condition: condition that will trigger this rule
        :param int attempts: number of times to retry
        :param Callable[[int], Union[float, int]] backoff:
          function taking the current attempt number and returning the number
          of seconds to wait before the next attempt

    .. py:method:: defaults()
        :classmethod:

        Returns the default :py:class:`RetryOptions`.

.. py:class:: RetryCondition

    Specific condition to retry on for fine-grained control

    .. py:attribute:: TransactionConflict

        Triggered when a TransactionConflictError occurs.

    .. py:attribute:: NetworkError

        Triggered when a ClientError occurs.

:py:class:`RetryOptions` can be set on :py:class:`~edgedb.Client` or
:py:class:`~edgedb.AsyncIOClient` using one of these methods:

* :py:meth:`edgedb.Client.with_retry_options`
* :py:meth:`edgedb.AsyncIOClient.with_retry_options`

These methods return a "shallow copy" of the current client object with modified
retry options. Both ``self`` and the returned object can be used, but different
retry options will applied respectively.


.. _edgedb-python-state:

State
=====

State is an execution context that affects the execution of EdgeQL commands in
different ways: default module, module aliases, session config and global values.

.. py:class:: State(module=None, aliases={}, config={}, globals_={})

    :type module: str or None
    :param module:
        The *default module* that the future commands will be executed with.
        ``None`` means the default *default module* on the server-side,
        which is usually just ``default``.

    :param dict[str, str] aliases:
        Module aliases mapping of alias -> target module.

    :param dict[str, object] config:
        Non system-level config settings mapping of config name -> config value.

        For available configuration parameters refer to the
        :ref:`Config documentation <ref_std_cfg>`.

    :param dict[str, object] globals_:
        Global values mapping of global name -> global value.

        Note, the global name can be either a qualified name like
        ``my_mod::glob2``, or a simple name under the default module. Simple
        names will be prefixed with the default module at execution time.
        Values under different types of names for the same global would
        overwrite each other, so try not to do that.

    .. py:method:: with_module_aliases(module=..., **aliases)

        Returns a new :py:class`State` copy with adjusted default module and/or
        module aliases.

        :type module: str or None
        :param module:
            Adjust the *default module*. If ``module`` is not set, the default
            behavior is not to adjust the *default module* in the copy.

            This is equivalent to using the ``set module`` command.

        :param dict[str, str] aliases:
            Adjust the module aliases by merging with the given alias -> target
            module mapping.

            This is equivalent to using the ``set alias`` command.

    .. py:method:: with_config(**config)

        Returns a new :py:class:`State` copy with adjusted session config.

        This is equivalent to using the ``configure session`` command.

        :param dict[str, object] config:
            Adjust the config settings by merging with the given config name ->
            config value mapping.

    .. py:method:: with_globals(**globals_)

        Returns a new :py:class:`State` copy with adjusted global values.

        This is equivalent to using the ``set global`` command.

        :param dict[str, object] globals_:
            Adjust the global values by merging with the given global name ->
            global value mapping.

:py:class:`State` can be set on :py:class:`~edgedb.Client` or
:py:class:`~edgedb.AsyncIOClient` using one of these methods:

* :py:meth:`edgedb.Client.with_state`
* :py:meth:`edgedb.AsyncIOClient.with_state`

These methods return a "shallow copy" of the current client object with modified
state, affecting all future commands executed using the returned copy.
Both ``self`` and the returned object can be used, but different state will
applied respectively.

Alternatively, shortcuts are available on client objects:

* :py:meth:`edgedb.Client.with_module_aliases`
* :py:meth:`edgedb.Client.with_config`
* :py:meth:`edgedb.Client.with_globals`
* :py:meth:`edgedb.AsyncIOClient.with_module_aliases`
* :py:meth:`edgedb.AsyncIOClient.with_config`
* :py:meth:`edgedb.AsyncIOClient.with_globals`

They work the same way as ``with_state``, and adjusts the corresponding state values.
