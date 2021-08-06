.. _edgedb-python-blocking-api-reference:

============
Blocking API
============

.. py:currentmodule:: edgedb


.. _edgedb-blocking-api-connection:

Connection
==========

.. py:function:: connect(dsn=None, *, \
            host=None, port=None, \
            admin=None, \
            user=None, password=None, \
            database=None, \
            timeout=60)

    Establish a connection to an EdgeDB server.

    :param dsn:
        If this parameter does not start with ``edgedb://`` then this is
        a :ref:`name of an instance <edgedb-instances>`.

        Otherwise it specifies a single string in the connection URI format:
        ``edgedb://user:password@host:port/database?option=value``.
        The following options are recognized: host, port,
        user, database, password.

    :param host:
        Database host address as one of the following:

        - an IP address or a domain name;
        - an absolute path to the directory containing the database
          server Unix-domain socket (not supported on Windows);
        - a sequence of any of the above, in which case the addresses
          will be tried in order, and the first successful connection
          will be returned.

        If not specified, the following will be tried, in order:

        - host address(es) parsed from the *dsn* argument,
        - the value of the ``EDGEDB_HOST`` environment variable,
        - on Unix, common directories used for EdgeDB Unix-domain
          sockets: ``"/run/edgedb"`` and ``"/var/run/edgedb"``,
        - ``"localhost"``.

    :param port:
        Port number to connect to at the server host
        (or Unix-domain socket file extension).  If multiple host
        addresses were specified, this parameter may specify a
        sequence of port numbers of the same length as the host sequence,
        or it may specify a single port number to be used for all host
        addresses.

        If not specified, the value parsed from the *dsn* argument is used,
        or the value of the ``EDGEB_PORT`` environment variable, or ``5656``
        if neither is specified.

    :param admin:
        If ``True``, try to connect to the special administration socket.

    :param user:
        The name of the database role used for authentication.

        If not specified, the value parsed from the *dsn* argument is used,
        or the value of the ``EDGEDB_USER`` environment variable, or the
        operating system name of the user running the application.

    :param database:
        The name of the database to connect to.

        If not specified, the value parsed from the *dsn* argument is used,
        or the value of the ``EDGEDB_DATABASE`` environment variable, or the
        operating system name of the user running the application.

    :param password:
        Password to be used for authentication, if the server requires
        one.  If not specified, the value parsed from the *dsn* argument
        is used, or the value of the ``EDGEDB_PASSWORD`` environment variable.
        Note that the use of the environment variable is discouraged as
        other users and applications may be able to read it without needing
        specific privileges.

    :param float timeout:
        Connection timeout in seconds.

    :return: A :py:class:`~edgedb.BlockingIOConnection` instance.

    The connection parameters may be specified either as a connection
    URI in *dsn*, or as specific keyword arguments, or both.
    If both *dsn* and keyword arguments are specified, the latter
    override the corresponding values parsed from the connection URI.

    Returns a new :py:class:`~edgedb.BlockingIOConnection` object.

    Example:

    .. code-block:: pycon

        >>> import edgedb
        >>> con = edgedb.connect(user='edgedeb')
        >>> con.query_single('SELECT 1 + 1')
        {2}


.. py:class:: BlockingIOConnection

    A representation of a database session.

    Connections are created by calling :py:func:`~edgedb.connect`.


    .. py:method:: query(query, *args, **kwargs)

        Run a query and return the results as a
        :py:class:`edgedb.Set <edgedb.Set>` instance.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            An instance of :py:class:`edgedb.Set <edgedb.Set>` containing
            the query result.

        Note that positional and named query arguments cannot be mixed.


    .. py:method:: query_single(query, *args, **kwargs)

        Run a singleton-returning query and return its element.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            Query result.

        The *query* must return exactly one element.  If the query returns
        more than one element, an ``edgedb.ResultCardinalityMismatchError``
        is raised, if it returns an empty set, an ``edgedb.NoDataError``
        is raised.

        Note, that positional and named query arguments cannot be mixed.


    .. py:method:: query_json(query, *args, **kwargs)

        Run a query and return the results as JSON.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            A JSON string containing an array of query results.

        Note, that positional and named query arguments cannot be mixed.

        .. note::

            Caution is advised when reading ``decimal`` values using
            this method. The JSON specification does not have a limit
            on significant digits, so a ``decimal`` number can be
            losslessly represented in JSON. However, the default JSON
            decoder in Python will read all such numbers as ``float``
            values, which may result in errors or precision loss. If
            such loss is unacceptable, then consider casting the value
            into ``str`` and decoding it on the client side into a
            more appropriate type, such as ``Decimal``.


    .. py:method:: query_single_json(query, *args, **kwargs)

        Run a singleton-returning query and return its element in JSON.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            Query result encoded in JSON.

        The *query* must return exactly one element.  If the query returns
        more than one element, an ``edgedb.ResultCardinalityMismatchError``
        is raised, if it returns an empty set, an ``edgedb.NoDataError``
        is raised.

        Note, that positional and named query arguments cannot be mixed.

        .. note::

            Caution is advised when reading ``decimal`` values using
            this method. The JSON specification does not have a limit
            on significant digits, so a ``decimal`` number can be
            losslessly represented in JSON. However, the default JSON
            decoder in Python will read all such numbers as ``float``
            values, which may result in errors or precision loss. If
            such loss is unacceptable, then consider casting the value
            into ``str`` and decoding it on the client side into a
            more appropriate type, such as ``Decimal``.


    .. py:method:: execute(query)

        Execute an EdgeQL command (or commands).

        :param str query: Query text.

        The commands must take no arguments.

        Example:

        .. code-block:: pycon

            >>> con.execute('''
            ...     CREATE TYPE MyType {
            ...         CREATE PROPERTY a -> int64
            ...     };
            ...     FOR x IN {100, 200, 300}
            ...     UNION INSERT MyType { a := x };
            ... ''')

        .. note::
            If the results of *query* are desired, :py:meth:`query` or
            :py:meth:`query_single` should be used instead.

    .. py:method:: retrying_transaction()

        Open a retryable transaction loop.

        This is the preferred method of initiating and running a database
        transaction in a robust fashion.  The ``retrying_transaction()``
        transaction loop will attempt to re-execute the transaction loop body
        if a transient error occurs, such as a network error or a transaction
        serialization error.

        Returns an instance of :py:class:`Retry`.

        See :ref:`edgedb-python-blocking-api-transaction` for more details.

        Example:

        .. code-block:: python

            for tx in con.retrying_transaction():
                with tx:
                    value = tx.query_single("SELECT Counter.value")
                    tx.execute(
                        "UPDATE Counter SET { value := <int64>$value }",
                        value=value + 1,
                    )

        Note that we are executing queries on the ``tx`` object rather
        than on the original connection.

    .. py:method:: raw_transaction()

        Execute a non-retryable transaction.

        Contrary to ``retrying_transaction()``, ``raw_transaction()``
        will not attempt to re-run the nested code block in case a retryable
        error happens.

        This is a low-level API and it is advised to use the
        ``retrying_transaction()`` method instead.

        A call to ``raw_transaction()`` returns
        :py:class:`AsyncIOTransaction`.

        Example:

        .. code-block:: python

            with con.raw_transaction() as tx:
                value = tx.query_single("SELECT Counter.value")
                tx.execute(
                    "UPDATE Counter SET { value := <int64>$value }",
                    value=value + 1,
                )

        Note that we are executing queries on the ``tx`` object,
        rather than on the original connection ``con``.


    .. py:method:: transaction(isolation=None, readonly=None, deferrable=None)

        **Deprecated**. Use :py:meth:`retrying_transaction` or
        :py:meth:`raw_transaction`.

        Create a :py:class:`Transaction` object.

        :param isolation:
            Transaction isolation mode, can be one of:
            ``'serializable'``, ``'repeatable_read'``.  If not specified,
            the server-side default is used.

        :param readonly:
            Specifies whether or not this transaction is read-only.  If not
            specified, the server-side default is used.

        :param deferrable:
            Specifies whether or not this transaction is deferrable.  If not
            specified, the server-side default is used.


    .. py:method:: close()

        Close the connection gracefully.


    .. py:method:: is_closed()

        Return ``True`` if the connection is closed.


.. _edgedb-python-blocking-api-transaction:

Transactions
============

The most robust way to execute transactional code is to use the
``retrying_transaction()`` loop API:

.. code-block:: python

    for tx in pool.retrying_transaction():
        with tx:
            tx.execute("INSERT User { name := 'Don' }")

Note that we execute queries on the ``tx`` object in the above
example, rather than on the original connection pool ``pool``
object.

The ``retrying_transaction()`` API guarantees that:

1. Transactions are executed atomically;
2. If a transaction is failed for any of the number of transient errors
   (i.e.  a network failure or a concurrent update error), the transaction
   would be retried;
3. If any other, non-retryable exception occurs, the transaction is
   rolled back, and the exception is propagated, immediately aborting the
   ``retrying_transaction()`` block.

The key implication of retrying transactions is that the entire
nested code block can be re-run, including any non-querying
Python code. Here is an example:

.. code-block:: python

    for tx in pool.retrying_transaction():
        with tx:
            user = tx.query_single(
                "SELECT User { email } FILTER .login = <str>$login",
                login=login,
            )
            data = httpclient.get(
                'https://service.local/email_info',
                params=dict(email=user.email),
            )
            user = tx.query_single('''
                    UPDATE User FILTER .login = <str>$login
                    SET { email_info := <json>$data}
                ''',
                login=login,
                data=data,
            )

In the above example, the execution of the HTTP request would be retried
too. The core of the issue is that whenever transaction is interrupted
user might have the email changed (as the result of concurrent
transaction), so we have to redo all the work done.

Generally it's recommended to not execute any long running
code within the transaction unless absolutely necessary.

Transactions allocate expensive server resources and having
too many concurrently running long-running transactions will
negatively impact the performance of the DB server.

See also:

* RFC1004_
* :py:meth:`BlockingIOConnection.retrying_transaction()`
* :py:meth:`BlockingIOConnection.raw_transaction()`


.. py:class:: Transaction()

    Represents a transaction or savepoint block.

    Transactions are created by calling the
    :py:meth:`BlockingIOConnection.transaction()` method.


    .. py:method:: start()

        Enter the transaction or savepoint block.

    .. py:method:: commit()

        Exit the transaction or savepoint block and commit changes.

    .. py:method:: rollback()

        Exit the transaction or savepoint block and discard changes.

    .. describe:: with c:

        start and commit/rollback the transaction or savepoint block
        automatically when entering and exiting the code inside the
        context manager block.


.. py:class:: Retry

    Represents a wrapper that yields :py:class:`Transaction`
    object when iterating.

    See :py:meth:`BlockingIOConnection.retrying_transaction()` method for
    an example.

    .. py:coroutinemethod:: __next__()

        Yields :py:class:`Transaction` object every time transaction has to
        be repeated.

.. _RFC1004: https://github.com/edgedb/rfcs/blob/master/text/1004-transactions-api.rst
