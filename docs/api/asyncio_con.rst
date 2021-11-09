.. _edgedb-python-asyncio-api-reference:

===========
AsyncIO API
===========

.. py:currentmodule:: edgedb


.. _edgedb-asyncio-api-connection:

Connection
==========

.. py:coroutinefunction:: async_connect(dsn=None, *, \
            host=None, port=None, \
            admin=None, \
            user=None, password=None, \
            database=None, \
            timeout=60)

    Establish a connection to an EdgeDB server.

    The connection parameters may be specified either as a connection
    URI in *dsn*, or as specific keyword arguments, or both.
    If both *dsn* and keyword arguments are specified, the latter
    override the corresponding values parsed from the connection URI.

    Returns a new :py:class:`AsyncIOConnection` object.

    :param str dsn:
        If this parameter does not start with ``edgedb://`` then this is
        a :ref:`name of an instance <edgedb-python-connection>`.

        Otherwise it specifies a single string in the following format:
        ``edgedb://user:password@host:port/database?option=value``.
        The following options are recognized: host, port,
        user, database, password. For a complete reference on DSN, see
        the :ref:`DSN Specification <ref_dsn>`.

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

    :return: A :py:class:`AsyncIOConnection` instance.

    Example:

    .. code-block:: pycon

        >>> import asyncio
        >>> import edgedb
        >>> async def main():
        ...     con = await edgedb.async_connect(user='edgedb')
        ...     print(await con.query_single('SELECT 1 + 1'))
        ...
        >>> asyncio.run(main())
        {2}


.. py:class:: AsyncIOConnection

    A representation of a database session.

    Connections are created by calling :py:func:`~edgedb.async_connect`.


    .. py:coroutinemethod:: query(query, *args, **kwargs)

        Run a query and return the results as a
        :py:class:`edgedb.Set <edgedb.Set>` instance.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            An instance of :py:class:`edgedb.Set <edgedb.Set>` containing
            the query result.

        Note that positional and named query arguments cannot be mixed.


    .. py:coroutinemethod:: query_single(query, *args, **kwargs)

        Run an optional singleton-returning query and return its element.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            Query result.

        The *query* must return no more than one element.  If the query returns
        more than one element, an ``edgedb.ResultCardinalityMismatchError``
        is raised, if it returns an empty set, ``None`` is returned.

        Note, that positional and named query arguments cannot be mixed.


    .. py:coroutinemethod:: query_required_single(query, *args, **kwargs)

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


    .. py:coroutinemethod:: query_json(query, *args, **kwargs)

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


    .. py:coroutinemethod:: query_single_json(query, *args, **kwargs)

        Run an optional singleton-returning query and return its element
        in JSON.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            Query result encoded in JSON.

        The *query* must return no more than one element.  If the query returns
        more than one element, an ``edgedb.ResultCardinalityMismatchError``
        is raised, if it returns an empty set, ``"null"`` is returned.

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


    .. py:coroutinemethod:: query_required_single_json(query, *args, **kwargs)

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


    .. py:coroutinemethod:: execute(query)

        Execute an EdgeQL command (or commands).

        :param str query: Query text.

        The commands must take no arguments.

        Example:

        .. code-block:: pycon

            >>> await con.execute('''
            ...     CREATE TYPE MyType {
            ...         CREATE PROPERTY a -> int64
            ...     };
            ...     FOR x IN {100, 200, 300}
            ...     UNION INSERT MyType { a := x };
            ... ''')

        .. note::
            If the results of *query* are desired, :py:meth:`query`,
            :py:meth:`query_single` or :py:meth:`query_required_single`
            should be used instead.

    .. py:method:: transaction()

        Start a transaction with auto-retry semantics.

        This is the preferred method of initiating and running a database
        transaction in a robust fashion.  The ``transaction()``
        transaction loop will attempt to re-execute the transaction loop
        body if a transient error occurs, such as a network error or a
        transaction serialization error.

        Returns an instance of :py:class:`AsyncIORetry`.

        See :ref:`edgedb-python-asyncio-api-transaction` for more details.

        Example:

        .. code-block:: python

            async for tx in con.transaction():
                async with tx:
                    value = await tx.query_single("SELECT Counter.value")
                    await tx.execute(
                        "UPDATE Counter SET { value := <int64>$value }",
                        value=value + 1,
                    )

        Note that we are executing queries on the ``tx`` object rather
        than on the original connection.

    .. py:method:: raw_transaction()

        **Deprecated**. Use :py:meth:`transaction` along with
        ``with_retry_options(RetryOptions(attempts=1))`` instead.

        Start a low-level transaction.

        Unlike ``transaction()``, ``raw_transaction()``
        will not attempt to re-run the nested code block in case a retryable
        error happens.

        This is a low-level API and it is advised to use the
        ``transaction()`` method instead.

        A call to ``raw_transaction()`` returns
        :py:class:`AsyncIOTransaction`.

        Example:

        .. code-block:: python

            async with con.raw_transaction() as tx:
                value = await tx.query_single("SELECT Counter.value")
                await tx.execute(
                    "UPDATE Counter SET { value := <int64>$value }",
                    value=value + 1,
                )

        Note that we are executing queries on the ``tx`` object,
        rather than on the original connection ``con``.


    .. py:coroutinemethod:: aclose()

        Close the connection gracefully.


    .. py:method:: is_closed()

        Return ``True`` if the connection is closed.


.. _edgedb-python-asyncio-api-pool:

Client Connection Pool
======================

.. py:function:: create_client

    Create an asynchronous lazy connection pool.

    :param str dsn:
        Connection arguments specified using as a single string in
        the following format:
        ``edgedb://user:pass@host:port/database?option=value``.

    :param \*\*connect_kwargs:
        Keyword arguments for the :py:func:`~edgedb.async_connect`
        function.

    :param AsyncIOConnection connection_class:
        The class to use for connections.  Must be a subclass of
        :py:class:`AsyncIOConnection`.

    :param int concurrency:
        Max number of connections in the pool. If not set, the suggested
        concurrency value provided by the server is used.

    :param on_acquire:
        **Deprecated**. Use the query methods on
        :py:class:`Client <edgedb.AsyncIOClient>` instead of
        acquiring a connection.

        A coroutine to prepare a connection right before it is returned
        from :py:meth:`Client.acquire() <edgedb.AsyncIOClient.acquire>`.

    :param on_release:
        **Deprecated**. Use the query methods on
        :py:class:`Client <edgedb.AsyncIOClient>` instead of
        acquiring a connection.

        A coroutine called when a connection is about to be released
        to the pool.

    :param on_connect:
        A coroutine to initialize a connection when it is created.

    :return: An instance of :py:class:`AsyncIOClient`.

    The connection pool has high-level APIs to access Connection[link]
    APIs directly, without manually acquiring and releasing connections
    from the pool:

    * :py:meth:`AsyncIOClient.query()`
    * :py:meth:`AsyncIOClient.query_single()`
    * :py:meth:`AsyncIOClient.query_required_single()`
    * :py:meth:`AsyncIOClient.query_json()`
    * :py:meth:`AsyncIOClient.query_single_json()`
    * :py:meth:`AsyncIOClient.query_required_single_json()`
    * :py:meth:`AsyncIOClient.execute()`
    * :py:meth:`AsyncIOClient.transaction()`

    .. code-block:: python

        async with edgedb.create_client(user='edgedb') as client:
            await client.query('SELECT {1, 2, 3}')

    Transactions can be executed as well:

    .. code-block:: python

        async with edgedb.create_client(user='edgedb') as client:
            async for tx in client.transaction():
                async with tx:
                    await tx.query('SELECT {1, 2, 3}')


.. py:class:: AsyncIOClient()

    A connection pool.

    A connection pool can be used in a similar manner as a single connection
    except that the pool is safe for concurrent use.

    Pools are created by calling
    :py:func:`~edgedb.create_client`.

    .. py:coroutinemethod:: acquire()

        **Deprecated**. Use the query methods on
        :py:class:`Client <edgedb.AsyncIOClient>` instead of
        acquiring a connection.

        Acquire a database connection from the pool.

        :return: An instance of :py:class:`AsyncIOConnection`.

        Can be used in an ``await`` expression or with an ``async with`` block.

        .. code-block:: python

            async with client.acquire() as con:
                await con.execute(...)

        Or:

        .. code-block:: python

            con = await client.acquire()
            try:
                await con.execute(...)
            finally:
                await client.release(con)

    .. py:coroutinemethod:: release(connection)

        **Deprecated**. Use the query methods on
        :py:class:`Client <edgedb.AsyncIOClient>` instead of
        acquiring a connection.

        Release a database connection back to the pool.

        :param AsyncIOConnection connection:
            A :py:class:`AsyncIOConnection` object
            to release.

    .. py:coroutinemethod:: aclose()

        Attempt to gracefully close all connections in the pool.

        Wait until all pool connections are released, close them and
        shut down the pool.  If any error (including cancellation) occurs
        in ``close()`` the pool will terminate by calling
        :py:meth:`Client.terminate() <edgedb.AsyncIOClient.terminate>`.

        It is advisable to use :py:func:`python:asyncio.wait_for` to set
        a timeout.

    .. py:method:: terminate()

        Terminate all connections in the pool.

    .. py:coroutinemethod:: expire_connections()

        Expire all currently open connections.

        Cause all currently open connections to get replaced on the
        next :py:meth:`~edgedb.AsyncIOClient.acquire()` call.

    .. py:method:: set_connect_args(dsn=None, **connect_kwargs)

        Set the new connection arguments for this pool.

        :param str dsn:
            If this parameter does not start with ``edgedb://`` then this is
            a :ref:`name of an instance <edgedb-python-connection>`.

            Otherwise it specifies a single string in the following format:
            ``edgedb://user:pass@host:port/database?option=value``.

        :param \*\*connect_kwargs:
            Keyword arguments for the :py:func:`~async_connect`
            function.

        The new connection arguments will be used for all subsequent
        new connection attempts.  Existing connections will remain until
        they expire. Use :py:meth:`Client.expire_connections()
        <edgedb.AsyncIOClient.expire_connections>` to expedite
        the connection expiry.

    .. py:coroutinemethod:: ensure_connected()

        If the client does not yet have any open connections in its pool,
        attempts to open a connection, else returns immediately.

        Since the client lazily creates new connections as needed (up to the
        configured ``concurrency`` limit), the first connection attempt will
        only occur when the first query is run on a client. ``ensureConnected``
        can be useful to catch any errors resulting from connection
        mis-configuration by triggering the first connection attempt
        explicitly.

    .. py:coroutinemethod:: query(query, *args, **kwargs)

        Acquire a connection and use it to run a query and return the results
        as an :py:class:`edgedb.Set <edgedb.Set>` instance. The temporary
        connection is automatically returned back to the pool.

        See :py:meth:`AsyncIOConnection.query()
        <edgedb.AsyncIOConnection.query>` for details.

    .. py:coroutinemethod:: query_single(query, *args, **kwargs)

        Acquire a connection and use it to run an optional singleton-returning
        query and return its element. The temporary connection is automatically
        returned back to the pool.

        See :py:meth:`AsyncIOConnection.query_single()
        <edgedb.AsyncIOConnection.query_single>` for details.

    .. py:coroutinemethod:: query_required_single(query, *args, **kwargs)

        Acquire a connection and use it to run a singleton-returning query
        and return its element. The temporary connection is automatically
        returned back to the pool.

        See :py:meth:`AsyncIOConnection.query_required_single()
        <edgedb.AsyncIOConnection.query_required_single>` for details.

    .. py:coroutinemethod:: query_json(query, *args, **kwargs)

        Acquire a connection and use it to run a query and
        return the results as JSON. The temporary connection is automatically
        returned back to the pool.

        See :py:meth:`AsyncIOConnection.query_json()
        <edgedb.AsyncIOConnection.query_json>` for details.

    .. py:coroutinemethod:: query_single_json(query, *args, **kwargs)

        Acquire a connection and use it to run an optional singleton-returning
        query and return its element in JSON. The temporary connection is
        automatically returned back to the pool.

        See :py:meth:`AsyncIOConnection.query_single_json()
        <edgedb.AsyncIOConnection.query_single_json>` for details.

    .. py:coroutinemethod:: query_required_single_json(query, *args, **kwargs)

        Acquire a connection and use it to run a singleton-returning
        query and return its element in JSON. The temporary connection is
        automatically returned back to the pool.

        See :py:meth:`AsyncIOConnection.query_required_single_json()
        <edgedb.AsyncIOConnection.query_required_single_json>` for details.

    .. py:coroutinemethod:: execute(query)

        Acquire a connection and use it to execute an EdgeQL command
        (or commands).  The temporary connection is automatically
        returned back to the pool.

        See :py:meth:`AsyncIOConnection.execute()
        <edgedb.AsyncIOConnection.execute>` for details.

    .. py:method:: transaction()

        Open a retryable transaction loop.

        This is the preferred method of initiating and running a database
        transaction in a robust fashion.  The ``transaction()``
        transaction loop will attempt to re-execute the transaction loop body
        if a transient error occurs, such as a network error or a transaction
        serialization error.

        Returns an instance of :py:class:`AsyncIORetry`.

        See :ref:`edgedb-python-asyncio-api-transaction` for more details.

        Example:

        .. code-block:: python

            async for tx in client.transaction():
                async with tx:
                    value = await tx.query_single("SELECT Counter.value")
                    await tx.execute(
                        "UPDATE Counter SET { value := <int64>$value",
                        value=value,
                    )

        Note that we are executing queries on the ``tx`` object rather
        than on the original client.

    .. py:method:: raw_transaction()

        **Deprecated**. Use :py:meth:`transaction` along with
        ``with_retry_options(RetryOptions(attempts=1))`` instead.

        Execute a non-retryable transaction.

        Contrary to ``transaction()``, ``raw_transaction()``
        will not attempt to re-run the nested code block in case a retryable
        error happens.

        This is a low-level API and it is advised to use the
        ``transaction()`` method instead.

        A call to ``raw_transaction()`` returns :py:class:`AsyncIOTransaction`.

        Example:

        .. code-block:: python

            async with client.raw_transaction() as tx:
                value = await tx.query_single("SELECT Counter.value")
                await tx.execute(
                    "UPDATE Counter SET { value := <int64>$value",
                    value=value,
                )

        Note executing queries on ``tx`` object rather than the original
        client.


.. _edgedb-python-asyncio-api-transaction:

Transactions
============

The most robust way to execute transactional code is to use
the ``transaction()`` loop API:

.. code-block:: python

    async for tx in client.transaction():
        async with tx:
            await tx.execute("INSERT User { name := 'Don' }")

Note that we execute queries on the ``tx`` object in the above
example, rather than on the original connection pool ``client``
object.

The ``transaction()`` API guarantees that:

1. Transactions are executed atomically;
2. If a transaction is failed for any of the number of transient errors (i.e.
   a network failure or a concurrent update error), the transaction would
   be retried;
3. If any other, non-retryable exception occurs, the transaction is rolled
   back, and the exception is propagated, immediately aborting the
   ``transaction()`` block.

The key implication of retrying transactions is that the entire
nested code block can be re-run, including any non-querying
Python code. Here is an example:

.. code-block:: python

    async for tx in client.transaction():
        async with tx:
            user = await tx.query_single(
                "SELECT User { email } FILTER .login = <str>$login",
                login=login,
            )
            data = await httpclient.get(
                'https://service.local/email_info',
                params=dict(email=user.email),
            )
            user = await tx.query_single('''
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
* :py:meth:`AsyncIOClient.transaction()`
* :py:meth:`AsyncIOClient.raw_transaction()`
* :py:meth:`AsyncIOConnection.transaction()`
* :py:meth:`AsyncIOConnection.raw_transaction()`


.. py:class:: AsyncIOTransaction

    Represents a transaction or a savepoint block.

    Instances of this type are created by calling the
    :py:meth:`AsyncIOConnection.raw_transaction()` method.


    .. py:coroutinemethod:: start()

        Start a transaction or create a savepoint.

    .. py:coroutinemethod:: commit()

        Exit the transaction or savepoint block and commit changes.

    .. py:coroutinemethod:: rollback()

        Exit the transaction or savepoint block and discard changes.

    .. describe:: async with c:

        Start and commit/rollback the transaction or savepoint block
        automatically when entering and exiting the code inside the
        context manager block.

    .. py:coroutinemethod:: query(query, *args, **kwargs)

        Acquire a connection and use it to run a query and return the results
        as an :py:class:`edgedb.Set <edgedb.Set>` instance. The temporary
        connection is automatically returned back to the pool.

        See :py:meth:`AsyncIOConnection.query()
        <edgedb.AsyncIOConnection.query>` for details.

    .. py:coroutinemethod:: query_single(query, *args, **kwargs)

        Acquire a connection and use it to run an optional singleton-returning
        query and return its element. The temporary connection is automatically
        returned back to the pool.

        See :py:meth:`AsyncIOConnection.query_single()
        <edgedb.AsyncIOConnection.query_single>` for details.

    .. py:coroutinemethod:: query_required_single(query, *args, **kwargs)

        Acquire a connection and use it to run a singleton-returning query
        and return its element. The temporary connection is automatically
        returned back to the pool.

        See :py:meth:`AsyncIOConnection.query_required_single()
        <edgedb.AsyncIOConnection.query_required_single>` for details.

    .. py:coroutinemethod:: query_json(query, *args, **kwargs)

        Acquire a connection and use it to run a query and
        return the results as JSON. The temporary connection is automatically
        returned back to the pool.

        See :py:meth:`AsyncIOConnection.query_json()
        <edgedb.AsyncIOConnection.query_json>` for details.

    .. py:coroutinemethod:: query_single_json(query, *args, **kwargs)

        Acquire a connection and use it to run an optional singleton-returning
        query and return its element in JSON. The temporary connection is
        automatically returned back to the pool.

        See :py:meth:`AsyncIOConnection.query_single_json()
        <edgedb.AsyncIOConnection.query_single_json>` for details.

    .. py:coroutinemethod:: query_required_single_json(query, *args, **kwargs)

        Acquire a connection and use it to run a singleton-returning
        query and return its element in JSON. The temporary connection is
        automatically returned back to the pool.

        See :py:meth:`AsyncIOConnection.query_requried_single_json()
        <edgedb.AsyncIOConnection.query_required_single_json>` for details.

    .. py:coroutinemethod:: execute(query)

        Acquire a connection and use it to execute an EdgeQL command
        (or commands).  The temporary connection is automatically
        returned back to the pool.

        See :py:meth:`AsyncIOConnection.execute()
        <edgedb.AsyncIOConnection.execute>` for details.


.. py:class:: AsyncIORetry

    Represents a wrapper that yields :py:class:`AsyncIOTransaction`
    object when iterating.

    See :py:meth:`AsyncIOConnection.transaction()`
    method for an example.

    .. py:coroutinemethod:: __anext__()

        Yields :py:class:`AsyncIOTransaction` object every time transaction
        has to be repeated.

.. _RFC1004: https://github.com/edgedb/rfcs/blob/master/text/1004-transactions-api.rst
