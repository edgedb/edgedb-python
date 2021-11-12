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

    Deprecated. Use ``create_async_client()`` instead.

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

.. _edgedb-python-asyncio-api-pool:

Client Connection Pool
======================

.. py:function:: create_async_client(dsn=None, *, \
            host=None, port=None, \
            admin=None, \
            user=None, password=None, \
            database=None, \
            timeout=60, \
            concurrency=None)

    Create an asynchronous lazy connection pool.

    The connection parameters may be specified either as a connection
    URI in *dsn*, or as specific keyword arguments, or both.
    If both *dsn* and keyword arguments are specified, the latter
    override the corresponding values parsed from the connection URI.

    Returns a new :py:class:`AsyncIOConnection` object.

    :param str dsn:
        If this parameter does not start with ``edgedb://`` then this is
        interpreted as the :ref:`name of a local instance
        <ref_reference_connection_instance_name>`.

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
        or the value of the ``EDGEDB_PORT`` environment variable, or ``5656``
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

    :param int concurrency:
        Max number of connections in the pool. If not set, the suggested
        concurrency value provided by the server is used.

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

        client = edgedb.create_async_client(user='edgedb')
        await client.query('SELECT {1, 2, 3}')

    Transactions can be executed as well:

    .. code-block:: python

        client = edgedb.create_async_client(user='edgedb')
        async for tx in client.transaction():
            async with tx:
                await tx.query('SELECT {1, 2, 3}')


.. py:class:: AsyncIOClient()

    A connection pool.

    A connection pool can be used in a similar manner as a single connection
    except that the pool is safe for concurrent use.

    Pools are created by calling
    :py:func:`~edgedb.create_client`.

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
