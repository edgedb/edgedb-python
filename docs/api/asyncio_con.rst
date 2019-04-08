.. _edgedb-python-asyncio-api-reference:

=====================
AsyncIO API Reference
=====================

.. module:: edgedb.asyncio_con

.. currentmodule:: edgedb.asyncio_con


.. _edgedb-asyncio-api-connection:

Connection
==========

.. coroutinefunction:: async_connect(dsn=None, *, \
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

    Returns a new :class:`~AsyncIOConnection` object.

    :param dsn:
        Connection arguments specified using as a single string in the
        connection URI format:
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

    :return: A :class:`~edgedb.asyncio_con.AsyncIOConnection` instance.

    Example:

    .. code-block:: pycon

        >>> import asyncio
        >>> import edgedb
        >>> async def run():
        ...     con = await edgedb.async_connect(user='edgedeb')
        ...     print(await con.fetchone('SELECT 1 + 1'))
        ...
        >>> asyncio.get_event_loop().run_until_complete(run())
        {2}


.. class:: AsyncIOConnection

    A representation of a database session.

    Connections are created by calling :func:`~edgedb.asyncio_con.connect`.


    .. coroutinemethod:: fetchall(query, *args, **kwargs)

        Run a query and return the results as a
        :class:`edgedb.Set <edgedb.types.Set>` instance.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        .. note::

            Positional and named query arguments cannot be mixed.

        :return:
            An instance of :class:`edgedb.Set <edgedb.types.Set>` containing
            the query result.


    .. coroutinemethod:: fetchone(query, *args, **kwargs)

        Run a singleton-returning query and return its element.

        The *query* must return exactly one element.  If the query returns
        more than one element, a :exc:`edgedb.ResultCardinalityMismatchError`
        is raised, if it returns an empty set, a :exc:`edgedb.NoDataError`
        is raised.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        .. note::

            Positional and named query arguments cannot be mixed.

        :return:
            Query result.


    .. coroutinemethod:: fetchall_json(query, *args, **kwargs)

        Run a query and return the results as JSON.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        .. note::

            Positional and named query arguments cannot be mixed.

        :return:
            A JSON string containing an array of query results.


    .. coroutinemethod:: fetchone_json(query, *args, **kwargs)

        Run a singleton-returning query and return its element in JSON.

        The *query* must return exactly one element.  If the query returns
        more than one element, a :exc:`edgedb.ResultCardinalityMismatchError`
        is raised, if it returns an empty set, a :exc:`edgedb.NoDataError`
        is raised.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        .. note::

            Positional and named query arguments cannot be mixed.

        :return:
            Query result encoded in JSON.


    .. coroutinemethod:: execute(query)

        Execute an EdgeQL command (or commands).

        The commands must take no arguments.

        Example:

        .. code-block:: pycon

            >>> await con.execute('''
            ...     CREATE TYPE MyType { CREATE PROPERTY a -> int64 };
            ...     FOR x IN {100, 200, 300} UNION INSERT MyType { a := x };
            ... ''')

        :param str query: Query text.


    .. method:: transaction(isolation=None, readonly=None, deferrable=None)

        Create a :class:`~transaction.Transaction` object.

        :param isolation:
            Transaction isolation mode, can be one of:
            `'serializable'`, `'repeatable_read'`.  If not specified,
            the server-side default is used.

        :param readonly:
            Specifies whether or not this transaction is read-only.  If not
            specified, the server-side default is used.

        :param deferrable:
            Specifies whether or not this transaction is deferrable.  If not
            specified, the server-side default is used.


    .. coroutinemethod:: close()

        Close the connection gracefully.


    .. method:: is_closed()

        Return ``True`` if the connection is closed.


.. _edgedb-python-asyncio-api-transaction:

Transactions
============

The most common way to use transactions is through a context manager statement:

.. code-block:: python

   async with connection.transaction():
       await connection.execute("INSERT User { name := 'Don' }")

It is possible to nest transactions (a nested transaction context will create
a savepoint):

.. code-block:: python

   async with connection.transaction():
       await connection.execute(
           'CREATE TYPE User { CREATE PROPERTY name -> str }')

       try:
           # Create a savepoint:
           async with connection.transaction():
               await connection.execute("INSERT User { name := 'Don' }")
               # This nested savepoint will be automatically rolled back:
               raise Exception
       except:
           # Ignore exception
           pass

       # Because the nested savepoint was rolled back, there
       # will be nothing in `User`.
       assert (await connection.fetchall('SELECT User')) == []

Alternatively, transactions can be used without an ``async with`` block:

.. code-block:: python

    tr = connection.transaction()
    await tr.start()
    try:
        ...
    except:
        await tr.rollback()
        raise
    else:
        await tr.commit()


See also the
:meth:`AsyncIOConnection.transaction()` function.


.. class:: edgedb.transaction.Transaction()

    Represents a transaction or savepoint block.

    Transactions are created by calling the
    :meth:`AsyncIOConnection.transaction()` method.


    .. coroutinemethod:: start()

        Enter the trasnaction or savepoint block.

    .. coroutinemethod:: commit()

        Exit the transaction or savepoint block and commit changes.

    .. coroutinemethod:: rollback()

        Exit the transaction or savepoint block and discard changes.

    .. describe:: async with c:

        start and commit/rollback the transaction or savepoint block
        automatically when entering and exiting the code inside the
        context manager block.


.. _edgedb-python-asyncio-api-pool:

Connection Pools
================

.. function:: edgedb.asyncio_pool.create_async_pool

    Create an asynchronous connection pool.

    Can be used either with an ``async with`` block:

    .. code-block:: python

        async with edgedb.create_pool(user='edgedb') as pool:
            async with pool.acquire() as con:
                await con.fetchall('SELECT {1, 2, 3}')

    Or directly with ``await``:

    .. code-block:: python

        pool = await edgedb.create_pool(user='edgedb')
        con = await pool.acquire()
        try:
            await con.fetchall('SELECT {1, 2, 3}')
        finally:
            await pool.release(con)

    :param str dsn:
        Connection arguments specified using as a single string in
        the following format:
        ``edgedb://user:pass@host:port/database?option=value``.

    :param \*\*connect_kwargs:
        Keyword arguments for the :func:`~edgedb.async_connect`
        function.

    :param Connection connection_class:
        The class to use for connections.  Must be a subclass of
        :class:`~edgedb.asyncio_con.AsyncIOConnection`.

    :param int min_size:
        Number of connection the pool will be initialized with.

    :param int max_size:
        Max number of connections in the pool.

    :param coroutine on_acquire:
        A coroutine to prepare a connection right before it is returned
        from :meth:`Pool.acquire() <pool.Pool.acquire>`.

    :param coroutine on_release:
        A coroutine called when a connection is about to be released
        to the pool.

    :param coroutine on_connect:
        A coroutine to initialize a connection when it is created.

    :return: An instance of :class:`~edgedb.asyncio_pool.Pool`.


.. class:: edgedb.asyncio_pool.Pool()

    A connection pool.

    Connection pool can be used to manage a set of connections to the database.
    Connections are first acquired from the pool, then used, and then released
    back to the pool.  Once a connection is released, it's reset to close all
    open cursors and other resources *except* prepared statements.

    Pools are created by calling
    :func:`~edgedb.asyncio_pool.create_async_pool`.

    .. coroutinemethod:: acquire()

        Acquire a database connection from the pool.

        :return: An instance of :class:`~edgedb.asyncio_con.AsyncIOConnection`.

        Can be used in an ``await`` expression or with an ``async with`` block.

        .. code-block:: python

            async with pool.acquire() as con:
                await con.execute(...)

        Or:

        .. code-block:: python

            con = await pool.acquire()
            try:
                await con.execute(...)
            finally:
                await pool.release(con)

    .. coroutinemethod:: release(connection)

        Release a database connection back to the pool.

        :param AsyncIOConnection connection:
            A :class:`~edgedb.asyncio_con.AsyncIOConnection` object
            to release.

    .. coroutinemethod:: close()

        Attempt to gracefully close all connections in the pool.

        Wait until all pool connections are released, close them and
        shut down the pool.  If any error (including cancellation) occurs
        in ``close()`` the pool will terminate by calling
        :meth:`Pool.terminate() <edgedb.asyncio_pool.Pool.terminate>`.

        It is advisable to use :func:`python:asyncio.wait_for` to set
        a timeout.

    .. method:: terminate()

        Terminate all connections in the pool.

    .. coroutinemethod:: expire_connections()

        Expire all currently open connections.

        Cause all currently open connections to get replaced on the
        next :meth:`~edgedb.asyncio_pool.Pool.acquire()` call.

    .. method:: set_connect_args(dsn=None, **connect_kwargs)

        Set the new connection arguments for this pool.

        The new connection arguments will be used for all subsequent
        new connection attempts.  Existing connections will remain until
        they expire. Use :meth:`Pool.expire_connections()
        <edgedb.asyncio_pool.Pool.expire_connections>` to expedite
        the connection expiry.

        :param str dsn:
            Connection arguments specified using as a single string in
            the following format:
            ``edgedb://user:pass@host:port/database?option=value``.

        :param \*\*connect_kwargs:
            Keyword arguments for the :func:`~async_connect`
            function.
