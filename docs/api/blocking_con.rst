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
        >>> con.fetchone('SELECT 1 + 1')
        {2}


.. py:class:: BlockingIOConnection

    A representation of a database session.

    Connections are created by calling :py:func:`~edgedb.connect`.


    .. py:method:: fetchall(query, *args, **kwargs)

        Run a query and return the results as a
        :py:class:`edgedb.Set <edgedb.Set>` instance.

        :param str query: Query text.
        :param args: Positional query arguments.
        :param kwargs: Named query arguments.

        :return:
            An instance of :py:class:`edgedb.Set <edgedb.Set>` containing
            the query result.

        Note, that positional and named query arguments cannot be mixed.


    .. py:method:: fetchone(query, *args, **kwargs)

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


    .. py:method:: fetchall_json(query, *args, **kwargs)

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


    .. py:method:: fetchone_json(query, *args, **kwargs)

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


    .. py:method:: transaction(isolation=None, readonly=None, deferrable=None)

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

The most common way to use transactions is through a context statement:

.. code-block:: python

   with connection.transaction():
       connection.execute("INSERT User { name := 'Don' }")

It is possible to nest transactions (a nested transaction context will create
a savepoint):

.. code-block:: python

   with connection.transaction():
       connection.execute(
           'CREATE TYPE User { CREATE PROPERTY name -> str }')

       try:
           # Create a savepoint:
           with connection.transaction():
               connection.execute(
                   "INSERT User { name := 'Don' }")
               # This nested savepoint will be
               # automatically rolled back:
               raise Exception
       except:
           # Ignore exception
           pass

       # Because the nested savepoint was rolled back, there
       # will be nothing in `User`.
       assert connection.fetchall('SELECT User') == []

Alternatively, transactions can be used without a ``with`` block:

.. code-block:: python

    tr = connection.transaction()
    tr.start()
    try:
        ...
    except:
        tr.rollback()
        raise
    else:
        tr.commit()


See also the
:py:meth:`BlockingIOConnection.transaction()` function.


.. py:class:: Transaction()

    Represents a transaction or savepoint block.

    Transactions are created by calling the
    :py:meth:`BlockingIOConnection.transaction()` method.


    .. py:method:: start()

        Enter the trasnaction or savepoint block.

    .. py:method:: commit()

        Exit the transaction or savepoint block and commit changes.

    .. py:method:: rollback()

        Exit the transaction or savepoint block and discard changes.

    .. describe:: with c:

        start and commit/rollback the transaction or savepoint block
        automatically when entering and exiting the code inside the
        context manager block.
