.. _edgedb-python-examples:

Basic Usage
===========


Connection
----------

The client library must be able to establish a connection to a running EdgeDB
instance to execute queries. Refer to the :ref:`Client Library Connection
<edgedb_client_connection>` docs for details on configuring connections.


Async vs blocking API
---------------------

This libraray provides two APIs: asynchronous and blocking. Both are
nearly equivalent, with the exception of connection pooling functionality,
which is currently only supported in asynchronous mode.

For an async client, call :py:func:`create_async_client()
<edgedb.create_async_client>` to create an instance of
:py:class:`AsyncIOClient <edgedb.AsyncIOClient>`. This class maintains
a pool of connections under the hood and provides methods for executing
queries and transactions.

For a blocking client, use :py:func:`connect() <edgedb.connect>` to create an
instance of :py:class:`BlockingIOConnection <edgedb.BlockingIOConnection>`.


Examples
--------

Blocking connection example:


.. code-block:: python

    import datetime
    import edgedb

    def main():
        # Establish a connection to an existing database
        # named "test" as an "edgedb" user.
        conn = edgedb.connect(
            'edgedb://edgedb@localhost/test')

        # Create a User object type
        conn.execute('''
            CREATE TYPE User {
                CREATE REQUIRED PROPERTY name -> str;
                CREATE PROPERTY dob -> cal::local_date;
            }
        ''')

        # Insert a new User object
        conn.query('''
            INSERT User {
                name := <str>$name,
                dob := <cal::local_date>$dob
            }
        ''', name='Bob', dob=datetime.date(1984, 3, 1))

        # Select User objects.
        user_set = conn.query(
            'SELECT User {name, dob} FILTER .name = <str>$name',
            name='Bob')

        # *user_set* now contains
        # Set{Object{name := 'Bob',
        #            dob := datetime.date(1984, 3, 1)}}
        print(user_set)

        # Close the connection.
        conn.close()

    if __name__ == '__main__':
        main()


An equivalent example using the **asyncio** API:

.. code-block:: python

    import asyncio
    import datetime
    import edgedb

    async def main():
        # Establish a connection to an existing database
        # named "test" as an "edgedb" user.
        conn = await edgedb.async_connect(
            'edgedb://edgedb@localhost/test')

        # Create a User object type
        await conn.execute('''
            CREATE TYPE User {
                CREATE REQUIRED PROPERTY name -> str;
                CREATE PROPERTY dob -> cal::local_date;
            }
        ''')

        # Insert a new User object
        await conn.query('''
            INSERT User {
                name := <str>$name,
                dob := <cal::local_date>$dob
            }
        ''', name='Bob', dob=datetime.date(1984, 3, 1))

        # Select User objects.
        user_set = await conn.query('''
            SELECT User {name, dob}
            FILTER .name = <str>$name
        ''', name='Bob')

        # *user_set* now contains
        # Set{Object{name := 'Bob',
        #            dob := datetime.date(1984, 3, 1)}}
        print(user_set)

        # Close the connection.
        await conn.aclose()

    if __name__ == '__main__':
        asyncio.run(main())


Type conversion
---------------

edgedb-python automatically converts EdgeDB types to the corresponding Python
types and vice versa.  See :ref:`edgedb-python-datatypes` for details.


.. _edgedb-python-connection-pool:

Client connection pools
-----------------------

For server-type type applications that handle frequent requests and need
the database connection for a short period time while handling a request,
the use of a connection pool is recommended.  The edgedb-python asyncio API
provides an implementation of such a pool.

To create a connection pool, use the
:py:func:`edgedb.create_async_client() <edgedb.create_async_client>`
function.  The resulting :py:class:`AsyncIOClient <edgedb.AsyncIOClient>`
object can then be used to borrow connections from the pool.

Below is an example of a connection pool usage:


.. code-block:: python

    import asyncio
    import edgedb
    from aiohttp import web


    async def handle(request):
        """Handle incoming requests."""
        client = request.app['client']
        username = int(request.match_info.get('name'))

        # Execute the query on any pool connection
        result = await client.query_single_json(
            '''
                SELECT User {first_name, email, bio}
                FILTER .name = <str>$username
            ''', username=username)
        return web.Response(
            text=result,
            content_type='application/json')


    def init_app():
        """Initialize the application server."""
        app = web.Application()
        # Create a database connection client
        app['client'] = edgedb.create_async_client(
            database='my_service',
            user='my_service')
        # Configure service routes
        app.router.add_route('GET', '/user/{name:\w+}', handle)
        return app


    loop = asyncio.get_event_loop()
    app = init_app()
    web.run_app(app)

But if you have a bunch of tightly related queries it's better to use
transactions.

Note that the client is created synchronously. Pool connections are created
lazily as they are needed. If you want to explicitly connect to the
database in ``init_app()``, use the ``ensure_connected()`` method on the client.

See :ref:`edgedb-python-asyncio-api-pool` API documentation for
more information.


Transactions
------------

The most robust way to create a
:ref:`transaction <edgedb-python-asyncio-api-transaction>` is the
``transaction()`` method:

* :py:meth:`AsyncIOClient.transaction() <edgedb.AsyncIOClient.transaction>`
* :py:meth:`BlockingIOConnection.transaction() <edgedb.BlockingIOConnection.transaction>`


Example:

.. code-block:: python

    for tx in connection.transaction():
        with tx:
            tx.execute("INSERT User {name := 'Don'}")

or, if using the async API on connection pool:

.. code-block:: python

    async for tx in connection.transaction():
        async with tx:
            await tx.execute("INSERT User {name := 'Don'}")

.. note::

   When not in an explicit transaction block, any changes to the database
   will be applied immediately.

See :ref:`edgedb-python-asyncio-api-transaction` API documentation for
more information.
