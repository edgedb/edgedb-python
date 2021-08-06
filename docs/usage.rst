.. _edgedb-python-examples:


Basic Usage
===========

**edgedb-python** has two APIs: blocking and asynchronous.  Both are
almost entirely equivalent, with the exception of pool functionality, which
is currently only supported in asynchronous mode.

The interaction with the database normally starts with a call to
:py:func:`connect() <edgedb.connect>`, or
:py:func:`async_connect() <edgedb.async_connect>`,
which establishes a new database session and returns a new
:py:class:`BlockingIOConnection <edgedb.BlockingIOConnection>`
or :py:class:`AsyncIOConnection <edgedb.AsyncIOConnection>` instance
correspondingly.  The connection instance provides methods to run queries
and manage transactions.

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


Type Conversion
---------------

edgedb-python automatically converts EdgeDB types to the corresponding Python
types and vice versa.  See :ref:`edgedb-python-datatypes` for details.


.. _edgedb-python-connection-pool:

Connection Pools
----------------

For server-type type applications that handle frequent requests and need
the database connection for a short period time while handling a request,
the use of a connection pool is recommended.  The edgedb-python asyncio API
provides an implementation of such a pool.

To create a connection pool, use the
:py:func:`edgedb.create_async_pool() <edgedb.create_async_pool>`
function.  The resulting :py:class:`AsyncIOPool <edgedb.AsyncIOPool>`
object can then be used to borrow connections from the pool.

Below is an example of a connection pool usage:


.. code-block:: python

    import asyncio
    import edgedb
    from aiohttp import web


    async def handle(request):
        """Handle incoming requests."""
        pool = request.app['pool']
        username = int(request.match_info.get('name'))

        # Execute the query on any pool connection
        result = await pool.query_single_json(
            '''
                SELECT User {first_name, email, bio}
                FILTER .name = <str>$username
            ''', username=username)
        return web.Response(
            text=result,
            content_type='application/json')


    async def init_app():
        """Initialize the application server."""
        app = web.Application()
        # Create a database connection pool
        app['pool'] = await edgedb.create_async_pool(
            database='my_service',
            user='my_service')
        # Configure service routes
        app.router.add_route('GET', '/user/{name:\w+}', handle)
        return app


    loop = asyncio.get_event_loop()
    app = loop.run_until_complete(init_app())
    web.run_app(app)

You can also acquire connection from the pool:

.. code-block:: python

    async with pool.acquire() as conn:
        result = await conn.query_single_json(
            '''
                SELECT User {first_name, email, bio}
                FILTER .name = <str>$username
            ''', username=username)

But if you have a bunch of tightly related queries it's better to use
transactions.

See :ref:`edgedb-python-asyncio-api-pool` API documentation for
more information.


Transactions
------------

The most robust way to create a
:ref:`transaction <edgedb-python-asyncio-api-transaction>` is ``retry`` method:

* :py:meth:`AsyncIOPool.retrying_transaction() <edgedb.AsyncIOPool.retrying_transaction>`
* :py:meth:`BlockingIOConnection.retrying_transaction() <edgedb.BlockingIOConnection.retrying_transaction>`
* :py:meth:`AsyncIOConnection.retrying_transaction() <edgedb.AsyncIOConnection.retrying_transaction>`

Example:

.. code-block:: python

    for tx in connection.retrying_transaction():
        with tx:
            tx.execute("INSERT User {name := 'Don'}")

or, if using the async API on connection pool:

.. code-block:: python

    async for tx in connection.retrying_transaction():
        async with tx:
            await tx.execute("INSERT User {name := 'Don'}")

.. note::

   When not in an explicit transaction block, any changes to the database
   will be applied immediately.

See :ref:`edgedb-python-asyncio-api-transaction` API documentation for
more information.
