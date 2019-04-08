.. _edgedb-python-examples:


Basic EdgeDB-Python Usage
=========================

**EdgeDB-Python** has two APIs: blocking and asynchronous.  Both are
almost entirely equivalent, with the exception of pool functionality, which
is currently only supported in asynchronous mode.

The interaction with the database normally starts with a call to
:func:`connect() <edgedb.blocking_con.connect>`, or
:func:`connect_async() <edgedb.asyncio_con.connect_async>`,
which establishes a new database session and returns a new
:class:`BlockingIOConnection <edgedb.blocking_con.BlockingIOConnection>`
or :class:`AsyncIOConnection <edgedb.asyncio_con.AsyncIOConnection>` instance
correspondingly.  The connection instance provides methods to run queries
and manage transactions.

Blocking connection example:


.. code-block:: python

    import edgedb

    import datetime
    import edgedb

    def main():
        # Establish a connection to an existing database named "test"
        # as an "edgedb" user.
        conn = edgedb.connect('edgedb://edgedb@localhost/test')
        # Create a User object type
        conn.execute('''
            CREATE TYPE User {
                CREATE REQUIRED PROPERTY name -> str;
                CREATE PROPERTY dob -> local_date;
            }
        ''')

        # Insert a new User object
        conn.fetchall('''
            INSERT User {
                name := <str>$name,
                dob := <local_date>$dob
            }
        ''', name='Bob', dob=datetime.date(1984, 3, 1))

        # Select User objects.
        user_set = conn.fetchall(
            'SELECT User {name, dob} FILTER .name = <str>$name', name='Bob')
        # *user_set* now contains
        # Set{Object{name := 'Bob', dob := datetime.date(1984, 3, 1)}}

        # Close the connection.
        conn.close()

    if __name__ == '__main__':
        main()


An equivalent example using the asyncio API:

.. code-block:: python

    import asyncio
    import datetime
    import edgedb

    async def main():
        # Establish a connection to an existing database named "test"
        # as an "edgedb" user.
        conn = await edgedb.async_connect('edgedb://edgedb@localhost/test')
        # Create a User object type
        await conn.execute('''
            CREATE TYPE User {
                CREATE REQUIRED PROPERTY name -> str;
                CREATE PROPERTY dob -> local_date;
            }
        ''')

        # Insert a new User object
        await conn.fetchall('''
            INSERT User {
                name := <str>$name,
                dob := <local_date>$dob
            }
        ''', name='Bob', dob=datetime.date(1984, 3, 1))

        # Select User objects.
        user_set = await conn.fetchall(
            'SELECT User {name, dob} FILTER .name = <str>$name', name='Bob')
        # *user_set* now contains
        # Set{Object{name := 'Bob', dob := datetime.date(1984, 3, 1)}}

        # Close the connection.
        await conn.close()

    if __name__ == '__main__':
        asyncio.get_event_loop().run_until_complete(main())


Type Conversion
---------------

EdgeDB-Python automatically converts EdgeDB types to the corresponding Python
types and vice versa.  See :ref:`edgedb-python-datatypes` for details.


Transactions
------------

To create transactions, the
:meth:`BlockingIOConnection.transaction()
<edgedb.blocking_con.BlockingIOConnection.transaction>` method or
its asyncio equivalent :meth:`AsyncIOConnection.transaction()
<edgedb.asyncio_con.AsyncIOConnection.transaction>`
should be used.

The most common way to use transactions is through a context manager:

.. code-block:: python

   with connection.transaction():
       connection.execute("INSERT User {name := 'Don'}")

or, if using the async API:

.. code-block:: python

   async with connection.transaction():
       await connection.execute("INSERT User {name := 'Don'}")

.. note::

   When not in an explicit transaction block, any changes to the database
   will be applied immediately.


.. _edgedb-python-connection-pool:

Connection Pools
----------------

For server-type type applications, that handle frequent requests and need
the database connection for a short period time while handling a request,
the use of a connection pool is recommended.  The EdgeDB-Python asyncio API
provides an implementation of such a pool.

To create a connection pool, use the
:func:`edgedb.create_async_pool() <edgedb.asyncio_pool.create_async_pool>`
function.  The resulting :class:`AsyncIOPool <edgedb.asyncio_pool.AsyncIOPool>`
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

        # Take a connection from the pool.
        async with pool.acquire() as connection:
            # Run the query passing the request argument.
            result = await connection.fetchone_json(
                '''
                    SELECT User {first_name, email, bio}
                    FILTER .name = <str>$username
                ''', username=username)
            return web.Response(text=result, content_type='application/json')


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

See :ref:`edgedb-python-asyncio-api-pool` API documentation for
more information.
