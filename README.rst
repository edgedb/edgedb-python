EdgeDB-Python -- The Python driver for EdgeDB
=============================================

**Edgedb-Python** is the official EdgeDB driver for Python.  It supports
both blocking and asyncio programming paradigms.

EdgeDB-Python requires Python 3.6 or later.


Installation
------------

EdgeDB-Python is available on PyPI.  Use pip to install::

    $ pip install edgedb


Basic Usage
-----------

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


License
-------

EdgeDB-Python is developed and distributed under the Apache 2.0 license.
