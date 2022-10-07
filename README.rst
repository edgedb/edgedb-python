The Python driver for EdgeDB
============================

.. image:: https://github.com/edgedb/edgedb-python/workflows/Tests/badge.svg?event=push&branch=master
    :target: https://github.com/edgedb/edgedb-python/actions

.. image:: https://img.shields.io/pypi/v/edgedb.svg
    :target: https://pypi.python.org/pypi/edgedb

.. image:: https://img.shields.io/badge/join-github%20discussions-green
    :target: https://github.com/edgedb/edgedb/discussions


**edgedb-python** is the official EdgeDB driver for Python.
It provides both blocking IO and asyncio implementations.

The library requires Python 3.7 or later.


Documentation
-------------

The project documentation can be found
`here <https://edgedb.com/docs/clients/00_python/index>`_.


Installation
------------

The library is available on PyPI.  Use ``pip`` to install it::

    $ pip install edgedb


Basic Usage
-----------

.. code-block:: python

    import datetime
    import edgedb

    def main():
        client = edgedb.create_client()
        # Create a User object type
        client.execute('''
            CREATE TYPE User {
                CREATE REQUIRED PROPERTY name -> str;
                CREATE PROPERTY dob -> cal::local_date;
            }
        ''')

        # Insert a new User object
        client.query('''
            INSERT User {
                name := <str>$name,
                dob := <cal::local_date>$dob
            }
        ''', name='Bob', dob=datetime.date(1984, 3, 1))

        # Select User objects.
        user_set = client.query(
            'SELECT User {name, dob} FILTER .name = <str>$name', name='Bob')
        # *user_set* now contains
        # Set{Object{name := 'Bob', dob := datetime.date(1984, 3, 1)}}

        # Close the client.
        client.close()

    if __name__ == '__main__':
        main()

Development
-----------

Instructions for installing EdgeDB and edgedb-python locally can be found at
`edgedb.com/docs/reference/dev <https://edgedb.com/docs/reference/dev>`_.

To run the test suite, run ``$ python setup.py test``.

License
-------

edgedb-python is developed and distributed under the Apache 2.0 license.
