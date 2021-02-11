.. _edgedb-instances:

Instance Names
==============

Here are some ways to connect to an EdgeDB instance by a name:

.. code-block:: python

   conn = edgedb.connect('my_name')
   conn = await edgedb.async_connect('my_name')
   pool = await edgedb.create_async_pool('my_name')

This usually refers to instances created by the command-line tool:

.. code-block:: shell

   edgedb server init my_name

When the command is run, it puts a credentials file into the user's 
home directory::

    $HOME/.edgedb/credentials/my_name.json

This file is read by Python bindings to discover the database instance.

You are free to add additional JSON files with access to remote databases into
the ``credentials`` directory.
