.. _edgedb-python-connection:

Connection
----------

There are a couple ways to provide connection information to the client
library.

- If you've :ref:`initialized a project <ref_guide_using_projects>`, the
  linked instance will be auto-discovered by the library.
- Pass the name of a local instance to :py:func:`edgedb.create_client()
  <edgedb.create_client>`. You can create new
  instances with :ref:`the CLI <ref_cli_edgedb_instance_create>`.

  .. code-block:: python

    client = await edgedb.create_client('my_instance')

- Pass a DSN (connection URL) to :py:func:`edgedb.create_client()
  <edgedb.create_client>`. For a guide to DSNs, see the :ref:`DSN
  Specification <ref_dsn>`.

  .. code-block:: python

    client = await edgedb.create_client(
      'edgedb://user:pass@host:port/database'
    )

For a complete reference on connection parameters and how they are resolved by
the client library, see :ref:`Reference > Connection Parameters
<ref_reference_connection>`
