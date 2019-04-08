.. _edgedb-python-datatypes:

=======================
EdgeDB Python Datatypes
=======================

EdgeDB-Python automatically converts EdgeDB types to the corresponding Python
types and vice versa.

The table below shows the correspondence between EdgeDB and Python types.

+----------------------+-----------------------------------------------------+
| EdgeDB Type          |  Python Type                                        |
+======================+=====================================================+
| ``array<anytype>``   | :class:`edgedb.Array`                               |
+----------------------+-----------------------------------------------------+
| ``anytuple``         | :class:`edgedb.Tuple` or                            |
|                      | :class:`edgedb.NamedTuple`                          |
+----------------------+-----------------------------------------------------+
| ``anyenum``          | :class:`str <python:str>`                           |
+----------------------+-----------------------------------------------------+
| ``Object``           | :class:`edgedb.Object`                              |
+----------------------+-----------------------------------------------------+
| ``bool``             | :class:`bool <python:bool>`                         |
+----------------------+-----------------------------------------------------+
| ``bytes``            | :class:`bytes <python:bytes>`                       |
+----------------------+-----------------------------------------------------+
| ``str``              | :class:`str <python:str>`                           |
+----------------------+-----------------------------------------------------+
| ``local_date``       | :class:`datetime.date <python:datetime.date>`       |
+----------------------+-----------------------------------------------------+
| ``local_time``       | offset-naïve :class:`datetime.time \                |
|                      | <python:datetime.time>`                             |
+----------------------+-----------------------------------------------------+
| ``local_datetime``   | offset-naïve :class:`datetime.datetime \            |
|                      | <python:datetime.datetime>`                         |
+----------------------+-----------------------------------------------------+
| ``datetime``         | offset-aware :class:`datetime.datetime \            |
|                      | <python:datetime.datetime>`                         |
+----------------------+-----------------------------------------------------+
| ``duration``         | :class:`edgedb.Duration`                            |
+----------------------+-----------------------------------------------------+
| ``float32``,         | :class:`float <python:float>` [#f1]_                |
| ``float64``          |                                                     |
+----------------------+-----------------------------------------------------+
| ``int16``,           | :class:`int <python:int>`                           |
| ``int32``,           |                                                     |
| ``int64``            |                                                     |
+----------------------+-----------------------------------------------------+
| ``decimal``          | :class:`Decimal <python:decimal.Decimal>`           |
+----------------------+-----------------------------------------------------+
| ``json``             | :class:`str <python:str>`                           |
+----------------------+-----------------------------------------------------+
| ``uuid``             | :class:`uuid.UUID <python:uuid.UUID>`               |
+----------------------+-----------------------------------------------------+

.. [#f1] Inexact single-precision ``float`` values may have a different
         representation when decoded into a Python float.  This is inherent
         to the implementation of limited-precision floating point types.
         If you need the decimal representation to match, cast the expression
         to ``float64`` or ``decimal`` in your query.


.. _edgedb-python-types-set:

Sets
====

.. class:: edgedb.Set()

    A representation of an immutable set of values returned by a query.

    The :meth:`BlockingIOConnection.fetchall()
    <edgedb.blocking_con.BlockingIOConnection.fetchall>` and
    :meth:`AsyncIOConnection.fetchall()
    <edgedb.asyncio_con.AsyncIOConnection.fetchall>` methods return
    an instance of this type.  Nested sets in the result are also
    returned as ``Set`` objects.

    .. describe:: len(s)

       Return the number of fields in set *s*.

    .. describe:: iter(s)

       Return an iterator over the *values* of the set *s*.


.. _edgedb-python-types-object:

Objects
=======

.. class:: edgedb.Object()

    An immutable representation of an object instance returned from a query.

    The value of an object property or a link can be accessed through
    a corresponding attribute:

    .. code-block:: pycon

        >>> import edgedb
        >>> conn = edgedb.connect()
        >>> r = conn.fetchone('''
        ...     SELECT schema::ObjectType {name}
        ...     FILTER .name = 'std::Object';
        ...     LIMIT 1'''))
        >>> r
        Object{name := 'std::Object'}
        >>> r.name
        'std::Object'

    .. describe:: obj[linkname]

       Return a :class:`edgedb.Link` or a :class:`edgedb.LinkSet` instance
       representing the instance(s) of link *linkname* associated with
       *obj*.

       Example:

       .. code-block:: pycon

          >>> import edgedb
          >>> conn = edgedb.connect()
          >>> r = conn.fetchone('''
          ...     SELECT schema::Property {name, annotations: {name, @value}}
          ...     FILTER .name = 'listen_port'
          ...            AND .source.name = 'cfg::Config';
          ...     LIMIT 1'''))
          >>> r
          Object {
              name: 'listen_port',
              annotations: {
                  Object {
                      name: 'cfg::system',
                      @value: 'true'
                  }
              }
          }
          >>> r['annotations']
          LinkSet(name='annotations')
          >>> l = list(r['annotations])[0]
          >>> l.value
          'true'


Tuples
======

.. class:: edgedb.Tuple()

    An immutable value representing an EdgeDB tuple value.

    Instances of ``edgedb.Tuple`` generally behave exactly like
    standard Python tuples:

    .. code-block:: pycon

        >>> import edgedb
        >>> conn = edgedb.connect()
        >>> r = conn.fetchone('''SELECT (1, 'a', [3])''')
        >>> r
        (1, 'a', [3])
        >>> len(r)
        3
        >>> r[1]
        'a'
        >>> r == (1, 'a', [3])
        True


Named Tuples
============

.. class:: edgedb.NamedTuple()

    An immutable value representing an EdgeDB named tuple value.

    Instances of ``edgedb.NamedTuple`` generally behave similarly to
    :func:`namedtuple <python:collections.namedtuple>`:

    .. code-block:: pycon

        >>> import edgedb
        >>> conn = edgedb.connect()
        >>> r = conn.fetchone('''SELECT (a := 1, b := 'a', c := [3])''')
        >>> r
        (a := 1, b := 'a', c := [3])
        >>> r.b
        'a'
        >>> r[0]
        1
        >>> r == (1, 'a', [3])
        True


Arrays
======

.. class:: edgedb.Array()

    An immutable value representing an EdgeDB array value.

    .. code-block:: pycon

        >>> import edgedb
        >>> conn = edgedb.connect()
        >>> r = conn.fetchone('''SELECT [1, 2, 3]''')
        >>> r
        [1, 2, 3]
        >>> len(r)
        3
        >>> r[1]
        2
        >>> r == [1, 2, 3]
        True


Duration
========

.. class:: edgedb.Duration(*, months, days, microseconds)

    A Python representation of an EdgeDB ``duration`` value.

    .. attribute:: months

        The number of months in the duration.

    .. attribute:: days

        The number of days in the duration.

    .. attribute:: microseconds

        The number of microseconds in the duration.
