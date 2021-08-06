.. _edgedb-python-datatypes:

=========
Datatypes
=========

.. py:currentmodule:: edgedb


edgedb-python automatically converts EdgeDB types to the corresponding Python
types and vice versa.

The table below shows the correspondence between EdgeDB and Python types.

+----------------------------+-----------------------------------------------------+
| EdgeDB Type                |  Python Type                                        |
+============================+=====================================================+
| ``Set``                    | :py:class:`edgedb.Set`                              |
+----------------------------+-----------------------------------------------------+
| ``array<anytype>``         | :py:class:`edgedb.Array`                            |
+----------------------------+-----------------------------------------------------+
| ``anytuple``               | :py:class:`edgedb.Tuple` or                         |
|                            | :py:class:`edgedb.NamedTuple`                       |
+----------------------------+-----------------------------------------------------+
| ``anyenum``                | :py:class:`str <python:str>`                        |
+----------------------------+-----------------------------------------------------+
| ``Object``                 | :py:class:`edgedb.Object`                           |
+----------------------------+-----------------------------------------------------+
| ``bool``                   | :py:class:`bool <python:bool>`                      |
+----------------------------+-----------------------------------------------------+
| ``bytes``                  | :py:class:`bytes <python:bytes>`                    |
+----------------------------+-----------------------------------------------------+
| ``str``                    | :py:class:`str <python:str>`                        |
+----------------------------+-----------------------------------------------------+
| ``cal::local_date``        | :py:class:`datetime.date <python:datetime.date>`    |
+----------------------------+-----------------------------------------------------+
| ``cal::local_time``        | offset-naive :py:class:`datetime.time \             |
|                            | <python:datetime.time>`                             |
+----------------------------+-----------------------------------------------------+
| ``cal::local_datetime``    | offset-naive :py:class:`datetime.datetime \         |
|                            | <python:datetime.datetime>`                         |
+----------------------------+-----------------------------------------------------+
| ``cal::relative_duration`` | :py:class:`edgedb.RelativeDuration`                 |
+----------------------------+-----------------------------------------------------+
| ``datetime``               | offset-aware :py:class:`datetime.datetime \         |
|                            | <python:datetime.datetime>`                         |
+----------------------------+-----------------------------------------------------+
| ``duration``               | :py:class:`datetime.timedelta \                     |
|                            | <python:datetime.timedelta>`                        |
+----------------------------+-----------------------------------------------------+
| ``float32``,               | :py:class:`float <python:float>`                    |
| ``float64``                |                                                     |
+----------------------------+-----------------------------------------------------+
| ``int16``,                 | :py:class:`int <python:int>`                        |
| ``int32``,                 |                                                     |
| ``int64``,                 |                                                     |
| ``bigint``                 |                                                     |
+----------------------------+-----------------------------------------------------+
| ``decimal``                | :py:class:`Decimal <python:decimal.Decimal>`        |
+----------------------------+-----------------------------------------------------+
| ``json``                   | :py:class:`str <python:str>`                        |
+----------------------------+-----------------------------------------------------+
| ``uuid``                   | :py:class:`uuid.UUID <python:uuid.UUID>`            |
+----------------------------+-----------------------------------------------------+

.. note::

    Inexact single-precision ``float`` values may have a different
    representation when decoded into a Python float.  This is inherent
    to the implementation of limited-precision floating point types.
    If you need the decimal representation to match, cast the expression
    to ``float64`` or ``decimal`` in your query.


.. _edgedb-python-types-set:

Sets
====

.. py:class:: Set()

    A representation of an immutable set of values returned by a query.

    The :py:meth:`BlockingIOConnection.query()
    <edgedb.BlockingIOConnection.query>` and
    :py:meth:`AsyncIOConnection.query()
    <edgedb.AsyncIOConnection.query>` methods return
    an instance of this type.  Nested sets in the result are also
    returned as ``Set`` objects.

    .. describe:: len(s)

       Return the number of fields in set *s*.

    .. describe:: iter(s)

       Return an iterator over the *values* of the set *s*.


.. _edgedb-python-types-object:

Objects
=======

.. py:class:: Object()

    An immutable representation of an object instance returned from a query.

    The value of an object property or a link can be accessed through
    a corresponding attribute:

    .. code-block:: pycon

        >>> import edgedb
        >>> conn = edgedb.connect()
        >>> r = conn.query_single('''
        ...     SELECT schema::ObjectType {name}
        ...     FILTER .name = 'std::Object'
        ...     LIMIT 1''')
        >>> r
        Object{name := 'std::Object'}
        >>> r.name
        'std::Object'

    .. describe:: obj[linkname]

       Return a :py:class:`edgedb.Link` or a :py:class:`edgedb.LinkSet` instance
       representing the instance(s) of link *linkname* associated with
       *obj*.

       Example:

       .. code-block:: pycon

          >>> import edgedb
          >>> conn = edgedb.connect()
          >>> r = conn.query_single('''
          ...     SELECT schema::Property {name, annotations: {name, @value}}
          ...     FILTER .name = 'listen_port'
          ...            AND .source.name = 'cfg::Config'
          ...     LIMIT 1''')
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


Links
=====

.. py:class:: Link

    An immutable representation of an object link.

    Links are created when :py:class:`edgedb.Object` is accessed via
    a ``[]`` operator.  Using Link objects explicitly is useful for
    accessing link properties.


.. py:class:: LinkSet

    An immutable representation of a set of Links.

    LinkSets are created when a multi link on :py:class:`edgedb.Object`
    is accessed via a ``[]`` operator.


Tuples
======

.. py:class:: Tuple()

    An immutable value representing an EdgeDB tuple value.

    Instances of ``edgedb.Tuple`` generally behave exactly like
    standard Python tuples:

    .. code-block:: pycon

        >>> import edgedb
        >>> conn = edgedb.connect()
        >>> r = conn.query_single('''SELECT (1, 'a', [3])''')
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

.. py:class:: NamedTuple()

    An immutable value representing an EdgeDB named tuple value.

    Instances of ``edgedb.NamedTuple`` generally behave similarly to
    :py:func:`namedtuple <python:collections.namedtuple>`:

    .. code-block:: pycon

        >>> import edgedb
        >>> conn = edgedb.connect()
        >>> r = conn.query_single('''SELECT (a := 1, b := 'a', c := [3])''')
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

.. py:class:: Array()

    An immutable value representing an EdgeDB array value.

    .. code-block:: pycon

        >>> import edgedb
        >>> conn = edgedb.connect()
        >>> r = conn.query_single('''SELECT [1, 2, 3]''')
        >>> r
        [1, 2, 3]
        >>> len(r)
        3
        >>> r[1]
        2
        >>> r == [1, 2, 3]
        True

RelativeDuration
================

.. py:class:: RelativeDuration()

    An immutable value represeting an EdgeDB ``cal::relative_duration`` value.

    .. code-block:: pycon

        >>> import edgedb
        >>> conn = edgedb.connect()
        >>> r = conn.query_single('''SELECT <cal::relative_duration>"1 year 2 days"''')
        >>> r
        <edgedb.RelativeDuration "P1Y2D">
        >>> r.months
        12
        >>> r.days
        2
        >>> r.microseconds
        0
