from django.db.models.sql.compiler import (  # noqa
    SQLAggregateCompiler,
    SQLCompiler,
    SQLDeleteCompiler,
)
from django.db.models.sql.compiler import (  # noqa
    SQLInsertCompiler as BaseSQLInsertCompiler,
)
from django.db.models.sql.compiler import (  # noqa
    SQLUpdateCompiler as BaseSQLUpdateCompiler,
)


class GelSQLCompilerMixin:
    '''
    The reflected models have two special fields: `id` and `obj_type`. Both of
    those fields should be read-only as they are populated automatically by
    Gel and must not be modified.
    '''
    @property
    def readonly_gel_fields(self):
        try:
            # Verify that this is a Gel model reflected via Postgres protocol.
            gel_pg_meta = getattr(self.query.model, "GelPGMeta")
        except AttributeError:
            return set()
        else:
            return {'id', 'gel_type_id'}

    def as_sql(self):
        readonly_gel_fields = self.readonly_gel_fields
        if readonly_gel_fields:
            self.remove_readonly_gel_fields(readonly_gel_fields)
        return super().as_sql()


class SQLUpdateCompiler(GelSQLCompilerMixin, BaseSQLUpdateCompiler):
    def remove_readonly_gel_fields(self, names):
        '''
        Remove the values corresponding to the read-only fields.
        '''
        values = self.query.values
        # The tuple is (field, model, value)
        values[:] = (tup for tup in values if tup[0].name not in names)


class SQLInsertCompiler(GelSQLCompilerMixin, BaseSQLInsertCompiler):
    def remove_readonly_gel_fields(self, names):
        '''
        Remove the read-only fields.
        '''
        fields = self.query.fields

        try:
            fields[:] = (f for f in fields if f.name not in names)
        except AttributeError:
            # When deserializing, we might get an attribute error because this
            # list shoud be copied first:
            #
            # "AttributeError: The return type of 'local_concrete_fields'
            # should never be mutated. If you want to manipulate this list for
            # your own use, make a copy first."

            self.query.fields = [f for f in fields if f.name not in names]
