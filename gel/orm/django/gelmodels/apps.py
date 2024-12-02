from django.apps import AppConfig


class GelPGModel(AppConfig):
    name = "gel.orm.django.gelmodels"

    def ready(self):
        from django.db import connections, utils

        gel_compiler_module = "gel.orm.django.gelmodels.compiler"

        # Change the current compiler_module
        for c in connections:
            connections[c].ops.compiler_module = gel_compiler_module

        # Update the load_backend to use our DatabaseWrapper
        orig_load_backend = utils.load_backend

        def custom_load_backend(*args, **kwargs):
            backend = orig_load_backend(*args, **kwargs)

            class GelPGBackend:
                @staticmethod
                def DatabaseWrapper(*args2, **kwargs2):
                    connection = backend.DatabaseWrapper(*args2, **kwargs2)
                    connection.ops.compiler_module = gel_compiler_module
                    return connection

            return GelPGBackend

        utils.load_backend = custom_load_backend