import enum


class Capability(enum.IntFlag):

    NONE              = 0         # noqa
    MODIFICATIONS     = 1 << 0    # noqa
    SESSION_CONFIG    = 1 << 1    # noqa
    TRANSACTION       = 1 << 2    # noqa
    DDL               = 1 << 3    # noqa
    PERSISTENT_CONFIG = 1 << 4    # noqa

    ALL               = 0xFFFF_FFFF_FFFF_FFFF                 # noqa
    EXECUTE           = ALL & ~TRANSACTION & ~SESSION_CONFIG  # noqa
    LEGACY_EXECUTE    = ALL & ~TRANSACTION                    # noqa


class CompilationFlag(enum.IntFlag):

    INJECT_OUTPUT_TYPE_IDS   = 1 << 0    # noqa
    INJECT_OUTPUT_TYPE_NAMES = 1 << 1    # noqa
    INJECT_OUTPUT_OBJECT_IDS = 1 << 2    # noqa
