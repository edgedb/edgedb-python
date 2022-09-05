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


class Cardinality(enum.Enum):
    # Cardinality isn't applicable for the query:
    # * the query is a command like CONFIGURE that
    #   does not return any data;
    # * the query is composed of multiple queries.
    NO_RESULT = 0x6e

    # Cardinality is 1 or 0
    AT_MOST_ONE = 0x6f

    # Cardinality is 1
    ONE = 0x41

    # Cardinality is >= 0
    MANY = 0x6d

    # Cardinality is >= 1
    AT_LEAST_ONE = 0x4d


class ElementKind(enum.Enum):

    LINK            = 1 # noqa
    PROPERTY        = 2 # noqa
    LINK_PROPERTY   = 3 # noqa
