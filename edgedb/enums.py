import enum


class Capability(enum.IntFlag):

    MODIFICATIONS     = 1 << 0    # noqa
    SESSION_CONFIG    = 1 << 1    # noqa
    TRANSACTION       = 1 << 2    # noqa
    DDL               = 1 << 3    # noqa
    PERSISTENT_CONFIG = 1 << 4    # noqa


Capability.ALL = 0xFFFF_FFFF_FFFF_FFFF
Capability.EXECUTE = Capability.ALL & ~Capability.TRANSACTION
