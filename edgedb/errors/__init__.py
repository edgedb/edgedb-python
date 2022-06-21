# AUTOGENERATED FROM "edb/api/errors.txt" WITH
#    $ edb gen-errors \
#        --import 'from edgedb.errors._base import *\nfrom edgedb.errors.tags import *' \
#        --extra-all "_base.__all__" \
#        --stdout \
#        --client


# flake8: noqa


from edgedb.errors._base import *
from edgedb.errors.tags import *


__all__ = _base.__all__ + (  # type: ignore
    'InternalServerError',
    'UnsupportedFeatureError',
    'ProtocolError',
    'BinaryProtocolError',
    'UnsupportedProtocolVersionError',
    'TypeSpecNotFoundError',
    'UnexpectedMessageError',
    'InputDataError',
    'ParameterTypeMismatchError',
    'ResultCardinalityMismatchError',
    'CapabilityError',
    'UnsupportedCapabilityError',
    'DisabledCapabilityError',
    'QueryError',
    'InvalidSyntaxError',
    'EdgeQLSyntaxError',
    'SchemaSyntaxError',
    'GraphQLSyntaxError',
    'InvalidTypeError',
    'InvalidTargetError',
    'InvalidLinkTargetError',
    'InvalidPropertyTargetError',
    'InvalidReferenceError',
    'UnknownModuleError',
    'UnknownLinkError',
    'UnknownPropertyError',
    'UnknownUserError',
    'UnknownDatabaseError',
    'UnknownParameterError',
    'SchemaError',
    'SchemaDefinitionError',
    'InvalidDefinitionError',
    'InvalidModuleDefinitionError',
    'InvalidLinkDefinitionError',
    'InvalidPropertyDefinitionError',
    'InvalidUserDefinitionError',
    'InvalidDatabaseDefinitionError',
    'InvalidOperatorDefinitionError',
    'InvalidAliasDefinitionError',
    'InvalidFunctionDefinitionError',
    'InvalidConstraintDefinitionError',
    'InvalidCastDefinitionError',
    'DuplicateDefinitionError',
    'DuplicateModuleDefinitionError',
    'DuplicateLinkDefinitionError',
    'DuplicatePropertyDefinitionError',
    'DuplicateUserDefinitionError',
    'DuplicateDatabaseDefinitionError',
    'DuplicateOperatorDefinitionError',
    'DuplicateViewDefinitionError',
    'DuplicateFunctionDefinitionError',
    'DuplicateConstraintDefinitionError',
    'DuplicateCastDefinitionError',
    'SessionTimeoutError',
    'IdleSessionTimeoutError',
    'QueryTimeoutError',
    'TransactionTimeoutError',
    'IdleTransactionTimeoutError',
    'ExecutionError',
    'InvalidValueError',
    'DivisionByZeroError',
    'NumericOutOfRangeError',
    'AccessPolicyError',
    'IntegrityError',
    'ConstraintViolationError',
    'CardinalityViolationError',
    'MissingRequiredError',
    'TransactionError',
    'TransactionConflictError',
    'TransactionSerializationError',
    'TransactionDeadlockError',
    'ConfigurationError',
    'AccessError',
    'AuthenticationError',
    'AvailabilityError',
    'BackendUnavailableError',
    'BackendError',
    'UnsupportedBackendFeatureError',
    'LogMessage',
    'WarningMessage',
    'ClientError',
    'ClientConnectionError',
    'ClientConnectionFailedError',
    'ClientConnectionFailedTemporarilyError',
    'ClientConnectionTimeoutError',
    'ClientConnectionClosedError',
    'InterfaceError',
    'QueryArgumentError',
    'MissingArgumentError',
    'UnknownArgumentError',
    'InvalidArgumentError',
    'NoDataError',
    'InternalClientError',
)


class InternalServerError(EdgeDBError):
    _code = 0x_01_00_00_00


class UnsupportedFeatureError(EdgeDBError):
    _code = 0x_02_00_00_00


class ProtocolError(EdgeDBError):
    _code = 0x_03_00_00_00


class BinaryProtocolError(ProtocolError):
    _code = 0x_03_01_00_00


class UnsupportedProtocolVersionError(BinaryProtocolError):
    _code = 0x_03_01_00_01


class TypeSpecNotFoundError(BinaryProtocolError):
    _code = 0x_03_01_00_02


class UnexpectedMessageError(BinaryProtocolError):
    _code = 0x_03_01_00_03


class InputDataError(ProtocolError):
    _code = 0x_03_02_00_00


class ParameterTypeMismatchError(InputDataError):
    _code = 0x_03_02_01_00


class ResultCardinalityMismatchError(ProtocolError):
    _code = 0x_03_03_00_00


class CapabilityError(ProtocolError):
    _code = 0x_03_04_00_00


class UnsupportedCapabilityError(CapabilityError):
    _code = 0x_03_04_01_00


class DisabledCapabilityError(CapabilityError):
    _code = 0x_03_04_02_00


class QueryError(EdgeDBError):
    _code = 0x_04_00_00_00


class InvalidSyntaxError(QueryError):
    _code = 0x_04_01_00_00


class EdgeQLSyntaxError(InvalidSyntaxError):
    _code = 0x_04_01_01_00


class SchemaSyntaxError(InvalidSyntaxError):
    _code = 0x_04_01_02_00


class GraphQLSyntaxError(InvalidSyntaxError):
    _code = 0x_04_01_03_00


class InvalidTypeError(QueryError):
    _code = 0x_04_02_00_00


class InvalidTargetError(InvalidTypeError):
    _code = 0x_04_02_01_00


class InvalidLinkTargetError(InvalidTargetError):
    _code = 0x_04_02_01_01


class InvalidPropertyTargetError(InvalidTargetError):
    _code = 0x_04_02_01_02


class InvalidReferenceError(QueryError):
    _code = 0x_04_03_00_00


class UnknownModuleError(InvalidReferenceError):
    _code = 0x_04_03_00_01


class UnknownLinkError(InvalidReferenceError):
    _code = 0x_04_03_00_02


class UnknownPropertyError(InvalidReferenceError):
    _code = 0x_04_03_00_03


class UnknownUserError(InvalidReferenceError):
    _code = 0x_04_03_00_04


class UnknownDatabaseError(InvalidReferenceError):
    _code = 0x_04_03_00_05


class UnknownParameterError(InvalidReferenceError):
    _code = 0x_04_03_00_06


class SchemaError(QueryError):
    _code = 0x_04_04_00_00


class SchemaDefinitionError(QueryError):
    _code = 0x_04_05_00_00


class InvalidDefinitionError(SchemaDefinitionError):
    _code = 0x_04_05_01_00


class InvalidModuleDefinitionError(InvalidDefinitionError):
    _code = 0x_04_05_01_01


class InvalidLinkDefinitionError(InvalidDefinitionError):
    _code = 0x_04_05_01_02


class InvalidPropertyDefinitionError(InvalidDefinitionError):
    _code = 0x_04_05_01_03


class InvalidUserDefinitionError(InvalidDefinitionError):
    _code = 0x_04_05_01_04


class InvalidDatabaseDefinitionError(InvalidDefinitionError):
    _code = 0x_04_05_01_05


class InvalidOperatorDefinitionError(InvalidDefinitionError):
    _code = 0x_04_05_01_06


class InvalidAliasDefinitionError(InvalidDefinitionError):
    _code = 0x_04_05_01_07


class InvalidFunctionDefinitionError(InvalidDefinitionError):
    _code = 0x_04_05_01_08


class InvalidConstraintDefinitionError(InvalidDefinitionError):
    _code = 0x_04_05_01_09


class InvalidCastDefinitionError(InvalidDefinitionError):
    _code = 0x_04_05_01_0A


class DuplicateDefinitionError(SchemaDefinitionError):
    _code = 0x_04_05_02_00


class DuplicateModuleDefinitionError(DuplicateDefinitionError):
    _code = 0x_04_05_02_01


class DuplicateLinkDefinitionError(DuplicateDefinitionError):
    _code = 0x_04_05_02_02


class DuplicatePropertyDefinitionError(DuplicateDefinitionError):
    _code = 0x_04_05_02_03


class DuplicateUserDefinitionError(DuplicateDefinitionError):
    _code = 0x_04_05_02_04


class DuplicateDatabaseDefinitionError(DuplicateDefinitionError):
    _code = 0x_04_05_02_05


class DuplicateOperatorDefinitionError(DuplicateDefinitionError):
    _code = 0x_04_05_02_06


class DuplicateViewDefinitionError(DuplicateDefinitionError):
    _code = 0x_04_05_02_07


class DuplicateFunctionDefinitionError(DuplicateDefinitionError):
    _code = 0x_04_05_02_08


class DuplicateConstraintDefinitionError(DuplicateDefinitionError):
    _code = 0x_04_05_02_09


class DuplicateCastDefinitionError(DuplicateDefinitionError):
    _code = 0x_04_05_02_0A


class SessionTimeoutError(QueryError):
    _code = 0x_04_06_00_00


class IdleSessionTimeoutError(SessionTimeoutError):
    _code = 0x_04_06_01_00


class QueryTimeoutError(SessionTimeoutError):
    _code = 0x_04_06_02_00


class TransactionTimeoutError(SessionTimeoutError):
    _code = 0x_04_06_0A_00


class IdleTransactionTimeoutError(TransactionTimeoutError):
    _code = 0x_04_06_0A_01


class ExecutionError(EdgeDBError):
    _code = 0x_05_00_00_00


class InvalidValueError(ExecutionError):
    _code = 0x_05_01_00_00


class DivisionByZeroError(InvalidValueError):
    _code = 0x_05_01_00_01


class NumericOutOfRangeError(InvalidValueError):
    _code = 0x_05_01_00_02


class AccessPolicyError(InvalidValueError):
    _code = 0x_05_01_00_03


class IntegrityError(ExecutionError):
    _code = 0x_05_02_00_00


class ConstraintViolationError(IntegrityError):
    _code = 0x_05_02_00_01


class CardinalityViolationError(IntegrityError):
    _code = 0x_05_02_00_02


class MissingRequiredError(IntegrityError):
    _code = 0x_05_02_00_03


class TransactionError(ExecutionError):
    _code = 0x_05_03_00_00


class TransactionConflictError(TransactionError):
    _code = 0x_05_03_01_00
    tags = frozenset({SHOULD_RETRY})


class TransactionSerializationError(TransactionConflictError):
    _code = 0x_05_03_01_01
    tags = frozenset({SHOULD_RETRY})


class TransactionDeadlockError(TransactionConflictError):
    _code = 0x_05_03_01_02
    tags = frozenset({SHOULD_RETRY})


class ConfigurationError(EdgeDBError):
    _code = 0x_06_00_00_00


class AccessError(EdgeDBError):
    _code = 0x_07_00_00_00


class AuthenticationError(AccessError):
    _code = 0x_07_01_00_00


class AvailabilityError(EdgeDBError):
    _code = 0x_08_00_00_00


class BackendUnavailableError(AvailabilityError):
    _code = 0x_08_00_00_01
    tags = frozenset({SHOULD_RETRY})


class BackendError(EdgeDBError):
    _code = 0x_09_00_00_00


class UnsupportedBackendFeatureError(BackendError):
    _code = 0x_09_00_01_00


class LogMessage(EdgeDBMessage):
    _code = 0x_F0_00_00_00


class WarningMessage(LogMessage):
    _code = 0x_F0_01_00_00


class ClientError(EdgeDBError):
    _code = 0x_FF_00_00_00


class ClientConnectionError(ClientError):
    _code = 0x_FF_01_00_00


class ClientConnectionFailedError(ClientConnectionError):
    _code = 0x_FF_01_01_00


class ClientConnectionFailedTemporarilyError(ClientConnectionFailedError):
    _code = 0x_FF_01_01_01
    tags = frozenset({SHOULD_RECONNECT, SHOULD_RETRY})


class ClientConnectionTimeoutError(ClientConnectionError):
    _code = 0x_FF_01_02_00
    tags = frozenset({SHOULD_RECONNECT, SHOULD_RETRY})


class ClientConnectionClosedError(ClientConnectionError):
    _code = 0x_FF_01_03_00
    tags = frozenset({SHOULD_RECONNECT, SHOULD_RETRY})


class InterfaceError(ClientError):
    _code = 0x_FF_02_00_00


class QueryArgumentError(InterfaceError):
    _code = 0x_FF_02_01_00


class MissingArgumentError(QueryArgumentError):
    _code = 0x_FF_02_01_01


class UnknownArgumentError(QueryArgumentError):
    _code = 0x_FF_02_01_02


class InvalidArgumentError(QueryArgumentError):
    _code = 0x_FF_02_01_03


class NoDataError(ClientError):
    _code = 0x_FF_03_00_00


class InternalClientError(ClientError):
    _code = 0x_FF_04_00_00
