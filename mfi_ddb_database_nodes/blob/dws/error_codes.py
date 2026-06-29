class GrpcError(Exception):
    """Base class for all custom gRPC exceptions."""

    status_code = None
    default_message = "gRPC error"

    def __init__(self, message=None):
        super().__init__(message or self.default_message)


class CancelledError(GrpcError):
    status_code = "CANCELLED"
    default_message = "The operation was cancelled."


class UnknownError(GrpcError):
    status_code = "UNKNOWN"
    default_message = "Unknown error occurred."


class InvalidArgumentError(GrpcError):
    status_code = "INVALID_ARGUMENT"
    default_message = "Client specified an invalid argument."


class DeadlineExceededError(GrpcError):
    status_code = "DEADLINE_EXCEEDED"
    default_message = "Deadline expired before operation could complete."


class NotFoundError(GrpcError):
    status_code = "NOT_FOUND"
    default_message = "Requested entity was not found."


class AlreadyExistsError(GrpcError):
    status_code = "ALREADY_EXISTS"
    default_message = "Entity already exists."


class PermissionDeniedError(GrpcError):
    status_code = "PERMISSION_DENIED"
    default_message = "Permission denied."


class ResourceExhaustedError(GrpcError):
    status_code = "RESOURCE_EXHAUSTED"
    default_message = "Resource has been exhausted."


class FailedPreconditionError(GrpcError):
    status_code = "FAILED_PRECONDITION"
    default_message = "Operation rejected due to system state."


class AbortedError(GrpcError):
    status_code = "ABORTED"
    default_message = "Operation was aborted."


class OutOfRangeError(GrpcError):
    status_code = "OUT_OF_RANGE"
    default_message = "Operation attempted past valid range."


class UnimplementedError(GrpcError):
    status_code = "UNIMPLEMENTED"
    default_message = "Operation is not implemented or supported."


class InternalError(GrpcError):
    status_code = "INTERNAL"
    default_message = "Internal error occurred."


class UnavailableError(GrpcError):
    status_code = "UNAVAILABLE"
    default_message = "Service is currently unavailable."


class DataLossError(GrpcError):
    status_code = "DATA_LOSS"
    default_message = "Unrecoverable data loss or corruption."


class UnauthenticatedError(GrpcError):
    status_code = "UNAUTHENTICATED"
    default_message = "Authentication credentials are missing or invalid."
