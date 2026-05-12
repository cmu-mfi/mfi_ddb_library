from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ErrorCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ERROR_CODE_UNSPECIFIED: _ClassVar[ErrorCode]
    ERROR_CODE_TOPIC_NOT_FOUND: _ClassVar[ErrorCode]
    ERROR_CODE_INVALID_TIMESTAMP_RANGE: _ClassVar[ErrorCode]
    ERROR_CODE_DATA_CORRUPTION: _ClassVar[ErrorCode]
    ERROR_CODE_EMPTY_RESULT: _ClassVar[ErrorCode]
ERROR_CODE_UNSPECIFIED: ErrorCode
ERROR_CODE_TOPIC_NOT_FOUND: ErrorCode
ERROR_CODE_INVALID_TIMESTAMP_RANGE: ErrorCode
ERROR_CODE_DATA_CORRUPTION: ErrorCode
ERROR_CODE_EMPTY_RESULT: ErrorCode

class Datapoint(_message.Message):
    __slots__ = ("topic", "timestamp", "int_value", "float_value", "string_value", "json_value", "file_value")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    INT_VALUE_FIELD_NUMBER: _ClassVar[int]
    FLOAT_VALUE_FIELD_NUMBER: _ClassVar[int]
    STRING_VALUE_FIELD_NUMBER: _ClassVar[int]
    JSON_VALUE_FIELD_NUMBER: _ClassVar[int]
    FILE_VALUE_FIELD_NUMBER: _ClassVar[int]
    topic: str
    timestamp: _timestamp_pb2.Timestamp
    int_value: int
    float_value: float
    string_value: str
    json_value: _struct_pb2.Struct
    file_value: bytes
    def __init__(self, topic: _Optional[str] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., int_value: _Optional[int] = ..., float_value: _Optional[float] = ..., string_value: _Optional[str] = ..., json_value: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., file_value: _Optional[bytes] = ...) -> None: ...

class ServiceError(_message.Message):
    __slots__ = ("code", "message", "topic_context")
    CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    TOPIC_CONTEXT_FIELD_NUMBER: _ClassVar[int]
    code: ErrorCode
    message: str
    topic_context: str
    def __init__(self, code: _Optional[_Union[ErrorCode, str]] = ..., message: _Optional[str] = ..., topic_context: _Optional[str] = ...) -> None: ...
