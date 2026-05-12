from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class file_value(_message.Message):
    __slots__ = ("filename", "filetype", "content", "sha256")
    FILENAME_FIELD_NUMBER: _ClassVar[int]
    FILETYPE_FIELD_NUMBER: _ClassVar[int]
    CONTENT_FIELD_NUMBER: _ClassVar[int]
    SHA256_FIELD_NUMBER: _ClassVar[int]
    filename: str
    filetype: str
    content: bytes
    sha256: str
    def __init__(self, filename: _Optional[str] = ..., filetype: _Optional[str] = ..., content: _Optional[bytes] = ..., sha256: _Optional[str] = ...) -> None: ...

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
    file_value: file_value
    def __init__(self, topic: _Optional[str] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., int_value: _Optional[int] = ..., float_value: _Optional[float] = ..., string_value: _Optional[str] = ..., json_value: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., file_value: _Optional[_Union[file_value, _Mapping]] = ...) -> None: ...
