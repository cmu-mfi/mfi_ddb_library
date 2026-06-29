from google.protobuf import timestamp_pb2 as _timestamp_pb2
import models_pb2 as _models_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class GetDataPointRequest(_message.Message):
    __slots__ = ("topic", "user_id", "timestamp")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    TIMESTAMP_FIELD_NUMBER: _ClassVar[int]
    topic: str
    user_id: str
    timestamp: _timestamp_pb2.Timestamp
    def __init__(self, topic: _Optional[str] = ..., user_id: _Optional[str] = ..., timestamp: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class GetDataPointResponse(_message.Message):
    __slots__ = ("datapoint",)
    DATAPOINT_FIELD_NUMBER: _ClassVar[int]
    datapoint: _models_pb2.Datapoint
    def __init__(self, datapoint: _Optional[_Union[_models_pb2.Datapoint, _Mapping]] = ...) -> None: ...

class GetDataRangeRequest(_message.Message):
    __slots__ = ("topic", "user_id", "start_time", "end_time", "page_size", "page_token")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    PAGE_SIZE_FIELD_NUMBER: _ClassVar[int]
    PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    topic: str
    user_id: str
    start_time: _timestamp_pb2.Timestamp
    end_time: _timestamp_pb2.Timestamp
    page_size: int
    page_token: str
    def __init__(self, topic: _Optional[str] = ..., user_id: _Optional[str] = ..., start_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., end_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., page_size: _Optional[int] = ..., page_token: _Optional[str] = ...) -> None: ...

class GetDataRangeResponse(_message.Message):
    __slots__ = ("datapoints", "next_page_token")
    DATAPOINTS_FIELD_NUMBER: _ClassVar[int]
    NEXT_PAGE_TOKEN_FIELD_NUMBER: _ClassVar[int]
    datapoints: _containers.RepeatedCompositeFieldContainer[_models_pb2.Datapoint]
    next_page_token: str
    def __init__(self, datapoints: _Optional[_Iterable[_Union[_models_pb2.Datapoint, _Mapping]]] = ..., next_page_token: _Optional[str] = ...) -> None: ...

class StreamDataRequest(_message.Message):
    __slots__ = ("topic", "user_id", "start_from")
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    START_FROM_FIELD_NUMBER: _ClassVar[int]
    topic: str
    user_id: str
    start_from: _timestamp_pb2.Timestamp
    def __init__(self, topic: _Optional[str] = ..., user_id: _Optional[str] = ..., start_from: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class StreamDataResponse(_message.Message):
    __slots__ = ("datapoint",)
    DATAPOINT_FIELD_NUMBER: _ClassVar[int]
    datapoint: _models_pb2.Datapoint
    def __init__(self, datapoint: _Optional[_Union[_models_pb2.Datapoint, _Mapping]] = ...) -> None: ...
