from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class PublishRequest(_message.Message):
    __slots__ = ("data",)
    DATA_FIELD_NUMBER: _ClassVar[int]
    data: str
    def __init__(self, data: _Optional[str] = ...) -> None: ...

class PublishResponse(_message.Message):
    __slots__ = ("success", "leader_id", "message")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_FIELD_NUMBER: _ClassVar[int]
    success: bool
    leader_id: str
    message: str
    def __init__(self, success: _Optional[bool] = ..., leader_id: _Optional[str] = ..., message: _Optional[str] = ...) -> None: ...

class Empty(_message.Message):
    __slots__ = ()
    def __init__(self) -> None: ...

class ConsumeResponse(_message.Message):
    __slots__ = ("success", "leader_id", "commited_data")
    SUCCESS_FIELD_NUMBER: _ClassVar[int]
    LEADER_ID_FIELD_NUMBER: _ClassVar[int]
    COMMITED_DATA_FIELD_NUMBER: _ClassVar[int]
    success: bool
    leader_id: str
    commited_data: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, success: _Optional[bool] = ..., leader_id: _Optional[str] = ..., commited_data: _Optional[_Iterable[str]] = ...) -> None: ...
