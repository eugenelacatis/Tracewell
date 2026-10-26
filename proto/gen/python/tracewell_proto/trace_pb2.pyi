from google.protobuf import timestamp_pb2 as _timestamp_pb2
from google.protobuf import struct_pb2 as _struct_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Mapping as _Mapping, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class SpanKind(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SPAN_KIND_UNSPECIFIED: _ClassVar[SpanKind]
    SPAN_KIND_GENERATION: _ClassVar[SpanKind]
    SPAN_KIND_RETRIEVAL: _ClassVar[SpanKind]
    SPAN_KIND_TOOL: _ClassVar[SpanKind]
    SPAN_KIND_CHAIN: _ClassVar[SpanKind]

class Status(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STATUS_UNSPECIFIED: _ClassVar[Status]
    STATUS_OK: _ClassVar[Status]
    STATUS_ERROR: _ClassVar[Status]
SPAN_KIND_UNSPECIFIED: SpanKind
SPAN_KIND_GENERATION: SpanKind
SPAN_KIND_RETRIEVAL: SpanKind
SPAN_KIND_TOOL: SpanKind
SPAN_KIND_CHAIN: SpanKind
STATUS_UNSPECIFIED: Status
STATUS_OK: Status
STATUS_ERROR: Status

class Usage(_message.Message):
    __slots__ = ("prompt_tokens", "completion_tokens", "total_tokens", "cost_usd")
    PROMPT_TOKENS_FIELD_NUMBER: _ClassVar[int]
    COMPLETION_TOKENS_FIELD_NUMBER: _ClassVar[int]
    TOTAL_TOKENS_FIELD_NUMBER: _ClassVar[int]
    COST_USD_FIELD_NUMBER: _ClassVar[int]
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int
    cost_usd: float
    def __init__(self, prompt_tokens: _Optional[int] = ..., completion_tokens: _Optional[int] = ..., total_tokens: _Optional[int] = ..., cost_usd: _Optional[float] = ...) -> None: ...

class Generation(_message.Message):
    __slots__ = ("provider", "model", "prompt", "completion", "params", "usage")
    PROVIDER_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    PROMPT_FIELD_NUMBER: _ClassVar[int]
    COMPLETION_FIELD_NUMBER: _ClassVar[int]
    PARAMS_FIELD_NUMBER: _ClassVar[int]
    USAGE_FIELD_NUMBER: _ClassVar[int]
    provider: str
    model: str
    prompt: str
    completion: str
    params: _struct_pb2.Struct
    usage: Usage
    def __init__(self, provider: _Optional[str] = ..., model: _Optional[str] = ..., prompt: _Optional[str] = ..., completion: _Optional[str] = ..., params: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ..., usage: _Optional[_Union[Usage, _Mapping]] = ...) -> None: ...

class Span(_message.Message):
    __slots__ = ("span_id", "trace_id", "parent_span_id", "name", "kind", "start_time", "end_time", "status", "error_message", "generation", "metadata")
    SPAN_ID_FIELD_NUMBER: _ClassVar[int]
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    PARENT_SPAN_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    KIND_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    ERROR_MESSAGE_FIELD_NUMBER: _ClassVar[int]
    GENERATION_FIELD_NUMBER: _ClassVar[int]
    METADATA_FIELD_NUMBER: _ClassVar[int]
    span_id: str
    trace_id: str
    parent_span_id: str
    name: str
    kind: SpanKind
    start_time: _timestamp_pb2.Timestamp
    end_time: _timestamp_pb2.Timestamp
    status: Status
    error_message: str
    generation: Generation
    metadata: _struct_pb2.Struct
    def __init__(self, span_id: _Optional[str] = ..., trace_id: _Optional[str] = ..., parent_span_id: _Optional[str] = ..., name: _Optional[str] = ..., kind: _Optional[_Union[SpanKind, str]] = ..., start_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., end_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., status: _Optional[_Union[Status, str]] = ..., error_message: _Optional[str] = ..., generation: _Optional[_Union[Generation, _Mapping]] = ..., metadata: _Optional[_Union[_struct_pb2.Struct, _Mapping]] = ...) -> None: ...

class Trace(_message.Message):
    __slots__ = ("trace_id", "project_id", "session_id", "user_id", "name", "tags", "start_time", "end_time", "spans")
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    SESSION_ID_FIELD_NUMBER: _ClassVar[int]
    USER_ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    TAGS_FIELD_NUMBER: _ClassVar[int]
    START_TIME_FIELD_NUMBER: _ClassVar[int]
    END_TIME_FIELD_NUMBER: _ClassVar[int]
    SPANS_FIELD_NUMBER: _ClassVar[int]
    trace_id: str
    project_id: str
    session_id: str
    user_id: str
    name: str
    tags: _containers.RepeatedScalarFieldContainer[str]
    start_time: _timestamp_pb2.Timestamp
    end_time: _timestamp_pb2.Timestamp
    spans: _containers.RepeatedCompositeFieldContainer[Span]
    def __init__(self, trace_id: _Optional[str] = ..., project_id: _Optional[str] = ..., session_id: _Optional[str] = ..., user_id: _Optional[str] = ..., name: _Optional[str] = ..., tags: _Optional[_Iterable[str]] = ..., start_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., end_time: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ..., spans: _Optional[_Iterable[_Union[Span, _Mapping]]] = ...) -> None: ...

class IngestBatch(_message.Message):
    __slots__ = ("idempotency_key", "project_id", "traces", "sdk_instance_id", "batch_seq")
    IDEMPOTENCY_KEY_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    TRACES_FIELD_NUMBER: _ClassVar[int]
    SDK_INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    BATCH_SEQ_FIELD_NUMBER: _ClassVar[int]
    idempotency_key: str
    project_id: str
    traces: _containers.RepeatedCompositeFieldContainer[Trace]
    sdk_instance_id: str
    batch_seq: int
    def __init__(self, idempotency_key: _Optional[str] = ..., project_id: _Optional[str] = ..., traces: _Optional[_Iterable[_Union[Trace, _Mapping]]] = ..., sdk_instance_id: _Optional[str] = ..., batch_seq: _Optional[int] = ...) -> None: ...

class EvalResult(_message.Message):
    __slots__ = ("trace_id", "span_id", "project_id", "evaluator", "score", "label", "reasoning", "created_at")
    TRACE_ID_FIELD_NUMBER: _ClassVar[int]
    SPAN_ID_FIELD_NUMBER: _ClassVar[int]
    PROJECT_ID_FIELD_NUMBER: _ClassVar[int]
    EVALUATOR_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    LABEL_FIELD_NUMBER: _ClassVar[int]
    REASONING_FIELD_NUMBER: _ClassVar[int]
    CREATED_AT_FIELD_NUMBER: _ClassVar[int]
    trace_id: str
    span_id: str
    project_id: str
    evaluator: str
    score: float
    label: str
    reasoning: str
    created_at: _timestamp_pb2.Timestamp
    def __init__(self, trace_id: _Optional[str] = ..., span_id: _Optional[str] = ..., project_id: _Optional[str] = ..., evaluator: _Optional[str] = ..., score: _Optional[float] = ..., label: _Optional[str] = ..., reasoning: _Optional[str] = ..., created_at: _Optional[_Union[_timestamp_pb2.Timestamp, _Mapping]] = ...) -> None: ...

class DeadLetter(_message.Message):
    __slots__ = ("source_topic", "partition", "offset", "payload", "error", "attempts")
    SOURCE_TOPIC_FIELD_NUMBER: _ClassVar[int]
    PARTITION_FIELD_NUMBER: _ClassVar[int]
    OFFSET_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_FIELD_NUMBER: _ClassVar[int]
    ERROR_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    source_topic: str
    partition: int
    offset: int
    payload: bytes
    error: str
    attempts: int
    def __init__(self, source_topic: _Optional[str] = ..., partition: _Optional[int] = ..., offset: _Optional[int] = ..., payload: _Optional[bytes] = ..., error: _Optional[str] = ..., attempts: _Optional[int] = ...) -> None: ...

class IngestResponse(_message.Message):
    __slots__ = ("accepted",)
    ACCEPTED_FIELD_NUMBER: _ClassVar[int]
    accepted: int
    def __init__(self, accepted: _Optional[int] = ...) -> None: ...
