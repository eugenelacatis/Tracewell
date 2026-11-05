"""Unit tests for eval worker Kafka processing, span filtering, and DLQ routing."""

from typing import Any, Callable, Optional

from google.protobuf.timestamp_pb2 import Timestamp
from tracewell_proto import trace_pb2

from tracewell_evals.worker import EvalWorker, KafkaMessage


class FakeMessage:
    def __init__(
        self,
        topic: str = "traces.raw",
        partition: int = 0,
        offset: int = 10,
        key: Optional[bytes] = b"proj_test",
        value: Optional[bytes] = b"",
        error: Any = None,
    ) -> None:
        self._topic = topic
        self._partition = partition
        self._offset = offset
        self._key = key
        self._value = value
        self._error = error

    def topic(self) -> str:
        return self._topic

    def partition(self) -> int:
        return self._partition

    def offset(self) -> int:
        return self._offset

    def key(self) -> Optional[bytes]:
        return self._key

    def value(self) -> Optional[bytes]:
        return self._value

    def error(self) -> Any:
        return self._error


class FakeProducer:
    def __init__(self) -> None:
        self.messages: list[tuple[str, Optional[bytes], bytes]] = []
        self.flushed = False

    def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        callback: Optional[Callable[..., Any]] = None,
    ) -> None:
        self.messages.append((topic, key, value))

    def flush(self, timeout: float = -1.0) -> int:
        self.flushed = True
        return 0


class FakeConsumer:
    def __init__(self, messages: Optional[list[KafkaMessage]] = None) -> None:
        self.messages = list(messages or [])
        self.committed_messages: list[KafkaMessage] = []
        self.closed = False

    def poll(self, timeout: float = 1.0) -> Optional[KafkaMessage]:
        if self.messages:
            return self.messages.pop(0)
        return None

    def commit(self, message: Optional[KafkaMessage] = None, asynchronous: bool = False) -> None:
        if message:
            self.committed_messages.append(message)

    def close(self) -> None:
        self.closed = True


def make_ingest_batch(
    project_id: str = "proj_test",
    spans: Optional[list[trace_pb2.Span]] = None,
) -> trace_pb2.IngestBatch:
    if spans is None:
        start_time = Timestamp()
        start_time.FromMilliseconds(1_000_000)
        end_time = Timestamp()
        end_time.FromMilliseconds(1_001_500)

        spans = [
            trace_pb2.Span(
                span_id="span_gen1",
                trace_id="trace_1",
                kind=trace_pb2.SpanKind.SPAN_KIND_GENERATION,
                start_time=start_time,
                end_time=end_time,
                generation=trace_pb2.Generation(
                    provider="openai",
                    model="gpt-4o-mini",
                    prompt="Hello",
                    completion="World",
                ),
            )
        ]

    trace = trace_pb2.Trace(
        trace_id="trace_1",
        project_id=project_id,
        spans=spans,
    )

    return trace_pb2.IngestBatch(
        idempotency_key="instance1:1",
        project_id=project_id,
        traces=[trace],
    )


def test_generation_span_produces_two_evals() -> None:
    producer = FakeProducer()
    consumer = FakeConsumer()
    worker = EvalWorker(consumer=consumer, producer=producer)

    batch = make_ingest_batch(project_id="proj_alpha")
    msg = FakeMessage(value=batch.SerializeToString(), key=b"proj_alpha")

    success = worker.process_message(msg)

    assert success is True
    assert len(consumer.committed_messages) == 1
    assert producer.flushed is True

    # Expect 2 EvalResult messages on evals.results: latency and empty_completion
    assert len(producer.messages) == 2
    for topic, key, val in producer.messages:
        assert topic == "evals.results"
        assert key == b"proj_alpha"

        eval_res = trace_pb2.EvalResult()
        eval_res.ParseFromString(val)
        assert eval_res.trace_id == "trace_1"
        assert eval_res.span_id == "span_gen1"
        assert eval_res.project_id == "proj_alpha"
        assert eval_res.evaluator in ("latency", "empty_completion")
        assert eval_res.score == 1.0
        assert eval_res.label == "ok"


def test_non_generation_spans_skipped() -> None:
    producer = FakeProducer()
    consumer = FakeConsumer()
    worker = EvalWorker(consumer=consumer, producer=producer)

    tool_span = trace_pb2.Span(
        span_id="span_tool",
        trace_id="trace_tool",
        kind=trace_pb2.SpanKind.SPAN_KIND_TOOL,
    )
    chain_span = trace_pb2.Span(
        span_id="span_chain",
        trace_id="trace_chain",
        kind=trace_pb2.SpanKind.SPAN_KIND_CHAIN,
    )
    batch = make_ingest_batch(spans=[tool_span, chain_span])
    msg = FakeMessage(value=batch.SerializeToString())

    success = worker.process_message(msg)

    assert success is True
    assert len(consumer.committed_messages) == 1
    # No evals produced for non-generation spans
    assert len(producer.messages) == 0


def test_dlq_after_three_failures() -> None:
    producer = FakeProducer()
    consumer = FakeConsumer()
    worker = EvalWorker(consumer=consumer, producer=producer, max_attempts=3)

    # Corrupt payload that cannot be parsed as protobuf
    corrupt_msg = FakeMessage(
        topic="traces.raw",
        partition=2,
        offset=42,
        key=b"corrupt_key",
        value=b"this is definitely not a protobuf",
    )

    success = worker.process_message(corrupt_msg)

    assert success is False
    assert len(consumer.committed_messages) == 1
    assert producer.flushed is True

    # 1 message sent to DLQ
    assert len(producer.messages) == 1
    topic, key, val = producer.messages[0]
    assert topic == "traces.dlq"
    assert key == b"corrupt_key"

    dl = trace_pb2.DeadLetter()
    dl.ParseFromString(val)
    assert dl.source_topic == "traces.raw"
    assert dl.partition == 2
    assert dl.offset == 42
    assert dl.payload == b"this is definitely not a protobuf"
    assert dl.attempts == 3
    assert len(dl.error) > 0
