"""Kafka consumer worker for evaluating generation spans."""

import logging
from typing import Any, Callable, Optional, Protocol

from google.protobuf.timestamp_pb2 import Timestamp
from tracewell_proto import trace_pb2

from tracewell_evals.evaluators import BaseEvaluator, get_default_evaluators
from tracewell_evals.metrics import (
    DEAD_LETTERS_PRODUCED,
    EVALS_PRODUCED,
    IN_FLIGHT_MESSAGES,
    MESSAGES_PROCESSED,
    PROCESSING_ERRORS,
)

logger = logging.getLogger(__name__)


class KafkaMessage(Protocol):
    """Protocol matching confluent_kafka.Message for typing and testing."""

    def topic(self) -> str: ...
    def partition(self) -> int: ...
    def offset(self) -> int: ...
    def key(self) -> Optional[bytes]: ...
    def value(self) -> Optional[bytes]: ...
    def error(self) -> Any: ...


class ProducerProtocol(Protocol):
    """Protocol matching confluent_kafka.Producer."""

    def produce(
        self,
        topic: str,
        value: bytes,
        key: Optional[bytes] = None,
        callback: Optional[Callable[..., Any]] = None,
    ) -> None: ...
    def flush(self, timeout: float = -1.0) -> int: ...


class ConsumerProtocol(Protocol):
    """Protocol matching confluent_kafka.Consumer."""

    def poll(self, timeout: float = 1.0) -> Optional[KafkaMessage]: ...
    def commit(self, message: Optional[KafkaMessage] = None, asynchronous: bool = False) -> None: ...
    def close(self) -> None: ...


class EvalWorker:
    """Consumes IngestBatch from traces.raw, evaluates GENERATION spans, and emits EvalResults."""

    def __init__(
        self,
        consumer: Optional[ConsumerProtocol],
        producer: ProducerProtocol,
        evaluators: Optional[list[BaseEvaluator]] = None,
        raw_topic: str = "traces.raw",
        results_topic: str = "evals.results",
        dlq_topic: str = "traces.dlq",
        max_attempts: int = 3,
    ) -> None:
        self.consumer = consumer
        self.producer = producer
        self.evaluators = evaluators if evaluators is not None else get_default_evaluators()
        self.raw_topic = raw_topic
        self.results_topic = results_topic
        self.dlq_topic = dlq_topic
        self.max_attempts = max_attempts
        self._running = False

    def process_batch(self, batch: trace_pb2.IngestBatch) -> int:
        """Process an IngestBatch, evaluating all GENERATION spans.

        Returns the number of EvalResults produced.
        """
        produced_count = 0
        for trace in batch.traces:
            project_id = batch.project_id or trace.project_id
            for span in trace.spans:
                # Round 1 brief: Run every evaluator on every GENERATION span. Skip non-generation.
                if span.kind != trace_pb2.SpanKind.SPAN_KIND_GENERATION:
                    continue

                for evaluator in self.evaluators:
                    out = evaluator.evaluate(span)
                    if out is None:
                        continue

                    created_at = Timestamp()
                    created_at.GetCurrentTime()

                    eval_result = trace_pb2.EvalResult(
                        trace_id=span.trace_id or trace.trace_id,
                        span_id=span.span_id,
                        project_id=project_id,
                        evaluator=evaluator.name,
                        score=out.score,
                        label=out.label,
                        reasoning=out.reasoning,
                        created_at=created_at,
                    )

                    key = project_id.encode("utf-8") if project_id else b""
                    self.producer.produce(
                        topic=self.results_topic,
                        key=key,
                        value=eval_result.SerializeToString(),
                    )
                    EVALS_PRODUCED.labels(evaluator=evaluator.name).inc()
                    produced_count += 1

        return produced_count

    def process_message(self, msg: KafkaMessage) -> bool:
        """Process a single Kafka message with retry and DLQ routing.

        Returns True if successfully processed, False if routed to DLQ.
        """
        IN_FLIGHT_MESSAGES.inc()
        attempts = 0
        last_error: Optional[Exception] = None

        try:
            while attempts < self.max_attempts:
                attempts += 1
                try:
                    raw_val = msg.value()
                    if raw_val is None:
                        raise ValueError("Received message with empty value")

                    batch = trace_pb2.IngestBatch()
                    batch.ParseFromString(raw_val)

                    self.process_batch(batch)
                    self.producer.flush()

                    if self.consumer:
                        self.consumer.commit(message=msg, asynchronous=False)

                    MESSAGES_PROCESSED.inc()
                    return True

                except Exception as exc:
                    last_error = exc
                    PROCESSING_ERRORS.inc()
                    logger.warning(
                        "Processing error on attempt %d/%d (topic=%s, partition=%s, offset=%s): %s",
                        attempts,
                        self.max_attempts,
                        msg.topic(),
                        msg.partition(),
                        msg.offset(),
                        exc,
                    )

            # Max attempts exhausted: send to DLQ
            self._send_to_dlq(msg, last_error, attempts)
            if self.consumer:
                self.consumer.commit(message=msg, asynchronous=False)
            return False

        finally:
            IN_FLIGHT_MESSAGES.dec()

    def _send_to_dlq(
        self,
        msg: KafkaMessage,
        error: Optional[Exception],
        attempts: int,
    ) -> None:
        """Produce DeadLetter message to traces.dlq."""
        err_msg = str(error) if error else "Max processing attempts exceeded"
        logger.error(
            "Routing message to DLQ %s after %d attempts: %s",
            self.dlq_topic,
            attempts,
            err_msg,
        )

        dl = trace_pb2.DeadLetter(
            source_topic=msg.topic(),
            partition=msg.partition(),
            offset=msg.offset(),
            payload=msg.value() or b"",
            error=err_msg,
            attempts=attempts,
        )

        self.producer.produce(
            topic=self.dlq_topic,
            key=msg.key(),
            value=dl.SerializeToString(),
        )
        self.producer.flush()
        DEAD_LETTERS_PRODUCED.inc()

    def run(self) -> None:
        """Main worker consumer loop until stopped."""
        if not self.consumer:
            raise RuntimeError("Cannot run worker without a configured consumer")

        self._running = True
        logger.info("Eval worker consumer loop started on topic: %s", self.raw_topic)

        try:
            while self._running:
                msg = self.consumer.poll(timeout=1.0)
                if msg is None:
                    continue
                if msg.error():
                    logger.error("Kafka consumer error: %s", msg.error())
                    continue

                self.process_message(msg)
        finally:
            logger.info("Shutting down eval worker: flushing producer and closing consumer")
            self.producer.flush()
            self.consumer.close()

    def stop(self) -> None:
        """Signal worker to stop after the current message is processed."""
        self._running = False
