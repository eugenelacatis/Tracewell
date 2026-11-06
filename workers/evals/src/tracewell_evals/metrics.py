"""Prometheus metrics for eval worker."""

from prometheus_client import Counter, Gauge

MESSAGES_PROCESSED = Counter(
    "evals_messages_processed_total",
    "Total number of Kafka messages processed by the eval worker",
)

EVALS_PRODUCED = Counter(
    "evals_produced_total",
    "Total number of EvalResult records produced",
    ["evaluator"],
)

DEAD_LETTERS_PRODUCED = Counter(
    "evals_dead_letters_total",
    "Total number of messages sent to Dead Letter Queue (traces.dlq)",
)

PROCESSING_ERRORS = Counter(
    "evals_processing_errors_total",
    "Total number of processing errors encountered",
)

IN_FLIGHT_MESSAGES = Gauge(
    "evals_in_flight",
    "Number of messages currently being processed",
)

