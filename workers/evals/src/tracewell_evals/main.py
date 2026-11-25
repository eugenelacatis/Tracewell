"""Main entrypoint for eval worker service."""

import json
import logging
import os
import signal
import socket
import sys
from typing import Any

from confluent_kafka import Consumer, Producer

from tracewell_evals.evaluators import get_default_evaluators
from tracewell_evals.server import HealthServer
from tracewell_evals.worker import EvalWorker


class JsonLogFormatter(logging.Formatter):
    """Formats logs as JSON lines to match platform standards."""

    def format(self, record: logging.LogRecord) -> str:
        log_data: dict[str, Any] = {
            "time": self.formatTime(record, self.datefmt),
            "level": record.levelname,
            "message": record.getMessage(),
            "logger": record.name,
        }
        if record.exc_info:
            log_data["exception"] = self.formatException(record.exc_info)
        return json.dumps(log_data)


def setup_logging() -> None:
    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(JsonLogFormatter())
    root = logging.getLogger()
    root.setLevel(logging.INFO)
    root.handlers = [handler]


def check_kafka_ready(brokers_str: str, timeout: float = 1.0) -> bool:
    """Quickly check if at least one Kafka broker is reachable without hanging."""
    for broker in brokers_str.split(","):
        broker = broker.strip()
        if not broker:
            continue
        host, _, port_str = broker.partition(":")
        port = int(port_str) if port_str else 9092
        try:
            with socket.create_connection((host, port), timeout=timeout):
                return True
        except OSError:
            continue
    return False


def main() -> None:
    setup_logging()
    logger = logging.getLogger("evals.main")

    http_addr = os.getenv("EVALS_HTTP_ADDR", ":8085")
    kafka_brokers = os.getenv("EVALS_KAFKA_BROKERS", "kafka:29092")
    group_id = os.getenv("EVALS_KAFKA_GROUP_ID", "evals")
    raw_topic = os.getenv("EVALS_RAW_TOPIC", "traces.raw")
    results_topic = os.getenv("EVALS_RESULTS_TOPIC", "evals.results")
    dlq_topic = os.getenv("EVALS_DLQ_TOPIC", "traces.dlq")

    anthropic_key = os.getenv("EVALS_ANTHROPIC_API_KEY", "")
    judge_model = os.getenv("EVALS_JUDGE_MODEL", "claude-haiku-5-5")
    try:
        judge_sample_rate = float(os.getenv("EVALS_JUDGE_SAMPLE_RATE", "1.0"))
    except ValueError:
        judge_sample_rate = 1.0

    logger.info(
        "Starting eval worker (brokers=%s, group=%s, topic=%s, addr=%s, judge_model=%s)",
        kafka_brokers,
        group_id,
        raw_topic,
        http_addr,
        judge_model if anthropic_key else "off",
    )

    # Kafka consumer configuration with manual commits
    consumer_conf = {
        "bootstrap.servers": kafka_brokers,
        "group.id": group_id,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,
    }
    consumer = Consumer(consumer_conf)
    consumer.subscribe([raw_topic])

    # Kafka producer configuration with idempotence and acks=all
    producer_conf = {
        "bootstrap.servers": kafka_brokers,
        "acks": "all",
        "enable.idempotence": True,
    }
    producer = Producer(producer_conf)

    # Reliable, non-hanging ready check using socket connect with 1.0s timeout
    def kafka_ready_check() -> bool:
        return check_kafka_ready(kafka_brokers, timeout=1.0)

    health_server = HealthServer(addr=http_addr, ready_check=kafka_ready_check)
    health_server.start()

    evaluators = get_default_evaluators(
        anthropic_api_key=anthropic_key,
        judge_model=judge_model,
        judge_sample_rate=judge_sample_rate,
    )

    worker = EvalWorker(
        consumer=consumer,
        producer=producer,
        evaluators=evaluators,
        raw_topic=raw_topic,
        results_topic=results_topic,
        dlq_topic=dlq_topic,
    )

    def shutdown_handler(signum: int, frame: Any) -> None:
        logger.info("Received signal %s; shutting down gracefully...", signum)
        worker.stop()

    signal.signal(signal.SIGTERM, shutdown_handler)
    signal.signal(signal.SIGINT, shutdown_handler)

    try:
        worker.run()
    except Exception as exc:
        logger.exception("Fatal error in eval worker: %s", exc)
        sys.exit(1)
    finally:
        health_server.stop()
        logger.info("Eval worker exited cleanly")


if __name__ == "__main__":
    main()
