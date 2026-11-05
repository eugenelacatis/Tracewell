"""Main entrypoint for eval worker service."""

import json
import logging
import os
import signal
import sys
from typing import Any

from confluent_kafka import Consumer, Producer

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


def main() -> None:
    setup_logging()
    logger = logging.getLogger("evals.main")

    http_addr = os.getenv("EVALS_HTTP_ADDR", ":8085")
    kafka_brokers = os.getenv("EVALS_KAFKA_BROKERS", "kafka:29092")
    group_id = os.getenv("EVALS_KAFKA_GROUP_ID", "evals")
    raw_topic = os.getenv("EVALS_RAW_TOPIC", "traces.raw")
    results_topic = os.getenv("EVALS_RESULTS_TOPIC", "evals.results")
    dlq_topic = os.getenv("EVALS_DLQ_TOPIC", "traces.dlq")

    logger.info(
        "Starting eval worker (brokers=%s, group=%s, topic=%s, addr=%s)",
        kafka_brokers,
        group_id,
        raw_topic,
        http_addr,
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

    def kafka_ready_check() -> bool:
        try:
            metadata = consumer.list_topics(timeout=2.0)
            return metadata is not None and metadata.brokers is not None and len(metadata.brokers) > 0
        except Exception:
            return False

    health_server = HealthServer(addr=http_addr, ready_check=kafka_ready_check)
    health_server.start()

    worker = EvalWorker(
        consumer=consumer,
        producer=producer,
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
