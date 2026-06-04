"""
Kafka producer wrapper with retry logic and delivery callbacks.
"""

import json
import time
from typing import Any, Callable, Dict, Optional

from confluent_kafka import Producer
from confluent_kafka import KafkaException

from configs.logging_config import setup_logging

logger = setup_logging(__name__)

MAX_RETRIES = 3
RETRY_BACKOFF_SECONDS = 1.0


class EcommerceKafkaProducer:
    """Production-style Kafka producer with retries and keyed partitioning."""

    def __init__(self, bootstrap_servers: str):
        self._producer = Producer({
            "bootstrap.servers": bootstrap_servers,
            "client.id": "ecommerce-event-producer",
            "acks": "all",
            "retries": MAX_RETRIES,
            "retry.backoff.ms": 500,
            "linger.ms": 10,
            "batch.size": 16384,
            "compression.type": "lz4",
        })
        self._delivery_errors = 0

    def _delivery_callback(self, err, msg) -> None:
        if err is not None:
            self._delivery_errors += 1
            logger.error(
                "Delivery failed: topic=%s key=%s error=%s",
                msg.topic() if msg else "unknown",
                msg.key().decode() if msg and msg.key() else None,
                err,
            )
        else:
            logger.debug(
                "Delivered to %s [%d] @ %d",
                msg.topic(),
                msg.partition(),
                msg.offset(),
            )

    def produce(
        self,
        topic: str,
        event: Dict[str, Any],
        key: Optional[str] = None,
    ) -> bool:
        """Produce a JSON event to Kafka with retry on buffer full."""
        payload = json.dumps(event, default=str).encode("utf-8")
        key_bytes = key.encode("utf-8") if key else None

        for attempt in range(MAX_RETRIES):
            try:
                self._producer.produce(
                    topic=topic,
                    value=payload,
                    key=key_bytes,
                    callback=self._delivery_callback,
                )
                self._producer.poll(0)
                return True
            except BufferError:
                logger.warning("Producer buffer full, flushing (attempt %d)", attempt + 1)
                self._producer.poll(1)
                time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))
            except KafkaException as e:
                logger.error("Kafka produce error: %s", e)
                if attempt == MAX_RETRIES - 1:
                    return False
                time.sleep(RETRY_BACKOFF_SECONDS * (attempt + 1))

        return False

    def flush(self, timeout: float = 30.0) -> int:
        """Flush pending messages. Returns number of messages still in queue."""
        remaining = self._producer.flush(timeout)
        if self._delivery_errors:
            logger.warning("Total delivery errors: %d", self._delivery_errors)
        return remaining

    @property
    def delivery_errors(self) -> int:
        return self._delivery_errors
