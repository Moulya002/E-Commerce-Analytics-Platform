"""
Main entry point for e-commerce event producers.
Continuously streams realistic events to Kafka topics.
"""

import random
import signal
import sys
import time
from typing import Callable, Dict, List, Tuple

from configs.logging_config import setup_logging
from configs.settings import settings
from producers.event_generators import (
    generate_click_event,
    generate_inventory_update_event,
    generate_order_event,
    generate_payment_event,
    generate_return_event,
    generate_user_activity_event,
)
from producers.kafka_producer import EcommerceKafkaProducer

logger = setup_logging(__name__)

_running = True


def _shutdown_handler(signum, frame) -> None:
    global _running
    logger.info("Shutdown signal received, stopping producer...")
    _running = False


# (generator, topic, key_extractor, weight)
EVENT_STREAMS: List[Tuple[Callable, str, Callable, float]] = [
    (generate_order_event, settings.kafka.orders_topic, lambda e: e["order_id"], 0.25),
    (generate_payment_event, settings.kafka.payments_topic, lambda e: e["payment_id"], 0.20),
    (generate_click_event, settings.kafka.clicks_topic, lambda e: e["session_id"], 0.30),
    (generate_user_activity_event, settings.kafka.users_topic, lambda e: e["user_id"], 0.15),
    (generate_inventory_update_event, settings.kafka.inventory_updates_topic, lambda e: e["product_id"], 0.05),
    (generate_return_event, settings.kafka.orders_topic, lambda e: e["return_id"], 0.05),
]


def _weighted_choice() -> Tuple[Callable, str, Callable]:
    generators, topics, keys, weights = zip(*EVENT_STREAMS)
    idx = random.choices(range(len(generators)), weights=weights, k=1)[0]
    return generators[idx], topics[idx], keys[idx]


def run_producer() -> None:
    """Run the continuous event producer loop."""
    signal.signal(signal.SIGINT, _shutdown_handler)
    signal.signal(signal.SIGTERM, _shutdown_handler)

    bootstrap = settings.kafka.bootstrap_servers
    eps = settings.producer.events_per_second
    interval = 1.0 / max(eps, 1)

    logger.info(
        "Starting e-commerce event producer | bootstrap=%s | rate=%d events/sec",
        bootstrap,
        eps,
    )

    producer = EcommerceKafkaProducer(bootstrap)
    events_sent = 0
    events_failed = 0

    while _running:
        gen_fn, topic, key_fn = _weighted_choice()
        event = gen_fn()
        key = key_fn(event)

        if producer.produce(topic, event, key=key):
            events_sent += 1
            if events_sent % 100 == 0:
                logger.info("Events sent: %d | topic=%s", events_sent, topic)
        else:
            events_failed += 1
            logger.warning("Failed to produce event to %s", topic)

        time.sleep(interval)

    remaining = producer.flush(timeout=30)
    logger.info(
        "Producer stopped | sent=%d | failed=%d | pending=%d",
        events_sent,
        events_failed,
        remaining,
    )
    sys.exit(0 if events_failed == 0 else 1)


if __name__ == "__main__":
    run_producer()
