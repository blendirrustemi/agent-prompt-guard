"""P9 + P10 worker: prompts_raw -> burst window -> rate limiter -> PostgreSQL + `throttle_decisions`.

Runs beside the intake consumer in its own consumer group. For each raw prompt it stores the
decision, publishes it for P7 (keyed by prompt_id), and only then commits the Kafka offset.
Run from the project root: python -m backend.stream.burst_worker
"""

import json
import logging
import os
import signal
from datetime import datetime, timezone
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

from backend import db
from backend.intake.events import validate_raw_event
from backend.intake.producer import create_producer, flush_or_raise, publish_event
from backend.intake.topics import PROMPTS_RAW, THROTTLE_DECISIONS, bootstrap_servers

from .burst import BurstDetector
from .rate_limiter import RateLimiter
from .throttle_events import build_throttle_event, event_from_stored

LOGGER = logging.getLogger(__name__)
GROUP_ID = "pijd-burst-v1"


def env_int(name, default):
    value = int(os.getenv(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer.")
    return value


def publish_decision(producer, event, timeout=10):
    errors = []
    publish_event(
        producer, THROTTLE_DECISIONS, event["prompt_id"], event,
        on_delivery=lambda error, message: error and errors.append(error),
    )
    flush_or_raise(producer, errors, timeout=timeout)


def process_message(consumer, producer, message, database_url, detector, limiter):
    """Store and publish one decision before committing; permanent bad records are logged and skipped."""
    location = f"{message.topic()}:{message.partition()}:{message.offset()}"
    try:
        payload = message.value()
        if payload is None:
            raise ValueError("Raw prompt cannot be a tombstone.")
        raw = validate_raw_event(json.loads(payload))
        # Ensures the prompt row exists for the foreign key even if the intake consumer is behind.
        db.save_raw_event(database_url, raw)
    except (ValueError, UnicodeDecodeError, psycopg.IntegrityError, psycopg.DataError) as error:
        # Never log prompt text; PostgreSQL error messages may contain it.
        reason = str(error) if isinstance(error, ValueError) and not isinstance(error, UnicodeDecodeError) else type(error).__name__
        LOGGER.warning("Skipping invalid raw prompt at %s: %s", location, reason)
        consumer.commit(message=message, asynchronous=False)
        return None

    event_time = datetime.fromisoformat(raw["created_at"])
    # Always count the prompt so the window is rebuilt after a restart, even for redelivered records.
    observation = detector.observe(raw["source"], event_time)
    stored = db.get_throttle_decision(database_url, raw["prompt_id"])
    if stored is None:
        decision = limiter.decide(raw["prompt_id"], observation)
        stored = db.save_throttle_decision(database_url, build_throttle_event(decision))
        if decision.burst_started:
            LOGGER.warning(
                "Burst from source %s: %d prompts in %ds; blocked until %s",
                decision.source, decision.window_count, decision.window_seconds, decision.blocked_until.isoformat(),
            )
    # Redelivery re-publishes the first stored decision unchanged.
    event = event_from_stored(stored)
    publish_decision(producer, event)
    LOGGER.info("Throttle %s for prompt %s (source %s) at %s", event["action"], event["prompt_id"], event["source"], location)
    # Database, publish and commit failures propagate; never poll a later record after them.
    consumer.commit(message=message, asynchronous=False)
    return event


def main():
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    database_url = os.getenv("DATABASE_URL", "postgresql://localhost:5432/pijd")
    detector = BurstDetector(
        window_seconds=env_int("BURST_WINDOW_SECONDS", 60),
        threshold=env_int("BURST_THRESHOLD", 10),
    )
    limiter = RateLimiter(cooldown_seconds=env_int("BURST_COOLDOWN_SECONDS", 300))

    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers(),
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
    })
    producer = create_producer()
    running = True

    def stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        for source, blocked_until in db.get_active_source_blocks(database_url, datetime.now(timezone.utc)).items():
            limiter.restore(source, blocked_until)
            LOGGER.info("Restored block for source %s until %s", source, blocked_until.isoformat())
        consumer.subscribe([PROMPTS_RAW])
        LOGGER.info(
            "Consuming %s as %s (window %ds, threshold %d, cooldown %ds)",
            PROMPTS_RAW, GROUP_ID, detector.window_seconds, detector.threshold, limiter.cooldown_seconds,
        )
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            process_message(consumer, producer, message, database_url, detector, limiter)
    except (psycopg.Error, KafkaException) as error:
        LOGGER.error("Burst worker stopped without advancing further: %s", type(error).__name__)
        raise SystemExit(1) from None
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
