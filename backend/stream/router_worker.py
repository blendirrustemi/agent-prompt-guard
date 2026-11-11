"""P7: route stored P4 scores with P10 decisions; only allowed prompts reach the agent."""

import json
import logging
import os
import signal
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

from backend import db
from backend.intake.producer import build_allowed_event, create_producer, flush_or_raise, publish_event
from backend.intake.topics import PROMPTS_ALLOWED, THROTTLE_DECISIONS, bootstrap_servers
from .router import route_pending_row, validate_policy
from .throttle_events import validate_throttle_event

LOGGER = logging.getLogger(__name__)
GROUP_ID = "pijd-router-v1"


def env_float(name, default):
    value = float(os.getenv(name, default))
    if not 0 <= value <= 1:
        raise ValueError(f"{name} must be between 0 and 1.")
    return value


def env_seconds(name, default):
    value = float(os.getenv(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be positive.")
    return value


def process_throttle_message(consumer, message, database_url):
    """Persist the P10 decision and commit promptly; score joins run independently."""
    location = f"{message.topic()}:{message.partition()}:{message.offset()}"
    try:
        payload = message.value()
        if payload is None:
            raise ValueError("Throttle decision cannot be a tombstone.")
        event = validate_throttle_event(json.loads(payload))
    except (ValueError, UnicodeDecodeError) as error:
        LOGGER.warning("Skipping invalid throttle decision at %s: %s", location, error)
        consumer.commit(message=message, asynchronous=False)
        return None

    db.save_throttle_decision(
        database_url, {field: event[field] for field in db.THROTTLE_FIELDS}
    )
    if event["action"] == "block":
        # P10's block is an immediate override; it never waits for a classifier score.
        db.route_guard_result(
            database_url,
            event["prompt_id"],
            "block",
            f"throttle_{event['reason']}",
        )
        LOGGER.info("P10 block applied to prompt %s from source %s", event["prompt_id"], event["source"])
    consumer.commit(message=message, asynchronous=False)
    return event


def route_ready_prompts(database_url, *, allow_max, block_min, score_timeout_seconds, now=None):
    """Finalize all P10-allowed prompts with a score or an expired score deadline."""
    now = now or datetime.now(timezone.utc)
    ready_before = now - timedelta(seconds=score_timeout_seconds)
    routed = []
    for row in db.get_pending_routes(database_url, ready_before=ready_before):
        if row["throttle_action"] == "block":
            result = ("block", f"throttle_{row['throttle_reason']}")
        else:
            result = route_pending_row(row, now, allow_max, block_min, score_timeout_seconds)
        if result is None:
            continue
        decision, reason = result
        if db.route_guard_result(database_url, row["prompt_id"], decision, reason):
            LOGGER.info("P7 routed prompt %s as %s (%s)", row["prompt_id"], decision, reason)
            routed.append((str(row["prompt_id"]), decision))
    return routed


def publish_allowed_outbox(database_url, producer, *, limit=100, timeout=10):
    """Publish pending allowed events and mark them only after Kafka acknowledges delivery."""
    rows = db.get_unpublished_allowed_prompts(database_url, limit=limit)
    if not rows:
        return 0
    errors = []
    for row in rows:
        event = build_allowed_event(row["prompt_id"], row["text"], row["model_version"])
        publish_event(
            producer,
            PROMPTS_ALLOWED,
            event["prompt_id"],
            event,
            on_delivery=lambda error, message: errors.append(error) if error else None,
        )
    flush_or_raise(producer, errors, timeout=timeout)
    return db.mark_allowed_prompts_published(database_url, [row["prompt_id"] for row in rows])


def run(database_url, *, servers=None, allow_max=0.30, block_min=0.70,
        score_timeout_seconds=30):
    """Consume P10 decisions, resolve ready joins, and retry the allowed-prompt outbox."""
    validate_policy(allow_max, block_min, score_timeout_seconds)
    consumer = Consumer({
        "bootstrap.servers": servers or bootstrap_servers(),
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
    })
    producer = create_producer(servers)
    running = True

    def stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    consumer.subscribe([THROTTLE_DECISIONS])
    LOGGER.info(
        "P7 consuming %s (allow <= %.3f, block >= %.3f, score timeout %.1fs)",
        THROTTLE_DECISIONS, allow_max, block_min, score_timeout_seconds,
    )
    try:
        while running:
            message = consumer.poll(0.25)
            if message is not None:
                if message.error():
                    raise KafkaException(message.error())
                process_throttle_message(consumer, message, database_url)
            route_ready_prompts(
                database_url,
                allow_max=allow_max,
                block_min=block_min,
                score_timeout_seconds=score_timeout_seconds,
            )
            publish_allowed_outbox(database_url, producer)
    except (psycopg.Error, KafkaException) as error:
        LOGGER.error("P7 router stopped without advancing further: %s", type(error).__name__)
        raise SystemExit(1) from None
    finally:
        consumer.close()
        producer.flush(5)


def main():
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    database_url = os.getenv("DATABASE_URL", "postgresql://localhost:5432/pijd")
    allow_max = env_float("P7_ALLOW_MAX_SCORE", "0.30")
    block_min = env_float("P7_BLOCK_MIN_SCORE", "0.70")
    timeout = env_seconds("P7_SCORE_TIMEOUT_SECONDS", "30")
    validate_policy(allow_max, block_min, timeout)
    run(database_url, allow_max=allow_max, block_min=block_min, score_timeout_seconds=timeout)


if __name__ == "__main__":
    main()
