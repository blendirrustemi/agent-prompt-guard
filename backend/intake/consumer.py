"""Persist raw prompts. This is the entry point for the future guard stages."""

import json
import logging
import os
import signal
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

from backend import db
from .events import validate_raw_event
from .topics import PROMPTS_RAW, bootstrap_servers

LOGGER = logging.getLogger(__name__)
GROUP_ID = "pijd-intake-v1"


def process_message(consumer, message, database_url):
    """Finish storage before committing; permanent bad records are logged and skipped."""
    location = f"{message.topic()}:{message.partition()}:{message.offset()}"
    try:
        payload = message.value()
        if payload is None:
            raise ValueError("Raw prompt cannot be a tombstone.")
        event = validate_raw_event(json.loads(payload))
        prompt = db.save_raw_event(database_url, event)
    except (ValueError, UnicodeDecodeError, psycopg.IntegrityError, psycopg.DataError) as error:
        # PostgreSQL's detailed error text can contain a prompt; log only its error class.
        reason = str(error) if isinstance(error, ValueError) and not isinstance(error, UnicodeDecodeError) else type(error).__name__
        LOGGER.warning("Skipping invalid raw prompt at %s: %s", location, reason)
    else:
        LOGGER.info("Stored prompt %s at %s", prompt["id"], location)
    # Operational database failures and commit failures propagate. Never poll a later record after them.
    consumer.commit(message=message, asynchronous=False)


def main():
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers(),
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
    })
    running = True

    def stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    try:
        consumer.subscribe([PROMPTS_RAW])
        LOGGER.info("Consuming %s as %s", PROMPTS_RAW, GROUP_ID)
        while running:
            message = consumer.poll(1.0)
            if message is None:
                continue
            if message.error():
                raise KafkaException(message.error())
            process_message(consumer, message, os.getenv("DATABASE_URL", "postgresql://localhost:5432/pijd"))
    except (psycopg.Error, KafkaException) as error:
        LOGGER.error("Consumer stopped without advancing further: %s", type(error).__name__)
        raise SystemExit(1) from None
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
