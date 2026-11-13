"""Run P1/P2 preprocessing and P3/P4 scoring; P7 owns final decisions."""

import json
import logging
import os
import signal
from pathlib import Path

import psycopg
from confluent_kafka import Consumer, KafkaException
from dotenv import load_dotenv

from backend import db
from backend.stream.enrichment import SourceContextCache
from backend.stream.preprocessing import preprocess_prompt
from .events import validate_raw_event
from .topics import PROMPTS_RAW, bootstrap_servers

LOGGER = logging.getLogger(__name__)
GROUP_ID = "pijd-intake-v1"


def process_message(consumer, message, database_url, source_context_cache=None, scorer=None,
                    anomaly_detector=None):
    """Finish storage before committing; permanent bad records are logged and skipped."""
    location = f"{message.topic()}:{message.partition()}:{message.offset()}"
    try:
        payload = message.value()
        if payload is None:
            raise ValueError("Raw prompt cannot be a tombstone.")
        event = validate_raw_event(json.loads(payload))
        cache = source_context_cache if source_context_cache is not None else SourceContextCache(database_url)
        preprocessing = preprocess_prompt(event["text"], cache.get(event["source"]))
        prompt = db.save_raw_event(database_url, event, preprocessing=preprocessing)
    except (ValueError, UnicodeDecodeError, psycopg.IntegrityError, psycopg.DataError) as error:
        # PostgreSQL's detailed error text can contain a prompt; log only its error class.
        reason = str(error) if isinstance(error, ValueError) and not isinstance(error, UnicodeDecodeError) else type(error).__name__
        LOGGER.warning("Skipping invalid raw prompt at %s: %s", location, reason)
    else:
        LOGGER.info("Stored prompt %s at %s", prompt["id"], location)
        if scorer is not None:
            # Import lazily so intake fixtures need neither model artifacts nor downloads.
            from backend.stream.scoring import score_and_store
            score_and_store(database_url, event, scorer)
        if anomaly_detector is not None:
            from backend.stream.anomaly import score_and_store as score_anomaly_and_store
            score_anomaly_and_store(database_url, event["prompt_id"], anomaly_detector)
    # Operational database failures and commit failures propagate. Never poll a later record after them.
    consumer.commit(message=message, asynchronous=False)


def main():
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    database_url = os.getenv("DATABASE_URL", "postgresql://localhost:5432/pijd")
    try:
        from backend.stream.scoring import PromptScorer
        scorer = PromptScorer.load(database_url)
    except Exception as error:
        # Startup errors are configuration/artifact failures, not per-prompt outcomes.
        LOGGER.error("P3/P4 startup failed (%s): %s", type(error).__name__, error)
        raise SystemExit(1) from None
    try:
        from backend.stream.anomaly import PromptAnomalyDetector
        anomaly_detector = PromptAnomalyDetector.load(database_url)
    except Exception as error:
        LOGGER.error("P5 startup failed (%s): %s", type(error).__name__, error)
        raise SystemExit(1) from None
    consumer = Consumer({
        "bootstrap.servers": bootstrap_servers(),
        "group.id": GROUP_ID,
        "enable.auto.commit": False,
        "enable.auto.offset.store": False,
        "auto.offset.reset": "earliest",
    })
    running = True
    source_context_cache = SourceContextCache(database_url)

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
            process_message(consumer, message, database_url, source_context_cache, scorer, anomaly_detector)
    except (psycopg.Error, KafkaException) as error:
        LOGGER.error("Consumer stopped without advancing further: %s", type(error).__name__)
        raise SystemExit(1) from None
    finally:
        consumer.close()


if __name__ == "__main__":
    main()
