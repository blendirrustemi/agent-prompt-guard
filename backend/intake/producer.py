"""Publish raw prompt events (and allowed prompts for the agent) to Kafka.

Send one prompt:      python -m backend.intake.producer "Hello there" --source source_01
Send stdin lines:     python -m backend.intake.producer --source source_02 < prompts.txt
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from uuid import UUID, uuid4

from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

from .topics import PROMPTS_ALLOWED, PROMPTS_RAW, bootstrap_servers

# Version 2 renamed the fields to match the prompts table (see backend/schema.sql).
SCHEMA_VERSION = 2
ALLOWED_SCHEMA_VERSION = 1
MAX_PROMPT_LENGTH = 8_000


def build_raw_event(text, source="web", dataset_prompt_id=None, prompt_id=None, created_at=None):
    """Return the documented `prompts_raw` event; ids and timestamps are metadata, never parsed from text.

    Fields map one-to-one onto the prompts table: prompt_id -> id, text, source, created_at, dataset_prompt_id.
    """
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Prompt text must be non-empty.")
    if len(text) > MAX_PROMPT_LENGTH:
        raise ValueError(f"Prompt text must be at most {MAX_PROMPT_LENGTH} characters.")
    if not isinstance(source, str) or not source.strip():
        raise ValueError("Source must be non-empty.")
    return {
        "schema_version": SCHEMA_VERSION,
        "prompt_id": str(prompt_id or uuid4()),
        "text": text,
        "source": source,
        "created_at": (created_at or datetime.now(timezone.utc)).isoformat(),
        # Must be a dataset_prompts.id; raises ValueError if it is not a UUID.
        "dataset_prompt_id": str(UUID(str(dataset_prompt_id))) if dataset_prompt_id else None,
    }


def create_producer(servers=None):
    # Idempotence avoids duplicate messages when the client retries a send.
    return Producer({"bootstrap.servers": servers or bootstrap_servers(), "enable.idempotence": True})


def build_allowed_event(prompt_id, text, model_version):
    """Return the `prompts_allowed` event the guard publishes for a prompt it allowed."""
    return {
        "schema_version": ALLOWED_SCHEMA_VERSION,
        "prompt_id": str(UUID(str(prompt_id))),
        "text": text,
        "model_version": model_version,
    }


def publish_event(producer, topic, key, event, on_delivery=None):
    """Queue one JSON event on a topic."""
    producer.produce(
        topic,
        key=key.encode(),
        value=json.dumps(event, ensure_ascii=False).encode(),
        on_delivery=on_delivery,
    )
    producer.poll(0)


def publish_raw_prompt(producer, event, on_delivery=None, topic=PROMPTS_RAW):
    """Queue one event; keyed by source so each source's prompts stay in order."""
    publish_event(producer, topic, event["source"], event, on_delivery)


def flush_or_raise(producer, errors, servers=None, timeout=10):
    """Wait until Kafka confirms every queued message; raise on timeout or delivery errors."""
    if producer.flush(timeout):
        raise KafkaException(f"Timed out delivering messages to Kafka at {servers or bootstrap_servers()}.")
    if errors:
        raise KafkaException(errors[0])


def send_prompts(texts, source="web", dataset_prompt_id=None, servers=None, timeout=10):
    """Publish each text and wait until Kafka confirms delivery; returns the sent events."""
    producer = create_producer(servers)
    events, errors = [], []

    def on_delivery(error, message):
        if error:
            errors.append(error)

    for text in texts:
        event = build_raw_event(text, source=source, dataset_prompt_id=dataset_prompt_id)
        publish_raw_prompt(producer, event, on_delivery)
        events.append(event)
    flush_or_raise(producer, errors, servers, timeout)
    return events


def publish_allowed_prompt(prompt_id, text, model_version, servers=None, timeout=10):
    """Publish one allowed prompt to `prompts_allowed`, keyed by prompt_id; returns the event."""
    producer = create_producer(servers)
    errors = []
    event = build_allowed_event(prompt_id, text, model_version)
    publish_event(
        producer, PROMPTS_ALLOWED, event["prompt_id"], event,
        on_delivery=lambda error, message: error and errors.append(error),
    )
    flush_or_raise(producer, errors, servers, timeout)
    return event


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Publish raw prompt events to Kafka.")
    parser.add_argument("text", nargs="*", help="Prompt text; reads one prompt per stdin line if omitted")
    parser.add_argument("--source", default="web", help="source for the events, e.g. source_01 (default: web)")
    parser.add_argument("--dataset-prompt-id", help="Optional dataset_prompts.id (UUID) the prompt was replayed from")
    args = parser.parse_args()

    texts = args.text or [line.rstrip("\n") for line in sys.stdin if line.strip()]
    for event in send_prompts(texts, source=args.source, dataset_prompt_id=args.dataset_prompt_id):
        print(f"Sent {event['prompt_id']} to {PROMPTS_RAW}")


if __name__ == "__main__":
    main()
