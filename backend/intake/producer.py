"""Publish raw prompt events to Kafka.

Send one prompt:      python -m backend.intake.producer "Hello there" --source source_01
Send stdin lines:     python -m backend.intake.producer --source source_02 < prompts.txt
"""

import argparse
import json
import sys
from datetime import datetime, timezone
from uuid import uuid4

from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

from .topics import PROMPTS_RAW, bootstrap_servers

SCHEMA_VERSION = 1
MAX_PROMPT_LENGTH = 8_000


def build_raw_event(text, source_id="web", dataset_ref=None, prompt_id=None, received_at=None):
    """Return the documented `prompts_raw` event; ids and timestamps are metadata, never parsed from text."""
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Prompt text must be non-empty.")
    if len(text) > MAX_PROMPT_LENGTH:
        raise ValueError(f"Prompt text must be at most {MAX_PROMPT_LENGTH} characters.")
    return {
        "schema_version": SCHEMA_VERSION,
        "prompt_id": str(prompt_id or uuid4()),
        "received_at": (received_at or datetime.now(timezone.utc)).isoformat(),
        "source_id": source_id,
        "dataset_ref": dataset_ref,
        "text": text,
    }


def create_producer(servers=None):
    # Idempotence avoids duplicate messages when the client retries a send.
    return Producer({"bootstrap.servers": servers or bootstrap_servers(), "enable.idempotence": True})


def publish_raw_prompt(producer, event, on_delivery=None, topic=PROMPTS_RAW):
    """Queue one event; keyed by source so each source's prompts stay in order."""
    producer.produce(
        topic,
        key=event["source_id"].encode(),
        value=json.dumps(event, ensure_ascii=False).encode(),
        on_delivery=on_delivery,
    )
    producer.poll(0)


def send_prompts(texts, source_id="web", dataset_ref=None, servers=None, timeout=10):
    """Publish each text and wait until Kafka confirms delivery; returns the sent events."""
    producer = create_producer(servers)
    events, errors = [], []

    def on_delivery(error, message):
        if error:
            errors.append(error)

    for text in texts:
        event = build_raw_event(text, source_id=source_id, dataset_ref=dataset_ref)
        publish_raw_prompt(producer, event, on_delivery)
        events.append(event)
    if producer.flush(timeout):
        raise KafkaException(f"Timed out delivering prompts to Kafka at {servers or bootstrap_servers()}.")
    if errors:
        raise KafkaException(errors[0])
    return events


def main():
    load_dotenv()
    parser = argparse.ArgumentParser(description="Publish raw prompt events to Kafka.")
    parser.add_argument("text", nargs="*", help="Prompt text; reads one prompt per stdin line if omitted")
    parser.add_argument("--source", default="web", help="source_id for the events (default: web)")
    parser.add_argument("--dataset-ref", help="Optional dataset and row identifier")
    args = parser.parse_args()

    texts = args.text or [line.rstrip("\n") for line in sys.stdin if line.strip()]
    for event in send_prompts(texts, source_id=args.source, dataset_ref=args.dataset_ref):
        print(f"Sent {event['prompt_id']} to {PROMPTS_RAW}")


if __name__ == "__main__":
    main()
