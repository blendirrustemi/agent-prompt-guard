"""Publish raw prompt events (and allowed prompts for the agent) to Kafka.

Send one prompt:      python -m backend.intake.producer "Hello there" --source source_01
Send stdin lines:     python -m backend.intake.producer --source source_02 < prompts.txt
"""

import argparse
import json
import sys
from time import monotonic
from uuid import UUID

from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

from .events import MAX_PROMPT_LENGTH, SCHEMA_VERSION, build_raw_event, validate_raw_event
from .topics import PROMPTS_ALLOWED, PROMPTS_RAW, bootstrap_servers

ALLOWED_SCHEMA_VERSION = 1


def create_producer(servers=None, timeout=5):
    # Idempotence avoids duplicate messages when the client retries a send.
    return Producer({
        "bootstrap.servers": servers or bootstrap_servers(),
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": int(timeout * 1000),
    })


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
    """Queue a validated raw event; preserve the ordering of each source."""
    event = validate_raw_event(event)
    publish_event(producer, topic, event["source"], event, on_delivery)


def send_raw_event(producer, event, timeout=5):
    """Wait for this event's delivery callback, without flushing other requests."""
    delivered = []
    deadline = monotonic() + timeout
    publish_raw_prompt(producer, event, lambda error, message: delivered.append(error))
    while not delivered:
        remaining = deadline - monotonic()
        if remaining <= 0:
            raise KafkaException("Could not confirm prompt delivery before the timeout.")
        producer.poll(min(remaining, 0.1))
    if delivered[0] is not None:
        raise KafkaException(delivered[0])


def flush_or_raise(producer, errors, servers=None, timeout=10):
    """Wait until Kafka confirms every queued message; raise on timeout or delivery errors."""
    if producer.flush(timeout):
        raise KafkaException(f"Timed out delivering messages to Kafka at {servers or bootstrap_servers()}.")
    if errors:
        raise KafkaException(errors[0])


def send_prompts(texts, source="web", dataset_prompt_id=None, servers=None, timeout=10):
    """Publish each text and wait until Kafka confirms delivery; returns the sent events."""
    producer = create_producer(servers, timeout=timeout)
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
    producer = create_producer(servers, timeout=timeout)
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
