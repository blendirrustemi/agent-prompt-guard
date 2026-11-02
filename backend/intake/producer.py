"""Publish raw prompt events to Kafka.

Send one prompt:      python -m backend.intake.producer "Hello there" --source source_01
Send stdin lines:     python -m backend.intake.producer --source source_02 < prompts.txt
"""

import argparse
import json
import sys
from time import monotonic

from confluent_kafka import KafkaException, Producer
from dotenv import load_dotenv

from .events import MAX_PROMPT_LENGTH, SCHEMA_VERSION, build_raw_event, validate_raw_event
from .topics import PROMPTS_RAW, bootstrap_servers


def create_producer(servers=None, timeout=5):
    # Idempotence avoids duplicate messages when the client retries a send.
    return Producer({
        "bootstrap.servers": servers or bootstrap_servers(),
        "enable.idempotence": True,
        "acks": "all",
        "delivery.timeout.ms": int(timeout * 1000),
    })


def publish_raw_prompt(producer, event, on_delivery=None, topic=PROMPTS_RAW):
    """Queue one event; keyed by source so each source's prompts stay in order."""
    event = validate_raw_event(event)
    producer.produce(
        topic,
        key=event["source"].encode(),
        value=json.dumps(event, ensure_ascii=False).encode(),
        on_delivery=on_delivery,
    )
    producer.poll(0)


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
    if producer.flush(timeout):
        raise KafkaException(f"Timed out delivering prompts to Kafka at {servers or bootstrap_servers()}.")
    if errors:
        raise KafkaException(errors[0])
    return events


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
