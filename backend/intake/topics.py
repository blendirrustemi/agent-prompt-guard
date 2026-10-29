"""Shared Kafka topic names and connection settings."""

import os

PROMPTS_RAW = "prompts_raw"
PROMPTS_ALLOWED = "prompts_allowed"
PROMPTS_LABELS = "prompts_labels"
ALL_TOPICS = (PROMPTS_RAW, PROMPTS_ALLOWED, PROMPTS_LABELS)


def bootstrap_servers():
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9094")
