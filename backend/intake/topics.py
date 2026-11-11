"""Shared Kafka topic names and connection settings."""

import os

PROMPTS_RAW = "prompts_raw"
PROMPTS_ALLOWED = "prompts_allowed"
PROMPTS_LABELS = "prompts_labels"
# P10 rate-limiter decisions (one per raw prompt), consumed by P7 score routing.
THROTTLE_DECISIONS = "throttle_decisions"
ALL_TOPICS = (PROMPTS_RAW, PROMPTS_ALLOWED, PROMPTS_LABELS, THROTTLE_DECISIONS)


def bootstrap_servers():
    return os.getenv("KAFKA_BOOTSTRAP_SERVERS", "127.0.0.1:9094")
