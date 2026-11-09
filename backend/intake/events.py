"""The version-two raw prompt contract shared by intake and the consumer."""

from datetime import datetime, timezone
from uuid import UUID, uuid4

SCHEMA_VERSION = 2
MAX_PROMPT_LENGTH = 8_000
EVENT_FIELDS = {"schema_version", "prompt_id", "text", "source", "created_at", "dataset_prompt_id"}


def validate_raw_event(event):
    """Validate and normalize metadata while keeping prompt text literally."""
    if not isinstance(event, dict) or set(event) != EVENT_FIELDS:
        raise ValueError("Expected the six version-two raw prompt fields.")
    if type(event["schema_version"]) is not int or event["schema_version"] != SCHEMA_VERSION:
        raise ValueError("Unsupported raw prompt schema version.")
    text = event["text"]
    if not isinstance(text, str) or not text.strip():
        raise ValueError("Prompt text must be non-empty.")
    if len(text) > MAX_PROMPT_LENGTH:
        raise ValueError(f"Prompt text must be at most {MAX_PROMPT_LENGTH} characters.")
    if "\x00" in text:
        raise ValueError("Prompt text cannot contain null characters.")
    source = event["source"]
    if not isinstance(source, str) or not source.strip() or "\x00" in source:
        raise ValueError("Source must be non-empty text without null characters.")

    normalized = dict(event)
    for field in ("prompt_id", "dataset_prompt_id"):
        if field == "dataset_prompt_id" and event[field] is None:
            continue
        try:
            normalized[field] = str(UUID(event[field]))
        except (ValueError, TypeError, AttributeError):
            raise ValueError(f"{field} must be a UUID.") from None
    try:
        timestamp = datetime.fromisoformat(event["created_at"].replace("Z", "+00:00"))
        if timestamp.utcoffset() is None:
            raise ValueError
        normalized["created_at"] = timestamp.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError, AttributeError, OverflowError):
        raise ValueError("created_at must be a timestamp with a timezone.") from None
    return normalized


def build_raw_event(text, source="web", dataset_prompt_id=None, prompt_id=None, created_at=None):
    timestamp = created_at if created_at is not None else datetime.now(timezone.utc)
    if not isinstance(timestamp, datetime):
        raise ValueError("created_at must be a datetime with a timezone.")
    return validate_raw_event({
        "schema_version": SCHEMA_VERSION,
        "prompt_id": str(prompt_id) if prompt_id is not None else str(uuid4()),
        "text": text,
        "source": source,
        "created_at": timestamp.isoformat(),
        "dataset_prompt_id": str(dataset_prompt_id) if dataset_prompt_id is not None else None,
    })
