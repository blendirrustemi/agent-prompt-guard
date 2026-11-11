"""The version-one `throttle_decisions` event: P10's per-prompt decision, consumed by P7.

P7 applies `action == "block"` as an instant override before score routing; `allow` means the
rate limiter has no objection and routing follows the model scores.
"""

from datetime import datetime, timezone
from uuid import UUID

from .rate_limiter import ALLOW, BLOCK, BURST_DETECTED, SOURCE_BLOCKED

SCHEMA_VERSION = 1
EVENT_FIELDS = {
    "schema_version", "prompt_id", "source", "action", "reason", "window_count", "window_seconds",
    "threshold", "blocked_until", "burst_started", "event_time", "decided_at",
}
REASONS = {ALLOW: {None}, BLOCK: {BURST_DETECTED, SOURCE_BLOCKED}}


def _timestamp(value, field, nullable=False):
    if value is None and nullable:
        return None
    try:
        if isinstance(value, str):
            value = datetime.fromisoformat(value.replace("Z", "+00:00"))
        if not isinstance(value, datetime) or value.utcoffset() is None:
            raise ValueError
        return value.astimezone(timezone.utc).isoformat()
    except (ValueError, TypeError, OverflowError):
        raise ValueError(f"{field} must be a timestamp with a timezone.") from None


def validate_throttle_event(event):
    """Validate a throttle event and normalize its IDs and timestamps (UTC ISO strings)."""
    if not isinstance(event, dict) or set(event) != EVENT_FIELDS:
        raise ValueError("Expected the version-one throttle decision fields.")
    if type(event["schema_version"]) is not int or event["schema_version"] != SCHEMA_VERSION:
        raise ValueError("Unsupported throttle decision schema version.")
    if event["action"] not in REASONS or event["reason"] not in REASONS[event["action"]]:
        raise ValueError("Invalid action/reason combination.")
    source = event["source"]
    if not isinstance(source, str) or not source.strip() or "\x00" in source:
        raise ValueError("Source must be non-empty text without null characters.")
    for field, minimum in (("window_count", 0), ("window_seconds", 1), ("threshold", 1)):
        if type(event[field]) is not int or event[field] < minimum:
            raise ValueError(f"{field} must be an integer >= {minimum}.")
    if type(event["burst_started"]) is not bool:
        raise ValueError("burst_started must be a boolean.")
    if event["burst_started"] and event["reason"] != BURST_DETECTED:
        raise ValueError("Only a burst_detected decision can start a burst.")
    if (event["action"] == BLOCK) != (event["blocked_until"] is not None):
        raise ValueError("blocked_until is required for block and must be null for allow.")

    normalized = dict(event)
    try:
        normalized["prompt_id"] = str(UUID(str(event["prompt_id"])))
    except ValueError:
        raise ValueError("prompt_id must be a UUID.") from None
    normalized["blocked_until"] = _timestamp(event["blocked_until"], "blocked_until", nullable=True)
    normalized["event_time"] = _timestamp(event["event_time"], "event_time")
    normalized["decided_at"] = _timestamp(event["decided_at"], "decided_at")
    return normalized


def build_throttle_event(decision, decided_at=None):
    """Turn a rate_limiter.ThrottleDecision into a validated event."""
    return validate_throttle_event({
        "schema_version": SCHEMA_VERSION,
        "prompt_id": decision.prompt_id,
        "source": decision.source,
        "action": decision.action,
        "reason": decision.reason,
        "window_count": decision.window_count,
        "window_seconds": decision.window_seconds,
        "threshold": decision.threshold,
        "blocked_until": decision.blocked_until,
        "burst_started": decision.burst_started,
        "event_time": decision.event_time,
        "decided_at": decided_at or datetime.now(timezone.utc),
    })


def event_from_stored(stored):
    """Turn a db.serialize_throttle_decision row into the event P7 receives."""
    return validate_throttle_event({"schema_version": SCHEMA_VERSION, **stored})
