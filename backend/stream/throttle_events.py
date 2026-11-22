"""The `throttle_decisions` event: P10's per-prompt decision, consumed by P7.

P7 applies `action == "block"` as an instant override before score routing; `allow` means the
rate limiter has no objection and routing follows the model scores.

Version 2 adds `windows`: P9's count and threshold for every elastic window. The top-level
`window_count`/`window_seconds`/`threshold` describe the deciding window. Version-one events
(single window, no `windows`) are still accepted and normalized to version 2.
"""

from datetime import datetime, timezone
from uuid import UUID

from .rate_limiter import ALLOW, BLOCK, BURST_DETECTED, SOURCE_BLOCKED

SCHEMA_VERSION = 2
V1_FIELDS = {
    "schema_version", "prompt_id", "source", "action", "reason", "window_count", "window_seconds",
    "threshold", "blocked_until", "burst_started", "event_time", "decided_at",
}
EVENT_FIELDS = V1_FIELDS | {"windows"}
WINDOW_FIELDS = {"seconds", "count", "threshold"}
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


def _int_at_least(value, field, minimum):
    if type(value) is not int or value < minimum:
        raise ValueError(f"{field} must be an integer >= {minimum}.")
    return value


def _windows(value, event):
    if not isinstance(value, list) or not value:
        raise ValueError("windows must be a non-empty list.")
    windows = []
    for window in value:
        if not isinstance(window, dict) or set(window) != WINDOW_FIELDS:
            raise ValueError("Each window needs exactly seconds, count and threshold.")
        windows.append({
            "seconds": _int_at_least(window["seconds"], "windows.seconds", 1),
            "count": _int_at_least(window["count"], "windows.count", 0),
            "threshold": _int_at_least(window["threshold"], "windows.threshold", 1),
        })
    if [w["seconds"] for w in windows] != sorted({w["seconds"] for w in windows}):
        raise ValueError("windows must have unique sizes, shortest first.")
    deciding = {"seconds": event["window_seconds"], "count": event["window_count"], "threshold": event["threshold"]}
    if deciding not in windows:
        raise ValueError("window_seconds/window_count/threshold must match one of the windows.")
    return windows


def validate_throttle_event(event):
    """Validate a throttle event and normalize it to version 2 (UUID text, UTC ISO timestamps)."""
    if not isinstance(event, dict):
        raise ValueError("Expected a throttle decision object.")
    version = event.get("schema_version")
    if type(version) is not int or version not in (1, SCHEMA_VERSION):
        raise ValueError("Unsupported throttle decision schema version.")
    if set(event) != (V1_FIELDS if version == 1 else EVENT_FIELDS):
        raise ValueError(f"Expected the version-{version} throttle decision fields.")
    if event["action"] not in REASONS or event["reason"] not in REASONS[event["action"]]:
        raise ValueError("Invalid action/reason combination.")
    source = event["source"]
    if not isinstance(source, str) or not source.strip() or "\x00" in source:
        raise ValueError("Source must be non-empty text without null characters.")
    for field, minimum in (("window_count", 0), ("window_seconds", 1), ("threshold", 1)):
        _int_at_least(event[field], field, minimum)
    if type(event["burst_started"]) is not bool:
        raise ValueError("burst_started must be a boolean.")
    if event["burst_started"] and event["reason"] != BURST_DETECTED:
        raise ValueError("Only a burst_detected decision can start a burst.")
    if (event["action"] == BLOCK) != (event["blocked_until"] is not None):
        raise ValueError("blocked_until is required for block and must be null for allow.")

    normalized = dict(event, schema_version=SCHEMA_VERSION)
    if version == 1:
        normalized["windows"] = [{
            "seconds": event["window_seconds"], "count": event["window_count"], "threshold": event["threshold"],
        }]
    else:
        normalized["windows"] = _windows(event["windows"], event)
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
    windows = [window.as_dict() for window in decision.windows] or [{
        "seconds": decision.window_seconds, "count": decision.window_count, "threshold": decision.threshold,
    }]
    return validate_throttle_event({
        "schema_version": SCHEMA_VERSION,
        "prompt_id": decision.prompt_id,
        "source": decision.source,
        "action": decision.action,
        "reason": decision.reason,
        "window_count": decision.window_count,
        "window_seconds": decision.window_seconds,
        "threshold": decision.threshold,
        "windows": windows,
        "blocked_until": decision.blocked_until,
        "burst_started": decision.burst_started,
        "event_time": decision.event_time,
        "decided_at": decided_at or datetime.now(timezone.utc),
    })


def event_from_stored(stored):
    """Turn a db.serialize_throttle_decision row into the event P7 receives."""
    return validate_throttle_event({"schema_version": SCHEMA_VERSION, **stored})
