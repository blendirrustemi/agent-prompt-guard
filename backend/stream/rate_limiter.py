"""P10: rate limiting / throttling. Turns P9 bursts into per-prompt allow/block decisions.

Policy (block only): when a source's window reaches the threshold, the source is blocked for a
cooldown. Prompts arriving during the cooldown are blocked but do not extend it; continued
traffic simply triggers a new block once the cooldown ends.
"""

from dataclasses import dataclass
from datetime import datetime, timedelta

ALLOW, BLOCK = "allow", "block"
BURST_DETECTED, SOURCE_BLOCKED = "burst_detected", "source_blocked"


@dataclass(frozen=True)
class ThrottleDecision:
    prompt_id: str
    source: str
    action: str
    reason: str | None
    window_count: int
    window_seconds: int
    threshold: int
    blocked_until: datetime | None
    burst_started: bool
    event_time: datetime


class RateLimiter:
    def __init__(self, cooldown_seconds=300):
        if cooldown_seconds <= 0:
            raise ValueError("cooldown_seconds must be positive.")
        self.cooldown_seconds = cooldown_seconds
        self._cooldown = timedelta(seconds=cooldown_seconds)
        self._blocked_until = {}  # source -> end of its current block

    def decide(self, prompt_id, observation):
        source, now = observation.source, observation.event_time
        until = self._blocked_until.get(source)
        if until is not None and now < until:
            return self._decision(prompt_id, observation, BLOCK, SOURCE_BLOCKED, until, burst_started=False)
        if observation.burst:
            until = now + self._cooldown
            self._blocked_until[source] = until
            return self._decision(prompt_id, observation, BLOCK, BURST_DETECTED, until, burst_started=True)
        if until is not None:
            del self._blocked_until[source]  # expired
        return self._decision(prompt_id, observation, ALLOW, None, None, burst_started=False)

    def restore(self, source, blocked_until):
        """Reload a block stored before a restart."""
        current = self._blocked_until.get(source)
        if current is None or blocked_until > current:
            self._blocked_until[source] = blocked_until

    def blocked_until(self, source):
        return self._blocked_until.get(source)

    @staticmethod
    def _decision(prompt_id, observation, action, reason, until, burst_started):
        return ThrottleDecision(
            prompt_id=str(prompt_id),
            source=observation.source,
            action=action,
            reason=reason,
            window_count=observation.window_count,
            window_seconds=observation.window_seconds,
            threshold=observation.threshold,
            blocked_until=until,
            burst_started=burst_started,
            event_time=observation.event_time,
        )
