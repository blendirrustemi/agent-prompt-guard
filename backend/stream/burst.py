"""P9: sliding-window burst detection on the number of prompts per source.

P9 runs beside P3/P5, so it has no classifier score: it counts every prompt from a source.
Time is event time (the raw event's created_at), so replays produce the same bursts.
"""

from bisect import bisect_right, insort
from dataclasses import dataclass
from datetime import datetime, timedelta

PRUNE_EVERY = 1_000


@dataclass(frozen=True)
class BurstObservation:
    source: str
    event_time: datetime
    window_count: int  # prompts from this source in (latest - window, latest], including this one
    window_seconds: int
    threshold: int
    burst: bool  # window_count >= threshold


class BurstDetector:
    def __init__(self, window_seconds=60, threshold=10):
        if window_seconds <= 0 or threshold <= 0:
            raise ValueError("window_seconds and threshold must be positive.")
        self.window_seconds = window_seconds
        self.threshold = threshold
        self._window = timedelta(seconds=window_seconds)
        self._times = {}  # source -> sorted list of event times inside the window
        self._latest = {}  # source -> latest event time seen
        self._observed = 0

    def observe(self, source, event_time):
        """Count one prompt and report the source's current window."""
        latest = max(self._latest.get(source, event_time), event_time)
        self._latest[source] = latest
        times = self._times.setdefault(source, [])
        insort(times, event_time)
        # Late events are judged against the source's latest time; anything at or before the edge expires.
        del times[:bisect_right(times, latest - self._window)]

        self._observed += 1
        if self._observed % PRUNE_EVERY == 0:
            self.prune(latest)
        count = len(times)
        return BurstObservation(
            source=source,
            event_time=event_time,
            window_count=count,
            window_seconds=self.window_seconds,
            threshold=self.threshold,
            burst=count >= self.threshold,
        )

    def window_count(self, source):
        return len(self._times.get(source, ()))

    def prune(self, now):
        """Forget sources with no prompts in the window ending at `now`, keeping memory bounded."""
        edge = now - self._window
        for source in [s for s, latest in self._latest.items() if latest <= edge]:
            del self._latest[source]
            self._times.pop(source, None)

    def sources(self):
        return set(self._times)
