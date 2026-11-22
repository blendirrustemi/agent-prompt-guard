"""P9: elastic sliding-window burst detection on the number of prompts per source.

Follows the elastic-burst model of Zhu & Shasha, "Efficient Elastic Burst Detection in Data
Streams" (KDD 2003, doi:10.1145/956750.956789): several window sizes are monitored at once, each
with its own threshold f(w), and a burst is reported when any window reaches its threshold.
Short windows catch sharp bursts early; long windows catch slow, steady sources. With only a few
windows we count each one exactly (the paper's direct computation); its Shifted Wavelet Tree is
the way to scale to many window sizes.

P9 runs beside P3/P5, so it has no classifier score: it counts every prompt from a source.
Time is event time (the raw event's created_at), so replays produce the same bursts.
"""

from bisect import bisect_right, insort
from dataclasses import dataclass
from datetime import datetime, timedelta

PRUNE_EVERY = 1_000
# (window seconds, threshold): the allowed rate falls as the window grows (0.6/s, 0.17/s, 0.067/s).
DEFAULT_WINDOWS = ((10, 6), (60, 10), (600, 40))


@dataclass(frozen=True)
class WindowCount:
    seconds: int
    count: int  # prompts from the source in (latest - seconds, latest], including this one
    threshold: int

    @property
    def burst(self):
        return self.count >= self.threshold

    def as_dict(self):
        return {"seconds": self.seconds, "count": self.count, "threshold": self.threshold}


@dataclass(frozen=True)
class BurstObservation:
    source: str
    event_time: datetime
    # The deciding window: the shortest window that reached its threshold, otherwise the one closest to it.
    window_count: int
    window_seconds: int
    threshold: int
    burst: bool  # any window reached its threshold
    windows: tuple = ()  # WindowCount for every configured window, shortest first


def validate_windows(windows):
    """Return the windows as a tuple of (seconds, threshold) int pairs, or raise ValueError."""
    try:
        pairs = tuple((int(seconds), int(threshold)) for seconds, threshold in windows)
    except (TypeError, ValueError):
        raise ValueError("Windows must be (seconds, threshold) pairs of integers.") from None
    if not pairs:
        raise ValueError("At least one burst window is required.")
    if any(seconds <= 0 or threshold <= 0 for seconds, threshold in pairs):
        raise ValueError("Window seconds and thresholds must be positive.")
    for (short, short_threshold), (long, long_threshold) in zip(pairs, pairs[1:]):
        if long <= short:
            raise ValueError("Window sizes must be unique and sorted from shortest to longest.")
        if long_threshold <= short_threshold:
            # A longer window always contains the shorter one, which would then never fire first.
            raise ValueError("Thresholds must increase with the window size.")
    return pairs


def deciding_window(counts):
    """The shortest window at its threshold, otherwise the highest count/threshold (ties: shorter)."""
    bursting = [window for window in counts if window.burst]
    if bursting:
        return bursting[0]
    return max(counts, key=lambda window: (window.count / window.threshold, -window.seconds))


class BurstDetector:
    def __init__(self, windows=DEFAULT_WINDOWS):
        self.windows = validate_windows(windows)
        self._deltas = tuple(timedelta(seconds=seconds) for seconds, _ in self.windows)
        self._longest = self._deltas[-1]
        self._times = {}  # source -> sorted list of event times inside the longest window
        self._latest = {}  # source -> latest event time seen
        self._observed = 0

    def observe(self, source, event_time):
        """Count one prompt and report every window for the source."""
        latest = max(self._latest.get(source, event_time), event_time)
        self._latest[source] = latest
        times = self._times.setdefault(source, [])
        insort(times, event_time)
        # Late events are judged against the source's latest time; anything at or before the edge expires.
        del times[:bisect_right(times, latest - self._longest)]

        self._observed += 1
        if self._observed % PRUNE_EVERY == 0:
            self.prune(latest)
        counts = tuple(
            WindowCount(seconds, len(times) - bisect_right(times, latest - delta), threshold)
            for (seconds, threshold), delta in zip(self.windows, self._deltas)
        )
        decider = deciding_window(counts)
        return BurstObservation(
            source=source,
            event_time=event_time,
            window_count=decider.count,
            window_seconds=decider.seconds,
            threshold=decider.threshold,
            burst=decider.burst,
            windows=counts,
        )

    def window_count(self, source, seconds=None):
        """Prompts from the source in the given window (default: the longest)."""
        times = self._times.get(source, ())
        if seconds is None or not times:
            return len(times)
        return len(times) - bisect_right(times, self._latest[source] - timedelta(seconds=seconds))

    def prune(self, now):
        """Forget sources with no prompts in the longest window ending at `now`, keeping memory bounded."""
        edge = now - self._longest
        for source in [s for s, latest in self._latest.items() if latest <= edge]:
            del self._latest[source]
            self._times.pop(source, None)

    def sources(self):
        return set(self._times)
