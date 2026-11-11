"""P2: short-lived source history, shared across messages in one consumer process."""

from datetime import datetime, timezone
from time import monotonic

from backend import db


class SourceContextCache:
    def __init__(self, database_url, *, clock=monotonic, now=None):
        self.database_url = database_url
        self.clock = clock
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.entries = {}

    def get(self, source):
        tick = self.clock()
        self.entries = {
            key: entry for key, entry in self.entries.items() if entry[0] > tick
        }
        if source not in self.entries:
            context = db.get_source_context(self.database_url, source, as_of=self.now())
            self.entries[source] = (self.clock() + 30, context)
        # Downstream code must not modify the cached snapshot.
        return dict(self.entries[source][1])
