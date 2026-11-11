"""P8 periodic event-time aggregation worker; persists current and historical snapshots."""

import logging
import os
import signal
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from dotenv import load_dotenv

from backend import db
from .aggregation import build_snapshot

LOGGER = logging.getLogger(__name__)
DEFAULT_WINDOW_SECONDS = 60
DEFAULT_SNAPSHOT_INTERVAL_SECONDS = 10
DEFAULT_HISTORY_RETENTION_SECONDS = 7 * 24 * 60 * 60


def env_positive_int(name, default):
    value = int(os.getenv(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer.")
    return value


def run_once(database_url, *, now=None, window_seconds=DEFAULT_WINDOW_SECONDS,
             history_retention_seconds=DEFAULT_HISTORY_RETENTION_SECONDS):
    now = now or datetime.now(timezone.utc)
    window_start = now - timedelta(seconds=window_seconds)
    rows = db.get_attack_window_rows(database_url, window_start, now)
    snapshot = build_snapshot(rows, now, window_seconds=window_seconds, top_n=10)
    db.save_attack_aggregate_snapshot(
        database_url,
        snapshot,
        history_before=now - timedelta(seconds=history_retention_seconds),
    )
    LOGGER.info(
        "P8 saved snapshot at %s with %d classified prompts",
        now.isoformat(), sum(snapshot["attack_type_counts"].values()),
    )
    return snapshot


def run(database_url, *, window_seconds=DEFAULT_WINDOW_SECONDS,
        snapshot_interval_seconds=DEFAULT_SNAPSHOT_INTERVAL_SECONDS,
        history_retention_seconds=DEFAULT_HISTORY_RETENTION_SECONDS):
    if min(window_seconds, snapshot_interval_seconds, history_retention_seconds) <= 0:
        raise ValueError("P8 window, snapshot interval, and history retention must be positive.")
    running = True

    def stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    next_snapshot = time.monotonic()
    LOGGER.info(
        "P8 aggregation started (window %ds, refresh %ds, retention %ds)",
        window_seconds, snapshot_interval_seconds, history_retention_seconds,
    )
    while running:
        delay = next_snapshot - time.monotonic()
        if delay > 0:
            time.sleep(min(delay, 1.0))
            continue
        run_once(
            database_url,
            window_seconds=window_seconds,
            history_retention_seconds=history_retention_seconds,
        )
        next_snapshot += snapshot_interval_seconds
        if next_snapshot < time.monotonic():
            next_snapshot = time.monotonic() + snapshot_interval_seconds


def main():
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    database_url = os.getenv("DATABASE_URL", "postgresql://localhost:5432/pijd")
    run(
        database_url,
        window_seconds=env_positive_int("P8_WINDOW_SECONDS", DEFAULT_WINDOW_SECONDS),
        snapshot_interval_seconds=env_positive_int(
            "P8_SNAPSHOT_INTERVAL_SECONDS", DEFAULT_SNAPSHOT_INTERVAL_SECONDS
        ),
        history_retention_seconds=env_positive_int(
            "P8_HISTORY_RETENTION_SECONDS", DEFAULT_HISTORY_RETENTION_SECONDS
        ),
    )


if __name__ == "__main__":
    main()
