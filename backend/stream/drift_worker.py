"""P6: monitor persisted P4 malicious probabilities for distribution drift."""

import logging
import os
import signal
import time
from datetime import datetime, timezone
from pathlib import Path
from uuid import UUID

from dotenv import load_dotenv
from river.drift import ADWIN

from backend import db

LOGGER = logging.getLogger(__name__)


def positive_int(name, default):
    value = int(os.getenv(name, default))
    if value <= 0:
        raise ValueError(f"{name} must be a positive integer.")
    return value


def run_once(database_url, detector, cursor, *, batch_size=500):
    """Consume a bounded batch and return the last processed database cursor."""
    rows = db.get_p6_score_batch(database_url, cursor, limit=batch_size)
    for row in rows:
        score = float(row["malicious_score"])
        detector.update(score)
        cursor = (row["scored_at"], row["prompt_id"])
        if detector.drift_detected:
            LOGGER.warning(
                "P6 RETRAINING ALERT: P4 malicious-score distribution drift detected "
                "(samples=%d, latest_mean=%.4f, model_data_score_only=true)",
                detector.width, detector.estimation,
            )
    return cursor, len(rows)


def run(database_url, *, poll_seconds=1, batch_size=500, delta=0.002):
    if not 0 < delta < 1:
        raise ValueError("P6_ADWIN_DELTA must be between 0 and 1.")
    detector = ADWIN(delta=delta, clock=1, min_window_length=5, grace_period=20)
    # Rebuild the detector from persisted scores after restart. This keeps history in
    # PostgreSQL and lets P6 catch up without changing the P4 scoring path.
    cursor = (datetime.min.replace(tzinfo=timezone.utc), UUID(int=0))
    running = True

    def stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGTERM, stop)
    signal.signal(signal.SIGINT, stop)
    LOGGER.info("P6 drift monitor started (ADWIN delta=%s, poll=%ds)", delta, poll_seconds)
    while running:
        cursor, count = run_once(database_url, detector, cursor, batch_size=batch_size)
        if count == batch_size:
            continue
        time.sleep(poll_seconds)


def main():
    load_dotenv(Path(__file__).resolve().parents[2] / ".env")
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    run(
        os.getenv("DATABASE_URL", "postgresql://localhost:5432/pijd"),
        poll_seconds=positive_int("P6_POLL_SECONDS", 1),
        batch_size=positive_int("P6_BATCH_SIZE", 500),
        delta=float(os.getenv("P6_ADWIN_DELTA", "0.002")),
    )


if __name__ == "__main__":
    main()
