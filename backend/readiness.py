"""Container startup readiness markers; no application data is stored here."""

from pathlib import Path


def mark_ready(service, ready=True):
    marker = Path(f"/tmp/pijd-{service}-ready")
    if ready:
        marker.touch()
    else:
        marker.unlink(missing_ok=True)
