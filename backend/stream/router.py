"""P7 policy: combine a P4 score with P10 throttling and finalize the prompt route."""

import math


def validate_policy(allow_max, block_min, score_timeout_seconds):
    if not (0 <= allow_max < block_min <= 1):
        raise ValueError("P7 thresholds must satisfy 0 <= allow_max < block_min <= 1.")
    if not math.isfinite(score_timeout_seconds) or score_timeout_seconds <= 0:
        raise ValueError("P7 score timeout must be positive.")


def decision_for_score(score, allow_max, block_min):
    """Return (final_decision, reason) using inclusive configured thresholds."""
    if isinstance(score, bool) or not isinstance(score, (int, float)) or not math.isfinite(score):
        raise ValueError("malicious score must be a finite number")
    if not 0 <= score <= 1:
        raise ValueError("malicious score must be between 0 and 1")
    if score <= allow_max:
        return "allow", "score_below_allow_threshold"
    if score >= block_min:
        return "block", "score_above_block_threshold"
    return "review", "score_in_review_band"


def route_pending_row(row, now, allow_max, block_min, score_timeout_seconds):
    """Decide a P10-allowed row when P4 is ready or the configured deadline passes."""
    from datetime import timedelta

    if row.get("throttle_action") is None and "throttle_action" in row:
        if now >= row["stored_at"] + timedelta(seconds=score_timeout_seconds):
            return "review", "throttle_decision_timeout"
        return None

    deadline = row["decided_at"] + timedelta(seconds=score_timeout_seconds)
    score = row["malicious_score"]
    completed_at = row.get("scored_at") or row["checked_at"]
    metadata = row.get("scoring_metadata")

    if completed_at is not None:
        if metadata and metadata.get("truncated") is True:
            return "review", "model_input_too_long"
        if score is None:
            return "review", "classifier_inference_failed"
        if not metadata or metadata.get("truncated") is not False:
            return "review", "classifier_input_coverage_unknown"

    if score is not None:
        if completed_at is None:
            return "review", "score_completion_time_missing"
        if completed_at > deadline:
            return "review", "score_arrived_after_timeout"
        return decision_for_score(score, allow_max, block_min)

    if now >= deadline:
        return "review", "classifier_score_timeout"
    return None
