"""P8 aggregation helpers for predicted attack types and repeated normalized prompts."""

import hashlib
import unicodedata
from collections import Counter
from datetime import timedelta

ATTACK_TYPES = (
    "adversarial_attacks",
    "indirect_agent_manipulation",
    "information_extraction",
    "instruction_manipulation",
    "jailbreak_persona",
    "obfuscation",
)


def normalize_prompt(text):
    """Match P1's NFC normalization and whitespace collapsing."""
    return " ".join(unicodedata.normalize("NFC", text).split())


def build_snapshot(rows, snapshot_at, *, window_seconds=60, top_n=10):
    """Aggregate classified rows whose event timestamps are in (start, snapshot_at]."""
    if window_seconds <= 0 or top_n <= 0:
        raise ValueError("window_seconds and top_n must be positive")
    counts = Counter({attack_type: 0 for attack_type in ATTACK_TYPES})
    patterns = {attack_type: Counter() for attack_type in ATTACK_TYPES}
    pattern_text = {}

    for row in rows:
        attack_type = row["attack_type"]
        if attack_type not in patterns:
            continue
        counts[attack_type] += 1
        preprocessing = row.get("preprocessing") or {}
        text = preprocessing.get("cleaned_text")
        if not isinstance(text, str) or not text:
            text = normalize_prompt(row["text"])
        if not text:
            continue
        pattern_id = hashlib.sha256(text.encode("utf-8")).hexdigest()
        key = (attack_type, pattern_id)
        patterns[attack_type][key] += 1
        pattern_text[key] = text

    top_patterns = {}
    for attack_type in ATTACK_TYPES:
        repeated = [
            (key, count)
            for key, count in patterns[attack_type].items()
            if count >= 2
        ]
        repeated.sort(key=lambda item: (-item[1], pattern_text[item[0]], item[0][1]))
        top_patterns[attack_type] = [
            {"pattern_id": key[1], "text": pattern_text[key], "count": count}
            for key, count in repeated[:top_n]
        ]

    window_start = snapshot_at - timedelta(seconds=window_seconds)
    return {
        "snapshot_at": snapshot_at,
        "window_start": window_start,
        "window_end": snapshot_at,
        "attack_type_counts": dict(counts),
        "top_patterns": top_patterns,
    }
