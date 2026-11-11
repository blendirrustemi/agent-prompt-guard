"""P8 rolling-window aggregation and optional PostgreSQL persistence checks."""

import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import patch
from uuid import uuid4

from backend import db
from backend.intake.events import build_raw_event
from backend.stream.aggregation import ATTACK_TYPES, build_snapshot, normalize_prompt
from backend.stream.aggregation_worker import run_once

NOW = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)


class AggregationTests(unittest.TestCase):
    def test_normalization_matches_p1_nfc_and_whitespace(self):
        self.assertEqual(normalize_prompt("Cafe\u0301\n  attack\ttext"), "Café attack text")

    def test_counts_and_repeated_patterns_are_grouped_by_attack_type(self):
        rows = [
            {"attack_type": "obfuscation", "text": "ignored raw", "preprocessing": {"cleaned_text": "same prompt"}},
            {"attack_type": "obfuscation", "text": "ignored raw", "preprocessing": {"cleaned_text": "same prompt"}},
            {"attack_type": "jailbreak_persona", "text": "same prompt", "preprocessing": None},
            {"attack_type": "jailbreak_persona", "text": "  same   prompt ", "preprocessing": None},
            {"attack_type": "not_a_class", "text": "x", "preprocessing": None},
        ]
        snapshot = build_snapshot(rows, NOW)
        self.assertEqual(snapshot["attack_type_counts"]["obfuscation"], 2)
        self.assertEqual(snapshot["attack_type_counts"]["jailbreak_persona"], 2)
        self.assertEqual(snapshot["attack_type_counts"]["instruction_manipulation"], 0)
        self.assertEqual(snapshot["top_patterns"]["obfuscation"][0]["text"], "same prompt")
        self.assertEqual(snapshot["top_patterns"]["jailbreak_persona"][0]["count"], 2)
        self.assertEqual(snapshot["top_patterns"]["instruction_manipulation"], [])
        self.assertEqual(snapshot["window_start"], NOW - timedelta(seconds=60))
        self.assertEqual(snapshot["window_end"], NOW)

    def test_top_ten_per_type_excludes_singletons_and_sorts_ties_stably(self):
        rows = []
        for index in range(12):
            phrase = f"repeated prompt {index:02d}"
            rows.extend([
                {"attack_type": "adversarial_attacks", "text": phrase, "preprocessing": None},
                {"attack_type": "adversarial_attacks", "text": phrase, "preprocessing": None},
            ])
        rows.append({"attack_type": "adversarial_attacks", "text": "singleton", "preprocessing": None})
        result = build_snapshot(rows, NOW, top_n=10)
        top = result["top_patterns"]["adversarial_attacks"]
        self.assertEqual(len(top), 10)
        self.assertTrue(all(item["count"] == 2 for item in top))
        self.assertEqual([item["text"] for item in top], [f"repeated prompt {index:02d}" for index in range(10)])

    def test_run_once_queries_event_time_window_and_persists_retention_cutoff(self):
        with patch("backend.stream.aggregation_worker.db.get_attack_window_rows", return_value=[]) as fetch, \
             patch("backend.stream.aggregation_worker.db.save_attack_aggregate_snapshot") as save:
            snapshot = run_once("unused", now=NOW, window_seconds=60, history_retention_seconds=604800)
        fetch.assert_called_once_with("unused", NOW - timedelta(seconds=60), NOW)
        self.assertEqual(save.call_args.kwargs["history_before"], NOW - timedelta(days=7))
        self.assertEqual(set(snapshot["attack_type_counts"]), set(ATTACK_TYPES))


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL for P8 PostgreSQL checks")
class AggregationStorageTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["TEST_DATABASE_URL"]
        db.init_db(self.url)
        self.prompt_ids = []
        with db.connect(self.url) as conn:
            conn.execute(
                "DELETE FROM attack_aggregate_history WHERE snapshot_at = ANY(%s)",
                ([NOW, NOW - timedelta(days=8)],),
            )
            conn.execute("DELETE FROM attack_aggregate_current WHERE snapshot_at = %s", (NOW,))

    def tearDown(self):
        with db.connect(self.url) as conn:
            for prompt_id in self.prompt_ids:
                conn.execute("DELETE FROM prompts WHERE id = %s", (prompt_id,))
            conn.execute(
                "DELETE FROM attack_aggregate_history WHERE snapshot_at = ANY(%s)",
                ([NOW, NOW - timedelta(days=8)],),
            )
            conn.execute("DELETE FROM attack_aggregate_current WHERE snapshot_at = %s", (NOW,))

    def test_event_window_query_and_snapshot_current_history_retention(self):
        start, end = NOW - timedelta(seconds=60), NOW
        for created_at in (start, start + timedelta(seconds=1), end):
            raw = build_raw_event(
                "Repeated  prompt", source=f"aggregation_{uuid4().hex[:8]}", created_at=created_at
            )
            db.save_raw_event(self.url, raw, preprocessing={"cleaned_text": "Repeated prompt"})
            self.prompt_ids.append(raw["prompt_id"])
            db.save_guard_score(self.url, raw["prompt_id"], 0.9, "obfuscation", "b3-v1", checked_at=created_at)

        rows = db.get_attack_window_rows(self.url, start, end)
        self.assertEqual(len(rows), 2)  # start is exclusive; end is inclusive
        snapshot = build_snapshot(rows, end)
        db.save_attack_aggregate_snapshot(self.url, snapshot)
        current = db.get_current_attack_aggregate(self.url)
        history = db.get_attack_aggregate_history(self.url, limit=10)
        self.assertEqual(current["snapshot_at"], end)
        self.assertEqual(current["attack_type_counts"]["obfuscation"], 2)
        self.assertEqual(current["top_patterns"]["obfuscation"][0]["count"], 2)
        self.assertEqual(history[0]["snapshot_at"], end)

        old = {
            **snapshot,
            "snapshot_at": NOW - timedelta(days=8),
            "window_start": NOW - timedelta(days=8, seconds=60),
            "window_end": NOW - timedelta(days=8),
        }
        db.save_attack_aggregate_snapshot(self.url, old)
        db.save_attack_aggregate_snapshot(self.url, snapshot, history_before=NOW - timedelta(days=7))
        history = db.get_attack_aggregate_history(self.url, limit=1000)
        self.assertNotIn(old["snapshot_at"], [row["snapshot_at"] for row in history])


if __name__ == "__main__":
    unittest.main()
