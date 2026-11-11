"""P1/P2 preprocessing and optional PostgreSQL persistence checks."""

import json
import os
import unittest
from contextlib import contextmanager
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from uuid import UUID, uuid4

import psycopg

from backend import db
from backend.intake.events import build_raw_event
from backend.stream.enrichment import SourceContextCache
from backend.stream.preprocessing import preprocess_prompt


class PreprocessingTests(unittest.TestCase):
    def test_cleans_a_copy_and_preserves_case_and_punctuation(self):
        event = build_raw_event(" \tCafe\u0301!\r\n  Keep CASE? 🛡\u200d\n")
        original = dict(event)
        result = preprocess_prompt(event["text"], {"block_count": 2})
        self.assertEqual(result["cleaned_text"], "Café! Keep CASE? 🛡\u200d")
        self.assertEqual(event, original)
        self.assertEqual(result["features"]["char_count"], len(original["text"]))
        self.assertEqual(result["source_context"], {"block_count": 2})

    def test_features_use_original_characters_and_whitespace(self):
        result = preprocess_prompt(" A!\nB? ", {})
        self.assertEqual(result["features"], {"char_count": 7, "word_count": 2, "symbol_ratio": 2 / 7})
        self.assertEqual(preprocess_prompt("漢字", {})["features"]["symbol_ratio"], 0)

    def test_does_not_strip_markup_decode_or_truncate(self):
        text = "<script>Keep</script> SGVsbG8= " + "x" * 7900
        result = preprocess_prompt(text, {})
        self.assertEqual(result["cleaned_text"], text)
        self.assertEqual(result["features"]["char_count"], len(text))


class SourceCacheTests(unittest.TestCase):
    def test_sources_are_isolated_and_refresh_at_thirty_seconds(self):
        tick = [0.0]
        wall = [datetime(2026, 11, 11, 12, tzinfo=timezone.utc)]
        cache = SourceContextCache("unused", clock=lambda: tick[0], now=lambda: wall[0])
        with patch("backend.stream.enrichment.db.get_source_context") as load:
            load.side_effect = lambda url, source, as_of: {
                "allow_count": 0, "block_count": int(source == "attacker"),
                "review_count": 0, "as_of": as_of.isoformat(),
            }
            first = cache.get("attacker")
            first["block_count"] = 999
            self.assertEqual(cache.get("attacker")["block_count"], 1)
            self.assertEqual(cache.get("other")["block_count"], 0)
            self.assertEqual(load.call_count, 2)
            tick[0] = 29.9
            cache.get("attacker")
            self.assertEqual(load.call_count, 2)
            tick[0] = 30
            wall[0] += timedelta(seconds=30)
            refreshed = cache.get("attacker")
            self.assertEqual(load.call_count, 3)
            self.assertEqual(refreshed["as_of"], wall[0].isoformat())
            self.assertNotIn("other", cache.entries)
            SourceContextCache("unused").get("attacker")
            self.assertEqual(load.call_count, 4)

    def test_failed_lookup_is_not_cached(self):
        cache = SourceContextCache("unused")
        with patch("backend.stream.enrichment.db.get_source_context", side_effect=psycopg.OperationalError):
            with self.assertRaises(psycopg.OperationalError):
                cache.get("source")
        self.assertEqual(cache.entries, {})


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL for P1/P2 storage checks")
class StreamStorageTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["TEST_DATABASE_URL"]
        self.ids = []
        db.init_db(self.url)

    def tearDown(self):
        with db.connect(self.url) as conn:
            conn.execute("DELETE FROM prompts WHERE id = ANY(%s)", (self.ids,))

    def store(self, source=None):
        event = build_raw_event("  Cafe\u0301!\n", source=source or str(uuid4()))
        self.ids.append(UUID(event["prompt_id"]))
        db.save_raw_event(self.url, event)
        return event

    def snapshot(self, prompt_id):
        with db.connect(self.url) as conn:
            return conn.execute("SELECT preprocessing FROM prompts WHERE id = %s", (prompt_id,)).fetchone()["preprocessing"]

    def test_recent_completed_history_uses_checked_at_and_exact_source(self):
        now = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)
        source = str(uuid4())
        for decision, minutes, event_source in (
            ("allow", 1, source), ("block", 10, source), ("review", 3, source),
            ("block", 11, source), ("block", -1, source),
            ("pending", 1, source), ("block", 1, source + "-other"),
        ):
            event = self.store(event_source)
            with db.connect(self.url) as conn:
                conn.execute(
                    "UPDATE guard_results SET decision = %s, checked_at = %s WHERE prompt_id = %s",
                    (decision, now - timedelta(minutes=minutes), event["prompt_id"]),
                )
        self.assertEqual(db.get_source_context(self.url, source, as_of=now), {
            "allow_count": 1, "block_count": 1, "review_count": 1, "as_of": now.isoformat(),
        })
        expired = db.get_source_context(self.url, source, as_of=now + timedelta(minutes=30))
        self.assertEqual([expired[key] for key in ("allow_count", "block_count", "review_count")], [0, 0, 0])
        unknown = db.get_source_context(self.url, str(uuid4()), as_of=now)
        self.assertEqual(unknown["block_count"], 0)

    def test_backfill_on_redelivery_preserves_first_snapshot_and_verdict(self):
        event = self.store()
        self.assertIsNone(self.snapshot(event["prompt_id"]))
        db.init_db(self.url)
        self.assertIsNone(self.snapshot(event["prompt_id"]))
        db.save_guard_result(self.url, event["prompt_id"], "block", malicious_score=0.9, model_version="fixture")
        before = db.get_guard_result(self.url, event["prompt_id"])
        first = preprocess_prompt(event["text"], {"block_count": 0})
        duplicate = db.save_raw_event(self.url, {**event, "created_at": "2026-11-12T12:00:00+00:00"}, first)
        self.assertEqual(duplicate["created_at"], event["created_at"])
        db.save_raw_event(self.url, event, preprocess_prompt(event["text"], {"block_count": 99}))
        self.assertEqual(self.snapshot(event["prompt_id"]), first)
        self.assertEqual(db.get_guard_result(self.url, event["prompt_id"]), before)
        with self.assertRaises(ValueError):
            db.save_raw_event(self.url, {**event, "source": "conflict"}, {"changed": True})
        self.assertEqual(self.snapshot(event["prompt_id"]), first)
        self.assertEqual(db.get_prompt_text(self.url, event["prompt_id"]), event["text"])

    def test_new_prompt_and_snapshot_roll_back_if_initial_result_fails(self):
        event = build_raw_event("Atomic snapshot")
        self.ids.append(UUID(event["prompt_id"]))
        original_connect = db.connect

        @contextmanager
        def failing_connection(url):
            with original_connect(url) as connection:
                class Connection:
                    def execute(self, sql, parameters):
                        if sql.startswith("INSERT INTO guard_results"):
                            raise psycopg.OperationalError("injected result failure")
                        return connection.execute(sql, parameters)
                yield Connection()

        with patch("backend.db.connect", failing_connection):
            with self.assertRaises(psycopg.OperationalError):
                db.save_raw_event(self.url, event, preprocess_prompt(event["text"], {}))
        self.assertIsNone(db.get_prompt(self.url, event["prompt_id"]))
        saved = db.save_raw_event(self.url, event, preprocess_prompt(event["text"], {}))
        self.assertEqual(saved["decision"], "pending")
        self.assertIsNotNone(self.snapshot(event["prompt_id"]))

    def test_intake_and_burst_workers_share_storage_in_either_order(self):
        from backend.intake.consumer import process_message as process_intake
        from backend.stream.burst import BurstDetector
        from backend.stream.burst_worker import process_message as process_burst
        from backend.stream.rate_limiter import RateLimiter

        for burst_first in (True, False):
            with self.subTest(burst_first=burst_first):
                event = build_raw_event("  Shared\tCafe\u0301!\n", source=str(uuid4()))
                self.ids.append(UUID(event["prompt_id"]))
                message = Mock()
                message.value.return_value = json.dumps(event).encode()
                message.topic.return_value = "prompts_raw"
                message.partition.return_value = 0
                message.offset.return_value = 1
                intake_consumer, burst_consumer, producer = Mock(), Mock(), Mock()
                producer.flush.return_value = 0
                cache = SourceContextCache(self.url)
                detector, limiter = BurstDetector(threshold=1), RateLimiter()

                def intake():
                    process_intake(intake_consumer, message, self.url, cache)

                def burst():
                    process_burst(burst_consumer, producer, message, self.url, detector, limiter)

                first, second = (burst, intake) if burst_first else (intake, burst)
                first()
                initial_snapshot = self.snapshot(event["prompt_id"])
                second()
                snapshot = self.snapshot(event["prompt_id"])
                self.assertEqual(snapshot["cleaned_text"], "Shared Café!")
                if initial_snapshot is not None:
                    self.assertEqual(snapshot, initial_snapshot)
                # P10's throttle signal is separate from the final guard verdict.
                throttle = db.get_throttle_decision(self.url, event["prompt_id"])
                self.assertEqual(throttle["action"], "block")
                self.assertEqual(snapshot["source_context"]["block_count"], 0)
                self.assertEqual(db.get_guard_result(self.url, event["prompt_id"])["decision"], "pending")
                db.save_guard_result(self.url, event["prompt_id"], "block", model_version="fixture")
                intake()
                burst()
                self.assertEqual(self.snapshot(event["prompt_id"]), snapshot)
                self.assertEqual(db.get_guard_result(self.url, event["prompt_id"])["decision"], "block")
                self.assertEqual(db.get_throttle_decision(self.url, event["prompt_id"]), throttle)
                self.assertEqual(db.get_prompt_text(self.url, event["prompt_id"]), event["text"])


if __name__ == "__main__":
    unittest.main()
