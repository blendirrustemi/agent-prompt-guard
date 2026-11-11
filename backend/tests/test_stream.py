"""P9 burst detection and P10 rate limiting, plus optional PostgreSQL and Kafka round trips."""

import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from uuid import uuid4

import psycopg

from backend import db
from backend.intake.events import build_raw_event
from backend.intake.topics import PROMPTS_RAW, THROTTLE_DECISIONS
from backend.stream.burst import BurstDetector
from backend.stream.burst_worker import process_message
from backend.stream.rate_limiter import BURST_DETECTED, SOURCE_BLOCKED, RateLimiter
from backend.stream.throttle_events import EVENT_FIELDS, build_throttle_event, validate_throttle_event

T0 = datetime(2026, 11, 10, 12, tzinfo=timezone.utc)


def seconds(n):
    return T0 + timedelta(seconds=n)


class BurstDetectorTests(unittest.TestCase):
    def test_counts_within_window_and_evicts_at_the_edge(self):
        detector = BurstDetector(window_seconds=60, threshold=10)
        self.assertEqual(detector.observe("a", seconds(0)).window_count, 1)
        self.assertEqual(detector.observe("a", seconds(59)).window_count, 2)
        # Exactly 60 s after the first prompt, that prompt has left the window.
        self.assertEqual(detector.observe("a", seconds(60)).window_count, 2)
        self.assertEqual(detector.observe("a", seconds(200)).window_count, 1)

    def test_sources_are_independent(self):
        detector = BurstDetector(window_seconds=60, threshold=3)
        for i in range(3):
            detector.observe("a", seconds(i))
        observation = detector.observe("b", seconds(3))
        self.assertEqual(observation.window_count, 1)
        self.assertFalse(observation.burst)
        self.assertEqual(detector.window_count("a"), 3)

    def test_burst_flag_at_threshold(self):
        detector = BurstDetector(window_seconds=60, threshold=10)
        flags = [detector.observe("a", seconds(i)).burst for i in range(10)]
        self.assertEqual(flags, [False] * 9 + [True])

    def test_late_events_use_latest_time(self):
        detector = BurstDetector(window_seconds=60, threshold=10)
        detector.observe("a", seconds(100))
        self.assertEqual(detector.observe("a", seconds(10)).window_count, 1)  # already outside the window
        self.assertEqual(detector.observe("a", seconds(50)).window_count, 2)

    def test_prune_forgets_quiet_sources(self):
        detector = BurstDetector(window_seconds=60, threshold=10)
        detector.observe("old", seconds(0))
        detector.observe("new", seconds(100))
        detector.prune(seconds(100))
        self.assertEqual(detector.sources(), {"new"})

    def test_invalid_settings(self):
        with self.assertRaises(ValueError):
            BurstDetector(window_seconds=0)
        with self.assertRaises(ValueError):
            RateLimiter(cooldown_seconds=0)


class RateLimiterTests(unittest.TestCase):
    def setUp(self):
        self.detector = BurstDetector(window_seconds=60, threshold=10)
        self.limiter = RateLimiter(cooldown_seconds=300)

    def send(self, source, at):
        return self.limiter.decide(str(uuid4()), self.detector.observe(source, at))

    def test_tenth_prompt_starts_block(self):
        decisions = [self.send("a", seconds(i)) for i in range(10)]
        self.assertEqual([d.action for d in decisions], ["allow"] * 9 + ["block"])
        burst = decisions[-1]
        self.assertTrue(burst.burst_started)
        self.assertEqual(burst.reason, BURST_DETECTED)
        self.assertEqual(burst.blocked_until, seconds(9 + 300))
        self.assertTrue(all(d.blocked_until is None and d.reason is None for d in decisions[:-1]))

    def test_cooldown_blocks_without_extending(self):
        for i in range(10):
            self.send("a", seconds(i))
        during = self.send("a", seconds(200))
        self.assertEqual((during.action, during.reason, during.burst_started), ("block", SOURCE_BLOCKED, False))
        self.assertEqual(during.blocked_until, seconds(309))
        self.assertEqual(self.limiter.blocked_until("a"), seconds(309))
        self.assertEqual(self.send("b", seconds(200)).action, "allow")

    def test_allowed_again_after_quiet_cooldown(self):
        for i in range(10):
            self.send("a", seconds(i))
        after = self.send("a", seconds(309))
        self.assertEqual((after.action, after.blocked_until), ("allow", None))
        self.assertIsNone(self.limiter.blocked_until("a"))

    def test_continued_spam_reblocks_after_cooldown(self):
        decision = None
        for i in range(0, 400, 5):  # 12 prompts per minute, steadily; block ends at 45 + 300
            decision = self.send("a", seconds(i))
            if seconds(i) >= seconds(45 + 300):
                break
        self.assertEqual((decision.action, decision.reason, decision.burst_started), ("block", BURST_DETECTED, True))

    def test_restore_keeps_latest_block(self):
        self.limiter.restore("a", seconds(100))
        self.limiter.restore("a", seconds(50))
        self.assertEqual(self.limiter.blocked_until("a"), seconds(100))
        decision = self.send("a", seconds(10))
        self.assertEqual((decision.action, decision.reason), ("block", SOURCE_BLOCKED))


def burst_decision(count=10):
    """The decision for the count-th prompt from one source in 60 s (threshold 10)."""
    detector, limiter = BurstDetector(60, 10), RateLimiter(300)
    decision = None
    for i in range(count):
        decision = limiter.decide(str(uuid4()), detector.observe("source_01", seconds(i)))
    return decision


class ThrottleEventTests(unittest.TestCase):
    def test_event_round_trip(self):
        event = build_throttle_event(burst_decision(), decided_at=seconds(10))
        self.assertEqual(set(event), EVENT_FIELDS)
        self.assertEqual(event["schema_version"], 1)
        self.assertEqual((event["action"], event["reason"], event["burst_started"]), ("block", BURST_DETECTED, True))
        self.assertEqual(event["blocked_until"], seconds(309).isoformat())
        self.assertEqual(validate_throttle_event(json.loads(json.dumps(event))), event)

    def test_allow_event(self):
        event = build_throttle_event(burst_decision(count=1))
        self.assertEqual((event["action"], event["reason"], event["blocked_until"]), ("allow", None, None))

    def test_invalid_events_are_rejected(self):
        event = build_throttle_event(burst_decision())
        for field, value in (
            ("schema_version", 2), ("schema_version", True), ("action", "throttle"), ("reason", None),
            ("prompt_id", "nope"), ("window_count", -1), ("threshold", 0), ("burst_started", 1),
            ("blocked_until", None), ("event_time", "2026-11-10T12:00:00"), ("source", " "),
        ):
            with self.subTest(field=field, value=value), self.assertRaises(ValueError):
                validate_throttle_event({**event, field: value})
        with self.assertRaises(ValueError):
            validate_throttle_event({k: v for k, v in event.items() if k != "decided_at"})


def stored_from(event):
    return {k: v for k, v in event.items() if k != "schema_version"}


class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.raw = build_raw_event("Hello", source="source_01", created_at=T0)
        self.message = Mock()
        self.message.value.return_value = json.dumps(self.raw).encode()
        self.message.topic.return_value = PROMPTS_RAW
        self.message.partition.return_value = 0
        self.message.offset.return_value = 7
        self.consumer = Mock()
        self.producer = Mock()
        self.producer.flush.return_value = 0
        self.detector, self.limiter = BurstDetector(60, 10), RateLimiter(300)

    def run_worker(self):
        return process_message(self.consumer, self.producer, self.message, "unused", self.detector, self.limiter)

    def test_store_then_publish_then_commit(self):
        order = []
        self.consumer.commit.side_effect = lambda **kwargs: order.append("committed")
        self.producer.produce.side_effect = lambda *args, **kwargs: order.append("published")

        def save(url, event):
            order.append("saved")
            return stored_from(event)

        with patch("backend.stream.burst_worker.db.save_raw_event"), \
             patch("backend.stream.burst_worker.db.get_throttle_decision", return_value=None), \
             patch("backend.stream.burst_worker.db.save_throttle_decision", side_effect=save):
            event = self.run_worker()
        self.assertEqual(order, ["saved", "published", "committed"])
        self.assertEqual(event["action"], "allow")
        args, kwargs = self.producer.produce.call_args
        self.assertEqual(args[0], THROTTLE_DECISIONS)
        self.assertEqual(kwargs["key"], self.raw["prompt_id"].encode())
        self.assertEqual(json.loads(kwargs["value"]), event)

    def test_redelivery_republishes_stored_decision(self):
        stored = stored_from(build_throttle_event(burst_decision()))
        stored["prompt_id"] = self.raw["prompt_id"]
        with patch("backend.stream.burst_worker.db.save_raw_event"), \
             patch("backend.stream.burst_worker.db.get_throttle_decision", return_value=stored), \
             patch("backend.stream.burst_worker.db.save_throttle_decision") as save:
            event = self.run_worker()
        save.assert_not_called()
        self.assertEqual(event["action"], "block")
        self.assertEqual(self.detector.window_count("source_01"), 1)  # still counted for the window

    def test_invalid_records_are_skipped_and_committed(self):
        for payload in (b"not json", None, json.dumps({**self.raw, "schema_version": 1}).encode()):
            with self.subTest(payload=payload):
                self.message.value.return_value = payload
                with patch("backend.stream.burst_worker.db.save_raw_event") as save:
                    with self.assertLogs("backend.stream.burst_worker", level="WARNING") as logs:
                        self.assertIsNone(self.run_worker())
                save.assert_not_called()
                self.assertIn("prompts_raw:0:7", logs.output[0])
        self.assertEqual(self.consumer.commit.call_count, 3)
        self.producer.produce.assert_not_called()

    def test_database_outage_does_not_commit(self):
        with patch("backend.stream.burst_worker.db.save_raw_event", side_effect=psycopg.OperationalError("down")):
            with self.assertRaises(psycopg.OperationalError):
                self.run_worker()
        self.consumer.commit.assert_not_called()

    def test_publish_failure_does_not_commit(self):
        from confluent_kafka import KafkaException
        self.producer.flush.return_value = 1  # still queued after the timeout
        with patch("backend.stream.burst_worker.db.save_raw_event"), \
             patch("backend.stream.burst_worker.db.get_throttle_decision", return_value=None), \
             patch("backend.stream.burst_worker.db.save_throttle_decision", side_effect=lambda url, e: stored_from(e)):
            with self.assertRaises(KafkaException):
                self.run_worker()
        self.consumer.commit.assert_not_called()


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL for PostgreSQL checks")
class ThrottleStorageTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["TEST_DATABASE_URL"]
        db.init_db(self.url)
        self.source = f"source_test_{uuid4().hex[:8]}"
        self.prompt_ids = []

    def tearDown(self):
        with db.connect(self.url) as conn:
            for prompt_id in self.prompt_ids:
                conn.execute("DELETE FROM prompts WHERE id = %s", (prompt_id,))

    def stored_event(self, at, decision):
        raw = build_raw_event("Hello", source=self.source, created_at=at)
        db.save_raw_event(self.url, raw)
        self.prompt_ids.append(raw["prompt_id"])
        event = build_throttle_event(decision)
        return {**event, "prompt_id": raw["prompt_id"]}

    def test_save_is_idempotent_and_blocks_are_restorable(self):
        detector, limiter = BurstDetector(60, 2), RateLimiter(300)
        now = datetime.now(timezone.utc)
        first = self.stored_event(now, limiter.decide(str(uuid4()), detector.observe(self.source, now)))
        burst = self.stored_event(now, limiter.decide(str(uuid4()), detector.observe(self.source, now)))

        saved = db.save_throttle_decision(self.url, stored_from(first))
        self.assertEqual(saved["action"], "allow")
        again = db.save_throttle_decision(self.url, {**stored_from(first), "window_count": 99})
        self.assertEqual(again, saved)
        self.assertEqual(db.get_throttle_decision(self.url, first["prompt_id"]), saved)

        saved_burst = db.save_throttle_decision(self.url, stored_from(burst))
        self.assertTrue(saved_burst["burst_started"])
        blocks = db.get_active_source_blocks(self.url, now)
        self.assertEqual(blocks[self.source], now + timedelta(seconds=300))
        self.assertNotIn(self.source, db.get_active_source_blocks(self.url, now + timedelta(seconds=301)))


@unittest.skipUnless(
    os.getenv("TEST_DATABASE_URL") and os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS"),
    "Set TEST_DATABASE_URL and TEST_KAFKA_BOOTSTRAP_SERVERS for the Kafka round trip",
)
class ThrottleKafkaTests(unittest.TestCase):
    def test_burst_reaches_topic_and_database(self):
        from confluent_kafka import Consumer, TopicPartition

        from backend.intake.producer import create_producer
        from backend.scripts.create_topics import create_topics

        url, servers = os.environ["TEST_DATABASE_URL"], os.environ["TEST_KAFKA_BOOTSTRAP_SERVERS"]
        db.init_db(url)
        create_topics(servers)
        source = f"source_test_{uuid4().hex[:8]}"
        raws = [build_raw_event(f"Prompt {i}", source=source, created_at=seconds(i)) for i in range(11)]
        reader = Consumer({"bootstrap.servers": servers, "group.id": f"test-{uuid4()}", "enable.auto.commit": False})
        _, high = reader.get_watermark_offsets(TopicPartition(THROTTLE_DECISIONS, 0), timeout=10)
        detector, limiter, producer = BurstDetector(60, 10), RateLimiter(300), create_producer(servers)
        try:
            for raw in raws:
                message = Mock()
                message.value.return_value = json.dumps(raw).encode()
                process_message(Mock(), producer, message, url, detector, limiter)

            reader.assign([TopicPartition(THROTTLE_DECISIONS, 0, high)])
            received = {}
            for _ in range(50):
                message = reader.poll(5)
                if message is None:
                    break
                self.assertIsNone(message.error())
                event = json.loads(message.value())
                if event["source"] == source:
                    received[event["prompt_id"]] = event["action"]
                if len(received) == len(raws):
                    break
            expected = ["allow"] * 9 + ["block", "block"]
            self.assertEqual([received.get(raw["prompt_id"]) for raw in raws], expected)
            self.assertEqual([db.get_throttle_decision(url, raw["prompt_id"])["action"] for raw in raws], expected)
        finally:
            reader.close()
            with db.connect(url) as conn:
                for raw in raws:
                    conn.execute("DELETE FROM prompts WHERE id = %s", (raw["prompt_id"],))


if __name__ == "__main__":
    unittest.main()
