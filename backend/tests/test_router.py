"""P7 routing policy, worker acknowledgement, and optional storage/Kafka checks."""

import json
import os
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from uuid import uuid4

from backend import db
from backend.intake.events import build_raw_event
from backend.intake.topics import PROMPTS_ALLOWED, THROTTLE_DECISIONS
from backend.stream.router import decision_for_score, route_pending_row, validate_policy
from backend.stream.router_worker import (
    process_throttle_message,
    publish_allowed_outbox,
    route_ready_prompts,
)
from backend.stream.throttle_events import build_throttle_event

NOW = datetime(2026, 11, 11, 12, tzinfo=timezone.utc)


def throttle_event(prompt_id, action="allow", decided_at=NOW, reason=None):
    blocked = action == "block"
    return {
        "schema_version": 1,
        "prompt_id": prompt_id,
        "source": "source_01",
        "action": action,
        "reason": reason,
        "window_count": 1,
        "window_seconds": 60,
        "threshold": 10,
        "blocked_until": (NOW + timedelta(minutes=5)).isoformat() if blocked else None,
        "burst_started": blocked and reason == "burst_detected",
        "event_time": NOW.isoformat(),
        "decided_at": decided_at.isoformat(),
    }


class RouterPolicyTests(unittest.TestCase):
    def test_thresholds_are_inclusive_and_middle_is_review(self):
        self.assertEqual(decision_for_score(0.30, 0.30, 0.70), ("allow", "score_below_allow_threshold"))
        self.assertEqual(decision_for_score(0.70, 0.30, 0.70), ("block", "score_above_block_threshold"))
        self.assertEqual(decision_for_score(0.50, 0.30, 0.70), ("review", "score_in_review_band"))

    def test_invalid_scores_and_policy_are_rejected(self):
        validate_policy(0.3, 0.7, 30)
        for values in ((0.7, 0.7, 30), (0.8, 0.2, 30), (-0.1, 0.7, 30), (0.3, 1.1, 30), (0.3, 0.7, 0)):
            with self.subTest(values=values), self.assertRaises(ValueError):
                validate_policy(*values)
        for value in (float("nan"), float("inf"), -0.1, 1.1, True):
            with self.subTest(value=value), self.assertRaises(ValueError):
                decision_for_score(value, 0.3, 0.7)

    def test_missing_and_late_scores_fail_closed(self):
        base = {"decided_at": NOW, "malicious_score": None, "checked_at": None}
        self.assertIsNone(route_pending_row(base, NOW + timedelta(seconds=29), 0.3, 0.7, 30))
        failed = {**base, "checked_at": NOW + timedelta(seconds=2)}
        self.assertIsNone(route_pending_row(failed, NOW + timedelta(seconds=29), 0.3, 0.7, 30))
        self.assertEqual(
            route_pending_row(failed, NOW + timedelta(seconds=30), 0.3, 0.7, 30),
            ("review", "classifier_inference_failed"),
        )
        self.assertEqual(
            route_pending_row(base, NOW + timedelta(seconds=30), 0.3, 0.7, 30),
            ("review", "classifier_score_timeout"),
        )
        late = {**base, "malicious_score": 0.1, "checked_at": NOW + timedelta(seconds=31)}
        self.assertEqual(
            route_pending_row(late, NOW + timedelta(seconds=40), 0.3, 0.7, 30),
            ("review", "score_arrived_after_timeout"),
        )

    def test_allow_event_commits_without_waiting_for_score(self):
        event = throttle_event(str(uuid4()))
        consumer, message = Mock(), Mock()
        message.value.return_value = json.dumps(event).encode()
        message.topic.return_value = THROTTLE_DECISIONS
        with patch("backend.stream.router_worker.db.save_throttle_decision") as save, \
             patch("backend.stream.router_worker.db.route_guard_result") as route:
            result = process_throttle_message(consumer, message, "unused")
        save.assert_called_once()
        route.assert_not_called()
        consumer.commit.assert_called_once_with(message=message, asynchronous=False)
        self.assertEqual(result["action"], "allow")

    def test_p10_block_routes_immediately_before_offset_commit(self):
        event = throttle_event(str(uuid4()), action="block", reason="source_blocked")
        consumer, message, order = Mock(), Mock(), []
        message.value.return_value = json.dumps(event).encode()
        message.topic.return_value = THROTTLE_DECISIONS
        consumer.commit.side_effect = lambda **kwargs: order.append("commit")
        with patch("backend.stream.router_worker.db.save_throttle_decision", side_effect=lambda *args: order.append("save")), \
             patch("backend.stream.router_worker.db.route_guard_result", side_effect=lambda *args: order.append(("route", args[2], args[3]))):
            process_throttle_message(consumer, message, "unused")
        self.assertEqual(order, ["save", ("route", "block", "throttle_source_blocked"), "commit"])

    def test_ready_score_routes_and_only_allow_reaches_outbox(self):
        rows = [{
            "prompt_id": str(uuid4()), "throttle_action": "allow", "decided_at": NOW,
            "malicious_score": 0.2, "checked_at": NOW + timedelta(seconds=1),
        }]
        with patch("backend.stream.router_worker.db.get_pending_routes", return_value=rows), \
             patch("backend.stream.router_worker.db.route_guard_result", return_value=True) as route:
            result = route_ready_prompts("unused", allow_max=0.3, block_min=0.7,
                                         score_timeout_seconds=30, now=NOW + timedelta(seconds=2))
        self.assertEqual(result, [(rows[0]["prompt_id"], "allow")])
        route.assert_called_once_with("unused", rows[0]["prompt_id"], "allow", "score_below_allow_threshold")

    def test_outbox_publishes_existing_contract_and_marks_acknowledged_rows(self):
        prompt_id = str(uuid4())
        producer = Mock()
        producer.flush.return_value = 0
        rows = [{"prompt_id": prompt_id, "text": "hello", "model_version": "b2-v1"}]
        with patch("backend.stream.router_worker.db.get_unpublished_allowed_prompts", return_value=rows), \
             patch("backend.stream.router_worker.db.mark_allowed_prompts_published", return_value=1) as mark:
            published = publish_allowed_outbox("unused", producer)
        self.assertEqual(published, 1)
        args, kwargs = producer.produce.call_args
        self.assertEqual(args[0], PROMPTS_ALLOWED)
        event = json.loads(kwargs["value"])
        self.assertEqual(event, {
            "schema_version": 1, "prompt_id": prompt_id,
            "text": "hello", "model_version": "b2-v1",
        })
        mark.assert_called_once_with("unused", [prompt_id])


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL for P7 storage checks")
class RouterStorageTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["TEST_DATABASE_URL"]
        db.init_db(self.url)
        self.prompt_ids = []

    def tearDown(self):
        with db.connect(self.url) as conn:
            for prompt_id in self.prompt_ids:
                conn.execute("DELETE FROM prompts WHERE id = %s", (prompt_id,))

    def create_pending(self, source="source_router_test"):
        raw = build_raw_event("Routing fixture", source=source, created_at=NOW)
        db.save_raw_event(self.url, raw)
        self.prompt_ids.append(raw["prompt_id"])
        return raw["prompt_id"]

    def test_score_is_saved_without_finalizing_and_allow_outbox_is_atomic(self):
        prompt_id = self.create_pending()
        score = db.save_guard_score(self.url, prompt_id, 0.1, "jailbreak_persona", "b2-v1", checked_at=NOW)
        self.assertEqual(score["decision"], "pending")
        self.assertEqual(score["attack_type"], "jailbreak_persona")
        self.assertTrue(db.route_guard_result(self.url, prompt_id, "allow", "score_below_allow_threshold"))
        rows = db.get_unpublished_allowed_prompts(self.url)
        self.assertEqual([str(row["prompt_id"]) for row in rows], [prompt_id])
        self.assertEqual(db.mark_allowed_prompts_published(self.url, [rows[0]["prompt_id"]]), 1)
        self.assertEqual(db.get_unpublished_allowed_prompts(self.url), [])
        self.assertFalse(db.route_guard_result(self.url, prompt_id, "block", "late_duplicate"))

    def test_throttle_and_pending_score_join(self):
        prompt_id = self.create_pending()
        allow = build_throttle_event(type("D", (), {
            "prompt_id": prompt_id, "source": "source_router_test", "action": "allow", "reason": None,
            "window_count": 1, "window_seconds": 60, "threshold": 10, "blocked_until": None,
            "burst_started": False, "event_time": NOW,
        })(), decided_at=NOW)
        db.save_throttle_decision(self.url, {field: allow[field] for field in db.THROTTLE_FIELDS})
        ready = db.get_pending_routes(self.url, ready_before=NOW - timedelta(seconds=30))
        self.assertEqual(ready, [])
        db.save_guard_score(self.url, prompt_id, 0.8, "obfuscation", "b2-v1", checked_at=NOW + timedelta(seconds=1))
        ready = db.get_pending_routes(self.url, ready_before=NOW - timedelta(seconds=30))
        self.assertEqual([str(row["prompt_id"]) for row in ready], [prompt_id])

    def test_p10_block_overrides_a_low_p4_score(self):
        prompt_id = self.create_pending(source="source_01")
        db.save_guard_score(self.url, prompt_id, 0.1, "jailbreak_persona", "b2-v1", checked_at=NOW)
        event = throttle_event(prompt_id, action="block", decided_at=NOW, reason="burst_detected")
        consumer, message = Mock(), Mock()
        message.value.return_value = json.dumps(event).encode()
        message.topic.return_value = THROTTLE_DECISIONS
        process_throttle_message(consumer, message, self.url)
        result = db.get_guard_result(self.url, prompt_id)
        self.assertEqual(result["decision"], "block")
        self.assertEqual(result["malicious_score"], 0.1)
        self.assertEqual(result["reason"], "throttle_burst_detected")
        self.assertEqual(db.get_unpublished_allowed_prompts(self.url), [])
        # A late P4 write may fill in analytics, but must not revise P7's final verdict.
        db.save_guard_score(self.url, prompt_id, 0.95, "adversarial_attacks", "b2-v1",
                            checked_at=NOW + timedelta(seconds=2))
        updated = db.get_guard_result(self.url, prompt_id)
        self.assertEqual(updated["decision"], "block")
        self.assertEqual(updated["reason"], "throttle_burst_detected")

    def test_persisted_p10_block_routes_without_waiting_for_kafka_join_or_score(self):
        prompt_id = self.create_pending(source="source_01")
        event = throttle_event(prompt_id, action="block", decided_at=NOW, reason="source_blocked")
        db.save_throttle_decision(self.url, {field: event[field] for field in db.THROTTLE_FIELDS})
        routed = route_ready_prompts(
            self.url, allow_max=0.3, block_min=0.7,
            score_timeout_seconds=30, now=NOW,
        )
        self.assertEqual(routed, [(prompt_id, "block")])
        self.assertEqual(db.get_guard_result(self.url, prompt_id)["decision"], "block")


@unittest.skipUnless(
    os.getenv("TEST_DATABASE_URL") and os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS"),
    "Set TEST_DATABASE_URL and TEST_KAFKA_BOOTSTRAP_SERVERS for P7 Kafka integration",
)
class RouterKafkaTests(unittest.TestCase):
    def test_throttle_topic_block_is_applied_and_committed(self):
        from confluent_kafka import Consumer, Producer, TopicPartition
        from backend.scripts.create_topics import create_topics

        url, servers = os.environ["TEST_DATABASE_URL"], os.environ["TEST_KAFKA_BOOTSTRAP_SERVERS"]
        db.init_db(url)
        create_topics(servers)
        raw = build_raw_event("P7 Kafka block fixture", source=f"router_{uuid4().hex[:8]}", created_at=NOW)
        db.save_raw_event(url, raw)
        consumer = Consumer({"bootstrap.servers": servers, "group.id": f"router-test-{uuid4()}", "enable.auto.commit": False})
        topic_partition = TopicPartition(THROTTLE_DECISIONS, 0)
        _, high = consumer.get_watermark_offsets(topic_partition, timeout=10)
        producer = Producer({"bootstrap.servers": servers})
        event = throttle_event(raw["prompt_id"], action="block", reason="burst_detected")
        event["burst_started"] = True
        event["blocked_until"] = (NOW + timedelta(minutes=5)).isoformat()
        producer.produce(THROTTLE_DECISIONS, key=raw["prompt_id"].encode(), value=json.dumps(event).encode())
        producer.flush(10)
        consumer.assign([TopicPartition(THROTTLE_DECISIONS, 0, high)])
        try:
            message = consumer.poll(10)
            self.assertIsNotNone(message)
            mock_consumer = Mock()
            process_throttle_message(mock_consumer, message, url)
            self.assertEqual(db.get_guard_result(url, raw["prompt_id"])["decision"], "block")
            mock_consumer.commit.assert_called_once()
        finally:
            consumer.close()
            with db.connect(url) as conn:
                conn.execute("DELETE FROM prompts WHERE id = %s", (raw["prompt_id"],))


if __name__ == "__main__":
    unittest.main()
