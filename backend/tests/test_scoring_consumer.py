"""P1/P2 -> P3/P4 acknowledgement and P7 integration, with injected models."""

import json
import unittest
from unittest.mock import Mock, patch

import psycopg

from backend.intake.consumer import process_message
from backend.intake.events import build_raw_event
from backend.stream.router_worker import process_throttle_message, route_ready_prompts
from backend.tests.test_scoring import NOW, make_scorer
from backend.tests.test_router import throttle_event


class ScoringConsumerTests(unittest.TestCase):
    def setUp(self):
        self.event = build_raw_event("  original\ntext ")
        self.message = Mock()
        self.message.value.return_value = json.dumps(self.event).encode()
        self.message.topic.return_value = "prompts_raw"
        self.message.partition.return_value = 0
        self.message.offset.return_value = 7
        self.consumer, self.cache = Mock(), Mock()
        self.cache.get.return_value = {}
        self.scorer = make_scorer(0.2)
        self.raw = self.enterContext(patch("backend.intake.consumer.db.save_raw_event", return_value={"id": self.event["prompt_id"]}))
        self.read = self.enterContext(patch("backend.stream.scoring.db.get_guard_result", return_value={"malicious_score": None, "scored_at": None}))
        self.save = self.enterContext(patch("backend.stream.scoring.db.save_guard_score", return_value={"decision": "pending"}))

    def process(self):
        process_message(self.consumer, self.message, "unused", self.cache, self.scorer)

    def test_raw_then_score_then_commit_and_original_text_is_used(self):
        order = []
        self.raw.side_effect = lambda *args, **kwargs: order.append("raw") or {"id": self.event["prompt_id"]}
        self.save.side_effect = lambda *args, **kwargs: order.append("score") or {}
        self.consumer.commit.side_effect = lambda **kwargs: order.append("commit")
        self.process()
        self.assertEqual(order, ["raw", "score", "commit"])
        self.assertEqual(self.scorer.encoder.encode.call_args.args[0], [self.event["text"]])
        self.assertEqual(self.raw.call_args.kwargs["preprocessing"]["cleaned_text"], "original text")

    def test_storage_failure_does_not_commit_and_retry_reuses_completed_score(self):
        self.save.side_effect = psycopg.OperationalError("down")
        with self.assertRaises(psycopg.OperationalError):
            self.process()
        self.consumer.commit.assert_not_called()
        self.save.side_effect = None
        self.read.return_value = {"malicious_score": 0.2, "scored_at": NOW.isoformat()}
        self.scorer.encoder.encode.reset_mock()
        self.process()
        self.scorer.encoder.encode.assert_not_called()
        self.consumer.commit.assert_called_once()

    def test_inference_failure_is_recorded_before_commit(self):
        self.scorer.encoder.encode.side_effect = RuntimeError("private text")
        self.process()
        self.assertEqual(self.save.call_args.args[2:4], (None, None))
        self.consumer.commit.assert_called_once()

    def test_malformed_message_never_calls_model(self):
        self.message.value.return_value = b"not JSON"
        with self.assertLogs("backend.intake.consumer", "WARNING"):
            self.process()
        self.scorer.encoder.encode.assert_not_called()
        self.save.assert_not_called()
        self.consumer.commit.assert_called_once()

    def test_stored_p4_scores_drive_p7_without_p4_finalizing(self):
        for score, expected in ((0.2, "allow"), (0.4, "review"), (0.8, "block")):
            self.scorer = make_scorer(score)
            self.process()
            args, kwargs = self.save.call_args
            row = {"prompt_id": args[1], "malicious_score": args[2], "attack_type": args[3],
                   "model_version": args[4], "checked_at": kwargs["checked_at"],
                   "decided_at": NOW, "throttle_action": "allow"}
            with patch("backend.stream.router_worker.db.get_pending_routes", return_value=[row]), \
                 patch("backend.stream.router_worker.db.route_guard_result", return_value=True) as route:
                route_ready_prompts("unused", allow_max=0.3, block_min=0.7, score_timeout_seconds=30, now=NOW)
            self.assertEqual(route.call_args.args[2], expected)

    def test_p10_block_overrides_saved_low_score(self):
        self.process()
        event = throttle_event(self.event["prompt_id"], action="block", reason="source_blocked")
        message = Mock()
        message.value.return_value = json.dumps(event).encode()
        with patch("backend.stream.router_worker.db.save_throttle_decision"), \
             patch("backend.stream.router_worker.db.route_guard_result") as route:
            process_throttle_message(Mock(), message, "unused")
        self.assertEqual(route.call_args.args[2:], ("block", "throttle_source_blocked"))
