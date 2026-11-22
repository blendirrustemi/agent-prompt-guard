"""Prompt-to-answer policy, lifecycle and real Kafka/PostgreSQL integration checks."""

import json
import os
import subprocess
import time
import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import Mock, patch
from urllib.error import HTTPError
from uuid import uuid4

from backend import db
from backend.app import create_app
from backend.ask import wait_for_result
from backend.agent.agent import AgentAnswer
from backend.agent.worker import ask_in_subprocess, handle_message
from backend.intake.events import build_raw_event, validate_allowed_event
from backend.stream.router import route_pending_row


class FlowPolicyTests(unittest.TestCase):
    def test_required_throttle_and_full_text_coverage_cannot_be_bypassed(self):
        now = datetime.now(timezone.utc)
        row = {"stored_at": now, "throttle_action": None, "decided_at": None,
               "checked_at": now, "scored_at": now, "malicious_score": 0.1,
               "scoring_metadata": {"truncated": False}}
        self.assertIsNone(route_pending_row(row, now, 0.3, 0.7, 30))
        self.assertEqual(route_pending_row(row, now + timedelta(seconds=30), 0.3, 0.7, 30),
                         ("review", "throttle_decision_timeout"))
        row.update(throttle_action="allow", decided_at=now, scoring_metadata={"truncated": True})
        self.assertEqual(route_pending_row(row, now, 0.3, 0.7, 30), ("review", "model_input_too_long"))
        row["scoring_metadata"] = None
        self.assertEqual(route_pending_row(row, now, 0.3, 0.7, 30),
                         ("review", "classifier_input_coverage_unknown"))

    def test_polling_survives_pending_storage_and_waits_for_the_answer(self):
        values = [HTTPError("url", 404, "missing", {}, None),
                  {"state": "checking"}, {"state": "agent_queued"},
                  {"state": "agent_running"}, {"state": "completed", "agent": {"response": "84"}}]
        states = []
        with patch("backend.ask.request_json", side_effect=values), patch("backend.ask.time.sleep"):
            result = wait_for_result("http://test", uuid4(), on_state=states.append)
        self.assertEqual(result["agent"]["response"], "84")
        self.assertEqual(states, ["checking", "agent_queued", "agent_running", "completed"])

    def test_agent_subprocess_has_a_hard_timeout_and_does_not_expose_stderr(self):
        with patch("backend.agent.worker.subprocess.run", side_effect=subprocess.TimeoutExpired("child", 1)):
            with self.assertRaisesRegex(TimeoutError, "execution limit"):
                ask_in_subprocess("private text", model="fixture", timeout=1)
        with patch("backend.agent.worker.subprocess.run", return_value=Mock(returncode=1, stderr="private text")):
            with self.assertRaisesRegex(RuntimeError, "Agent process failed"):
                ask_in_subprocess("private text", model="fixture")

    def test_allowed_event_validation_rejects_poison_messages(self):
        valid = {"schema_version": 1, "prompt_id": str(uuid4()), "text": "Hello", "model_version": "fixture"}
        self.assertEqual(validate_allowed_event(valid), valid)
        for event in (None, [], {}, {**valid, "schema_version": True}, {**valid, "prompt_id": "invalid"},
                      {**valid, "text": ""}, {**valid, "model_version": None}):
            with self.subTest(event=event), self.assertRaises(ValueError):
                validate_allowed_event(event)


class FlowFixtures:
    def setUp(self):
        self.url = os.environ["TEST_DATABASE_URL"]
        db.init_db(self.url)
        self.ids = []
        self.client = create_app({"TESTING": True, "DATABASE_URL": self.url}).test_client()

    def tearDown(self):
        with db.connect(self.url) as conn:
            for prompt_id in self.ids:
                conn.execute("DELETE FROM prompts WHERE id = %s", (prompt_id,))

    def new_prompt(self):
        event = build_raw_event("What is 12 times 7?")
        db.save_raw_event(self.url, event)
        self.ids.append(event["prompt_id"])
        return event

    def result(self, event):
        response = self.client.get(f"/api/prompts/{event['prompt_id']}")
        self.assertEqual(response.status_code, 200)
        return response.json


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL for lifecycle checks")
class FlowStorageTests(FlowFixtures, unittest.TestCase):
    def test_running_and_completed_answers_are_observable_and_claimed_once(self):
        event = self.new_prompt()
        self.assertEqual(self.result(event)["state"], "checking")
        db.route_guard_result(self.url, event["prompt_id"], "allow", "fixture")
        self.assertEqual(self.result(event)["state"], "agent_queued")
        token = db.claim_agent_run(self.url, event["prompt_id"], "fixture", 120)
        self.assertIsNotNone(token)
        self.assertIsNone(db.claim_agent_run(self.url, event["prompt_id"], "fixture", 120))
        self.assertEqual(self.result(event)["state"], "agent_running")
        db.finish_agent_run(self.url, event["prompt_id"], token, "completed", response="84")
        result = self.result(event)
        self.assertEqual((result["status"], result["state"], result["agent"]["response"]),
                         ("stored", "completed", "84"))
        self.assertIsNotNone(result["agent"]["finished_at"])

    def test_interrupted_runs_expire_without_executing_again(self):
        event = self.new_prompt()
        db.route_guard_result(self.url, event["prompt_id"], "allow", "fixture")
        token = db.claim_agent_run(self.url, event["prompt_id"], "fixture", 120)
        with db.connect(self.url) as conn:
            conn.execute("UPDATE agent_runs SET deadline_at = NOW() - INTERVAL '1 second' WHERE prompt_id = %s",
                         (event["prompt_id"],))
        db.expire_agent_runs(self.url)
        self.assertEqual(self.result(event)["state"], "agent_failed")
        self.assertIsNone(db.finish_agent_run(self.url, event["prompt_id"], token, "completed", response="late"))
        ask = Mock()
        self.assertIsNone(handle_message(self.url, event, ask, "fixture"))
        ask.assert_not_called()

    def test_missing_throttle_expires_from_storage_time_not_event_time(self):
        from backend.stream.router_worker import route_ready_prompts
        event = self.new_prompt()
        with db.connect(self.url) as conn:
            conn.execute("UPDATE prompts SET stored_at = NOW() - INTERVAL '31 seconds' WHERE id = %s",
                         (event["prompt_id"],))
        route_ready_prompts(self.url, allow_max=0.3, block_min=0.7, score_timeout_seconds=30)
        result = self.result(event)
        self.assertEqual((result["state"], result["guard"]["reason"]), ("review", "throttle_decision_timeout"))
        self.assertIsNone(db.claim_agent_run(self.url, event["prompt_id"], "fixture", 120))


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL") and os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS"),
                     "Set both test connections for the complete Kafka flow")
class KafkaFlowTests(FlowFixtures, unittest.TestCase):
    def setUp(self):
        super().setUp()
        from confluent_kafka import Consumer
        from backend.scripts.create_topics import create_topics
        self.servers = os.environ["TEST_KAFKA_BOOTSTRAP_SERVERS"]
        create_topics(self.servers)
        self.client = create_app({"TESTING": True, "DATABASE_URL": self.url,
                                  "KAFKA_BOOTSTRAP_SERVERS": self.servers}).test_client()
        self.consumer = Consumer({"bootstrap.servers": self.servers, "group.id": f"flow-test-{uuid4()}",
                                  "auto.offset.reset": "earliest", "enable.auto.commit": False})
        self.consumer.subscribe(["prompts_raw", "throttle_decisions", "prompts_allowed"])
        self.inbox = {}

    def tearDown(self):
        self.consumer.close()
        super().tearDown()

    def receive(self, topic, prompt_id):
        key = (topic, prompt_id)
        deadline = time.monotonic() + 20
        while key not in self.inbox and time.monotonic() < deadline:
            message = self.consumer.poll(0.2)
            if message is None:
                continue
            if message.error():
                self.fail(str(message.error()))
            try:
                event = json.loads(message.value())
                self.inbox[(message.topic(), event["prompt_id"])] = message
            except (ValueError, TypeError, KeyError):
                pass
        self.assertIn(key, self.inbox, f"No {topic} message for {prompt_id}")
        return self.inbox.pop(key)

    def test_http_kafka_guard_outbox_and_agent_with_all_routing_outcomes(self):
        from backend.intake.consumer import process_message
        from backend.intake.producer import create_producer
        from backend.stream.burst import BurstDetector
        from backend.stream.burst_worker import process_message as process_burst
        from backend.stream.enrichment import SourceContextCache
        from backend.stream.rate_limiter import RateLimiter
        from backend.stream.router_worker import process_throttle_message, route_ready_prompts, publish_allowed_outbox
        from backend.tests.test_scoring import make_scorer

        producer = create_producer(self.servers)
        for score, tokens, threshold, expected in ((0.2, 12, 10, "completed"), (0.5, 12, 10, "review"),
                                                  (0.9, 12, 10, "blocked"), (0.1, 257, 10, "review"),
                                                  (0.1, 12, 1, "blocked")):
            with self.subTest(expected=expected, score=score, tokens=tokens, threshold=threshold):
                prompt_id = str(uuid4())
                self.ids.append(prompt_id)
                accepted = self.client.post("/api/prompts", json={"prompt_id": prompt_id, "text": "What is 12 times 7?"})
                self.assertEqual(accepted.status_code, 202)
                raw = self.receive("prompts_raw", prompt_id)
                scorer = make_scorer(score, tokens=tokens)
                scorer.now = lambda: datetime.now(timezone.utc)
                process_message(self.consumer, raw, self.url, SourceContextCache(self.url), scorer)
                process_burst(self.consumer, producer, raw, self.url, BurstDetector(((60, threshold),)), RateLimiter())
                throttle = self.receive("throttle_decisions", prompt_id)
                process_throttle_message(self.consumer, throttle, self.url)
                route_ready_prompts(self.url, allow_max=0.3, block_min=0.7, score_timeout_seconds=30)
                ask = Mock(return_value=AgentAnswer("84", [{"name": "calculator", "args": {"a": 12, "operation": "multiply", "b": 7}}]))
                if expected == "completed":
                    publish_allowed_outbox(self.url, producer)
                    allowed = json.loads(self.receive("prompts_allowed", prompt_id).value())
                    handle_message(self.url, allowed, ask, "fixture")
                    handle_message(self.url, allowed, ask, "fixture")
                    ask.assert_called_once_with("What is 12 times 7?")
                else:
                    self.assertFalse(any(str(row["prompt_id"]) == prompt_id
                                         for row in db.get_unpublished_allowed_prompts(self.url)))
                    # Even an injected allowed event cannot bypass the stored verdict.
                    handle_message(self.url, {"prompt_id": prompt_id}, ask, "fixture")
                    ask.assert_not_called()
                result = self.result({"prompt_id": prompt_id})
                self.assertEqual(result["state"], expected)
                self.assertIsNotNone(result["intake"]["preprocessing"])
                self.assertIsNotNone(result["guard"]["scoring_metadata"])
