"""Agent tool checks, worker checks against PostgreSQL/Kafka, and an optional live Ollama run."""

import os
import unittest
from unittest.mock import Mock
from uuid import uuid4

from backend import db
from backend.agent.agent import AgentAnswer
from backend.agent.tools import COURSE_INFO, calculate, find_course_info
from backend.agent.worker import handle_message


class ToolTests(unittest.TestCase):
    def test_calculator_operations(self):
        for operation, expected in (("add", "15"), ("subtract", "9"), ("multiply", "36"), ("divide", "4")):
            with self.subTest(operation=operation):
                self.assertEqual(calculate(12, operation, 3), expected)
        self.assertEqual(calculate(7, " Divide ", 2), "3.5")

    def test_calculator_errors_are_returned_as_text(self):
        self.assertEqual(calculate(1, "divide", 0), "Cannot divide by zero.")
        self.assertIn("Unknown operation", calculate(1, "__import__('os')", 2))

    def test_course_lookup_matches_topics_and_keywords(self):
        self.assertIn("26 November 2026", find_course_info("Final submission DEADLINE"))
        self.assertIn("Nandor", find_course_info("who owns p9"))
        unknown = find_course_info("cafeteria menu")
        for topic in COURSE_INFO:
            self.assertIn(topic, unknown)


class AllowedEventTests(unittest.TestCase):
    def test_allowed_event_has_documented_fields(self):
        try:
            from backend.intake.producer import build_allowed_event
        except ImportError as error:  # Windows Application Control can block the Kafka client DLL
            self.skipTest(f"Kafka client unavailable: {error}")
        prompt_id, text = uuid4(), "  '; DROP TABLE prompts; -- 🛡️\n"
        event = build_allowed_event(prompt_id, text, "stub")
        self.assertEqual(event, {"schema_version": 1, "prompt_id": str(prompt_id), "text": text, "model_version": "stub"})
        with self.assertRaises(ValueError):
            build_allowed_event("not-a-uuid", "Hi", "stub")


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL for PostgreSQL checks")
class WorkerTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["TEST_DATABASE_URL"]
        db.init_db(self.url)
        self.ids = []

    def tearDown(self):
        with db.connect(self.url) as conn:
            for prompt_id in self.ids:
                conn.execute("DELETE FROM prompts WHERE id = %s", (prompt_id,))

    def prompt_with_decision(self, decision, text="What is 2 + 3?"):
        prompt_id = db.save_prompt(self.url, text)["id"]
        self.ids.append(prompt_id)
        if decision != "pending":
            db.save_guard_result(self.url, prompt_id, decision, model_version="test")
        return {"schema_version": 1, "prompt_id": prompt_id, "text": text, "model_version": "test"}

    def test_allowed_prompt_is_answered_and_stored_once(self):
        event = self.prompt_with_decision("allow")
        calls = [{"name": "calculator", "args": {"a": 2, "operation": "add", "b": 3}}]
        ask_fn = Mock(return_value=AgentAnswer("5", calls))
        run = handle_message(self.url, event, ask_fn, "test-model")
        ask_fn.assert_called_once_with("What is 2 + 3?")
        self.assertEqual((run["status"], run["response"], run["tool_calls"], run["model"]), ("completed", "5", calls, "test-model"))
        self.assertGreaterEqual(run["latency_ms"], 0)
        # Redelivery of the same message neither runs the agent again nor adds a row.
        self.assertIsNone(handle_message(self.url, event, ask_fn, "test-model"))
        ask_fn.assert_called_once()

    def test_agent_runs_the_stored_text_not_the_message_text(self):
        event = self.prompt_with_decision("allow")
        event["text"] = "Something the guard never checked"
        ask_fn = Mock(return_value=AgentAnswer("5"))
        handle_message(self.url, event, ask_fn, "test-model")
        ask_fn.assert_called_once_with("What is 2 + 3?")

    def test_not_allowed_prompts_are_rejected_without_running_the_agent(self):
        for decision in ("block", "review", "pending"):
            with self.subTest(decision=decision):
                ask_fn = Mock()
                run = handle_message(self.url, self.prompt_with_decision(decision), ask_fn, "test-model")
                ask_fn.assert_not_called()
                self.assertEqual(run["status"], "rejected")
                self.assertIn(decision, run["error"])
                self.assertIsNone(run["response"])

    def test_agent_failure_is_recorded(self):
        ask_fn = Mock(side_effect=ConnectionError("Ollama is not running"))
        run = handle_message(self.url, self.prompt_with_decision("allow"), ask_fn, "test-model")
        self.assertEqual(run["status"], "failed")
        self.assertIn("Ollama is not running", run["error"])

    def test_unknown_prompt_is_skipped(self):
        event = {"schema_version": 1, "prompt_id": str(uuid4()), "text": "Hi", "model_version": "test"}
        self.assertIsNone(handle_message(self.url, event, Mock(), "test-model"))


@unittest.skipUnless(
    os.getenv("TEST_DATABASE_URL") and os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS"),
    "Set TEST_DATABASE_URL and TEST_KAFKA_BOOTSTRAP_SERVERS for the Kafka round trip",
)
class KafkaWorkerTests(unittest.TestCase):
    def test_published_allowed_prompt_is_processed_by_the_worker(self):
        from backend.agent.worker import run
        from backend.intake.producer import publish_allowed_prompt
        from backend.scripts.create_topics import create_topics

        url, servers = os.environ["TEST_DATABASE_URL"], os.environ["TEST_KAFKA_BOOTSTRAP_SERVERS"]
        db.init_db(url)
        create_topics(servers)
        prompt_id = db.save_prompt(url, "What is 2 + 3?")["id"]
        try:
            db.save_guard_result(url, prompt_id, "allow", model_version="test")
            publish_allowed_prompt(prompt_id, "What is 2 + 3?", "test", servers=servers)
            # A fresh consumer group reads the topic from the start; other prompts' messages are skipped or
            # rejected, so the fake agent only matters for this prompt.
            run(url, lambda text: AgentAnswer("5"), "test-model", servers=servers,
                group_id=f"test-{uuid4()}", once=True, idle_seconds=5)
            self.assertEqual(db.get_agent_run(url, prompt_id)["status"], "completed")
        finally:
            with db.connect(url) as conn:
                conn.execute("DELETE FROM prompts WHERE id = %s", (prompt_id,))


@unittest.skipUnless(os.getenv("TEST_OLLAMA_MODEL"), "Set TEST_OLLAMA_MODEL to run the agent against Ollama")
class LiveAgentTests(unittest.TestCase):
    def test_agent_uses_its_tools(self):
        from backend.agent.agent import ask, build_agent

        agent = build_agent(os.environ["TEST_OLLAMA_MODEL"])
        answer = ask(agent, "What is 12 times 7?")
        self.assertIn("84", answer.response)
        self.assertIn("calculator", [call["name"] for call in answer.tool_calls])
        answer = ask(agent, "When is the final submission deadline?")
        self.assertIn("26 November", answer.response)
        self.assertIn("lookup_course_info", [call["name"] for call in answer.tool_calls])


if __name__ == "__main__":
    unittest.main()
