"""Route and optional real-PostgreSQL integration checks."""

import os
import unittest
from unittest.mock import patch
from uuid import uuid4

import psycopg

from backend import db
from backend.app import create_app


class IntakeRouteTests(unittest.TestCase):
    def setUp(self):
        self.client = create_app({"TESTING": True}).test_client()

    def test_page_and_input_validation_work_without_database_access(self):
        self.assertEqual(self.client.get("/").status_code, 200)
        for body in ({}, {"text": " \n\t "}, {"text": 4}, {"text": "x" * 8001}, []):
            with self.subTest(body_type=type(body).__name__):
                self.assertEqual(self.client.post("/api/prompts", json=body).status_code, 400)
        self.assertEqual(self.client.post("/api/prompts", data="plain text").status_code, 415)

    def test_unavailable_database_returns_actionable_error(self):
        with patch("backend.db.save_prompt", side_effect=psycopg.OperationalError("unavailable")):
            response = self.client.post("/api/prompts", json={"text": "Keep this prompt"})
        self.assertEqual(response.status_code, 503)
        self.assertIn("not confirmed saved", response.json["error"])


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL for PostgreSQL checks")
class PostgreSQLIntakeTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["TEST_DATABASE_URL"]
        db.init_db(self.url)
        self.client = create_app({"TESTING": True, "DATABASE_URL": self.url}).test_client()
        self.ids = []

    def tearDown(self):
        with db.connect(self.url) as conn:
            for prompt_id in self.ids:
                conn.execute("DELETE FROM prompts WHERE id = %s", (prompt_id,))

    def test_submission_preserves_literal_text_and_pending_result_after_reload(self):
        text = "  <script>alert('test')</script>\n'; DROP TABLE prompts; -- 🛡️  "
        response = self.client.post("/api/prompts", json={"text": text})
        self.assertEqual(response.status_code, 201)
        prompt = response.json["prompt"]
        self.ids.append(prompt["id"])
        self.assertEqual(prompt["text"], text)
        self.assertEqual(prompt["decision"], "pending")
        loaded = {item["id"]: item for item in self.client.get("/api/prompts").json["prompts"]}
        self.assertEqual(loaded[prompt["id"]]["text"], text)
        with db.connect(self.url) as conn:
            result = conn.execute("SELECT * FROM guard_results WHERE prompt_id = %s", (prompt["id"],)).fetchone()
        self.assertEqual(result["decision"], "pending")
        self.assertIsNone(result["malicious_score"])
        self.assertIsNone(result["checked_at"])

    def test_foreign_key_rejects_result_without_prompt(self):
        with self.assertRaises(psycopg.errors.ForeignKeyViolation):
            with db.connect(self.url) as conn:
                conn.execute("INSERT INTO guard_results (prompt_id) VALUES (%s)", (uuid4(),))


if __name__ == "__main__":
    unittest.main()
