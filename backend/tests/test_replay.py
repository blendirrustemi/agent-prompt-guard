"""Replay selection, pacing, failure handling and an optional real intake pipeline."""

import io
import json
import os
import unittest
from contextlib import redirect_stderr, redirect_stdout
from unittest.mock import MagicMock, patch
from uuid import uuid4

import psycopg
from confluent_kafka import KafkaException

from backend import db
from backend.intake import replay


class ReplayTests(unittest.TestCase):
    def setUp(self):
        self.rows = [{"id": uuid4(), "text": "  Example!\n"} for _ in range(3)]
        self.out = io.StringIO()
        self.err = io.StringIO()
        self.enterContext(redirect_stdout(self.out))
        self.enterContext(redirect_stderr(self.err))
        self.producer = self.enterContext(patch.object(replay, "create_producer"))
        self.send = self.enterContext(patch.object(replay, "send_raw_event"))

    def test_reuses_producer_rotates_sources_and_sends_only_raw_fields(self):
        for _ in range(2):
            self.assertEqual(replay.replay_rows(self.rows, rate=0, sources=("first", "second")), 0)
        self.assertEqual(self.producer.call_count, 2)
        events = [call.args[1] for call in self.send.call_args_list]
        self.assertEqual([event["source"] for event in events[:3]], ["first", "second", "first"])
        self.assertEqual(len({event["prompt_id"] for event in events}), 6)
        for row, event in zip(self.rows, events):
            self.assertEqual(set(event), {"schema_version", "prompt_id", "text", "source", "created_at", "dataset_prompt_id"})
            self.assertEqual(event["text"], row["text"])
            self.assertEqual(event["dataset_prompt_id"], str(row["id"]))
        self.assertIn("acknowledged: 3", self.out.getvalue())

    def test_paces_sends_without_catch_up_after_slow_acknowledgement(self):
        tick = [0.0]
        waits = []
        starts = []
        def wait(delay):
            waits.append(delay)
            tick[0] += delay
        def send(producer, event):
            starts.append(tick[0])
            tick[0] += 2 if len(starts) == 2 else 0.1
        self.send.side_effect = send
        replay.replay_rows(self.rows * 2, rate=2, clock=lambda: tick[0], wait=wait)
        self.assertEqual(starts, [0, 0.5, 2.5, 3, 3.5, 4])
        self.assertTrue(all(b - a >= 0.5 for a, b in zip(starts, starts[1:])))
        self.assertTrue(all(delay > 0 for delay in waits))

    def test_unlimited_rate_has_no_intentional_wait(self):
        wait = MagicMock()
        replay.replay_rows(self.rows, rate=0, wait=wait)
        wait.assert_not_called()

    def test_invalid_rows_are_skipped_without_text_in_logs_or_truncation(self):
        invalid = [{"id": uuid4(), "text": text} for text in ("x" * 8001, " \n", "secret\x00text")]
        replay.replay_rows(invalid + self.rows[:1], rate=0)
        self.send.assert_called_once()
        self.assertIn("Selected: 4; acknowledged: 1; skipped: 3", self.out.getvalue())
        self.assertNotIn("secret", self.err.getvalue())

    def test_empty_or_all_invalid_selection_fails_without_creating_producer(self):
        self.assertEqual(replay.replay_rows([], rate=0), 1)
        self.assertEqual(replay.replay_rows([{"id": uuid4(), "text": " "}], rate=0), 1)
        self.producer.assert_not_called()

    def test_delivery_failure_reports_id_and_stops_later_rows(self):
        for error in (KafkaException("down"), BufferError("full")):
            self.send.reset_mock()
            self.send.side_effect = [None, error]
            self.assertEqual(replay.replay_rows(self.rows, rate=0), 1)
            self.assertEqual(self.send.call_count, 2)
            failed = self.send.call_args.args[1]
            self.assertIn(failed["prompt_id"], self.err.getvalue())
            self.assertIn("acknowledged: 1", self.out.getvalue())

    def test_selection_is_parameterized_and_orders_before_applying_limit(self):
        connection = MagicMock()
        connection.__enter__.return_value = connection
        with patch.object(replay.db, "connect", return_value=connection):
            replay.select_rows("unused", "neuralchemy", "test'; DROP TABLE prompts;--", 10, 1, "obfuscation")
        sql, parameters = connection.execute.call_args.args
        self.assertIn("ORDER BY id LIMIT %s", sql)
        self.assertNotIn("DROP TABLE", sql)
        self.assertEqual(parameters, ["neuralchemy", "test'; DROP TABLE prompts;--", 1, "obfuscation", 10])
        self.assertTrue(sql.startswith("SELECT id, text "))

    def test_cli_defaults_and_filters(self):
        with patch.dict(os.environ, {"DATABASE_URL": "fixture"}):
            with patch.object(replay, "select_rows", return_value=self.rows) as select:
                self.assertEqual(replay.main(["--dataset", "neuralchemy", "--split", "test", "--rate", "0"]), 0)
        select.assert_called_once_with("fixture", "neuralchemy", "test", 100, None, None)
        self.assertEqual(self.send.call_args.args[1]["source"], "192.0.2.1")

    def test_invalid_cli_arguments_never_query_or_publish(self):
        for option in (["--rate", "nan"], ["--rate", "inf"], ["--rate", "-1"],
                       ["--limit", "0"], ["--sources", "one,,two"], ["--label", "3"]):
            with self.subTest(option=option):
                with patch.object(replay, "select_rows") as select:
                    with self.assertRaises(SystemExit) as exit:
                        replay.main(["--dataset", "deepset", "--split", "external_test", *option])
                self.assertEqual(exit.exception.code, 2)
                select.assert_not_called()
        self.send.assert_not_called()

    def test_database_outage_fails_without_publishing(self):
        with patch.dict(os.environ, {"DATABASE_URL": "fixture"}):
            with patch.object(replay, "select_rows", side_effect=psycopg.OperationalError("private text")):
                self.assertEqual(replay.main(["--dataset", "deepset", "--split", "external_test"]), 1)
        self.producer.assert_not_called()
        self.assertNotIn("private text", self.err.getvalue())


@unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL for replay selection checks")
class ReplayStorageTests(unittest.TestCase):
    def setUp(self):
        self.url = os.environ["TEST_DATABASE_URL"]
        self.split = "replay-fixture-" + str(uuid4())
        self.ids = [uuid4() for _ in range(4)]
        db.init_db(self.url)
        with db.connect(self.url) as conn:
            for record_id, label, group in zip(self.ids, (0, 1, 1, 1), (None, "obfuscation", "obfuscation", "other")):
                conn.execute(
                    """INSERT INTO dataset_prompts (id, text, label, source_dataset, split, attack_group)
                       VALUES (%s, %s, %s, 'neuralchemy', %s, %s)""",
                    (record_id, "x" * 8001 if record_id == self.ids[2] else "  Cafe\u0301!\n", label, self.split, group),
                )

    def tearDown(self):
        with db.connect(self.url) as conn:
            conn.execute("DELETE FROM prompts WHERE dataset_prompt_id = ANY(%s)", (self.ids,))
            conn.execute("DELETE FROM dataset_prompts WHERE id = ANY(%s)", (self.ids,))

    def test_real_selection_filters_and_limit_do_not_prevalidate_text(self):
        rows = replay.select_rows(self.url, "neuralchemy", self.split, 100, 1, "obfuscation")
        self.assertEqual([row["id"] for row in rows], sorted(self.ids[1:3]))
        limited = replay.select_rows(self.url, "neuralchemy", self.split, 1)
        self.assertEqual([row["id"] for row in limited], [min(self.ids)])
        self.assertEqual(replay.select_rows(self.url, "deepset", self.split, 100), [])

    @unittest.skipUnless(os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS"), "Set TEST_KAFKA_BOOTSTRAP_SERVERS for full replay")
    def test_replay_through_kafka_and_consumer_stores_preprocessing(self):
        from confluent_kafka import Consumer, TopicPartition
        from backend.intake.consumer import process_message
        from backend.intake.topics import PROMPTS_RAW
        from backend.scripts.create_topics import create_topics
        from backend.stream.enrichment import SourceContextCache

        servers = os.environ["TEST_KAFKA_BOOTSTRAP_SERVERS"]
        create_topics(servers)
        consumer = Consumer({"bootstrap.servers": servers, "group.id": "replay-test-" + str(uuid4()), "enable.auto.commit": False})
        partition = TopicPartition(PROMPTS_RAW, 0)
        _, high = consumer.get_watermark_offsets(partition, timeout=10)
        consumer.assign([TopicPartition(PROMPTS_RAW, 0, high)])
        source = "replay-source-" + str(uuid4())
        history = db.save_prompt(self.url, "Historical fixture", source=source)
        db.save_guard_result(self.url, history["id"], "block")
        try:
            with patch.dict(os.environ, {"DATABASE_URL": self.url, "KAFKA_BOOTSTRAP_SERVERS": servers}):
                with redirect_stdout(io.StringIO()), redirect_stderr(io.StringIO()):
                    self.assertEqual(replay.main([
                        "--dataset", "neuralchemy", "--split", self.split, "--label", "1",
                        "--rate", "0", "--sources", source,
                    ]), 0)
            cache = SourceContextCache(self.url)
            for _ in range(2):
                message = consumer.poll(10)
                self.assertIsNotNone(message)
                self.assertIsNone(message.error())
                event = json.loads(message.value())
                self.assertEqual(message.key().decode(), source)
                self.assertNotIn("label", event)
                process_message(consumer, message, self.url, cache)
                with db.connect(self.url) as conn:
                    row = conn.execute("SELECT * FROM prompts WHERE id = %s", (event["prompt_id"],)).fetchone()
                self.assertEqual(row["text"], event["text"])
                self.assertEqual(row["preprocessing"]["cleaned_text"], "Café!")
                self.assertEqual(row["preprocessing"]["source_context"]["block_count"], 1)
                self.assertEqual(db.get_guard_result(self.url, event["prompt_id"])["decision"], "pending")
                process_message(consumer, message, self.url, cache)
                with db.connect(self.url) as conn:
                    count = conn.execute(
                        """SELECT count(*) AS n FROM prompts p JOIN guard_results g ON g.prompt_id = p.id
                           WHERE p.id = %s""", (event["prompt_id"],),
                    ).fetchone()["n"]
                self.assertEqual(count, 1)
        finally:
            consumer.close()
            with db.connect(self.url) as conn:
                conn.execute("DELETE FROM prompts WHERE id = %s", (history["id"],))

    @unittest.skipUnless(os.getenv("TEST_KAFKA_BOOTSTRAP_SERVERS"), "Set TEST_KAFKA_BOOTSTRAP_SERVERS for browser intake")
    def test_browser_submission_stores_snapshot_without_changing_response(self):
        from confluent_kafka import Consumer, TopicPartition
        from backend.app import create_app
        from backend.intake.consumer import process_message
        from backend.intake.topics import PROMPTS_RAW
        from backend.scripts.create_topics import create_topics

        servers = os.environ["TEST_KAFKA_BOOTSTRAP_SERVERS"]
        create_topics(servers)
        consumer = Consumer({"bootstrap.servers": servers, "group.id": "browser-test-" + str(uuid4()), "enable.auto.commit": False})
        _, high = consumer.get_watermark_offsets(TopicPartition(PROMPTS_RAW, 0), timeout=10)
        consumer.assign([TopicPartition(PROMPTS_RAW, 0, high)])
        prompt_id = uuid4()
        text = "  Browser\tCafe\u0301!\n"
        app = create_app({"TESTING": True, "DATABASE_URL": self.url, "KAFKA_BOOTSTRAP_SERVERS": servers})
        try:
            with app.test_client() as client:
                response = client.post("/api/prompts", json={"text": text, "prompt_id": str(prompt_id)})
                self.assertEqual(response.status_code, 202)
                self.assertEqual(response.json["status"], "queued")
                message = consumer.poll(10)
                self.assertIsNotNone(message)
                self.assertIsNone(message.error())
                process_message(consumer, message, self.url)
                stored = client.get(f"/api/prompts/{prompt_id}").json
            self.assertEqual(stored["status"], "stored")
            self.assertEqual(stored["prompt"]["text"], text)
            self.assertEqual(stored["prompt"]["decision"], "pending")
            self.assertNotIn("preprocessing", stored["prompt"])
            with db.connect(self.url) as conn:
                snapshot = conn.execute("SELECT preprocessing FROM prompts WHERE id = %s", (prompt_id,)).fetchone()["preprocessing"]
            self.assertEqual(snapshot["cleaned_text"], "Browser Café!")
        finally:
            consumer.close()
            with db.connect(self.url) as conn:
                conn.execute("DELETE FROM prompts WHERE id = %s", (prompt_id,))


if __name__ == "__main__":
    unittest.main()
