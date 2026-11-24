import csv
import tempfile
import unittest
from pathlib import Path
from unittest.mock import Mock, patch
from uuid import uuid4

import numpy as np

from backend.batch import export_review_queue, train_batch4


class Batch4ReviewTests(unittest.TestCase):
    def test_export_deduplicates_normalized_text_and_splits_each_label(self):
        rows = []
        for label in (0, 1):
            for index in range(3):
                rows.append({"id": uuid4(), "text": f"Label {label} prompt {index}",
                             "label": label, "reviewer": "reviewer"})
        rows.append({"id": uuid4(), "text": "  LABEL 0 PROMPT 0 ", "label": 0,
                     "reviewer": "reviewer"})
        connection = Mock()
        connection.__enter__ = Mock(return_value=connection)
        connection.__exit__ = Mock(return_value=False)
        connection.execute.side_effect = [Mock(fetchall=Mock(return_value=rows)),
                                          Mock(fetchall=Mock(return_value=[]))]
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(export_review_queue.db, "connect", return_value=connection):
            destination = Path(directory) / "reviewed.csv"
            count = export_review_queue.export(destination)
            with destination.open(newline="", encoding="utf-8") as file:
                exported = list(csv.DictReader(file))
        self.assertEqual(count, 6)
        self.assertEqual(len(exported), 6)
        for label in ("0", "1"):
            splits = {row["split"] for row in exported if row["label"] == label}
            self.assertEqual(splits, {"train", "validation"})

    def test_export_fails_clearly_for_small_or_conflicting_label_sets(self):
        rows = [{"id": uuid4(), "text": "only benign", "label": 0, "reviewer": "r"}]
        connection = Mock()
        connection.__enter__ = Mock(return_value=connection)
        connection.__exit__ = Mock(return_value=False)
        connection.execute.side_effect = [Mock(fetchall=Mock(return_value=rows)),
                                          Mock(fetchall=Mock(return_value=[]))]
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(export_review_queue.db, "connect", return_value=connection):
            with self.assertRaisesRegex(ValueError, "at least two eligible"):
                export_review_queue.export(Path(directory) / "reviewed.csv")

    def test_export_rejects_conflicting_labels_for_duplicate_text(self):
        text = "same normalized prompt"
        rows = [{"id": uuid4(), "text": text, "label": 0, "reviewer": "r"},
                {"id": uuid4(), "text": text.upper(), "label": 1, "reviewer": "r"}]
        connection = Mock()
        connection.__enter__ = Mock(return_value=connection)
        connection.__exit__ = Mock(return_value=False)
        connection.execute.side_effect = [Mock(fetchall=Mock(return_value=rows)),
                                          Mock(fetchall=Mock(return_value=[]))]
        with tempfile.TemporaryDirectory() as directory, \
                patch.object(export_review_queue.db, "connect", return_value=connection):
            with self.assertRaisesRegex(ValueError, "conflicting labels"):
                export_review_queue.export(Path(directory) / "reviewed.csv")

    def test_csv_loader_deduplicates_same_text_within_a_split(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reviewed.csv"
            path.write_text(
                "prompt_id,text,label,split\n"
                "id1,benign one,0,train\n"
                "id2,malicious one,1,train\n"
                "id3,benign two,0,validation\n"
                "id4,malicious two,1,validation\n"
                "id5, BENIGN ONE ,0,train\n",
                encoding="utf-8",
            )
            rows = train_batch4.load_reviewed_csv(path)
        self.assertEqual(len(rows), 4)

    def test_csv_loader_rejects_duplicate_text_across_splits(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "reviewed.csv"
            path.write_text(
                "text,label,split\n"
                "benign one,0,train\n"
                "malicious one,1,train\n"
                "benign two,0,validation\n"
                "malicious two,1,validation\n"
                " BENIGN ONE ,0,validation\n",
                encoding="utf-8",
            )
            with self.assertRaisesRegex(ValueError, "across labels or splits"):
                train_batch4.load_reviewed_csv(path)

    def test_queue_export_is_verified_against_review_state_and_text(self):
        prompt_id = str(uuid4())
        record = {"id": prompt_id, "text": "reviewed text", "label": 1,
                  "reason": "score_in_review_band"}
        connection = Mock()
        connection.__enter__ = Mock(return_value=connection)
        connection.__exit__ = Mock(return_value=False)
        connection.execute.return_value.fetchall.return_value = [record]
        rows = [{"prompt_id": prompt_id, "text": "reviewed text", "label": 1}]
        with patch.object(train_batch4.db, "connect", return_value=connection):
            train_batch4.verify_queue_export("postgresql://unused", rows)
            with self.assertRaisesRegex(ValueError, "no longer matches"):
                train_batch4.verify_queue_export(
                    "postgresql://unused", [{**rows[0], "text": "changed text"}]
                )

    def test_p7_route_counts_use_inclusive_allow_and_block_thresholds(self):
        classifier = Mock()
        classifier.classes_ = [0, 1]
        classifier.predict_proba.return_value = np.array([
            [0.70, 0.30], [0.50, 0.50], [0.30, 0.70]
        ])
        counts = train_batch4.p7_route_counts(classifier, [1, 2, 3], 0.30, 0.70)
        self.assertEqual(counts, {"allow": 1, "review": 1, "block": 1})

    def test_regression_gate_uses_p4_token_coverage(self):
        encoder = Mock()
        encoder.max_seq_length = 256
        encoder.tokenizer.return_value = {"length": [120, 257, 256]}
        rows = [{"text": "short benign", "label": 0},
                {"text": "long malicious", "label": 1},
                {"text": "boundary malicious", "label": 1}]
        covered, excluded = train_batch4.p4_covered_rows(rows, encoder)
        self.assertEqual([row["text"] for row in covered], ["short benign", "boundary malicious"])
        self.assertEqual(excluded, 1)


if __name__ == "__main__":
    unittest.main()
