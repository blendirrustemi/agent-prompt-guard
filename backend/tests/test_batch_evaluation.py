"""Offline unit checks for batch evaluation metrics and inference wrappers."""

import importlib.util
import unittest
from unittest.mock import patch

HAS_EVAL_DEPS = all(importlib.util.find_spec(name) for name in ("numpy", "sklearn", "dotenv"))


@unittest.skipUnless(HAS_EVAL_DEPS, "Install the batch environment to run evaluator checks")
class BatchEvaluationTests(unittest.TestCase):
    def setUp(self):
        from backend.batch import evaluation
        self.evaluation = evaluation

    def test_metrics_use_malicious_as_positive_and_compute_false_positive_rate(self):
        result = self.evaluation._metrics([0, 0, 1, 1], [0, 1, 1, 0], [0.1, 0.8, 0.9, 0.2])
        self.assertEqual(result["confusion_matrix"], {"tn": 1, "fp": 1, "fn": 1, "tp": 1})
        self.assertEqual(result["false_positive_rate"], 0.5)
        self.assertEqual(result["precision_malicious"], 0.5)
        self.assertEqual(result["recall_malicious"], 0.5)
        self.assertEqual(result["roc_auc"], 0.75)

    def test_b1_measures_transform_and_prediction_per_prompt(self):
        import numpy as np

        class Vectorizer:
            def transform(self, texts):
                return np.asarray([[len(text)] for text in texts])

        class Classifier:
            classes_ = np.asarray([0, 1])

            def predict(self, features):
                return (features[:, 0] > 3).astype(int)

            def predict_proba(self, features):
                score = np.clip(features[:, 0] / 10, 0, 1)
                return np.column_stack([1 - score, score])

        samples = [{"text": "one", "label": 0}, {"text": "malicious", "label": 1}]
        with patch.object(self.evaluation.time, "perf_counter", side_effect=[0, .001, .002, .004]):
            result = self.evaluation.evaluate_b1(samples, Vectorizer(), Classifier())
        self.assertEqual(result["n"], 2)
        self.assertEqual(result["latency_ms"]["mean"], 1.5)

    def test_external_rows_are_filtered_and_grouped_by_source(self):
        rows = [
            {"id": "d0", "text": "benign", "label": 0, "source_dataset": "deepset"},
            {"id": "d1", "text": "attack", "label": 1, "source_dataset": "deepset"},
            {"id": "j0", "text": "benign", "label": 0, "source_dataset": "jackhhao"},
            {"id": "j1", "text": "attack", "label": 1, "source_dataset": "jackhhao"},
        ]

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, *_args):
                class Cursor:
                    def fetchall(self):
                        return rows
                return Cursor()

        with patch.object(self.evaluation, "connect", return_value=Connection()):
            datasets = self.evaluation.load_external_data("unused")
        self.assertEqual([row["id"] for row in datasets["deepset"]], ["d0", "d1"])
        self.assertEqual([row["id"] for row in datasets["jackhhao"]], ["j0", "j1"])


if __name__ == "__main__":
    unittest.main()
