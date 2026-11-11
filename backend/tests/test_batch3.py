"""Offline checks for B3 row selection, embeddings, metrics, and saved metadata."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

HAS_BATCH_DEPS = all(importlib.util.find_spec(name) for name in ("pandas", "numpy", "sklearn", "dotenv"))


@unittest.skipUnless(HAS_BATCH_DEPS, "Install the batch environment to run B3 checks")
class Batch3Tests(unittest.TestCase):
    def setUp(self):
        from backend.batch import train_batch3
        self.b3 = train_batch3

    def test_loader_queries_only_mapped_malicious_neuralchemy_rows(self):
        rows = [
            {"id": str(index), "text": f"attack {index}", "attack_group": label, "split": split}
            for split in self.b3.SPLITS
            for index, label in enumerate(self.b3.CLASS_ORDER)
        ]

        class Connection:
            def __enter__(self):
                return self

            def __exit__(self, *args):
                return False

            def execute(self, query, parameters):
                self.query = query
                self.parameters = parameters

                class Cursor:
                    def fetchall(self):
                        return rows
                return Cursor()

        connection = Connection()
        with patch.object(self.b3, "connect", return_value=connection):
            frame = self.b3.load_attack_data("unused")
        self.assertIn("source_dataset = 'neuralchemy'", connection.query)
        self.assertIn("label = 1", connection.query)
        self.assertIn("attack_group = ANY(%s)", connection.query)
        self.assertEqual(connection.parameters, (list(self.b3.CLASS_ORDER),))
        self.assertEqual(set(frame["split"]), set(self.b3.SPLITS))
        self.assertEqual(set(frame["attack_group"]), set(self.b3.CLASS_ORDER))

    def test_embeddings_stay_aligned_to_split_labels(self):
        import numpy as np
        import pandas as pd

        class Tokenizer:
            def __call__(self, texts, **_kwargs):
                return {"length": [len(text.split()) for text in texts]}

        class Encoder:
            tokenizer = Tokenizer()
            max_seq_length = 256

            def encode(self, texts, **_kwargs):
                return np.asarray([[len(text)] for text in texts], dtype=float)

        frame = pd.DataFrame([
            {"text": f"{split} {label}", "attack_group": label, "split": split}
            for split in self.b3.SPLITS
            for label in self.b3.CLASS_ORDER
        ])
        encoded = self.b3.create_embeddings(frame, Encoder())
        for split in self.b3.SPLITS:
            expected = frame.loc[frame["split"] == split, "attack_group"].tolist()
            self.assertEqual(len(encoded[split]["X"]), len(expected))
            self.assertEqual(encoded[split]["y"].tolist(), expected)

    def test_classifier_uses_balanced_weights(self):
        import numpy as np

        classifier = self.b3.train_classifier({
            "X": np.repeat(np.eye(len(self.b3.CLASS_ORDER)), 2, axis=0),
            "y": np.asarray([label for label in self.b3.CLASS_ORDER for _ in range(2)]),
        })
        self.assertEqual(classifier.class_weight, "balanced")
        self.assertEqual(set(classifier.classes_), set(self.b3.CLASS_ORDER))

    def test_metrics_include_each_class_and_ordered_confusion_matrix(self):
        import numpy as np

        labels = np.asarray(self.b3.CLASS_ORDER)
        predictions = labels.copy()
        predictions[0] = labels[1]  # misclassify one rare class

        class Classifier:
            def predict(self, _features):
                return predictions

        result = self.b3.evaluate_classifier(
            Classifier(), {"X": np.zeros((len(labels), 1)), "y": labels}, "Fixture"
        )
        self.assertEqual(result["n"], len(self.b3.CLASS_ORDER))
        self.assertEqual(set(result["per_class"]), set(self.b3.CLASS_ORDER))
        self.assertEqual(result["confusion_matrix"]["labels"], list(self.b3.CLASS_ORDER))
        self.assertEqual(sum(map(sum, result["confusion_matrix"]["counts"])), len(labels))
        self.assertEqual(result["per_class"][self.b3.CLASS_ORDER[0]]["recall"], 0.0)

    def test_saved_config_records_classes_splits_and_metrics(self):
        import numpy as np
        from sklearn.linear_model import LogisticRegression

        classifier = LogisticRegression(class_weight="balanced", max_iter=1000).fit(
            np.repeat(np.eye(len(self.b3.CLASS_ORDER)), 2, axis=0),
            np.asarray([label for label in self.b3.CLASS_ORDER for _ in range(2)]),
        )
        metrics = {"macro_f1": 0.5, "per_class": {}, "confusion_matrix": {}}

        class Encoder:
            max_seq_length = 256

            def get_embedding_dimension(self):
                return 384

        with tempfile.TemporaryDirectory() as directory:
            self.b3.save_model(
                classifier, Encoder(), {"train": {"count": 12}}, metrics, metrics, directory
            )
            config = json.loads((Path(directory) / "config.json").read_text())
        self.assertEqual(config["class_order"], sorted(self.b3.CLASS_ORDER))
        self.assertEqual(config["training_data"]["split_counts"], {"train": {"count": 12}})
        self.assertEqual(config["classifier_parameters"]["class_weight"], "balanced")
        self.assertEqual(config["validation_macro_f1"], 0.5)
        self.assertEqual(config["test_macro_f1"], 0.5)


if __name__ == "__main__":
    unittest.main()
