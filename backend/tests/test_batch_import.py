"""Small local ETL fixtures; no dataset downloads. Run in the batch environment."""

import importlib.util
import os
import unittest
from unittest.mock import MagicMock, patch
from uuid import NAMESPACE_URL, uuid4, uuid5

HAS_BATCH_DEPS = all(importlib.util.find_spec(name) for name in ("pandas", "datasets"))


@unittest.skipUnless(HAS_BATCH_DEPS, "Run in the batch environment for dataset fixture checks")
class NeurAlchemyImportTests(unittest.TestCase):
    def setUp(self):
        from backend.batch import import_neuralchemy as etl
        self.etl = etl

    def fixtures(self):
        import pandas as pd
        from datasets import Dataset, DatasetDict

        rows = [
            ("Hello", 0, "benign", "neuralchemy_v1"),
            ("Override instructions", 1, "instruction_override", "neuralchemy_v1"),
            ("Control example", 1, "control", "neuralchemy_v1"),
            ("Reasoning example", 1, "chain_of_thought", "neuralchemy_v1"),
            ("External source", 1, "jailbreak", "original"),
        ]
        frame = pd.DataFrame(rows, columns=["text", "label", "category", "source"])
        frame["group_id"] = [f"fixture-{i}" for i in range(len(frame))]
        frame["severity"] = "low"
        frame["augmented"] = False
        frame["tags"] = [["fixture"] for _ in rows]
        dataset = DatasetDict({
            "train": Dataset.from_pandas(frame, preserve_index=False),
            "validation": Dataset.from_pandas(frame.iloc[:1], preserve_index=False),
            "test": Dataset.from_pandas(frame.iloc[1:2], preserve_index=False),
        })
        with patch.object(self.etl, "load_dataset", return_value=dataset) as load:
            prepared = self.etl.prepare_dataset()
        load.assert_called_once_with("neuralchemy/Prompt-injection-dataset", "core")
        return prepared

    def test_baseline_flags_and_attack_groups(self):
        frame = self.fixtures()
        self.assertEqual(frame["include_in_baseline"].tolist(), [True, True, False, False, False, True, True])
        self.assertIsNone(frame.iloc[0]["attack_group"])
        self.assertEqual(frame.iloc[1]["attack_group"], "instruction_manipulation")
        self.assertEqual(frame.iloc[4]["attack_group"], "jailbreak_persona")
        self.assertEqual(set(frame["source_dataset"]), {"neuralchemy"})
        self.assertEqual(frame["source_row_index"].tolist(), [0, 1, 2, 3, 4, 0, 0])

    def test_storage_preserves_row_identity_and_json_metadata(self):
        import json

        frame = self.fixtures().iloc[[1, 6]]
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value.__enter__.return_value
        with patch.object(self.etl, "connect", return_value=connection):
            self.etl.import_to_postgres(frame, "unused")
        records = cursor.executemany.call_args.args[1]
        self.assertEqual(records[0][0], uuid5(NAMESPACE_URL, "neuralchemy/core/train/1"))
        self.assertEqual(records[1][0], uuid5(NAMESPACE_URL, "neuralchemy/core/test/0"))
        self.assertEqual(json.loads(records[0][-1]), {"severity": "low", "augmented": False, "tags": ["fixture"]})

    @unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL for fixture storage checks")
    def test_fixture_storage_matches_prepared_data_and_is_repeatable(self):
        from backend import db

        url = os.environ["TEST_DATABASE_URL"]
        db.init_db(url)
        frame = self.fixtures()
        # Unique split names keep fixtures separate from real imported data.
        frame["split"] = str(uuid4()) + "-" + frame["split"]
        ids = [uuid5(NAMESPACE_URL, f"neuralchemy/core/{row.split}/{row.source_row_index}") for row in frame.itertuples()]
        try:
            self.etl.import_to_postgres(frame, url)
            self.etl.import_to_postgres(frame, url)
            with db.connect(url) as conn:
                rows = conn.execute("SELECT * FROM dataset_prompts WHERE id = ANY(%s)", (ids,)).fetchall()
            by_id = {row["id"]: row for row in rows}
            self.assertEqual(len(rows), len(frame))
            for record_id, expected in zip(ids, frame.itertuples()):
                stored = by_id[record_id]
                self.assertEqual(stored["text"], expected.text)
                self.assertEqual(stored["label"], expected.label)
                self.assertEqual(stored["include_in_baseline"], expected.include_in_baseline)
                self.assertEqual(stored["metadata"]["tags"], ["fixture"])
        finally:
            with db.connect(url) as conn:
                conn.execute("DELETE FROM dataset_prompts WHERE id = ANY(%s)", (ids,))


@unittest.skipUnless(HAS_BATCH_DEPS, "Run in the batch environment for dataset fixture checks")
class ExternalImportTests(unittest.TestCase):
    def setUp(self):
        from backend.batch import import_external_datasets as etl
        self.etl = etl

    def fixtures(self):
        def load(name, split):
            if name == "deepset/prompt-injections":
                return [{"text": "Shared example", "label": 0}, {"text": "Deepset example", "label": 1}]
            return [{"prompt": "Shared example", "type": "benign"}, {"prompt": "Jackhhao example", "type": "jailbreak"}]
        with patch.object(self.etl, "load_dataset", side_effect=load):
            return self.etl.process_deepset(), self.etl.process_jackhhao()

    def test_columns_and_dataset_names_match_database(self):
        deepset, jackhhao = self.fixtures()
        self.assertEqual(set(deepset["source_dataset"]), {"deepset"})
        self.assertEqual(set(jackhhao["source_dataset"]), {"jackhhao"})
        self.assertEqual(jackhhao["text"].tolist(), ["Shared example", "Jackhhao example"])
        self.assertEqual(jackhhao["label"].tolist(), [0, 1])
        self.assertFalse(jackhhao["include_in_baseline"].any())

    def test_ids_keep_original_row_identity_after_deduplication(self):
        import pandas as pd
        deepset, jackhhao = self.fixtures()
        combined = pd.concat([deepset, jackhhao], ignore_index=True).drop_duplicates("text").reset_index(drop=True)
        connection = MagicMock()
        connection.__enter__.return_value = connection
        cursor = connection.cursor.return_value.__enter__.return_value
        with patch.object(self.etl, "connect", return_value=connection):
            self.etl.import_to_postgres(combined, "unused")
        records = cursor.executemany.call_args.args[1]
        self.assertEqual(len(records), 3)
        self.assertEqual(records[-1][0], uuid5(NAMESPACE_URL, "jackhhao/train/1"))

    @unittest.skipUnless(os.getenv("TEST_DATABASE_URL"), "Set TEST_DATABASE_URL for fixture storage checks")
    def test_fixture_import_accepts_database_constraints_and_is_repeatable(self):
        import pandas as pd
        from backend import db
        url = os.environ["TEST_DATABASE_URL"]
        db.init_db(url)
        frames = self.fixtures()
        combined = pd.concat(frames, ignore_index=True).drop_duplicates("text")
        ids = [uuid5(NAMESPACE_URL, f"{row.source_dataset}/train/{row.source_row_index}") for row in combined.itertuples()]
        try:
            self.etl.import_to_postgres(combined, url)
            self.etl.import_to_postgres(combined, url)
            with db.connect(url) as conn:
                rows = conn.execute("SELECT * FROM dataset_prompts WHERE id = ANY(%s)", (ids,)).fetchall()
            self.assertEqual(len(rows), 3)
            self.assertEqual({row["source_dataset"] for row in rows}, {"deepset", "jackhhao"})
        finally:
            with db.connect(url) as conn:
                conn.execute("DELETE FROM dataset_prompts WHERE id = ANY(%s)", (ids,))


if __name__ == "__main__":
    unittest.main()
