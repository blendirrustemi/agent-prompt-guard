"""Small local ETL fixtures; no dataset downloads. Run in the batch environment."""

import importlib.util
import os
import unittest
from unittest.mock import MagicMock, patch
from uuid import NAMESPACE_URL, uuid5

HAS_BATCH_DEPS = all(importlib.util.find_spec(name) for name in ("pandas", "datasets"))


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
