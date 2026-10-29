import json
import os
import sys
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pandas as pd
from datasets import load_dataset
from dotenv import load_dotenv

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from db import connect


def process_deepset():
    print("Fetching deepset/prompt-injections...")
    ds = load_dataset("deepset/prompt-injections", split="train")
    df = pd.DataFrame(ds)

    df['source_dataset'] = 'deepset/prompt-injections'
    df['source'] = 'huggingface'
    df['split'] = 'external_test'
    df['category'] = None
    df['attack_group'] = None
    df['group_id'] = None
    df['include_in_baseline'] = False
    df['metadata'] = df.apply(lambda x: json.dumps({"original_label": x['label']}), axis=1)

    return df


def process_jackhhao():
    print("Fetching jackhhao/jailbreak-classification...")
    ds = load_dataset("jackhhao/jailbreak-classification", split="train")
    df = pd.DataFrame(ds)

    df['label'] = df['type'].map({'benign': 0, 'jailbreak': 1})
    df['source_dataset'] = 'jackhhao/jailbreak-classification'
    df['source'] = 'huggingface'
    df['split'] = 'external_test'
    df['category'] = None
    df['attack_group'] = None
    df['group_id'] = None
    df['include_in_baseline'] = False
    df['metadata'] = df.apply(lambda x: json.dumps({"original_type": x['type']}), axis=1)

    return df


def import_to_postgres(df, database_url):

    insert_sql = """
        INSERT INTO dataset_prompts (
            id, text, label, source_dataset, source,
            split, category, attack_group, group_id,
            include_in_baseline, metadata
        )
        VALUES (
            %s, %s, %s, %s, %s,
            %s, %s, %s, %s, %s, %s
        )
        ON CONFLICT (id) DO NOTHING
    """

    records = []

    # Track the row index for stable UUID generation
    df = df.reset_index(drop=True)

    for idx, row in df.iterrows():
        # Generate a stable ID
        record_id = uuid5(
            NAMESPACE_URL,
            f"{row['source_dataset']}/{row['split']}/{idx}",
        )

        records.append((
            record_id,
            row["text"],
            int(row["label"]),
            row["source_dataset"],
            row["source"],
            row["split"],
            row["category"],
            row["attack_group"],
            row["group_id"],
            bool(row["include_in_baseline"]),
            row["metadata"],
        ))

    # Insert records in a single transaction
    with connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, records)

    print(f"Processed and inserted {len(records)} dataset records.")


def main():
    # Load env variables
    load_dotenv(BACKEND_DIR.parent / ".env")
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")

    # Extract and Transform
    df_deepset = process_deepset()
    df_jackhhao = process_jackhhao()

    # Combine and Deduplicate
    df_combined = pd.concat([df_deepset, df_jackhhao], ignore_index=True)
    initial_count = len(df_combined)

    df_combined.drop_duplicates(subset=['text'], keep='first', inplace=True)
    print(f"Dropped {initial_count - len(df_combined)} duplicate prompts.")

    # Load into PostgreSQL
    import_to_postgres(df_combined, database_url)


if __name__ == "__main__":
    main()