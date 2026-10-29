
"""Prepare and import the NeurAlchemy dataset into PostgreSQL"""

import json
import os
import sys
from pathlib import Path
from uuid import NAMESPACE_URL, uuid5

import pandas as pd
from datasets import load_dataset
from dotenv import load_dotenv

# Use the team's existing PostgreSQL connection helper
BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from db import connect


# Group original attack categories into six broader categories (this will be used for batch 3)
category_mapping = {
    "direct_injection": "instruction_manipulation",
    "instruction_override": "instruction_manipulation",
    "prompt_injection": "instruction_manipulation",
    "system_manipulation": "instruction_manipulation",
    "context_confusion": "instruction_manipulation",
    "response_manipulation": "instruction_manipulation",
    "output_manipulation": "instruction_manipulation",

    "jailbreak": "jailbreak_persona",
    "persona_replacement": "jailbreak_persona",
    "crescendo": "jailbreak_persona",
    "many_shot": "jailbreak_persona",
    "multi_turn": "jailbreak_persona",

    "encoding": "obfuscation",
    "encoding_obfuscation": "obfuscation",
    "token_smuggling": "obfuscation",
    "token_injection": "obfuscation",

    "system_extraction": "information_extraction",
    "training_extraction": "information_extraction",
    "prompt_extraction": "information_extraction",
    "prompt_leak": "information_extraction",
    "model_fingerprinting": "information_extraction",

    "indirect_injection": "indirect_agent_manipulation",
    "agent_manipulation": "indirect_agent_manipulation",
    "rag_poisoning": "indirect_agent_manipulation",
    "payload_injection": "indirect_agent_manipulation",
    "code_execution": "indirect_agent_manipulation",

    "adversarial": "adversarial_attacks",
}

excluded_categories = {"control", "chain_of_thought"} # as per the cleaning instructions in the technical decisions sheet of the work plan


def prepare_dataset():
    """Download and prepare the dataset for database storage."""

    dataset = load_dataset(
        "neuralchemy/Prompt-injection-dataset",
        "core",
    )

    # Combine splits and retain original row positions
    frames = []

    for split_name, split in dataset.items():
        frame = split.to_pandas()
        frame["split"] = split_name
        frame["source_row_index"] = range(len(frame))
        frames.append(frame)

    df = pd.concat(frames, ignore_index=True)

    # Assign attack groups to malicious prompts
    df["attack_group"] = df["category"].map(category_mapping)
    df.loc[df["label"] == 0, "attack_group"] = None

    # Flag records selected for baseline modelling
    df["include_in_baseline"] = (
        df["source"].eq("neuralchemy_v1")
        & ~df["category"].isin(excluded_categories)
    )

    df["source_dataset"] = "neuralchemy"

    return df


def import_to_postgres(df, database_url):
    """Insert dataset records into PostgreSQL."""

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

    for _, row in df.iterrows():
        # Generate a stable ID for each dataset record
        record_id = uuid5(
            NAMESPACE_URL,
            f"neuralchemy/core/{row['split']}/{row['source_row_index']}",
        )

        metadata = {
            "severity": row.get("severity"),
            "augmented": row.get("augmented"),
            "tags": row.get("tags"),
        }

        metadata_json = json.dumps(
            metadata,
            default=lambda value: (
                value.tolist()
                if hasattr(value, "tolist")
                else str(value)
            ),
        )

        records.append((
            record_id,
            row["text"],
            int(row["label"]),
            row["source_dataset"],
            row["source"],
            row["split"],
            row["category"],
            row["attack_group"] if pd.notna(row["attack_group"]) else None,
            str(row["group_id"]) if pd.notna(row["group_id"]) else None,
            bool(row["include_in_baseline"]),
            metadata_json,
        ))

    # Insert records in a single transaction
    with connect(database_url) as conn:
        with conn.cursor() as cur:
            cur.executemany(insert_sql, records)

    print(f"Processed {len(records)} dataset records.")


def main():
    # .env lives in the project root, as for Flask
    load_dotenv(BACKEND_DIR.parent / ".env")
    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")

    df = prepare_dataset()

    print(f"Total records: {len(df)}")
    print(f"Baseline records: {df['include_in_baseline'].sum()}")

    import_to_postgres(df, database_url)


if __name__ == "__main__":
    main()
