"""Export labeled prompts that P7 routed to review as a Batch 4 CSV."""

import argparse
import csv
import hashlib
import os
import unicodedata
from pathlib import Path

from dotenv import load_dotenv

from backend import db

ROOT = Path(__file__).resolve().parents[2]


def export(path):
    load_dotenv(ROOT / ".env")
    database_url = os.getenv("DATABASE_URL", "postgresql://localhost:5432/pijd")
    with db.connect(database_url) as conn:
        rows = conn.execute(
            """SELECT p.id, p.text, r.label, r.reviewer, g.reason
               FROM prompt_reviews r
               JOIN prompts p ON p.id = r.prompt_id
               JOIN guard_results g ON g.prompt_id = p.id
               WHERE g.decision = 'review'
                 AND g.reason = 'score_in_review_band'
               ORDER BY p.stored_at, p.id"""
        ).fetchall()
        dataset_rows = conn.execute("SELECT text FROM dataset_prompts").fetchall()
    dataset_texts = {
        unicodedata.normalize("NFKC", " ".join(row["text"].split())).casefold()
        for row in dataset_rows
    }
    before_dataset_filter = len(rows)
    rows = [row for row in rows
            if unicodedata.normalize("NFKC", " ".join(row["text"].split())).casefold() not in dataset_texts]
    excluded_dataset_overlap = before_dataset_filter - len(rows)
    # Exclude duplicate/replayed text before splitting. Conflicting human labels need adjudication.
    unique = {}
    for row in rows:
        canonical = unicodedata.normalize("NFKC", " ".join(row["text"].split())).casefold()
        if canonical in unique:
            previous = unique[canonical]
            if previous["label"] != row["label"]:
                raise ValueError("Duplicate normalized prompt text has conflicting labels; resolve it before exporting.")
            continue
        row["text_hash"] = hashlib.sha256(canonical.encode("utf-8")).hexdigest()
        unique[canonical] = row
    rows = list(unique.values())
    by_label = {label: [row for row in rows if row["label"] == label] for label in (0, 1)}
    if any(len(by_label[label]) < 2 for label in (0, 1)):
        counts = {label: len(by_label[label]) for label in (0, 1)}
        raise ValueError(f"Need at least two eligible, unique P7 score-band prompts per class; found {counts}.")

    # Deterministic stratified split. Keep at least one example of each label in both partitions.
    split_by_id = {}
    for label, examples in by_label.items():
        validation_count = max(1, round(len(examples) * 0.25))
        validation_count = min(validation_count, len(examples) - 1)
        examples.sort(key=lambda row: row["text_hash"])
        for row in examples[:-validation_count]:
            split_by_id[row["id"]] = "train"
        for row in examples[-validation_count:]:
            split_by_id[row["id"]] = "validation"

    output = Path(path)
    output.parent.mkdir(parents=True, exist_ok=True)
    with output.open("w", newline="", encoding="utf-8") as file:
        writer = csv.DictWriter(file, fieldnames=("prompt_id", "text", "label", "split", "reviewer"))
        writer.writeheader()
        for row in rows:
            writer.writerow({"prompt_id": row["id"], "text": row["text"], "label": row["label"],
                             "split": split_by_id[row["id"]], "reviewer": row["reviewer"] or ""})
    print(f"Exported {len(rows)} labeled P7 review prompts to {output}; "
          f"excluded {excluded_dataset_overlap} exact normalized overlaps with known dataset rows.")
    return len(rows)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path, help="Destination CSV path")
    args = parser.parse_args()
    try:
        export(args.output)
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
