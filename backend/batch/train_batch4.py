"""Train and conditionally promote a binary classifier from reviewed prompt labels.

Input CSV columns: text,label,split where label is 0 (benign) or 1 (malicious),
and split is train or validation. Validation rows are used only for comparison.
"""

import argparse
import csv
import json
import os
import pickle
import shutil
import sys
import tempfile
import unicodedata
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path

import numpy as np
from dotenv import load_dotenv
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix, f1_score

BACKEND_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = BACKEND_DIR.parent
MODEL_DIR = BACKEND_DIR / "batch" / "models"
CURRENT_DIR = MODEL_DIR / "batch2"
RUNS_DIR = MODEL_DIR / "batch4_runs"
sys.path.insert(0, str(BACKEND_DIR))

ENCODER_NAME = "sentence-transformers/all-MiniLM-L6-v2"
MAX_SEQ_LENGTH = 256
BATCH_SIZE = 32


def load_reviewed_csv(path):
    """Load and validate manually labelled training and validation examples."""
    rows = []
    seen_text = {}
    with Path(path).open(newline="", encoding="utf-8-sig") as file:
        reader = csv.DictReader(file)
        required = {"text", "label", "split"}
        if not reader.fieldnames or not required.issubset(reader.fieldnames):
            raise ValueError("CSV must contain text,label,split columns.")
        for line, row in enumerate(reader, start=2):
            text = row["text"]
            if not text or not text.strip() or len(text) > 8000:
                raise ValueError(f"CSV row {line} has blank text or exceeds 8,000 characters.")
            try:
                label = int(row["label"])
            except (TypeError, ValueError):
                raise ValueError(f"CSV row {line} label must be 0 or 1.") from None
            split = row["split"].strip().lower()
            if label not in (0, 1) or split not in ("train", "validation"):
                raise ValueError(f"CSV row {line} must use label 0/1 and split train/validation.")
            normalized_text = unicodedata.normalize("NFKC", " ".join(text.split())).casefold()
            if normalized_text in seen_text:
                previous_line = seen_text[normalized_text]
                raise ValueError(
                    f"CSV rows {previous_line} and {line} contain duplicate prompt text; "
                    "keep each prompt in only one split."
                )
            seen_text[normalized_text] = line
            rows.append({"text": text, "label": label, "split": split})
    if not rows:
        raise ValueError("The reviewed-label CSV is empty.")
    for split in ("train", "validation"):
        part = [row for row in rows if row["split"] == split]
        if {row["label"] for row in part} != {0, 1}:
            raise ValueError(f"Reviewed {split} rows must include both labels 0 and 1.")
    return rows


def load_encoder(config):
    if (config["encoder_name"] != ENCODER_NAME
            or int(config["max_seq_length"]) != MAX_SEQ_LENGTH
            or config["normalize_embeddings"] is not True
            or int(config["embedding_dimension"]) != 384):
        raise ValueError("Current P4 model does not use the expected MiniLM embedding setup.")
    from sentence_transformers import SentenceTransformer

    encoder = SentenceTransformer(config["encoder_name"], device="cpu")
    encoder.max_seq_length = int(config["max_seq_length"])
    encoder.requires_grad_(False)
    encoder.eval()
    return encoder


def encode_rows(rows, encoder):
    vectors = []
    for start in range(0, len(rows), BATCH_SIZE):
        texts = [row["text"] for row in rows[start:start + BATCH_SIZE]]
        vectors.append(encoder.encode(
            texts, batch_size=BATCH_SIZE, show_progress_bar=False,
            convert_to_numpy=True, normalize_embeddings=True,
        ))
    matrix = np.concatenate(vectors, axis=0)
    if matrix.shape != (len(rows), 384) or not np.isfinite(matrix).all():
        raise ValueError("Encoder returned invalid reviewed-prompt embeddings.")
    return matrix


def metrics(classifier, features, labels):
    predictions = classifier.predict(features)
    matrix = confusion_matrix(labels, predictions, labels=[0, 1])
    return {
        "n": int(len(labels)),
        "macro_f1": float(f1_score(labels, predictions, labels=[0, 1], average="macro", zero_division=0)),
        "confusion_matrix": {
            "labels": ["benign", "malicious"],
            "counts": matrix.astype(int).tolist(),
        },
    }


def atomic_write(path, writer):
    path = Path(path)
    fd, temporary = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    os.close(fd)
    try:
        writer(Path(temporary))
        os.replace(temporary, path)
    finally:
        if os.path.exists(temporary):
            os.unlink(temporary)


def run_batch4(csv_path, *, promote=True):
    rows = load_reviewed_csv(csv_path)
    train = [row for row in rows if row["split"] == "train"]
    validation = [row for row in rows if row["split"] == "validation"]
    config_path = CURRENT_DIR / "config.json"
    classifier_path = CURRENT_DIR / "classifier.pkl"
    current_config = json.loads(config_path.read_text(encoding="utf-8"))
    with classifier_path.open("rb") as file:
        current = pickle.load(file)
    encoder = load_encoder(current_config)

    print(f"Encoding {len(train)} reviewed training prompts and {len(validation)} validation prompts...")
    train_x = encode_rows(train, encoder)
    validation_x = encode_rows(validation, encoder)
    train_y = np.asarray([row["label"] for row in train], dtype=int)
    validation_y = np.asarray([row["label"] for row in validation], dtype=int)

    candidate = LogisticRegression(
        C=1.0, solver="lbfgs", max_iter=1000, random_state=42,
        class_weight=current_config.get("classifier_parameters", {}).get("class_weight"),
    )
    candidate.fit(train_x, train_y)
    candidate_train_metrics = metrics(candidate, train_x, train_y)
    current_metrics = metrics(current, validation_x, validation_y)
    candidate_metrics = metrics(candidate, validation_x, validation_y)
    accepted = candidate_metrics["macro_f1"] > current_metrics["macro_f1"]

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    report = {
        "batch": 4,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "training_data": {
            "source": "reviewed CSV",
            "train_count": len(train),
            "validation_count": len(validation),
            "label_counts": {
                split: {str(label): sum(row["label"] == label for row in rows if row["split"] == split)
                        for label in (0, 1)}
                for split in ("train", "validation")
            },
        },
        "selection_metric": "validation macro-F1; candidate must strictly exceed current model",
        "candidate_train": candidate_train_metrics,
        "current_validation": current_metrics,
        "candidate_validation": candidate_metrics,
        "promoted": bool(accepted and promote),
    }
    (run_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")

    if accepted and promote:
        # Preserve recoverable artifacts before replacing the active P4 classifier.
        shutil.copy2(classifier_path, run_dir / "previous_classifier.pkl")
        shutil.copy2(config_path, run_dir / "previous_config.json")
        new_config = dict(current_config)
        new_config.update({
            "classifier_parameters": candidate.get_params(),
            "train_macro_f1": candidate_train_metrics["macro_f1"],
            # This candidate has not been evaluated on the original untouched test set.
            "neuralchemy_test_macro_f1": None,
            "training_data": {
                "source": "reviewed CSV",
                "train_count": len(train),
                "validation_count": len(validation),
                "selection_metric": report["selection_metric"],
                "validation_macro_f1": candidate_metrics["macro_f1"],
                "batch4_run": run_id,
            },
            "validation_macro_f1": candidate_metrics["macro_f1"],
            "package_versions": {
                name: version(name)
                for name in ("scikit-learn", "sentence-transformers", "numpy")
            },
        })
        atomic_write(classifier_path, lambda path: path.write_bytes(pickle.dumps(candidate)))
        atomic_write(config_path, lambda path: path.write_text(json.dumps(new_config, indent=2) + "\n", encoding="utf-8"))
        print("Candidate is better; promoted to the active Batch 2/P4 classifier.")
    elif not accepted:
        print("Candidate did not improve validation macro-F1; current model retained.")
    else:
        print("Candidate is better, but --no-promote was set; active model retained.")

    print(f"Current validation macro-F1:   {current_metrics['macro_f1']:.4f}")
    print(f"Candidate validation macro-F1: {candidate_metrics['macro_f1']:.4f}")
    print(f"Report: {run_dir / 'report.json'}")
    return report


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("csv", type=Path, help="Reviewed labels CSV (text,label,split)")
    parser.add_argument("--no-promote", action="store_true", help="Evaluate without replacing the active model")
    args = parser.parse_args()
    load_dotenv(PROJECT_DIR / ".env")
    run_batch4(args.csv, promote=not args.no_promote)


if __name__ == "__main__":
    main()
