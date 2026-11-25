"""Train and conditionally promote a binary classifier from reviewed prompt labels.

Input CSV is an export from the P7 review queue with prompt_id,text,label,split columns.
Validation rows are used only for an adaptation check and never fit the candidate.
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
from backend import db
from backend.stream.router import validate_policy

BACKEND_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = BACKEND_DIR.parent
MODEL_DIR = BACKEND_DIR / "batch" / "models"
CURRENT_DIR = MODEL_DIR / "batch2"
RUNS_DIR = MODEL_DIR / "batch4_runs"
sys.path.insert(0, str(BACKEND_DIR))

ENCODER_NAME = "sentence-transformers/all-MiniLM-L6-v2"
MAX_SEQ_LENGTH = 256
BATCH_SIZE = 32
REGRESSION_F1_TOLERANCE = 0.0
MIN_REVIEW_VALIDATION_PER_LABEL = 5


def load_reviewed_csv(path):
    """Load, deduplicate, and validate queue-labeled training and validation examples."""
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
                previous = seen_text[normalized_text]
                if previous["label"] != label or previous["split"] != split:
                    raise ValueError(f"CSV rows {previous['line']} and {line} duplicate prompt text across labels or splits.")
                continue
            seen_text[normalized_text] = {"label": label, "split": split, "line": line}
            rows.append({"prompt_id": (row.get("prompt_id") or "").strip(),
                         "reviewer": (row.get("reviewer") or "").strip(),
                         "text": text, "label": label, "split": split})
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


def p4_covered_rows(rows, encoder):
    """Return rows P4 would actually score (at most the configured 256 tokens)."""
    lengths = encoder.tokenizer(
        [row["text"] for row in rows], truncation=False, padding=False,
        return_length=True, verbose=False,
    )["length"]
    covered = [row for row, length in zip(rows, lengths) if length <= encoder.max_seq_length]
    return covered, len(rows) - len(covered)


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


def p7_route_counts(classifier, features, allow_max, block_min):
    """Count allow/review/block outcomes using P7's inclusive score thresholds."""
    probabilities = classifier.predict_proba(features)
    malicious_column = list(classifier.classes_).index(1)
    scores = probabilities[:, malicious_column]
    return {
        "allow": int(np.sum(scores <= allow_max)),
        "review": int(np.sum((scores > allow_max) & (scores < block_min))),
        "block": int(np.sum(scores >= block_min)),
    }


def load_gate_rows(database_url):
    """Load held-out evaluation rows used as Batch 4 promotion regression gates."""
    with db.connect(database_url) as conn:
        rows = conn.execute(
            """SELECT id, text, label, source_dataset
               FROM dataset_prompts
               WHERE (source_dataset = 'neuralchemy' AND split = 'test')
                  OR (source_dataset IN ('deepset', 'jackhhao') AND split = 'external_test')
               ORDER BY source_dataset, id"""
        ).fetchall()
    datasets = {name: [row for row in rows if row["source_dataset"] == name]
                for name in ("neuralchemy", "deepset", "jackhhao")}
    for name, examples in datasets.items():
        if not examples or {row["label"] for row in examples} != {0, 1}:
            raise ValueError(f"Regression gate requires both labels in imported {name} evaluation data.")
    return datasets


def verify_queue_export(database_url, rows):
    """Verify CSV IDs, text, and labels still match eligible human-labeled P7 rows."""
    if not rows or any(not row["prompt_id"] for row in rows):
        raise ValueError("Batch 4 requires a CSV exported from the labeled P7 review queue (with prompt_id values).")
    ids = [row["prompt_id"] for row in rows]
    if len(set(ids)) != len(ids):
        raise ValueError("The review export repeats a prompt_id.")
    with db.connect(database_url) as conn:
        stored = conn.execute(
            """SELECT p.id, p.text, r.label, g.reason
               FROM prompts p JOIN prompt_reviews r ON r.prompt_id = p.id
               JOIN guard_results g ON g.prompt_id = p.id
               WHERE p.id = ANY(%s::uuid[]) AND g.decision = 'review'""",
            (ids,),
        ).fetchall()
    by_id = {str(row["id"]): row for row in stored}
    for row in rows:
        source = by_id.get(row["prompt_id"])
        if (source is None or source["reason"] != "score_in_review_band"
                or source["text"] != row["text"] or int(source["label"]) != row["label"]):
            raise ValueError(f"CSV row for prompt {row['prompt_id']} no longer matches an eligible labeled P7 review record.")


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


def run_batch4(csv_path, *, promote=True, database_url=None):
    rows = load_reviewed_csv(csv_path)
    train = [row for row in rows if row["split"] == "train"]
    validation = [row for row in rows if row["split"] == "validation"]
    validation_counts = {label: sum(row["label"] == label for row in validation) for label in (0, 1)}
    if any(count < MIN_REVIEW_VALIDATION_PER_LABEL for count in validation_counts.values()):
        raise ValueError(
            "Batch 4 requires at least five review-validation prompts per class "
            f"(ten total); found {validation_counts}. Collect more reviewed prompts first."
        )
    config_path = CURRENT_DIR / "config.json"
    classifier_path = CURRENT_DIR / "classifier.pkl"
    bundle_path = CURRENT_DIR / "active_bundle.pkl"
    if bundle_path.is_file():
        with bundle_path.open("rb") as file:
            current_bundle = pickle.load(file)
        if (not isinstance(current_bundle, dict) or current_bundle.get("format_version") != 1
                or not isinstance(current_bundle.get("config"), dict)
                or not hasattr(current_bundle.get("classifier"), "predict_proba")):
            raise ValueError("The active Batch 2 model bundle has an unsupported format.")
        current_config = current_bundle["config"]
        current = current_bundle["classifier"]
    else:
        current_config = json.loads(config_path.read_text(encoding="utf-8"))
        with classifier_path.open("rb") as file:
            current = pickle.load(file)

    if not database_url:
        raise ValueError("DATABASE_URL is required to combine original training data and run regression gates.")
    verify_queue_export(database_url, rows)
    with db.connect(database_url) as conn:
        baseline_rows = conn.execute(
            """SELECT text, label FROM dataset_prompts
               WHERE source_dataset = 'neuralchemy' AND split = 'train'
               ORDER BY id"""
        ).fetchall()
    if not baseline_rows or {row["label"] for row in baseline_rows} != {0, 1}:
        raise ValueError("Original NeurAlchemy training rows are missing or one-sided; import the baseline dataset first.")
    baseline = [{"text": row["text"], "label": int(row["label"])} for row in baseline_rows]
    encoder = load_encoder(current_config)

    print(f"Encoding {len(baseline)} original training prompts plus {len(train)} reviewed prompts; "
          f"{len(validation)} reviewed prompts are held out for an adaptation check...")
    baseline_x = encode_rows(baseline, encoder)
    reviewed_train_x = encode_rows(train, encoder)
    candidate_train_x = np.concatenate((baseline_x, reviewed_train_x), axis=0)
    validation_x = encode_rows(validation, encoder)
    candidate_train_y = np.asarray([row["label"] for row in baseline] + [row["label"] for row in train], dtype=int)
    validation_y = np.asarray([row["label"] for row in validation], dtype=int)

    candidate = LogisticRegression(
        C=1.0, solver="lbfgs", max_iter=1000, random_state=42,
        class_weight=current_config.get("classifier_parameters", {}).get("class_weight"),
    )
    candidate.fit(candidate_train_x, candidate_train_y)
    candidate_train_metrics = metrics(candidate, candidate_train_x, candidate_train_y)
    current_metrics = metrics(current, validation_x, validation_y)
    candidate_metrics = metrics(candidate, validation_x, validation_y)
    allow_max = float(os.getenv("P7_ALLOW_MAX_SCORE", "0.30"))
    block_min = float(os.getenv("P7_BLOCK_MIN_SCORE", "0.70"))
    validate_policy(allow_max, block_min, 1.0)
    validation_routes = {
        "thresholds": {"allow_max": allow_max, "block_min": block_min},
        "current": p7_route_counts(current, validation_x, allow_max, block_min),
        "candidate": p7_route_counts(candidate, validation_x, allow_max, block_min),
    }
    gate_rows = load_gate_rows(database_url)
    gate_results = {}
    regression_passed = True
    for name, examples in gate_rows.items():
        covered, excluded_long = p4_covered_rows(examples, encoder)
        if not covered or {row["label"] for row in covered} != {0, 1}:
            raise ValueError(f"The P4-scored portion of {name} evaluation data must contain both labels.")
        features = encode_rows(covered, encoder)
        labels = np.asarray([row["label"] for row in covered], dtype=int)
        before = metrics(current, features, labels)
        after = metrics(candidate, features, labels)
        gate_results[name] = {"current": before, "candidate": after,
                              "comparison_population": "same P4-scored prompts for current and candidate",
                              "excluded_over_p4_token_limit": excluded_long,
                              "macro_f1_tolerance": REGRESSION_F1_TOLERANCE,
                              "passed": after["macro_f1"] >= before["macro_f1"] - REGRESSION_F1_TOLERANCE}
        regression_passed = regression_passed and gate_results[name]["passed"]
    # The review holdout is selected from P7's uncertainty band and is a weak, biased signal.
    accepted = candidate_metrics["macro_f1"] > current_metrics["macro_f1"] and regression_passed

    run_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    run_dir = RUNS_DIR / run_id
    run_dir.mkdir(parents=True, exist_ok=False)
    report = {
        "batch": 4,
        "created_at": datetime.now(timezone.utc).isoformat(),
        "training_data": {
            "source": "P7 review queue export",
            "original_train_count": len(baseline),
            "reviewed_train_count": len(train),
            "train_count": len(baseline) + len(train),
            "validation_count": len(validation),
            "prompt_ids": sorted(row["prompt_id"] for row in rows if row["prompt_id"]),
            "label_provenance": "human labels saved against P7 review prompt IDs",
            "reviewers": sorted({row["reviewer"] for row in rows if row["reviewer"]}),
            "label_counts": {
                split: {str(label): sum(row["label"] == label for row in rows if row["split"] == split)
                        for label in (0, 1)}
                for split in ("train", "validation")
            },
        },
        "selection_metric": "review-queue validation macro-F1 is a weak, selection-biased signal; candidate must improve it and pass the held-out promotion regression gates",
        "review_validation_caveat": "P7 sends uncertain prompts to review, so this validation set is selected on current-model uncertainty and its score is not an unbiased estimate of generalization.",
        "candidate_train": candidate_train_metrics,
        "current_validation": current_metrics,
        "candidate_validation": candidate_metrics,
        "p7_validation_route_counts": validation_routes,
        "regression_gate_policy": {
            "metric": "macro-F1",
            "minimum_change": -REGRESSION_F1_TOLERANCE,
            "tolerance": REGRESSION_F1_TOLERANCE,
        },
        "promotion_regression_gates": gate_results,
        "promotion_decision": {
            "review_validation_improved": candidate_metrics["macro_f1"] > current_metrics["macro_f1"],
            "all_regression_gates_passed": regression_passed,
        },
        "promoted": False,
    }

    if accepted and promote:
        # Preserve the active pair, then replace it with one atomic bundle write.
        shutil.copy2(bundle_path if bundle_path.is_file() else classifier_path,
                     run_dir / ("previous_active_bundle.pkl" if bundle_path.is_file() else "previous_classifier.pkl"))
        if not bundle_path.is_file():
            shutil.copy2(config_path, run_dir / "previous_config.json")
        new_config = dict(current_config)
        new_config.pop("neuralchemy_test_macro_f1", None)
        new_config.pop("external_test_macro_f1", None)
        new_config.update({
            "classifier_parameters": candidate.get_params(),
            "train_macro_f1": candidate_train_metrics["macro_f1"],
            "promotion_gate_scores": {
                name: {
                    "macro_f1": gate_results[name]["candidate"]["macro_f1"],
                    "used_for_promotion_selection": True,
                }
                for name in ("neuralchemy", "deepset", "jackhhao")
            },
            "training_data": {
                "source": report["training_data"]["source"],
                "original_train_count": len(baseline),
                "reviewed_train_count": len(train),
                "train_count": len(baseline) + len(train),
                "validation_count": len(validation),
                "prompt_ids": report["training_data"]["prompt_ids"],
                "label_provenance": report["training_data"]["label_provenance"],
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
        bundle = {"format_version": 1, "config": new_config, "classifier": candidate}
        atomic_write(bundle_path, lambda path: path.write_bytes(pickle.dumps(bundle)))
        report["promoted"] = True
        print("Candidate is better; promoted to the active Batch 2/P4 classifier.")
    elif not report["promotion_decision"]["review_validation_improved"]:
        print("Candidate did not improve review-validation macro-F1; current model retained.")
    elif not regression_passed:
        print("Candidate improved review-validation macro-F1 but failed regression gates; current model retained.")
    else:
        print("Candidate passed promotion checks, but --no-promote was set; active model retained.")

    (run_dir / "report.json").write_text(json.dumps(report, indent=2) + "\n", encoding="utf-8")
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
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        parser.error("DATABASE_URL is required for baseline training data and regression evaluation.")
    try:
        run_batch4(args.csv, promote=not args.no_promote, database_url=database_url)
    except ValueError as error:
        parser.error(str(error))


if __name__ == "__main__":
    main()
