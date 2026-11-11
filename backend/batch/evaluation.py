"""Evaluate saved batch classifiers on imported external test datasets.

Run from the repository root with ``python -m backend.batch.evaluation`` (or
``uv run --project backend/batch python backend/batch/evaluation.py``).
Pickle artifacts must be trusted because loading a pickle can execute code.
"""

import argparse
import json
import os
import pickle
import sys
import time
from pathlib import Path

import numpy as np
from dotenv import load_dotenv
from sklearn.metrics import (
    accuracy_score,
    confusion_matrix,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
)

BACKEND_DIR = Path(__file__).resolve().parents[1]
PROJECT_DIR = BACKEND_DIR.parent
MODEL_DIR = BACKEND_DIR / "batch" / "models"
sys.path.insert(0, str(BACKEND_DIR))

from db import connect  # noqa: E402

EXTERNAL_DATASETS = ("deepset", "jackhhao")
POSITIVE_LABEL = 1


def load_external_data(database_url):
    """Load only imported external-test rows, keeping the sources separate."""
    query = """
        SELECT id, text, label, source_dataset
        FROM dataset_prompts
        WHERE split = 'external_test'
          AND source_dataset IN ('deepset', 'jackhhao')
        ORDER BY source_dataset, id
    """
    with connect(database_url) as conn:
        rows = conn.execute(query).fetchall()
    datasets = {name: [] for name in EXTERNAL_DATASETS}
    for row in rows:
        source = row["source_dataset"]
        if source not in datasets:
            continue
        text = row["text"]
        label = row["label"]
        if not isinstance(text, str) or not text.strip():
            raise ValueError(f"{source} contains a missing or blank prompt")
        if label not in (0, 1):
            raise ValueError(f"{source} contains a label outside the binary 0/1 mapping")
        datasets[source].append({"id": row["id"], "text": text, "label": int(label)})

    empty = [name for name, samples in datasets.items() if not samples]
    if empty:
        raise ValueError(
            "No external_test rows found for: " + ", ".join(empty)
            + ". Run backend/batch/import_external_datasets.py first."
        )
    for name, samples in datasets.items():
        labels = {sample["label"] for sample in samples}
        if labels != {0, 1}:
            raise ValueError(f"{name} external_test rows must contain both labels 0 and 1")
    return datasets


def _read_config(model_name):
    path = MODEL_DIR / model_name / "config.json"
    if not path.is_file():
        raise FileNotFoundError(f"Missing model configuration: {path}")
    return json.loads(path.read_text(encoding="utf-8"))


def load_batch1():
    """Return B1's saved vectorizer and classifier."""
    model_dir = MODEL_DIR / "batch1"
    config = _read_config("batch1")
    for name in ("vectorizer.pkl", "classifier.pkl"):
        if not (model_dir / name).is_file():
            raise FileNotFoundError(f"Missing batch1 model artifact: {model_dir / name}")
    with (model_dir / "vectorizer.pkl").open("rb") as file:
        vectorizer = pickle.load(file)
    with (model_dir / "classifier.pkl").open("rb") as file:
        classifier = pickle.load(file)
    if config.get("label_mapping") != {"0": "benign", "1": "malicious"}:
        raise ValueError("batch1 config does not use the expected benign/malicious label mapping")
    return vectorizer, classifier


def load_batch2():
    """Return B2's saved classifier and frozen encoder configured from its JSON."""
    model_dir = MODEL_DIR / "batch2"
    config = _read_config("batch2")
    classifier_path = model_dir / "classifier.pkl"
    if not classifier_path.is_file():
        raise FileNotFoundError(f"Missing batch2 model artifact: {classifier_path}")
    if config.get("label_mapping") != {"0": "benign", "1": "malicious"}:
        raise ValueError("batch2 config does not use the expected benign/malicious label mapping")
    with classifier_path.open("rb") as file:
        classifier = pickle.load(file)
    from sentence_transformers import SentenceTransformer

    encoder = SentenceTransformer(config["encoder_name"])
    encoder.max_seq_length = int(config["max_seq_length"])
    encoder.requires_grad_(False)
    encoder.eval()
    return encoder, classifier, config


def _positive_scores(classifier, features):
    """Return scores for class 1, supporting predict_proba or decision_function."""
    classes = list(classifier.classes_)
    if POSITIVE_LABEL not in classes:
        raise ValueError("Classifier does not contain malicious class label 1")
    positive_index = classes.index(POSITIVE_LABEL)
    if hasattr(classifier, "predict_proba"):
        return classifier.predict_proba(features)[:, positive_index]
    scores = np.asarray(classifier.decision_function(features))
    return scores if classes[-1] == POSITIVE_LABEL else -scores


def _metrics(labels, predictions, scores):
    tn, fp, fn, tp = confusion_matrix(labels, predictions, labels=[0, 1]).ravel()
    return {
        "n": int(len(labels)),
        "accuracy": float(accuracy_score(labels, predictions)),
        "precision_malicious": float(precision_score(labels, predictions, pos_label=1, zero_division=0)),
        "recall_malicious": float(recall_score(labels, predictions, pos_label=1, zero_division=0)),
        "f1_malicious": float(f1_score(labels, predictions, pos_label=1, zero_division=0)),
        "macro_f1": float(f1_score(labels, predictions, labels=[0, 1], average="macro", zero_division=0)),
        "false_positive_rate": float(fp / (fp + tn)) if fp + tn else 0.0,
        "roc_auc": float(roc_auc_score(labels, scores)),
        "confusion_matrix": {"tn": int(tn), "fp": int(fp), "fn": int(fn), "tp": int(tp)},
    }


def evaluate_b1(samples, vectorizer, classifier):
    texts = [sample["text"] for sample in samples]
    labels = np.asarray([sample["label"] for sample in samples])
    features = vectorizer.transform(texts)
    predictions = classifier.predict(features)
    scores = _positive_scores(classifier, features)

    latency_ms = []
    for text in texts:
        started = time.perf_counter()
        feature = vectorizer.transform([text])
        classifier.predict(feature)
        latency_ms.append((time.perf_counter() - started) * 1000)
    result = _metrics(labels, predictions, scores)
    result["latency_ms"] = _latency_summary(latency_ms)
    return result


def evaluate_b2(samples, encoder, classifier, config):
    texts = [sample["text"] for sample in samples]
    labels = np.asarray([sample["label"] for sample in samples])

    def encode(text_batch):
        return encoder.encode(
            text_batch,
            batch_size=1,
            show_progress_bar=False,
            convert_to_numpy=True,
            normalize_embeddings=bool(config["normalize_embeddings"]),
        )

    # Score the complete set together for consistent output; latency below is
    # measured per prompt with encoding and prediction in the timed section.
    features = encode(texts)
    predictions = classifier.predict(features)
    scores = _positive_scores(classifier, features)
    latency_ms = []
    for text in texts:
        started = time.perf_counter()
        feature = encode([text])
        classifier.predict(feature)
        latency_ms.append((time.perf_counter() - started) * 1000)
    result = _metrics(labels, predictions, scores)
    result["latency_ms"] = _latency_summary(latency_ms)
    return result


def _latency_summary(values):
    values = np.asarray(values, dtype=float)
    return {
        "unit": "ms_per_prompt",
        "mean": float(np.mean(values)),
        "median": float(np.median(values)),
        "p95": float(np.percentile(values, 95)),
    }


def run_evaluation(database_url):
    datasets = load_external_data(database_url)
    vectorizer, batch1_classifier = load_batch1()
    encoder, batch2_classifier, batch2_config = load_batch2()
    results = {name: {} for name in EXTERNAL_DATASETS}
    for dataset_name, samples in datasets.items():
        results[dataset_name]["batch1"] = evaluate_b1(samples, vectorizer, batch1_classifier)
        results[dataset_name]["batch2"] = evaluate_b2(
            samples, encoder, batch2_classifier, batch2_config
        )
    return {
        "positive_label": "malicious (1)",
        "source": "database dataset_prompts rows with split=external_test",
        "models": {
            "batch1": str(MODEL_DIR / "batch1"),
            "batch2": {
                "artifacts": str(MODEL_DIR / "batch2"),
                "encoder": batch2_config["encoder_name"],
                "max_seq_length": batch2_config["max_seq_length"],
            },
        },
        "latency_protocol": "per-prompt preprocessing/encoding plus predict; model loading excluded",
        "results": results,
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--output", type=Path, help="Optional path for a JSON report")
    args = parser.parse_args()
    load_dotenv(PROJECT_DIR / ".env")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        parser.error("DATABASE_URL is not configured (set it in the project .env or environment)")
    report = run_evaluation(database_url)
    serialized = json.dumps(report, indent=2)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(serialized + "\n", encoding="utf-8")
        print(f"Wrote evaluation report to {args.output}")
    else:
        print(serialized)


if __name__ == "__main__":
    main()
