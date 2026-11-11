"""Train the B3 multiclass attack-type classifier on NeurAlchemy labels."""

import json
import os
import pickle
import sys
from importlib.metadata import version
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import confusion_matrix, f1_score, precision_recall_fscore_support

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from db import connect  # noqa: E402

ENCODER_NAME = "sentence-transformers/all-MiniLM-L6-v2"
BATCH_SIZE = 32
NORMALIZE_EMBEDDINGS = True
MAX_SEQ_LENGTH = 256
CLASS_ORDER = (
    "adversarial_attacks",
    "indirect_agent_manipulation",
    "information_extraction",
    "instruction_manipulation",
    "jailbreak_persona",
    "obfuscation",
)
MODEL_DIR = BACKEND_DIR / "batch" / "models" / "batch3"
SPLITS = ("train", "validation", "test")


def load_attack_data(database_url):
    """Load mapped malicious rows, preserving their imported split assignment."""
    query = """
        SELECT id, text, attack_group, split
        FROM dataset_prompts
        WHERE source_dataset = 'neuralchemy'
          AND label = 1
          AND attack_group = ANY(%s)
        ORDER BY split, id
    """
    with connect(database_url) as conn:
        rows = conn.execute(query, (list(CLASS_ORDER),)).fetchall()
    if not rows:
        raise ValueError("No mapped NeurAlchemy attack rows found. Import NeurAlchemy first.")

    df = pd.DataFrame(rows)
    if df["text"].isna().any() or df["text"].str.strip().eq("").any():
        raise ValueError("B3 data contains missing or blank prompt text.")
    unexpected = set(df["attack_group"]) - set(CLASS_ORDER)
    if unexpected:
        raise ValueError(f"Unexpected attack groups: {sorted(unexpected)}")
    if set(df["split"]) != set(SPLITS):
        raise ValueError(f"Expected the original {', '.join(SPLITS)} splits.")
    for split_name in SPLITS:
        labels = set(df.loc[df["split"] == split_name, "attack_group"])
        if labels != set(CLASS_ORDER):
            missing = sorted(set(CLASS_ORDER) - labels)
            raise ValueError(f"{split_name} split is missing attack groups: {missing}")
    return df


def load_encoder():
    """Load the same frozen MiniLM encoder and sequence settings as B2."""
    from sentence_transformers import SentenceTransformer

    print(f"\nLoading encoder: {ENCODER_NAME}")
    encoder = SentenceTransformer(ENCODER_NAME)
    encoder.max_seq_length = MAX_SEQ_LENGTH
    encoder.requires_grad_(False)
    encoder.eval()
    print(f"Encoder token limit: {encoder.max_seq_length}")
    return encoder


def create_embeddings(df, encoder):
    """Encode each stored split separately and retain its matching labels."""
    splits = {}
    for split_name in SPLITS:
        split = df.loc[df["split"] == split_name]
        texts = split["text"].tolist()
        token_lengths = encoder.tokenizer(
            texts, truncation=False, padding=False, return_length=True, verbose=False
        )["length"]
        over_limit = sum(length > encoder.max_seq_length for length in token_lengths)
        if over_limit:
            print(
                f"Warning: {split_name} has {over_limit} prompts longer than "
                f"{encoder.max_seq_length} tokens; embeddings use only the beginning."
            )
        print(f"\nEncoding {split_name}: {len(texts)} prompts")
        embeddings = encoder.encode(
            texts,
            batch_size=BATCH_SIZE,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=NORMALIZE_EMBEDDINGS,
        )
        labels = split["attack_group"].to_numpy()
        if len(embeddings) != len(labels):
            raise ValueError(f"{split_name} embedding count does not match its label count")
        splits[split_name] = {"X": embeddings, "y": labels}
        print(f"{split_name} embedding shape: {embeddings.shape}")
    return splits


def train_classifier(train_data):
    """Fit a balanced multinomial-capable logistic regression over embeddings."""
    print("\nTraining balanced multiclass Logistic Regression...")
    classifier = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=1000,
        random_state=42,
        class_weight="balanced",
    )
    classifier.fit(train_data["X"], train_data["y"])
    return classifier


def evaluate_classifier(classifier, split_data, split_name):
    """Report macro-F1, per-class scores, and confusion counts for one split."""
    labels = split_data["y"]
    predictions = classifier.predict(split_data["X"])
    precision, recall, f1, support = precision_recall_fscore_support(
        labels,
        predictions,
        labels=list(CLASS_ORDER),
        zero_division=0,
    )
    matrix = confusion_matrix(labels, predictions, labels=list(CLASS_ORDER))
    per_class = {
        class_name: {
            "precision": float(precision[index]),
            "recall": float(recall[index]),
            "f1": float(f1[index]),
            "support": int(support[index]),
        }
        for index, class_name in enumerate(CLASS_ORDER)
    }
    result = {
        "n": int(len(labels)),
        "macro_f1": float(f1_score(labels, predictions, labels=list(CLASS_ORDER), average="macro", zero_division=0)),
        "per_class": per_class,
        "confusion_matrix": {
            "labels": list(CLASS_ORDER),
            "counts": matrix.astype(int).tolist(),
        },
    }
    print(f"\n{split_name} macro-F1: {result['macro_f1']:.4f}")
    print("Per-class precision / recall / F1 / support:")
    for class_name, metrics in per_class.items():
        print(
            f"  {class_name}: {metrics['precision']:.4f} / {metrics['recall']:.4f} / "
            f"{metrics['f1']:.4f} / {metrics['support']}"
        )
    print("Confusion matrix (rows=true, columns=predicted; labels listed in matrix):")
    print(matrix)
    return result


def save_model(classifier, encoder, split_counts, validation_metrics, test_metrics, output_dir=MODEL_DIR):
    """Save B3's classifier and the inference/training metadata needed to use it."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)
    with (output_dir / "classifier.pkl").open("wb") as file:
        pickle.dump(classifier, file)

    config = {
        "encoder_name": ENCODER_NAME,
        "max_seq_length": int(encoder.max_seq_length),
        "normalize_embeddings": NORMALIZE_EMBEDDINGS,
        "embedding_dimension": int(encoder.get_embedding_dimension()),
        "task": "multiclass_attack_type",
        "label_mapping": {str(index): name for index, name in enumerate(classifier.classes_)},
        "class_order": [str(name) for name in classifier.classes_],
        "classifier_parameters": classifier.get_params(),
        "training_data": {
            "source_dataset": "neuralchemy",
            "attack_groups": list(CLASS_ORDER),
            "label_filter": "label = 1 and attack_group is one of the six canonical groups",
            "split_source": "preserved dataset_prompts.split values",
            "split_counts": split_counts,
        },
        "validation_macro_f1": validation_metrics["macro_f1"],
        "test_macro_f1": test_metrics["macro_f1"],
        "validation_metrics": validation_metrics,
        "test_metrics": test_metrics,
        "package_versions": {
            name: version(name)
            for name in ("scikit-learn", "sentence-transformers", "numpy", "pandas")
        },
    }
    with (output_dir / "config.json").open("w", encoding="utf-8") as file:
        json.dump(config, file, indent=2)
        file.write("\n")
    print(f"\nSaved B3 classifier and configuration to: {output_dir}")


def main():
    load_dotenv(BACKEND_DIR.parent / ".env")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")

    df = load_attack_data(database_url)
    counts = df.groupby(["split", "attack_group"]).size().unstack(fill_value=0)
    counts = counts.reindex(index=SPLITS, columns=CLASS_ORDER, fill_value=0)
    print(f"Loaded {len(df)} mapped malicious NeurAlchemy prompts.\n")
    print(counts.to_string())

    encoder = load_encoder()
    split_data = create_embeddings(df, encoder)
    classifier = train_classifier(split_data["train"])
    if list(classifier.classes_) != sorted(CLASS_ORDER):
        raise ValueError(f"Classifier classes do not match configured attack groups: {classifier.classes_}")
    validation_metrics = evaluate_classifier(classifier, split_data["validation"], "Validation")
    # Evaluate the frozen test split only after fitting and validation are complete.
    test_metrics = evaluate_classifier(classifier, split_data["test"], "Test")
    split_counts = {
        split_name: {class_name: int(counts.loc[split_name, class_name]) for class_name in CLASS_ORDER}
        for split_name in SPLITS
    }
    save_model(classifier, encoder, split_counts, validation_metrics, test_metrics)


if __name__ == "__main__":
    main()
