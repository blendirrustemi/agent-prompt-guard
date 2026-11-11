# Train batch 2 baseline model on the NeurAlchemy dataset. This script is intended to be run after import_neuralchemy.py has been executed, which imports the NeurAlchemy dataset into the database.

import json
import os
import pickle
import sys
from importlib.metadata import version
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from db import connect


ENCODER_NAME = "sentence-transformers/all-MiniLM-L6-v2"
BATCH_SIZE = 32
NORMALIZE_EMBEDDINGS = True
MODEL_DIR = BACKEND_DIR / "batch" / "models" / "batch2"


def load_baseline_data(database_url):

    query = """
        SELECT id, text, label, split, group_id
        FROM dataset_prompts
        WHERE source_dataset = 'neuralchemy'
        ORDER BY split, id
    """
    with connect(database_url) as conn:
        rows = conn.execute(query).fetchall()

    if not rows:
        raise ValueError("No NeurAlchemy rows found. Run import_neuralchemy.py first.")

    df = pd.DataFrame(rows)
    if set(df["split"]) != {"train", "validation", "test"}:
        raise ValueError("Expected the original train, validation and test splits.")
    if df["text"].isna().any() or df["text"].str.strip().eq("").any():
        raise ValueError("NeurAlchemy data contains missing or blank prompt text.")
    for split_name in ("train", "validation", "test"):
        labels = set(df.loc[df["split"] == split_name, "label"])
        if labels != {0, 1}:
            raise ValueError(f"The {split_name} split must contain labels 0 and 1.")

    return df


def load_encoder():
    """Load MiniLM for inference only; its weights will remain frozen."""
    from sentence_transformers import SentenceTransformer

    print(f"\nLoading encoder: {ENCODER_NAME}")
    encoder = SentenceTransformer(ENCODER_NAME)
    encoder.requires_grad_(False)
    encoder.eval()
    print(f"Encoder token limit: {encoder.max_seq_length}")
    return encoder


def create_embeddings(df, encoder):
    """Encode each split separately, preserving its row order and labels."""
    splits = {}
    for split_name in ("train", "validation", "test"):
        split = df.loc[df["split"] == split_name]
        texts = split["text"].tolist()

        # Report truncation so we can review coverage before training and
        # define long-prompt handling for P4.
        token_lengths = encoder.tokenizer(
            texts, truncation=False, padding=False, return_length=True, verbose=False
        )["length"]
        over_limit = sum(length > encoder.max_seq_length for length in token_lengths)
        if over_limit:
            print(
                f"Warning: {split_name} has {over_limit} prompts longer than "
                f"{encoder.max_seq_length} tokens; their embeddings use only "
                "the beginning of the prompt."
            )

        print(f"\nEncoding {split_name}: {len(texts)} prompts")
        embeddings = encoder.encode(
            texts,
            batch_size=BATCH_SIZE,
            show_progress_bar=True,
            convert_to_numpy=True,
            normalize_embeddings=NORMALIZE_EMBEDDINGS,
        )
        splits[split_name] = {
            "X": embeddings,
            "y": split["label"].to_numpy(),
        }
        print(f"{split_name} embedding shape: {embeddings.shape}")

    return splits


def train_classifier(train_data):
    """Fit the binary classifier using only the training embeddings and labels."""
    print("\nTraining Logistic Regression...")
    # Keep this configuration fixed when evaluating the held-out test set.
    classifier = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=1000,
        random_state=42,
    )
    classifier.fit(train_data["X"], train_data["y"])
    return classifier


def evaluate_classifier(classifier, split_data, split_name):
    """Measure macro-F1 on a dataset split without updating the classifier."""
    predictions = classifier.predict(split_data["X"])
    macro_f1 = f1_score(
        split_data["y"], predictions, labels=[0, 1], average="macro", zero_division=0
    )
    print(f"{split_name} macro-F1: {macro_f1:.4f}")
    return macro_f1


def save_model(classifier, encoder, train_f1, validation_f1, test_f1, output_dir=MODEL_DIR):
    """Save the trained classifier and the settings needed to recreate embeddings."""
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    # Reload only trusted pickle files, such as this locally trained classifier.
    with (output_dir / "classifier.pkl").open("wb") as file:
        pickle.dump(classifier, file)

    config = {
        "encoder_name": ENCODER_NAME,
        "max_seq_length": encoder.max_seq_length,
        "normalize_embeddings": NORMALIZE_EMBEDDINGS,
        "embedding_dimension": encoder.get_sentence_embedding_dimension(),
        "label_mapping": {"0": "benign", "1": "malicious"},
        "classifier_parameters": classifier.get_params(),
        "training_data": {
            "source_dataset": "neuralchemy",
            "include_in_baseline_filter": False,
        },
        "train_macro_f1": float(train_f1),
        "validation_macro_f1": float(validation_f1),
        "neuralchemy_test_macro_f1": float(test_f1),
        "package_versions": {
            name: version(name)
            for name in ("scikit-learn", "sentence-transformers", "numpy")
        },
    }
    with (output_dir / "config.json").open("w", encoding="utf-8") as file:
        json.dump(config, file, indent=2)
        file.write("\n")

    print(f"\nSaved classifier and configuration to: {output_dir}")


def main():
    load_dotenv(BACKEND_DIR.parent / ".env")
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")

    df = load_baseline_data(database_url)
    counts = df.groupby(["split", "label"]).size().unstack(fill_value=0)
    counts = counts.reindex(["train", "validation", "test"])
    counts = counts.rename(columns={0: "benign", 1: "malicious"})
    print(f"Loaded {len(df)} NeurAlchemy prompts.\n")
    print(counts.to_string())

    encoder = load_encoder()
    splits = create_embeddings(df, encoder)
    classifier = train_classifier(splits["train"])
    train_f1 = evaluate_classifier(classifier, splits["train"], "Train")
    validation_f1 = evaluate_classifier(classifier, splits["validation"], "Validation")
    test_f1 = evaluate_classifier(classifier, splits["test"], "NeurAlchemy test")
    save_model(classifier, encoder, train_f1, validation_f1, test_f1)


if __name__ == "__main__":
    main()
