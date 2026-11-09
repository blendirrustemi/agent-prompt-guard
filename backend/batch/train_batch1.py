# Train batch 1 baseline model (TF-IDF + Logistic Regression) on the NeurAlchemy dataset.

import json
import os
import pickle
import sys
from importlib.metadata import version
from pathlib import Path

import pandas as pd
from dotenv import load_dotenv
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import f1_score

BACKEND_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(BACKEND_DIR))

from db import connect

MAX_FEATURES = 10000
NGRAM_RANGE = (1, 2)
SUBLINEAR_TF = True
MODEL_DIR = BACKEND_DIR / "batch" / "models" / "batch1"


def load_baseline_data(database_url):
    #Load baseline splits from PostgreSQL.
    query = """
        SELECT id, text, label, split, group_id
        FROM dataset_prompts
        WHERE source_dataset = 'neuralchemy'
        ORDER BY split, id
    """
    with connect(database_url) as conn:
        rows = conn.execute(query).fetchall()

    if not rows:
        raise ValueError("No baseline rows found. Run import_neuralchemy.py first.")

    df = pd.DataFrame(rows)
    if set(df["split"]) != {"train", "validation", "test"}:
        raise ValueError("Expected the original train, validation and test splits.")
    if df["text"].isna().any() or df["text"].str.strip().eq("").any():
        raise ValueError("Baseline data contains missing or blank prompt text.")
    for split_name in ("train", "validation", "test"):
        labels = set(df.loc[df["split"] == split_name, "label"])
        if labels != {0, 1}:
            raise ValueError(f"The {split_name} split must contain labels 0 and 1.")

    return df


def create_tfidf_features(df):
    # Fit TF-IDF on the training split only, then transform all splits
    print(
        f"\nInitializing TfidfVectorizer (max_features={MAX_FEATURES}, "
        f"ngram_range={NGRAM_RANGE}, sublinear_tf={SUBLINEAR_TF})"
    )
    vectorizer = TfidfVectorizer(
        max_features=MAX_FEATURES,
        ngram_range=NGRAM_RANGE,
        sublinear_tf=SUBLINEAR_TF,
        strip_accents="unicode",
        lowercase=True,
    )

    splits = {}

    train_split = df.loc[df["split"] == "train"]
    train_texts = train_split["text"].tolist()
    print(f"\nFitting and transforming train: {len(train_texts)} prompts")
    X_train = vectorizer.fit_transform(train_texts)
    splits["train"] = {
        "X": X_train,
        "y": train_split["label"].to_numpy(),
    }
    print(f"train TF-IDF shape: {X_train.shape}")

    for split_name in ("validation", "test"):
        split = df.loc[df["split"] == split_name]
        texts = split["text"].tolist()
        print(f"\nTransforming {split_name}: {len(texts)} prompts")
        X_split = vectorizer.transform(texts)
        splits[split_name] = {
            "X": X_split,
            "y": split["label"].to_numpy(),
        }
        print(f"{split_name} TF-IDF shape: {X_split.shape}")

    return splits, vectorizer


def train_classifier(train_data):
    # Fit the binary classifier using only the training TF-IDF features and labels
    print("\nTraining Logistic Regression...")
    classifier = LogisticRegression(
        C=1.0,
        solver="lbfgs",
        max_iter=1000,
        random_state=42,
    )
    classifier.fit(train_data["X"], train_data["y"])
    return classifier


def evaluate_classifier(classifier, split_data, split_name):
    # Measure macro-F1 on a held-out split without updating the classifier
    predictions = classifier.predict(split_data["X"])
    macro_f1 = f1_score(
        split_data["y"], predictions, labels=[0, 1], average="macro", zero_division=0
    )
    print(f"{split_name} macro-F1: {macro_f1:.4f}")
    return macro_f1


def save_model(classifier, vectorizer, validation_f1, test_f1, output_dir=MODEL_DIR):
    # Save the trained classifier, TF-IDF vectorizer, and configuration
    output_dir = Path(output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    with (output_dir / "classifier.pkl").open("wb") as file:
        pickle.dump(classifier, file)

    with (output_dir / "vectorizer.pkl").open("wb") as file:
        pickle.dump(vectorizer, file)

    vectorizer_params = vectorizer.get_params()
    if "ngram_range" in vectorizer_params:
        vectorizer_params["ngram_range"] = list(vectorizer_params["ngram_range"])
    vectorizer_params = {
        k: (str(v) if isinstance(v, type) or callable(v) else v)
        for k, v in vectorizer_params.items()
    }

    config = {
        "vectorizer_type": "TfidfVectorizer",
        "vocabulary_size": len(vectorizer.vocabulary_),
        "max_features": MAX_FEATURES,
        "ngram_range": list(NGRAM_RANGE),
        "sublinear_tf": SUBLINEAR_TF,
        "label_mapping": {"0": "benign", "1": "malicious"},
        "vectorizer_parameters": vectorizer_params,
        "classifier_parameters": classifier.get_params(),
        "validation_macro_f1": float(validation_f1),
        "neuralchemy_test_macro_f1": float(test_f1),
        "package_versions": {
            name: version(name)
            for name in ("scikit-learn", "numpy", "pandas")
        },
    }
    with (output_dir / "config.json").open("w", encoding="utf-8") as file:
        json.dump(config, file, indent=2)
        file.write("\n")

    print(f"\nSaved classifier, vectorizer, and configuration to: {output_dir}")


def main():
    load_dotenv(BACKEND_DIR.parent / ".env", override=True)
    database_url = os.getenv("DATABASE_URL")
    if not database_url:
        raise RuntimeError("DATABASE_URL is not configured.")

    df = load_baseline_data(database_url)
    counts = df.groupby(["split", "label"]).size().unstack(fill_value=0)
    counts = counts.reindex(["train", "validation", "test"])
    counts = counts.rename(columns={0: "benign", 1: "malicious"})
    print(f"Loaded {len(df)} baseline prompts.\n")
    print(counts.to_string())

    splits, vectorizer = create_tfidf_features(df)
    classifier = train_classifier(splits["train"])
    validation_f1 = evaluate_classifier(classifier, splits["validation"], "Validation")
    test_f1 = evaluate_classifier(classifier, splits["test"], "NeurAlchemy test")
    save_model(classifier, vectorizer, validation_f1, test_f1)


if __name__ == "__main__":
    main()