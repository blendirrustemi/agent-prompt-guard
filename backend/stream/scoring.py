"""P3/P4 inference and durable score handoff to P7; no final decisions or topics."""

import hashlib
import json
import logging
import os
import pickle
import warnings
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from importlib.metadata import version
from pathlib import Path
from time import perf_counter

import numpy as np

from backend import db
from .aggregation import ATTACK_TYPES
from .similarity import SimilarityProbe, SimilarityResult

LOGGER = logging.getLogger(__name__)
MODEL_DIR = Path(__file__).resolve().parents[1] / "batch" / "models"


@dataclass(frozen=True)
class ScoringResult:
    """P4 handoff for future P6; embeddings and prompt content are not exported."""
    prompt_id: str
    outcome: str
    malicious_score: float | None
    attack_type: str | None
    model_version: str
    scored_at: datetime
    token_count: int | None
    truncated: bool | None
    similarity: SimilarityResult | None
    timings_ms: dict
    error_type: str | None = None

    def as_dict(self):
        result = asdict(self)
        result["scored_at"] = self.scored_at.isoformat()
        return result


def _number_setting(name, default, *, integer=False):
    value = int(os.getenv(name, default)) if integer else float(os.getenv(name, default))
    if not np.isfinite(value) or value <= 0:
        raise ValueError(f"{name} must be finite and positive.")
    return value


def _read_artifacts(model_dir):
    configs, classifiers = {}, {}
    fingerprint = hashlib.sha256()
    for name in ("batch2", "batch3"):
        config_path = model_dir / name / "config.json"
        config = json.loads(config_path.read_text(encoding="utf-8"))
        for package, expected in config["package_versions"].items():
            if package in {"scikit-learn", "sentence-transformers", "numpy"} and version(package) != expected:
                raise ValueError(f"{name} requires {package}=={expected}; installed {version(package)}.")
        classifier_path = model_dir / name / "classifier.pkl"
        for path in (config_path, classifier_path):
            fingerprint.update(f"{name}/{path.name}\0".encode())
            fingerprint.update(path.read_bytes())
        # Only repository-owned model artifacts are loaded. Reject version mismatch warnings.
        with warnings.catch_warnings():
            warnings.simplefilter("error")
            with classifier_path.open("rb") as file:
                classifiers[name] = pickle.load(file)
        configs[name] = config
    b2, b3 = configs["batch2"], configs["batch3"]
    for field in ("encoder_name", "max_seq_length", "normalize_embeddings", "embedding_dimension"):
        if b2[field] != b3[field]:
            raise ValueError(f"Batch 2/3 encoder setting differs: {field}.")
    if (b2["encoder_name"] != "sentence-transformers/all-MiniLM-L6-v2"
            or b2["max_seq_length"] != 256 or b2["embedding_dimension"] != 384
            or b2["normalize_embeddings"] is not True):
        raise ValueError("Expected normalized 384-dimensional MiniLM embeddings with a 256-token limit.")
    if b2["label_mapping"] != {"0": "benign", "1": "malicious"}:
        raise ValueError("Batch 2 binary label mapping is incompatible.")
    binary, attack = classifiers["batch2"], classifiers["batch3"]
    if set(binary.classes_) != {0, 1} or len(binary.classes_) != 2:
        raise ValueError("Batch 2 must contain exactly the benign/malicious classes.")
    if (list(attack.classes_) != b3["class_order"] or set(attack.classes_) != set(ATTACK_TYPES)
            or not callable(getattr(attack, "predict", None))):
        raise ValueError("Batch 3 classes do not match P8's six attack types.")
    for classifier in (binary, attack):
        if classifier.n_features_in_ != 384 or not callable(getattr(classifier, "predict_proba", None)):
            raise ValueError("Classifier does not accept the expected embedding features.")
    return b2, binary, attack, f"b2-b3:{fingerprint.hexdigest()[:16]}"


class PromptScorer:
    def __init__(self, encoder, binary, attack, probe, model_version, *, recent_min_score=0.90,
                 now=None, timer=perf_counter):
        if not np.isfinite(recent_min_score) or not 0 <= recent_min_score <= 1:
            raise ValueError("P3 recent-reference score must be between 0 and 1.")
        self.encoder, self.binary, self.attack = encoder, binary, attack
        self.probe, self.model_version = probe, model_version
        self.recent_min_score = recent_min_score
        self.now = now or (lambda: datetime.now(timezone.utc))
        self.timer = timer
        self.positive_index = list(binary.classes_).index(1)

    @classmethod
    def load(cls, database_url, *, model_dir=MODEL_DIR):
        config, binary, attack, model_version = _read_artifacts(Path(model_dir))
        from sentence_transformers import SentenceTransformer

        encoder = SentenceTransformer(config["encoder_name"], device="cpu")
        encoder.max_seq_length = config["max_seq_length"]
        encoder.requires_grad_(False)
        encoder.eval()
        if encoder.get_embedding_dimension() != config["embedding_dimension"]:
            raise ValueError("Downloaded encoder dimension does not match the model artifacts.")
        rows = db.get_similarity_training_rows(database_url)
        references, seen = [], set()
        for row in rows:
            if row["text"] not in seen:
                seen.add(row["text"])
                references.append(row)
        vectors = []
        # Encode in bounded batches, rather than materializing a second large text list.
        for start in range(0, len(references), 32):
            vectors.extend(encoder.encode(
                [row["text"] for row in references[start:start + 32]],
                batch_size=32, show_progress_bar=False, convert_to_numpy=True,
                normalize_embeddings=True,
            ))
        probe = SimilarityProbe(
            ((str(row["id"]), vector) for row, vector in zip(references, vectors)),
            threshold=_number_setting("P3_SIMILARITY_THRESHOLD", "0.85"),
            retention_seconds=_number_setting("P3_RECENT_SECONDS", "300"),
            capacity=_number_setting("P3_RECENT_CAPACITY", "1000", integer=True),
        )
        if not references:
            LOGGER.warning("P3 has no training attack references; using recent detections only")
        scorer = cls(encoder, binary, attack, probe, model_version,
                     recent_min_score=_number_setting("P3_RECENT_MIN_SCORE", "0.90"))
        # Warm the encoder/classifiers before joining the Kafka group.
        warmup, _ = scorer.score("startup-warmup", "Scorer startup check")
        if warmup.outcome != "completed":
            raise ValueError(f"P4 startup inference failed: {warmup.error_type}.")
        LOGGER.info("P3/P4 scorer ready: model=%s training_references=%d token_limit=%d",
                    model_version, len(references), encoder.max_seq_length)
        return scorer

    def score(self, prompt_id, text):
        """Return the public result plus a private embedding for post-commit memory."""
        started = self.timer()
        token_count = None
        truncated = None
        similarity = None
        embedding = None
        timings = {}
        try:
            token_count = int(self.encoder.tokenizer(
                [text], truncation=False, padding=False, return_length=True, verbose=False,
            )["length"][0])
            truncated = token_count > self.encoder.max_seq_length
            features = np.asarray(self.encoder.encode(
                [text], batch_size=1, show_progress_bar=False,
                convert_to_numpy=True, normalize_embeddings=True,
            ))
            if features.shape != (1, self.probe.dimension) or not np.isfinite(features).all():
                raise ValueError("Encoder returned an invalid embedding.")
            embedding = features[0]
            timings["encoding"] = (self.timer() - started) * 1000
            stage = self.timer()
            similarity = self.probe.probe(prompt_id, embedding)
            timings["similarity"] = (self.timer() - stage) * 1000
            stage = self.timer()
            score = float(self.binary.predict_proba(features)[0, self.positive_index])
            if not np.isfinite(score) or not 0 <= score <= 1:
                raise ValueError("Classifier returned an invalid malicious probability.")
            attack_type = str(self.attack.predict(features)[0]) if score >= 0.5 else None
            if attack_type is not None and attack_type not in ATTACK_TYPES:
                raise ValueError("Classifier returned an unknown attack type.")
            timings["classification"] = (self.timer() - stage) * 1000
            outcome, error_type = "completed", None
        except Exception as error:
            score, attack_type, outcome, error_type = None, None, "failed", type(error).__name__
        timings["total"] = (self.timer() - started) * 1000
        result = ScoringResult(str(prompt_id), outcome, score, attack_type, self.model_version,
                               self.now(), token_count, truncated, similarity, timings, error_type)
        # The embedding is not part of ScoringResult/as_dict, the handoff to P6.
        return result, embedding


def score_and_store(database_url, event, scorer):
    """Persist before remembering/acknowledging; retries preserve completed P4 outcomes."""
    stored = db.get_guard_result(database_url, event["prompt_id"])
    if stored is None:
        raise RuntimeError("P4 prompt row is missing after intake storage.")
    if stored["malicious_score"] is not None or stored.get("scored_at") is not None:
        return None
    result, embedding = scorer.score(event["prompt_id"], event["text"])
    # Keep database exceptions outside the inference handler: never acknowledge a failed write.
    saved = db.save_guard_score(
        database_url, result.prompt_id, result.malicious_score, result.attack_type,
        result.model_version, reason="classifier_inference_failed" if result.outcome == "failed" else None,
        checked_at=result.scored_at,
    )
    if saved is None:
        raise RuntimeError("P4 prompt row disappeared before score persistence.")
    if result.outcome == "completed" and result.malicious_score >= scorer.recent_min_score:
        scorer.probe.remember(result.prompt_id, embedding)
    LOGGER.info("P3/P4 prompt=%s outcome=%s model=%s tokens=%s truncated=%s similarity=%s timings_ms=%s error=%s",
                result.prompt_id, result.outcome, result.model_version, result.token_count,
                result.truncated, result.similarity, result.timings_ms, result.error_type)
    return result
