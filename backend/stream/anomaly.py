"""P5: streaming anomaly scores over prompt and guard features."""

import logging
import math

from river import anomaly as river_anomaly

from backend import db

LOGGER = logging.getLogger(__name__)

FEATURE_VERSION = "p5-v1"
WINDOW_SIZE = 250
N_TREES = 10
TREE_HEIGHT = 8
RANDOM_SEED = 42
MAX_PROMPT_CHARS = 8000
FEATURE_NAMES = (
    "char_count",
    "word_count",
    "symbol_ratio",
    "similarity_scaled",
    "similarity_available",
    "malicious_score",
    "malicious_score_available",
)
FEATURE_LIMITS = {name: (0.0, 1.0) for name in FEATURE_NAMES}


def build_features(preprocessing, malicious_score, similarity_score):
    """Convert persisted P1/P3/P4 outputs to a stable, bounded HST feature map."""
    if not isinstance(preprocessing, dict) or not isinstance(preprocessing.get("features"), dict):
        raise ValueError("P5 requires the P1 feature snapshot.")
    raw = preprocessing["features"]
    char_count = int(raw["char_count"])
    word_count = int(raw["word_count"])
    symbol_ratio = float(raw["symbol_ratio"])
    if not 0 <= char_count <= MAX_PROMPT_CHARS or not 0 <= word_count <= MAX_PROMPT_CHARS:
        raise ValueError("P1 prompt counts exceed the P5 feature bounds.")
    if not math.isfinite(symbol_ratio) or not 0 <= symbol_ratio <= 1:
        raise ValueError("P1 symbol ratio must be finite and between 0 and 1.")

    has_similarity = similarity_score is not None
    similarity = float(similarity_score) if has_similarity else 0.0
    if not math.isfinite(similarity) or not -1 <= similarity <= 1:
        raise ValueError("P3 similarity must be finite and between -1 and 1.")

    has_malicious_score = malicious_score is not None
    malicious = float(malicious_score) if has_malicious_score else 0.0
    if not math.isfinite(malicious) or not 0 <= malicious <= 1:
        raise ValueError("P4 malicious score must be finite and between 0 and 1.")

    return {
        "char_count": char_count / MAX_PROMPT_CHARS,
        "word_count": word_count / MAX_PROMPT_CHARS,
        "symbol_ratio": symbol_ratio,
        "similarity_scaled": (similarity + 1.0) / 2.0 if has_similarity else 0.0,
        "similarity_available": float(has_similarity),
        "malicious_score": malicious,
        "malicious_score_available": float(has_malicious_score),
    }


class PromptAnomalyDetector:
    """Stateful HST with score-before-learn and replay-based recovery."""

    def __init__(self):
        self.model = river_anomaly.HalfSpaceTrees(
            n_trees=N_TREES,
            height=TREE_HEIGHT,
            window_size=WINDOW_SIZE,
            limits=FEATURE_LIMITS,
            seed=RANDOM_SEED,
        )
        self.samples_seen = 0

    def score(self, features):
        """Return this event's score without allowing it to affect its own score."""
        if self.samples_seen < WINDOW_SIZE:
            return None, "warming_up"
        value = float(self.model.score_one(features))
        if not math.isfinite(value) or value < 0:
            raise ValueError("P5 returned an invalid anomaly score.")
        return value, "scored"

    def learn(self, features):
        self.model.learn_one(features)
        self.samples_seen += 1

    @classmethod
    def load(cls, database_url):
        detector = cls()
        history = db.get_p5_rebuild_rows(database_url, FEATURE_VERSION, WINDOW_SIZE)
        for row in history["rows"]:
            features = build_features(
                row["preprocessing"], row["malicious_score"], row["p3_similarity_score"]
            )
            detector.learn(features)
        # The queried tail is enough to rebuild the active two HST windows. Keep the
        # full count so warm-up behavior and window phase continue across restarts.
        detector.samples_seen = history["sample_count"]
        LOGGER.info("P5 detector ready: restored=%d total=%d", len(history["rows"]), detector.samples_seen)
        return detector


def score_and_store(database_url, prompt_id, detector):
    """Persist one P5 outcome before learning, making Kafka retries idempotent."""
    stored = db.get_p5_input(database_url, prompt_id)
    if stored is None:
        raise RuntimeError("P5 prompt row is missing after intake storage.")
    if stored["p5_feature_version"] == FEATURE_VERSION and stored["p5_status"] is not None:
        return None

    features = build_features(
        stored["preprocessing"], stored["malicious_score"], stored["p3_similarity_score"]
    )
    score, status = detector.score(features)
    if not db.save_p5_result(database_url, prompt_id, score, status, FEATURE_VERSION):
        return None
    detector.learn(features)
    LOGGER.info("P5 prompt=%s status=%s anomaly_score=%s", prompt_id, status, score)
    return {"anomaly_score": score, "status": status}
