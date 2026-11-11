"""P3: advisory cosine matching against training attacks and bounded recent detections.

Inspired by PromptGuard's semantic intent comparison, not its full defense stack:
https://www.nature.com/articles/s41598-025-31086-y
The expiring online reference bank is a project extension. It never changes P4's score.
"""

from collections import OrderedDict
from dataclasses import dataclass
from time import monotonic

import numpy as np


@dataclass(frozen=True)
class SimilarityResult:
    score: float | None
    reference_id: str | None
    reference_origin: str | None
    matched: bool


class SimilarityProbe:
    def __init__(self, references=(), *, dimension=384, threshold=0.85,
                 retention_seconds=300, capacity=1000, clock=monotonic):
        if not np.isfinite(threshold) or not -1 <= threshold <= 1:
            raise ValueError("P3 similarity threshold must be between -1 and 1.")
        if dimension <= 0 or retention_seconds <= 0 or capacity <= 0:
            raise ValueError("P3 dimension, retention and capacity must be positive.")
        self.dimension = dimension
        self.threshold = threshold
        self.retention_seconds = retention_seconds
        self.capacity = capacity
        self.clock = clock
        self.training_ids = []
        vectors = []
        for reference_id, embedding in references:
            self.training_ids.append(str(reference_id))
            vectors.append(self._unit(embedding))
        self.training = np.asarray(vectors).reshape((-1, dimension))
        self.recent = OrderedDict()

    def _unit(self, embedding):
        vector = np.asarray(embedding, dtype=float)
        if vector.shape != (self.dimension,) or not np.isfinite(vector).all():
            raise ValueError("P3 received an invalid embedding.")
        norm = np.linalg.norm(vector)
        if not norm:
            raise ValueError("P3 received a zero embedding.")
        return vector / norm

    def _expire(self):
        now = self.clock()
        for prompt_id in [key for key, (expires, _) in self.recent.items() if expires <= now]:
            del self.recent[prompt_id]

    def probe(self, prompt_id, embedding):
        self._expire()
        vector = self._unit(embedding)
        best = SimilarityResult(None, None, None, False)
        if len(self.training):
            scores = self.training @ vector
            for index in np.argsort(-scores, kind="stable"):
                if self.training_ids[index] != str(prompt_id):
                    score = float(np.clip(scores[index], -1, 1))
                    best = SimilarityResult(score, self.training_ids[index], "training", score >= self.threshold)
                    break
        for reference_id, (_, reference) in self.recent.items():
            if reference_id == str(prompt_id):
                continue
            score = float(np.clip(reference @ vector, -1, 1))
            if best.score is None or score > best.score:
                best = SimilarityResult(score, reference_id, "stream", score >= self.threshold)
        return best

    def remember(self, prompt_id, embedding):
        """Call only after a high-confidence P4 result has been persisted."""
        self._expire()
        prompt_id = str(prompt_id)
        if prompt_id in self.recent:
            return  # A retry must not refresh its lifetime or consume capacity.
        self.recent[prompt_id] = (self.clock() + self.retention_seconds, self._unit(embedding))
        while len(self.recent) > self.capacity:
            self.recent.popitem(last=False)
