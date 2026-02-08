# src/masphd/models/ensemble.py
from __future__ import annotations

import json
from typing import Any, Dict, Optional, Tuple

import joblib

from masphd.io.paths import MODELS, WEIGHTS


class WeightedEnsemblePredictor:
    """
    Weighted ensemble:
      pred = sum_i weight_i * model_i.predict(X)[0]

    Supports joblib artifacts that are either:
      - a fitted pipeline/object with .predict
      - a dict-like artifact that contains the fitted pipeline under 'pipeline'
        (your case)

    Weights json format:
      { "FIRST_SECOND": { "MODELNAME": weight, ... }, ... }
    """

    def __init__(self, weights_filename: str = "model_weights.json"):
        self._weights_path = WEIGHTS / weights_filename
        self._weights = self._load_weights(self._weights_path)

        # cache fitted pipelines only
        self._pipe_cache: Dict[Tuple[str, str, str], Any] = {}

    @staticmethod
    def _load_weights(path) -> Dict[str, Dict[str, float]]:
        with open(path, "r", encoding="utf-8") as f:
            data = json.load(f)
        return {k: {m: float(w) for m, w in v.items()} for k, v in data.items()}

    def available_pairs(self) -> Dict[str, Dict[str, float]]:
        return self._weights

    def _load_pipeline(self, first: str, second: str, model_name: str):
        """
        Loads artifact from disk and returns a fitted pipeline that has .predict.
        """
        key = (first, second, model_name)
        if key in self._pipe_cache:
            return self._pipe_cache[key]

        path = MODELS / f"{first}_{second}_{model_name}.joblib"
        artifact = joblib.load(path)

        # Your artifacts: dict with "pipeline"
        if isinstance(artifact, dict):
            if "pipeline" not in artifact:
                raise ValueError(f"Model artifact missing 'pipeline' key: {path}")
            pipe = artifact["pipeline"]
        else:
            # Sometimes the object itself is the pipeline
            pipe = artifact

        # sanity check
        if not hasattr(pipe, "predict"):
            raise TypeError(f"Loaded object has no .predict(): {path} (type={type(pipe)})")

        self._pipe_cache[key] = pipe
        return pipe

    def predict_one(self, first: str, second: str, X_row: Any) -> Optional[float]:
        """
        Predict one output for a single-row input X_row (DataFrame recommended).

        Returns None if no weights exist for this station pair.
        """
        pair_key = f"{first}_{second}"
        wdict = self._weights.get(pair_key)
        if not wdict:
            return None

        weighted_pred = 0.0
        total_w = 0.0

        for model_name, w in wdict.items():
            pipe = self._load_pipeline(first, second, model_name)
            pred = float(pipe.predict(X_row)[0])
            weighted_pred += w * pred
            total_w += w

        if total_w > 0:
            weighted_pred /= total_w

        return weighted_pred
