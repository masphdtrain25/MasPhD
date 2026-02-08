# src/masphd/models/transformers.py
from __future__ import annotations

from dataclasses import dataclass
from typing import List, Optional

import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin


@dataclass
class ColumnDropper(BaseEstimator, TransformerMixin):
    """
    Drops specified columns from a pandas DataFrame.

    This exists to support loading older joblib pipelines that were saved
    with ColumnDropper defined in __main__ (e.g., in a notebook).
    """

    columns: Optional[List[str]] = None
    errors: str = "ignore"  # match pandas drop errors behaviour

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        if self.columns is None:
            return X
        if isinstance(X, pd.DataFrame):
            return X.drop(columns=self.columns, errors=self.errors)
        # If it is not a DataFrame, just return unchanged (safe fallback)
        return X
