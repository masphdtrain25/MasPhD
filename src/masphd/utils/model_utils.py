import pandas as pd
from sklearn.base import BaseEstimator, TransformerMixin

class ColumnDropper(BaseEstimator, TransformerMixin):
    def __init__(self, cols_to_drop=None):
        self.cols_to_drop = list(cols_to_drop) if cols_to_drop else []

    def fit(self, X, y=None):
        return self

    def transform(self, X):
        if isinstance(X, pd.DataFrame):
            cols = [c for c in self.cols_to_drop if c in X.columns]
            return X.drop(columns=cols, errors="ignore")
        return X
