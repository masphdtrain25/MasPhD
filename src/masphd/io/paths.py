from pathlib import Path

ROOT = Path(__file__).resolve().parents[3]

DATA = ROOT / "data"
RAW = DATA / "raw"
MODELS = DATA / "models"
WEIGHTS = DATA / "weights"
DATABASE = DATA / "database/realtime_predictions.db"
TIPLOC_MAP = DATA / "resources/tiploc.csv"
