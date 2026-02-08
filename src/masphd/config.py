import os
from pathlib import Path
import yaml

# --------------------------------------------------
# Paths
# --------------------------------------------------
ROOT = Path(__file__).resolve().parents[2]
CONFIG_DIR = ROOT / "configs"

# --------------------------------------------------
# Load YAML config (non-secret)
# --------------------------------------------------
with open(CONFIG_DIR / "config.yaml", "r") as f:
    _cfg = yaml.safe_load(f)


DARWIN_TOPIC_HOST = _cfg["DARWIN_TOPIC_HOST"]
DARWIN_TOPIC_PORT = _cfg["DARWIN_TOPIC_PORT"]

DARWIN_TOPIC_NAME = _cfg["DARWIN_TOPIC_NAME"]
DARWIN_HEARTBEAT_MS = int(_cfg.get("DARWIN_HEARTBEAT_MS", 15000))
DARWIN_RECONNECT_DELAY_SECS = int(_cfg.get("DARWIN_RECONNECT_DELAY_SECS", 15))
DARWIN_SUBSCRIPTION_ID = str(_cfg.get("DARWIN_SUBSCRIPTION_ID", "1"))
DARWIN_ACK_MODE = str(_cfg.get("DARWIN_ACK_MODE", "auto"))

DARWIN_TOPIC_USERNAME = _cfg["DARWIN_TOPIC_USERNAME"]
DARWIN_TOPIC_PASSWORD = _cfg["DARWIN_TOPIC_PASSWORD"]

HSP_SERVICE_METRICS_URL = _cfg["HSP_SERVICE_METRICS_URL"]
HSP_SERVICE_DETAILS_URL = _cfg["HSP_SERVICE_DETAILS_URL"]
HSP_USERNAME = _cfg["HSP_USERNAME"]
HSP_PASSWORD = _cfg["HSP_PASSWORD"]

# # --------------------------------------------------
# # Load secrets from environment
# # --------------------------------------------------
# DARWIN_TOPIC_USERNAME = os.getenv("DARWIN_TOPIC_USERNAME")
# DARWIN_TOPIC_PASSWORD = os.getenv("DARWIN_TOPIC_PASSWORD")

# if not DARWIN_TOPIC_USERNAME or not DARWIN_TOPIC_PASSWORD:
#     raise RuntimeError(
#         "DARWIN_TOPIC credentials not set. "
#         "Check configs/secrets.env or environment variables."
#     )
