# src/masphd/hsp/client.py
import logging
from typing import Any, Dict, Optional

import requests

from masphd.config import (
    HSP_SERVICE_DETAILS_URL,
    HSP_USERNAME,
    HSP_PASSWORD,
)

log = logging.getLogger(__name__)


class HSPClient:
    """
    HTTP client for HSP service-details endpoint.
    Mirrors the style of masphd.darwin.DarwinClient, but uses requests + POST.
    """

    def __init__(
        self,
        timeout_secs: float = 20.0,
        session: Optional[requests.Session] = None,
        user_agent: str = "masphd-hsp-client/1.0",
    ):
        self._timeout_secs = timeout_secs
        self._session = session or requests.Session()
        self._session.headers.update({"User-Agent": user_agent})

    def get_service_details_raw(self, rid: str) -> Optional[Dict[str, Any]]:
        """
        Fetch raw JSON response for a service by RID.
        Returns None on failure (logs warning).
        """
        payload = {"rid": rid}

        try:
            resp = self._session.post(
                HSP_SERVICE_DETAILS_URL,
                auth=(HSP_USERNAME, HSP_PASSWORD),
                json=payload,
                timeout=self._timeout_secs,
            )
        except requests.RequestException as e:
            log.warning("HSP request failed for rid=%s: %s", rid, e)
            return None

        if resp.status_code != 200:
            # Keep log short; response bodies can be huge/noisy.
            log.warning(
                "HSP non-200 for rid=%s: status=%s",
                rid,
                resp.status_code,
            )
            return None

        try:
            return resp.json()
        except ValueError:
            log.warning("HSP invalid JSON for rid=%s", rid)
            return None
