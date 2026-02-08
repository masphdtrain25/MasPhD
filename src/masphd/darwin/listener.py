# src/masphd/darwin/listener.py
from __future__ import annotations

import logging
import time
import stomp
from typing import Any, Callable, Dict, List, Tuple

from .decoder import decode_message

log = logging.getLogger(__name__)

DecodedHandler = Callable[[List[Dict[str, Any]], List[Dict[str, Any]], bytes], None]
#                               forecasts              schedules          xml_bytes


class DarwinListener(stomp.ConnectionListener):
    """
    STOMP listener that:
      - decompresses frame.body
      - parses Forecast TS/Location + schedule OR/DT
      - calls on_decoded(forecasts, schedules, xml_bytes)
    """

    def __init__(self, on_decoded: DecodedHandler, reconnect_delay_secs: int = 15):
        self._on_decoded = on_decoded
        self._reconnect_delay_secs = reconnect_delay_secs

    def on_heartbeat(self):
        log.info("Received heartbeat")

    def on_heartbeat_timeout(self):
        log.error("Heartbeat timeout")

    def on_error(self, headers, message):
        log.error("STOMP error: %s", message)

    def on_disconnected(self):
        log.warning("Disconnected. Sleeping %s seconds.", self._reconnect_delay_secs)
        time.sleep(self._reconnect_delay_secs)

    def on_connecting(self, host_and_port):
        log.info("Connecting to %s:%s", host_and_port[0], host_and_port[1])

    def on_message(self, frame):
        try:
            forecasts, schedules, xml_bytes = decode_message(frame.body)
            self._on_decoded(forecasts, schedules, xml_bytes)
        except Exception:
            log.exception("Failed to decode Darwin message")
