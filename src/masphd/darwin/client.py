# src/masphd/darwin/client.py
import logging
import socket
import time
import stomp
from typing import Optional

from masphd.config import (
    DARWIN_TOPIC_HOST,
    DARWIN_TOPIC_PORT,
    DARWIN_TOPIC_NAME,
    DARWIN_TOPIC_USERNAME,
    DARWIN_TOPIC_PASSWORD,
    DARWIN_HEARTBEAT_MS,
    DARWIN_RECONNECT_DELAY_SECS,
    DARWIN_SUBSCRIPTION_ID,
    DARWIN_ACK_MODE,
)
from .listener import DarwinListener, DecodedHandler

log = logging.getLogger(__name__)

class DarwinClient:
    def __init__(self, on_decoded: DecodedHandler):
        self._on_decoded = on_decoded
        self._conn: Optional[stomp.Connection12] = None
        self._client_id = socket.getfqdn()

    def connect(self):
        self._conn = stomp.Connection12(
            [(DARWIN_TOPIC_HOST, DARWIN_TOPIC_PORT)],
            auto_decode=False,
            heartbeats=(DARWIN_HEARTBEAT_MS, DARWIN_HEARTBEAT_MS),
        )

        self._conn.set_listener(
            name="darwin",
            listener=DarwinListener(
                on_decoded=self._on_decoded,
                reconnect_delay_secs=DARWIN_RECONNECT_DELAY_SECS,
            ),
        )

        connect_headers = {"client-id": f"{DARWIN_TOPIC_USERNAME}-{self._client_id}"}
        subscribe_headers = {"activemq.subscriptionName": self._client_id}

        # stomp.py < 5 needs start()
        if stomp.__version__[0] < "5":
            self._conn.start()

        log.info("Connecting to %s:%s", DARWIN_TOPIC_HOST, DARWIN_TOPIC_PORT)
        self._conn.connect(
            username=DARWIN_TOPIC_USERNAME,
            passcode=DARWIN_TOPIC_PASSWORD,
            wait=True,
            headers=connect_headers,
        )

        log.info("Subscribing to %s", DARWIN_TOPIC_NAME)
        self._conn.subscribe(
            destination=DARWIN_TOPIC_NAME,
            id=DARWIN_SUBSCRIPTION_ID,
            ack=DARWIN_ACK_MODE,
            headers=subscribe_headers,
        )

    def run_forever(self, sleep_secs: float = 1.0):
        if not self._conn:
            raise RuntimeError("Call connect() before run_forever().")

        while True:
            time.sleep(sleep_secs)
    
    def run_for(self, duration_secs: float, sleep_secs: float = 0.5):
        if not self._conn:
            raise RuntimeError("Call connect() before run_for().")

        log.info("Running for %.2f seconds", duration_secs)
        end_time = time.time() + duration_secs

        try:
            while time.time() < end_time:
                time.sleep(sleep_secs)
        finally:
            self.disconnect()

    def disconnect(self):
        if self._conn:
            log.info("Disconnecting")
            self._conn.disconnect()
            self._conn = None
