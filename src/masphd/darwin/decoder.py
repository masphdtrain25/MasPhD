# src/masphd/darwin/decoder.py
from __future__ import annotations

import zlib
from typing import Any, Dict, List, Tuple, Union

from .parse_forecasts import extract_attr
from .parse_schedules import extract_schedule


def decompress_body(body: Union[bytes, bytearray]) -> bytes:
    # Darwin PushPort frames are typically zlib/gzip wrapped
    return zlib.decompress(body, zlib.MAX_WBITS | 32)


def decode_message(body: Union[bytes, bytearray]) -> Tuple[List[Dict[str, Any]], List[Dict[str, Any]], bytes]:
    """
    Returns:
      forecasts_list, schedules_list, xml_bytes
    """
    xml_bytes = decompress_body(body)
    forecasts = extract_attr(xml_bytes)
    schedules = extract_schedule(xml_bytes)
    return forecasts, schedules, xml_bytes
