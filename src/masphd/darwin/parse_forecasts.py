# src/masphd/darwin/parse_forecasts.py
from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Union

NS_V16 = "http://www.thalesgroup.com/rtti/PushPort/v16"
NS_FCST_V3 = "http://www.thalesgroup.com/rtti/PushPort/Forecasts/v3"


def extract_attr(xml_data: Union[str, bytes]) -> List[Dict[str, Any]]:
    """
    Extract TS/Location forecast data.
    Returns a list of dicts (one per Location).
    """
    root = ET.fromstring(xml_data)
    out: List[Dict[str, Any]] = []

    ts_path = f".//{{{NS_V16}}}TS"
    loc_path = f".//{{{NS_FCST_V3}}}Location"

    for ts_elem in root.iterfind(ts_path):
        base = {
            "updateOrigin": ts_elem.get("updateOrigin"),
            "rid": ts_elem.get("rid"),
            "uid": ts_elem.get("uid"),
            "ssd": ts_elem.get("ssd"),
        }

        for loc_elem in ts_elem.iterfind(loc_path):
            item: Dict[str, Any] = dict(base)

            # Location attributes (includes tpl, pta, ptd, wta, wtd, ata, atd, etc.)
            item.update(loc_elem.attrib)

            # Sub-elements (plat, length, and state sub-tags)
            for sub_elem in loc_elem:
                tag = sub_elem.tag.split("}")[-1]
                if sub_elem.text is not None and sub_elem.text.strip() != "":
                    item[tag] = sub_elem.text
                else:
                    item["state"] = tag
                    for k, v in sub_elem.attrib.items():
                        item[f"{tag}_{k}"] = v

            out.append(item)

    return out
