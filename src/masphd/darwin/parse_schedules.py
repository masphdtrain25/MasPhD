# src/masphd/darwin/parse_schedules.py
from __future__ import annotations

import xml.etree.ElementTree as ET
from typing import Any, Dict, List, Union

NS_V16 = "http://www.thalesgroup.com/rtti/PushPort/v16"
NS_SCHED_V3 = "http://www.thalesgroup.com/rtti/PushPort/Schedules/v3"


def extract_schedule(xml_data: Union[str, bytes]) -> List[Dict[str, Any]]:
    """
    Extract schedule OR/DT location entries.
    Returns a list of dicts (one per OR or DT).
    """
    root = ET.fromstring(xml_data)
    out: List[Dict[str, Any]] = []

    sched_path = f".//{{{NS_V16}}}schedule"
    or_path = f".//{{{NS_SCHED_V3}}}OR"
    dt_path = f".//{{{NS_SCHED_V3}}}DT"

    for sched_elem in root.iterfind(sched_path):
        base = {
            "rid": sched_elem.get("rid"),
            "uid": sched_elem.get("uid"),
            "ssd": sched_elem.get("ssd"),
        }

        for loc_elem in sched_elem.iterfind(or_path):
            out.append(_parse_sched_location(base, loc_elem, "OR"))

        for loc_elem in sched_elem.iterfind(dt_path):
            out.append(_parse_sched_location(base, loc_elem, "DT"))

    return out


def _parse_sched_location(base: Dict[str, Any], loc_elem: ET.Element, loc_type: str) -> Dict[str, Any]:
    item: Dict[str, Any] = dict(base)
    item.update(loc_elem.attrib)
    item["type"] = loc_type

    for sub_elem in loc_elem:
        tag = sub_elem.tag.split("}")[-1]
        if sub_elem.text is not None and sub_elem.text.strip() != "":
            item[tag] = sub_elem.text
        else:
            item["state"] = tag
            for k, v in sub_elem.attrib.items():
                item[f"{tag}_{k}"] = v

    return item
