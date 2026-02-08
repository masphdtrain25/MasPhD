# src/masphd/runtime/run_realtime_predict.py
from __future__ import annotations

import argparse
import logging
from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

from masphd.io.paths import DATABASE
from masphd.darwin.client import DarwinClient
from masphd.darwin.extract_segments import extract_segments
from masphd.darwin.realtime_filter import filter_segments_by_now
from masphd.features.segment_features import SegmentFeatureBuilder
from masphd.models.ensemble import WeightedEnsemblePredictor
from masphd.runtime.recent_cache import RecentSegmentCache
from masphd.models.transformers import ColumnDropper

from masphd.runtime.realtime_db import RealTimeSQLiteStore

log = logging.getLogger(__name__)
LONDON = ZoneInfo("Europe/London")


def build_argparser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        prog="masphd-realtime",
        description="Run real-time Darwin stream, extract segments, build features, and predict using weighted ensemble.",
    )
    p.add_argument(
        "--minutes",
        type=float,
        default=5,
        help="How long to run (minutes). Use -1 for unlimited.",
    )
    p.add_argument(
        "--print",
        dest="do_print",
        action="store_true",
        help="Print predictions to terminal.",
    )
    p.add_argument(
        "--no-print",
        dest="do_print",
        action="store_false",
        help="Do not print to terminal.",
    )
    p.set_defaults(do_print=True)

    p.add_argument(
        "--cache-size",
        type=int,
        default=500,
        help="Max number of recent keys kept in memory (default 500).",
    )
    p.add_argument(
        "--weights",
        type=str,
        default="model_weights.json",
        help="Weights filename in WEIGHTS folder (default model_weights.json).",
    )
    return p


def main():
    args = build_argparser().parse_args()

    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s\t%(message)s",
    )

    cache = RecentSegmentCache(max_size=args.cache_size)
    store = RealTimeSQLiteStore(DATABASE)
    feature_builder = SegmentFeatureBuilder(tz=LONDON)
    ensemble = WeightedEnsemblePredictor(weights_filename=args.weights)

    def on_decoded(forecasts, schedules, xml_bytes):
        now = datetime.now(LONDON)

        segs = extract_segments(
            forecasts,
            schedules,
            tz=LONDON,
            drop_wrong_direction=True,
        )
        if not segs:
            return

        for s in segs:
            loc_second = s.get("loc_second") or {}
            s["planned_arr_second"] = loc_second.get("pta") or loc_second.get("wta")

        segs = filter_segments_by_now(
            segs,
            now=now,
            tz=LONDON,
            mode="in_progress",
            dep_grace_after_now_mins=5,
            arr_grace_before_now_mins=2,
        )
        if not segs:
            return

        for s in segs:
            feat = feature_builder.build(s)
            if feat is None:
                continue

            X = pd.DataFrame([feat])

            yhat = ensemble.predict_one(s["first"], s["second"], X)
            if yhat is None:
                continue

            dep_time = s.get("dep_time_for_prediction")
            dep_kind = s.get("dep_time_kind")  # "actual" | "estimate" | "missing"
            has_actual = bool(s.get("has_actual_dep"))
            planned_dep = s.get("planned_dep")

            seg_id = (s.get("rid"), s.get("first"), s.get("second"), planned_dep)

            # Cache state helpers: keep minimal assumptions about internal structure
            prev_dep = cache.get_last_dep_time(seg_id) if hasattr(cache, "get_last_dep_time") else None
            prev_kind = cache.get_last_kind(seg_id) if hasattr(cache, "get_last_kind") else None
            prev_actual_saved = cache.get_actual_saved(seg_id) if hasattr(cache, "get_actual_saved") else False

            cache.touch(seg_id, dep_time=dep_time, kind=dep_kind, has_actual=has_actual)

            should_insert_all = (prev_dep is None and prev_kind is None) or (dep_time != prev_dep) or (dep_kind != prev_kind)
            should_insert_actual = has_actual and (not prev_actual_saved)

            rec = {
                "rid": s.get("rid"),
                "ssd": s.get("ssd"),
                "first": s.get("first"),
                "second": s.get("second"),

                "planned_dep": planned_dep,
                "dep_time": dep_time,
                "dep_time_kind": dep_kind,
                "has_actual_dep": has_actual,
                "actual_dep_confirmed": s.get("actual_dep_confirmed"),

                "departure_delay": feat.get("departure_delay"),
                "dwell_delay": feat.get("dwell_delay"),

                "peak": feat.get("peak"),
                "day_of_week": feat.get("day_of_week"),
                "day_of_month": feat.get("day_of_month"),
                "hour_of_day": feat.get("hour_of_day"),
                "weekend": feat.get("weekend"),
                "season": feat.get("season"),
                "month": feat.get("month"),
                "holiday": feat.get("holiday"),

                "predicted_delay": float(yhat),
            }

            if should_insert_all:
                store.insert_all(rec)

            if should_insert_actual:
                inserted = store.insert_actual(rec)
                if inserted:
                    if hasattr(cache, "mark_actual_saved"):
                        cache.mark_actual_saved(seg_id)

            if args.do_print:
                flag = "ACTUAL" if has_actual else "EST"
                dep_delay = rec["departure_delay"]
                dwell = rec["dwell_delay"]
                dep_delay_s = f"{dep_delay:.1f}" if dep_delay is not None else "NA"
                dwell_s = f"{dwell:.1f}" if dwell is not None else "NA"

                print(
                    f'{now.strftime("%Y-%m-%d %H:%M:%S")} | {flag} | '
                    f'{rec["rid"]} {rec["first"]}->{rec["second"]} '
                    f'planned_dep={rec["planned_dep"]} dep_time={rec["dep_time"]} '
                    f'dep_delay={dep_delay_s} dwell={dwell_s} '
                    f'pred={rec["predicted_delay"]:.2f} | cache={len(cache)}'
                )

    client = DarwinClient(on_decoded=on_decoded)
    client.connect()

    try:
        if args.minutes == -1:
            log.info("Running unlimited (Ctrl+C to stop). DB: %s", store.db_path)
            client.run_forever()
        else:
            secs = float(args.minutes) * 60.0
            log.info("Running for %.2f minutes. DB: %s", float(args.minutes), store.db_path)
            client.run_for(secs)
    except KeyboardInterrupt:
        log.info("Stopped by user.")
    finally:
        # drain=True ensures pending records are written before exit
        store.close(drain=True, join_timeout=10.0)

    # If anything printed very late, flush can help, but close should prevent the crash.
    try:
        import sys
        sys.stdout.flush()
    except Exception:
        pass


if __name__ == "__main__":
    main()
