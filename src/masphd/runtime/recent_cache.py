# src/masphd/runtime/recent_cache.py
from __future__ import annotations

from collections import OrderedDict
from dataclasses import dataclass
from typing import Hashable, Optional


@dataclass
class SegmentState:
    last_dep_time: Optional[str] = None
    last_kind: str = "missing"         # "estimate" | "actual" | "missing"
    actual_saved: bool = False         # True once we have saved ACTUAL to DB
    last_seen_order: int = 0           # for eviction bookkeeping


@dataclass
class RecentSegmentCache:
    """
    Bounded cache storing segment state, allowing EST -> ACTUAL upgrade.
    Evicts oldest segments when size exceeds max_size.
    """
    max_size: int = 500

    def __post_init__(self):
        self._od: OrderedDict[Hashable, SegmentState] = OrderedDict()
        self._tick = 0

    def touch(self, seg_id: Hashable, dep_time: Optional[str], kind: str, has_actual: bool) -> SegmentState:
        """
        Upserts segment state and returns current state object.
        """
        self._tick += 1

        state = self._od.get(seg_id)
        if state is None:
            state = SegmentState()
            self._od[seg_id] = state

        # mark as most recently used by moving to end
        self._od.move_to_end(seg_id, last=True)

        state.last_dep_time = dep_time
        state.last_kind = kind
        state.last_seen_order = self._tick

        # don't set actual_saved here; we set it only after DB insert succeeds
        if has_actual and state.last_kind != "actual":
            state.last_kind = "actual"

        # evict if too big
        while len(self._od) > self.max_size:
            self._od.popitem(last=False)

        return state

    def mark_actual_saved(self, seg_id: Hashable):
        st = self._od.get(seg_id)
        if st:
            st.actual_saved = True

    def __len__(self) -> int:
        return len(self._od)
