"""Pure barrier arithmetic derived from committed receipts and live lane turns."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from _parallel_flame_chase.core.models import LaneName

Receipt = Mapping[str, Any]


def frozen_turns(receipts: Sequence[Receipt], lane: LaneName) -> int:
    """Return the lane's turns frozen by the latest barrier, or 0 before any barrier."""
    if not receipts:
        return 0
    return int(receipts[-1]["lanes"][lane]["turns"])


def quota_end(receipts: Sequence[Receipt], lane: LaneName, interval: int) -> int:
    """Return the turn count at which a lane must wait for the next barrier.

    Measured from the lane's own frozen turns, so a lane unblocked by a replan owes
    exactly ``interval`` sessions rather than the backlog it missed while blocked.
    """
    return frozen_turns(receipts, lane) + interval


def held(turns: int, quota: int) -> bool:
    """Return whether a live lane has finished its sessions for the current epoch."""
    return turns >= quota


def barrier_ready(
    lanes: Mapping[LaneName, tuple[int, bool]],
    receipts: Sequence[Receipt],
    interval: int,
    *,
    in_flight: bool,
) -> bool:
    """Return whether the next barrier may commit now.

    ``lanes`` maps each lane to ``(turns, blocked)``. Blocked lanes are exempt from the
    hold, and a run with no live lane never fires, so an all-blocked run cannot commit a
    barrier on every control pass.
    """
    if in_flight:
        return False
    live: list[tuple[LaneName, int]] = [
        (lane, turns) for lane, (turns, blocked) in lanes.items() if not blocked
    ]
    return bool(live) and all(
        held(turns, quota_end(receipts, lane, interval)) for lane, turns in live
    )


def first_after_barrier(
    receipts: Sequence[Receipt], lane: LaneName, turns: int
) -> bool:
    """Return whether the lane's next session is its first since the latest barrier."""
    return bool(receipts) and turns == frozen_turns(receipts, lane)


__all__ = [
    "Receipt",
    "barrier_ready",
    "first_after_barrier",
    "frozen_turns",
    "held",
    "quota_end",
]
